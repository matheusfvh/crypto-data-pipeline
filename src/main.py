import functions_framework
import requests
import pandas as pd
import os
import logging
from datetime import datetime, timezone
from google.cloud import bigquery

# Configuração de Logs para ambiente Cloud
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_elt_process(bq_client, url, table_full_id, schema_cols, api_headers, timestamp):
    """
    Executa o ciclo ELT (Extract, Load, Transform) para um endpoint específico.
    
    Estratégia:
    1. Extração: Coleta dados da API REST.
    2. Tratamento Leve: Normalização de tipagem (String) e filtro de colunas (Schema Enforcement).
    3. Carga (Load): Inserção no BigQuery via Append.
    
    Args:
        bq_client: Cliente autenticado do BigQuery.
        url (str): Endpoint da API CoinCap.
        table_full_id (str): ID completo da tabela destino (projeto.dataset.tabela).
        schema_cols (list): Lista de colunas permitidas (deve coincidir com o DDL).
        api_headers (dict): Headers de autenticação.
        timestamp (datetime): Carimbo de tempo para auditoria de carga.
        
    Returns:
        bool: True se houve dados processados, False caso contrário.
    """
    try:
        response = requests.get(url, headers=api_headers, timeout=30)
        response.raise_for_status() # Garante que erros HTTP (4xx, 5xx) levantem exceção
        
        data = response.json().get('data', [])
        
        # Se a resposta for vazia, não há o que processar
        if not data:
            logger.warning(f"Endpoint retornou lista vazia: {url}")
            return False

        df = pd.DataFrame(data)

        # 1. Blindagem de Tipagem (Architecture Decision):
        # Converter tudo para String na camada Raw evita quebras de pipeline 
        # caso a API envie tipos inesperados (ex: "N/A" num campo float).
        df = df.astype(str)

        # 2. Enriquecimento:
        # Adição de coluna de controle para particionamento e auditoria.
        df['processado_em'] = timestamp

        # 3. Tratamento de Casos Específicos (Renomeação):
        # A API retorna 'percentExchangeVolume', mas o DDL espera 'volumePercent'.
        if 'percentExchangeVolume' in df.columns:
            df = df.rename(columns={'percentExchangeVolume': 'volumePercent'})

        # 4. Schema Enforcement (Governança):
        # O método reindex garante que o DataFrame tenha EXATAMENTE as colunas esperadas.
        # - Colunas extras da API são descartadas (evita erro "Schema Mismatch").
        # - Colunas faltantes são criadas como NaN/Null (evita quebra de código).
        df = df.reindex(columns=schema_cols)

        # 5. Carga (Load):
        # Utiliza 'if_exists=append' para preservar o histórico e respeitar o DDL existente.
        df.to_gbq(
            destination_table=table_full_id,
            project_id=table_full_id.split('.')[0],
            if_exists='append'
        )
        
        logger.info(f"Carga realizada com sucesso em: {table_full_id} | Registros: {len(df)}")
        return True

    except Exception as e:
        logger.error(f"Falha no processamento da tabela {table_full_id}: {str(e)}")
        raise e # Propaga o erro para ser capturado no handler principal

@functions_framework.http
def handler(request):
    """
    Função Cloud Function principal (Entrypoint).
    Orquestra a extração de Ativos, Exchanges e Mercados, seguida da transformação SQL.
    """
    
    # --- CONFIGURAÇÃO DE AMBIENTE ---
    API_KEY = os.environ.get('COINCAP_API_KEY')
    PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
    DATASET_ID = os.environ.get('GCP_DATASET_ID')
    
    # Headers de autenticação (Opcional, mas recomendado para evitar Rate Limiting)
    headers = {'Authorization': f"Bearer {API_KEY}"} if API_KEY else {}
    base_url = "https://rest.coincap.io/v3"
    timestamp_processamento = datetime.now(timezone.utc)
    
    bq_client = bigquery.Client(project=PROJECT_ID)

    # --- DEFINIÇÃO DE SCHEMAS (METADADOS) ---
    # Define estritamente quais colunas serão ingeridas para garantir integridade.
    cols_assets = [
        'id', 'rank', 'symbol', 'name', 'supply', 'maxSupply', 
        'marketCapUsd', 'volumeUsd24Hr', 'priceUsd', 'changePercent24Hr', 
        'vwap24Hr', 'explorer', 'processado_em'
    ]
    
    cols_exchanges = [
        'exchangeId', 'name', 'rank', 'percentTotalVolume', 
        'volumeUsd', 'tradingPairs', 'socket', 'exchangeUrl', 
        'updated', 'processado_em'
    ]
    
    cols_markets = [
        'exchangeId', 'baseId', 'quoteId', 'baseSymbol', 
        'quoteSymbol', 'volumeUsd24Hr', 'priceQuote', 
        'volumePercent', 'processado_em'
    ]

    try:
        # =====================================================================
        # 1. INGESTÃO PARALELA (Raw Layer)
        # =====================================================================
        
        # 1.1 Tabela de Ativos (Top 20 por Market Cap)
        # Estratégia: Limitar a 20 reduz latência e foca nos ativos de maior liquidez para o case.
        assets_processed = run_elt_process(
            bq_client,
            url=f"{base_url}/assets?limit=20",
            table_full_id=f"{PROJECT_ID}.{DATASET_ID}.tb_assets",
            schema_cols=cols_assets,
            api_headers=headers,
            timestamp=timestamp_processamento
        )

        # 1.2 Tabela de Exchanges
        run_elt_process(
            bq_client,
            url=f"{base_url}/exchanges?limit=150",
            table_full_id=f"{PROJECT_ID}.{DATASET_ID}.tb_exchanges",
            schema_cols=cols_exchanges,
            api_headers=headers,
            timestamp=timestamp_processamento
        )

        # 1.3 Tabela de Mercados (Dependência de Dados)
        # Necessita dos IDs dos ativos coletados no passo 1.1 para buscar seus mercados específicos.
        if assets_processed:
            # Recupera IDs distintos diretamente da API novamente ou poderia usar a memória.
            # Para simplificação e stateless, fazemos uma nova chamada rápida ou guardamos em memória se fosse crítico.
            # Aqui, optamos por iterar sobre a resposta da API (simulada lógica):
            res_ids = requests.get(f"{base_url}/assets?limit=20", headers=headers).json().get('data', [])
            
            all_markets = []
            for asset in res_ids:
                asset_id = asset.get('id')
                # Busca os top 10 mercados onde este ativo é negociado
                m_url = f"{base_url}/markets?baseId={asset_id}&limit=10"
                m_res = requests.get(m_url, headers=headers)
                if m_res.status_code == 200:
                    all_markets.extend(m_res.json().get('data', []))
            
            # Processa o lote consolidado de mercados
            if all_markets:
                # Criamos um DataFrame temporário para passar para a função de carga
                # Nota: Adaptamos a função run_elt_process para aceitar DF ou URL, 
                # mas para manter a consistência simples neste exemplo, faremos a carga manual aqui
                # reaproveitando a lógica de tratamento.
                df_m = pd.DataFrame(all_markets)
                df_m = df_m.astype(str)
                df_m['processado_em'] = timestamp_processamento
                
                if 'percentExchangeVolume' in df_m.columns:
                    df_m = df_m.rename(columns={'percentExchangeVolume': 'volumePercent'})
                
                df_m = df_m.reindex(columns=cols_markets)
                df_m.to_gbq(f"{DATASET_ID}.tb_markets", project_id=PROJECT_ID, if_exists='append')
                logger.info(f"Carga de Mercados finalizada. Registros: {len(df_m)}")

        # =====================================================================
        # 2. TRANSFORMAÇÃO & ANALYTICS (Refined Layer)
        # =====================================================================
        # Aciona Stored Procedures no BigQuery para processar as regras de negócio.
        # Isso garante que o Dashboard sempre reflita o dado recém-ingerido.
        
        logger.info("Iniciando procedimentos de transformação SQL (ELT)...")
        
        query_arbitragem = f"CALL `{PROJECT_ID}.{DATASET_ID}.prc_load_tb_arbitragem_analitica`('{PROJECT_ID}', '{DATASET_ID}')"
        query_macro = f"CALL `{PROJECT_ID}.{DATASET_ID}.prc_load_tb_mercado_macro_analitica`('{PROJECT_ID}', '{DATASET_ID}')"
        
        bq_client.query(query_arbitragem).result() # .result() força espera síncrona
        logger.info("Procedure de Arbitragem concluída com sucesso.")
        
        bq_client.query(query_macro).result()
        logger.info("Procedure de Mercado Macro concluída com sucesso.")

        return f"✅ Pipeline ELT executado com sucesso. Timestamp: {timestamp_processamento}", 200

    except Exception as e:
        logger.critical(f"Erro fatal no pipeline: {str(e)}")
        return f"❌ Erro no pipeline: {str(e)}", 500