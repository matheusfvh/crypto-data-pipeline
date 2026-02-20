import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import os
import logging
from dotenv import load_dotenv

# Importando a classe de transformação que criamos
from transformador import CoinCapTransformer

# Configuração de Logs
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class BigQueryLoader:
    def __init__(self):
        """
        Inicializa o carregador do BigQuery lendo configurações do .env
        e autenticando via Service Account.
        """
        load_dotenv()
        
        # Carrega variáveis de ambiente
        self.projeto_id = os.getenv('GCP_PROJECT_ID')
        self.dataset_id = os.getenv('GCP_DATASET_ID')
        
        # Caminho da chave JSON do GCP
        self.caminho_chave = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', 'google_credentials.json')

        # Validação de Segurança e Conexão
        if os.path.exists(self.caminho_chave):
            try:
                self.credentials = service_account.Credentials.from_service_account_file(self.caminho_chave)
                self.client = bigquery.Client(credentials=self.credentials, project=self.projeto_id)
                logging.info(f"Conectado ao GCP. Projeto: {self.projeto_id} | Dataset: {self.dataset_id}")
            except Exception as e:
                logging.error(f"Erro de autenticação no GCP: {e}")
                raise
        else:
            logging.error(f"CRÍTICO: Arquivo de chave '{self.caminho_chave}' não encontrado!")
            raise FileNotFoundError("Chave de acesso do Google Cloud não localizada.")

    def carregar_tabela(self, df, nome_tabela):
        """
        Envia um DataFrame Pandas para o BigQuery.
        Modo: 'replace' (substitui a tabela inteira a cada carga para evitar duplicatas no teste).
        """
        if df is None or df.empty:
            logging.warning(f"DataFrame vazio ou nulo para '{nome_tabela}'. Carga ignorada.")
            return

        tabela_destino = f"{self.dataset_id}.{nome_tabela}"
        
        try:
            logging.info(f"Iniciando carga de {len(df)} linhas em '{tabela_destino}'...")
            
            # Utiliza o motor pandas-gbq que é otimizado para DataFrames
            df.to_gbq(
                destination_table=tabela_destino,
                project_id=self.projeto_id,
                if_exists='replace',  # Use 'append' se quiser histórico, 'replace' para snapshot atual
                credentials=self.credentials
            )
            logging.info(f"✅ Sucesso! Tabela '{nome_tabela}' carregada.")
            
        except Exception as e:
            logging.error(f"❌ Falha ao carregar '{nome_tabela}': {e}")

# --- Bloco de Orquestração (Pipeline Final) ---
if __name__ == "__main__":
    print("\n--- Iniciando Pipeline de Carga (Load) ---")
    
    # 1. Instancia o Transformador para pegar os dados tratados (Silver Layer)
    transformer = CoinCapTransformer()
    
    # 2. Instancia o Carregador
    try:
        loader = BigQueryLoader()
        
        # 3. Executa a carga das 3 tabelas principais
        # Assets (Dimensão)
        df_assets = transformer.tratar_ativos()
        loader.carregar_tabela(df_assets, "tb_assets")
        
        # Exchanges (Dimensão)
        df_exchanges = transformer.tratar_exchanges()
        loader.carregar_tabela(df_exchanges, "tb_exchanges")
        
        # Markets (Fatos)
        df_markets = transformer.tratar_mercados()
        loader.carregar_tabela(df_markets, "tb_markets")

        # Histórico (Fatos - Série Temporal)
        df_history = transformer.tratar_historico()
        loader.carregar_tabela(df_history, "tb_history")
        
        print("\n--- Pipeline Finalizado com Sucesso! Verifique o BigQuery. ---")
        
    except Exception as e:
        logging.critical(f"O Pipeline falhou: {e}")