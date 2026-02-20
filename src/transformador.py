import pandas as pd
import json
import os
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class CoinCapTransformer:
    def __init__(self):
        # Data de processamento para auditoria (Lineage)
        self.timestamp_processamento = datetime.now()

    def _carregar_json(self, caminho_arquivo):
        """Método auxiliar para ler os arquivos de cache."""
        if not os.path.exists(caminho_arquivo):
            logging.error(f"Arquivo não encontrado: {caminho_arquivo}")
            return None
        with open(caminho_arquivo, 'r') as f:
            return json.load(f)

    def tratar_ativos(self):
        """Processa a tabela de Assets (Dimensão Moedas)."""
        dados = self._carregar_json("assets_cache.json")
        if not dados: return None
        
        df = pd.DataFrame(dados['data'])

        #Limpeza de ruídos
        if 'tokens' in df.columns:
            df = df.drop(columns=['tokens'])
        
        # Conversão de tipos conforme o JSON enviado
        cols_numericas = ['supply', 'maxSupply', 'marketCapUsd', 'volumeUsd24Hr', 'priceUsd', 'changePercent24Hr', 'vwap24Hr']
        for col in cols_numericas:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        df['processado_em'] = self.timestamp_processamento
        logging.info("Tabela de Ativos tratada com sucesso.")
        return df

    def tratar_exchanges(self):
        """Processa a tabela de Exchanges (Dimensão Corretoras)."""
        dados = self._carregar_json("exchanges_cache.json")
        if not dados: return None
        
        df = pd.DataFrame(dados['data'])
        
        # Conversão de tipos
        cols_numericas = ['percentTotalVolume', 'volumeUsd', 'tradingPairs']
        for col in cols_numericas:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Converte timestamp de atualização
        df['updated'] = pd.to_datetime(df['updated'], unit='ms')
        
        logging.info("Tabela de Exchanges tratada com sucesso.")
        return df

    def tratar_mercados(self, prefixo="markets_"):
        """Une e processa todos os arquivos de Markets (Fato de Ligação)."""
        arquivos = [f for f in os.listdir('.') if f.startswith(prefixo) and f.endswith('.json')]
        lista_dfs = []

        for arq in arquivos:
            dados = self._carregar_json(arq)
            if dados:
                temp_df = pd.DataFrame(dados['data'])
                lista_dfs.append(temp_df)

        if not lista_dfs: return None
        
        df_final = pd.concat(lista_dfs, ignore_index=True)
        
        # Tratamento de tipos específicos do Markets
        cols_numericas = ['priceQuote', 'priceUsd', 'volumeUsd24Hr', 'percentExchangeVolume']
        for col in cols_numericas:
            df_final[col] = pd.to_numeric(df_final[col], errors='coerce')
            
        df_final['updated'] = pd.to_datetime(df_final['updated'], unit='ms')
        
        logging.info(f"Tabela de Mercados unificada: {len(df_final)} linhas.")
        return df_final
    
    def tratar_historico(self, prefixo="history_"):
        """Lê os arquivos de histórico, une todos e cria a Fato de Série Temporal."""
        arquivos = [f for f in os.listdir('.') if f.startswith(prefixo) and f.endswith('.json')]
        lista_dfs = []

        for arq in arquivos:
            dados = self._carregar_json(arq)
            if dados and 'data' in dados:
                # Extrai o ID da moeda do nome do arquivo (ex: history_bitcoin_cache.json -> bitcoin)
                id_moeda = arq.replace(prefixo, "").replace("_cache.json", "")
                
                temp_df = pd.DataFrame(dados['data'])
                temp_df['id_moeda'] = id_moeda # Injeta a chave estrangeira
                lista_dfs.append(temp_df)

        if not lista_dfs: return None
        
        df_final = pd.concat(lista_dfs, ignore_index=True)
        
        # Tratamento de tipos
        df_final['priceUsd'] = pd.to_numeric(df_final['priceUsd'], errors='coerce')
        df_final['time'] = pd.to_datetime(df_final['time'], unit='ms')
        
        # Remove a coluna 'date' que vem como string, pois já convertemos o 'time' para datetime real
        if 'date' in df_final.columns:
            df_final = df_final.drop(columns=['date'])
            
        logging.info(f"Tabela de Histórico unificada: {len(df_final)} linhas.")
        return df_final

# --- Bloco de Teste ---
if __name__ == "__main__":
    transformer = CoinCapTransformer()
    
    # Executando as transformações
    df_assets = transformer.tratar_ativos()
    df_exchanges = transformer.tratar_exchanges()
    df_markets = transformer.tratar_mercados()

    if df_assets is not None:
        print("\n--- Amostra Ativos ---")
        print(df_assets[['id', 'priceUsd']].head())

    if df_markets is not None:
        print("\n--- Amostra Mercados (O elo do JOIN) ---")
        print(df_markets[['exchangeId', 'baseId', 'priceUsd', 'percentExchangeVolume']].head())