import requests
import json
import os
import logging
import time # Adicionado para controle de Rate Limit
from datetime import datetime
from dotenv import load_dotenv

# Configuração básica de Logging (Essencial para rastreabilidade em produção)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class CoinCapExtractor:
    def __init__(self, api_key=None):
        """
        Inicializa o extrator com a URL base e a chave de API (opcional).
        """
        self.base_url = "https://rest.coincap.io/v3"
        self.api_key = api_key
        self.headers = {}
        if self.api_key:
            self.headers['Authorization'] = f"Bearer {self.api_key}"

    def get_assets(self, limit=5, use_cache=True):
        """
        Busca dados de ativos. Implementa estratégia de Cache Local para desenvolvimento.
        
        Args:
            limit (int): Número de ativos a retornar (padrão baixo para economizar créditos).
            use_cache (bool): Se True, tenta ler de um arquivo local antes de chamar a API.
        """
        cache_file = "assets_cache.json"

        # 1. Estratégia de Cache (Economia de Créditos)
        if use_cache and os.path.exists(cache_file):
            logging.info(f"Lendo dados do cache local: {cache_file}")
            with open(cache_file, 'r') as f:
                return json.load(f)

        # 2. Chamada Real à API
        logging.info("Cache não encontrado ou desativado. Chamando API para Ativos...")
        url = f"{self.base_url}/assets"
        params = {'limit': limit}

        try:
            response = requests.get(url, headers=self.headers, params=params, timeout=10)
            response.raise_for_status() # Levanta erro se status != 200
            
            data = response.json()
            
            # 3. Salva no Cache para a próxima execução
            with open(cache_file, 'w') as f:
                json.dump(data, f, indent=4)
            logging.info(f"Dados salvos localmente em {cache_file}")
            
            return data

        except requests.exceptions.RequestException as e:
            logging.error(f"Falha na requisição da API: {e}")
            return None
        
    def get_exchanges(self, use_cache=True):
        """
        Extrai dados das corretoras (Exchanges) para análise de liquidez.
        """
        cache_file = "exchanges_cache.json"

        if use_cache and os.path.exists(cache_file):
            logging.info(f"Lendo exchanges do cache local: {cache_file}")
            with open(cache_file, 'r') as f:
                return json.load(f)

        logging.info("Chamando API para Exchanges...")
        url = f"{self.base_url}/exchanges"

        try:
            response = requests.get(url, headers=self.headers, timeout=15)
            response.raise_for_status()
            data = response.json()
            
            with open(cache_file, 'w') as f:
                json.dump(data, f, indent=4)
            logging.info(f"Exchanges salvas localmente em {cache_file}")
            return data
        except Exception as e:
            logging.error(f"Erro ao extrair exchanges: {e}")
            return None

    def get_history(self, asset_id, interval='d1', use_cache=True):
        """
        Extrai o histórico de preços de um ativo específico.
        """
        cache_file = f"history_{asset_id}_cache.json"

        if use_cache and os.path.exists(cache_file):
            logging.info(f"Lendo histórico de {asset_id} do cache local.")
            with open(cache_file, 'r') as f:
                return json.load(f)

        logging.info(f"Chamando API para Histórico de {asset_id}...")
        url = f"{self.base_url}/assets/{asset_id}/history"
        params = {'interval': interval}

        try:
            response = requests.get(url, headers=self.headers, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()
            
            with open(cache_file, 'w') as f:
                json.dump(data, f, indent=4)
            logging.info(f"Histórico de {asset_id} salvo localmente.")
            return data
        except Exception as e:
            logging.error(f"Erro ao extrair histórico de {asset_id}: {e}")
            return None

    def get_markets(self, asset_id, limit=5, use_cache=True):
        """
        Extrai os mercados onde um ativo específico é negociado (Elo para o JOIN).
        """
        cache_file = f"markets_{asset_id}_cache.json"

        if use_cache and os.path.exists(cache_file):
            logging.info(f"Lendo mercados de {asset_id} do cache local.")
            with open(cache_file, 'r') as f:
                return json.load(f)

        logging.info(f"Chamando API para Mercados de {asset_id}...")
        url = f"{self.base_url}/markets"
        params = {'baseId': asset_id, 'limit': limit}

        try:
            response = requests.get(url, headers=self.headers, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()

            with open(cache_file, 'w') as f:
                json.dump(data, f, indent=4)
            logging.info(f"Mercados de {asset_id} salvos localmente.")
            return data
        except Exception as e:
            logging.error(f"Erro ao extrair mercados de {asset_id}: {e}")
            return None


# --- Bloco de Execução de Teste (Só roda se chamar este arquivo direto) ---
if __name__ == "__main__":
    # 1. Carrega as variáveis ocultas do .env 
    load_dotenv()
    
    # 2. Busca a chave com segurança
    API_KEY = os.getenv('COINCAP_API_KEY') 

    if API_KEY:
        logging.info("Chave de API carregada com sucesso!")
    else:
        logging.warning("Rodando sem chave de API (Free Mode)")
    
    extractor = CoinCapExtractor(api_key=API_KEY)
    
    print("\n--- Iniciando Ingestão Dinâmica ---")
    
    # 3. Busca Ativos (Define quantos ativos vamos analisar)
    LIMITE_ATIVOS = 3
    dados_assets = extractor.get_assets(limit=LIMITE_ATIVOS, use_cache=False) # use_cache=False para forçar atualização no teste
    
    if dados_assets and 'data' in dados_assets:
        print(f"OK: {len(dados_assets['data'])} ativos recuperados da API.")
        
        # 4. Loop Dinâmico: Busca o histórico e os mercados apenas para os ativos extraídos acima
        for ativo in dados_assets['data']:
            id_moeda = ativo['id']
            
            # Coleta Histórico
            logging.info(f"Processando histórico para: {id_moeda}")
            extractor.get_history(asset_id=id_moeda, interval='d1', use_cache=False)
            time.sleep(0.5) 
            
            # Coleta Mercados (Novo passo para o Tabelão completo)
            logging.info(f"Processando mercados para: {id_moeda}")
            extractor.get_markets(asset_id=id_moeda, limit=5, use_cache=False)
            time.sleep(0.5) 
            
    else:
        logging.error("Falha ao recuperar ativos base. Interrompendo pipeline.")

    # 5. Busca Exchanges (Contexto de mercado)
    extractor.get_exchanges(use_cache=False)

    print("\n--- Ingestão Concluída! Verifique os arquivos JSON gerados na pasta. ---")