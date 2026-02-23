import os
from dotenv import load_dotenv
from main import handler # Importa a função do seu arquivo original

print("🚀 Iniciando teste local do pipeline ELT...")

# 1. Carrega as variáveis do arquivo .env
load_dotenv()

# Validação rápida de segurança
if not os.environ.get('GCP_PROJECT_ID'):
    print("⚠️ ERRO: GCP_PROJECT_ID não encontrado! Verifique se o arquivo .env está na raiz do projeto.")
    exit(1)

# 2. Simula a requisição HTTP que o Google Cloud envia
class MockRequest:
    pass
    
# 3. Executa a função principal do Cloud Functions
resultado, status_code = handler(MockRequest())

print(f"\n✅ Status da Execução: {status_code}")
print(f"📄 Resposta: {resultado}")