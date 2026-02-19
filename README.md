🚀 Pipeline de Ingestão de Criptos (CoinCap v3)
Pipeline de Engenharia de Dados focado na extração de métricas do mercado de criptomoedas, processamento em Python e armazenamento no Google BigQuery.

📋 Objetivo
Este projeto visa automatizar a coleta de dados da CoinCap API v3, estruturando camadas de dados (Raw e Refined) para suportar análises de preço, volume e liquidez em um dashboard no Looker Studio.

🏗️ Estrutura do Projeto
- Ingestão: Script Python modularizado para consumo de API com autenticação Bearer.
- Armazenamento: Tabelas estruturadas na Sandbox do BigQuery.
- Refinamento: Criação de Tabela Refinada via SQL para otimização de Dataviz.

🛠️ Tecnologias
- Python 3.12.7
- Pandas (Tratamento de dados)
- Google BigQuery (Data Warehouse)
- Postman (Validação de API)

📂 Organização de Pastas
Plaintext
├── src/           # Scripts Python (Extração e Carga)
├── sql/           # Queries de refinamento e Views
├── docs/          # Evidências de testes e documentação da API
├── requirements.txt
└── README.md

🚀 Como Configurar
1. Clone o repositório.
2. Crie o seu ambiente virtual: python -m venv venv.
3. Instale as dependências: pip install -r requirements.txt.
4. Adicione suas chaves no arquivo .env (baseado no .env.example).