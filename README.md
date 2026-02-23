# 🚀 Crypto Data Pipeline (ELT) - Case Técnico 2

Este repositório contém a solução completa de **Extração de Dados via API**, implementando um pipeline moderno de engenharia de dados (ELT) 100% serverless no Google Cloud Platform (GCP).

O objetivo é extrair dados em tempo real da API pública da CoinCap, processar assincronamente e disponibilizar insights de arbitragem de criptomoedas em um dashboard analítico.

![Python](https://img.shields.io/badge/Python-3.12-blue?style=for-the-badge&logo=python&logoColor=white)
![Google Cloud](https://img.shields.io/badge/Google_Cloud-BigQuery%20|%20Functions-4285F4?style=for-the-badge&logo=google-cloud&logoColor=white)
![Looker](https://img.shields.io/badge/Looker-Studio-4285F4?style=for-the-badge&logo=google-analytics&logoColor=white)

## 📋 Visão Geral da Arquitetura

A solução adota a arquitetura **ELT (Extract, Load, Transform)**. O processamento pesado foi delegado ao Data Warehouse, garantindo resiliência na extração e alta performance analítica.

  <img src="docs/img/00_arquitetura.png" alt="Arquitetura do projeto">
  *Imagem gerada com IA

### 🛠️ Stack Tecnológica
* **Linguagem:** Python 3.12 (`pandas`, `requests`, `google-cloud-bigquery`)
* **Orquestração e Computação:** Cloud Functions (Gen 2 / Cloud Run) + Cloud Scheduler
* **Data Warehouse:** Google BigQuery
* **Visualização de Dados (BI):** Looker Studio
* **Infraestrutura / Segurança:** IAM (Service Accounts + Autenticação OIDC)

---

## 🏗️ Modelagem de Dados e Fluxo

O Data Warehouse foi segregado em duas camadas lógicas para garantir governança e performance:

### 1. Camada Raw (Ingestão)
* **Objetivo:** Armazenar o payload original de forma resiliente.
* **Tabelas:** `tb_assets`, `tb_markets`, `tb_exchanges`.
* **Engenharia:** Dados ingeridos estritamente como `STRING` para prevenir quebras contratuais (Schema Evolution) da API e particionados por data (`processado_em`).

### 2. Camada Refined (Analítica)
* **Objetivo:** Dados tipados, higienizados e modelados para o BI.
* **Tabelas:** 
    * `tb_arbitragem_analitica` (Focada em spread entre corretoras, otimizada com `CLUSTER BY id_moeda, nome_moeda, nome_corretora_compra, nome_corretora_venda`).
    * `tb_mercado_macro_analitica` (Visão global do mercado).
* **Engenharia:** Tabelas geradas via Stored Procedures dinâmicas. O fuso horário é ajustado nativamente para `America/Sao_Paulo` antes da disponibilização para o Looker Studio.

---

## 🛡️ DevSecOps e Boas Práticas

O pipeline não apenas extrai dados, mas aplica padrões corporativos de resiliência:

1. **Segurança Zero Trust:** A Cloud Function é privada. O gatilho público (`allUsers`) foi bloqueado. O pipeline é invocado exclusivamente pelo Cloud Scheduler utilizando um Token OIDC atrelado a uma Service Account com o princípio de Menor Privilégio (*Cloud Run Invoker*).
2. **Schema Enforcement Dinâmico:** O script Python utiliza `df.reindex()` para garantir que anomalias na API (colunas extras) sejam ignoradas, protegendo a integridade estrutural do BigQuery.
3. **Data Quality / Circuit Breakers:** As procedures em SQL possuem validações antes de truncar as tabelas analíticas. Se a origem não apresentar dados recentes (falha na API), a transação sofre `ROLLBACK`, impedindo que o dashboard exiba telas em branco.
4. **Princípio DRY:** Código Python modularizado com rotina genérica de ingestão (`run_elt_process`), centralizando logs sistêmicos (`logging`) e tratamento de exceções.

---

## 📊 Dashboard Interativo e Evidências

O pipeline está configurado em produção rodando autonomamente duas vezes ao dia (09:00 e 19:00 BRT).

👉 **[Acessar o Dashboard Interativo (Looker Studio)](https://lookerstudio.google.com/reporting/8e57ea39-ac2d-4562-923c-c882ec0821b8)**

Abaixo estão as evidências da infraestrutura em operação na nuvem:

<details>
  <summary><b>1. Execução Autenticada (Cloud Scheduler)</b></summary>
  <br>
  <img src="docs/img/02_cloud_scheduler.png" alt="Cloud Scheduler OIDC">
</details>

<details>
  <summary><b>2. Camada Refined (BigQuery)</b></summary>
  <br>
  <img src="docs/img/03_refined_tb_mercado_macro_analitica.png" alt="BigQuery Schema">
  <br>
  <img src="docs/img/04_refined_tb_arbitragem_analitica.png" alt="BigQuery Schema">
</details>

<details>
  <summary><b>3. Dataviz (Looker Studio)</b></summary>
  <br>
  <img src="docs/img/05_looker_inteligencia_oportunidades_criptoativos.png" alt="Dashboard Looker">
</details>

---

## 💻 Como Executar Localmente (Setup de Desenvolvimento)

*Nota de Arquitetura: O pipeline foi projetado com separação de responsabilidades. O arquivo `main.py` contém exclusivamente a lógica de produção (Cloud Functions). Para testes locais, criamos o wrapper `run_local.py`, que simula o ambiente da nuvem injetando as variáveis de ambiente sem alterar o código principal.*

Caso deseje replicar este pipeline em seu próprio ambiente, siga os passos abaixo:

**1. Pré-requisitos e Infraestrutura:**
* Python 3.12+ instalado.
* Um projeto ativo no Google Cloud com a API do BigQuery habilitada.
* **Banco de Dados:** Antes de rodar o código, é necessário espelhar a estrutura no seu BigQuery. Execute os scripts SQL disponíveis nas pastas `sql/ddl/` e `sql/procedures/` para criar as tabelas e lógicas de transformação.
* **Service Account (SA):** Crie uma Conta de Serviço no IAM do GCP com o papel de `Editor de Dados do BigQuery`. Gere uma chave no formato **JSON** e baixe para a sua máquina (Mantenha este arquivo seguro).

**2. Configuração do Ambiente:**

```bash
# 1. Clone o repositório
git clone https://github.com/matheusfvh/crypto-data-pipeline.git
cd crypto-data-pipeline

# 2. Crie e ative o ambiente virtual
python -m venv venv
source venv/bin/activate  # No Windows utilize: venv\Scripts\activate

# 3. Instale as dependências
pip install -r src/requirements.txt
```

**3. Variáveis de Ambiente (.env):**
Crie um arquivo chamado `.env` na raiz do projeto. Ele será lido automaticamente pelo script de teste local.

*(Nota: O arquivo `.env` já está mapeado no `.gitignore` do repositório para garantir a segurança das credenciais).*

```env
GCP_PROJECT_ID=seu-id-do-projeto
GCP_DATASET_ID=seu_nome_do_dataset
COINCAP_API_KEY=sua_api_key_coincap_aqui
GOOGLE_APPLICATION_CREDENTIALS="/caminho/absoluto/para/sua/chave-sa.json"
```

*Dica para usuários de Windows: no caminho da credencial, utilize barras normais (/) ou barras invertidas duplas (\\) para evitar erros de leitura do caminho.

**4. Execução do Pipeline:**
Com a infraestrutura criada e o ambiente configurado, execute o script de teste local:

```bash
python src/run_local.py
```
O terminal exibirá os logs do processo (Extract, Load, Transform), executando a extração da API e gravando os dados diretamente no seu BigQuery.

---

## 📂 Estrutura do Repositório

```text
crypto-data-pipeline/
├── docs/               # Documentação, evidências visuais e arquitetura
│   └── img/            # Prints de execução (Scheduler, BigQuery, Looker)
├── sql/
│   ├── ddl/            # Scripts de criação das tabelas (Camadas Raw e Refined)
│   └── procedures/     # Lógicas de negócio e qualidade de dados (PL/SQL)
├── src/
│   ├── main.py         # Código-fonte principal (Entrypoint da Cloud Function)
│   ├── run_local.py    # Wrapper para execução e testes em ambiente local
│   └── requirements.txt# Dependências do projeto (Pandas, BigQuery, Dotenv)
├── .env.example        # Modelo de variáveis de ambiente (sem dados sensíveis)
└── README.md           # Documentação técnica completa do projeto
```

---
Desenvolvido por **Matheus Furlanetto von Hoonholtz**.
