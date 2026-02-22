CREATE TABLE `crypto-pipeline-teste.crypto_data.tb_mercado_macro_analitica`
(
    id_moeda STRING OPTIONS(description="Identificador unico da criptomoeda na API CoinCap (ex: bitcoin)"),
    nome_moeda STRING OPTIONS(description="Nome comercial formatado da criptomoeda (ex: Bitcoin)"),
    sigla_moeda STRING OPTIONS(description="Simbolo oficial de negociacao no mercado (ex: BTC)"),
    vlr_preco_usd FLOAT64 OPTIONS(description="Preco atual do ativo cotado em Dolares Americanos (USD)"),
    vlr_market_cap_usd FLOAT64 OPTIONS(description="Capitalizacao de mercado em USD (Preco multiplicado pelo fornecimento circulante)"),
    vlr_volume_24h_usd FLOAT64 OPTIONS(description="Volume financeiro total negociado nas ultimas 24 horas em USD. Termometro de liquidez"),
    vlr_variacao_24h_pct FLOAT64 OPTIONS(description="Percentual de variacao do preco nas ultimas 24 horas. Valores positivos indicam valorizacao"),
    vlr_vwap_24h FLOAT64 OPTIONS(description="Volume-Weighted Average Price (Preco Medio Ponderado por Volume) das ultimas 24h"),
    desc_sentimento_tendencia STRING OPTIONS(description="Classificacao categorica do sentimento do ativo (Alta, Baixa ou Neutro) baseada na variacao diaria"),
    dt_hr_processado TIMESTAMP OPTIONS(description="Carimbo de tempo exato indicando quando o lote de dados foi extraido da origem")
)
CLUSTER BY id_moeda, nome_moeda
OPTIONS(
  description="Tabela analitica com a visao macro e indicadores globais do mercado de criptomoedas (Snapshot da ultima execucao).",
  labels=[("projeto", "crypto_arbitragem"), ("camada", "refined")]
);