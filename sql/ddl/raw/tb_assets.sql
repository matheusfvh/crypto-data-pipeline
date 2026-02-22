CREATE TABLE `crypto-pipeline-teste.crypto_data.tb_assets`
(
    id STRING OPTIONS(description="Identificador unico da moeda na API"),
    rank STRING OPTIONS(description="Posicao no ranking de capitalizacao"),
    symbol STRING OPTIONS(description="Sigla de negociacao (ex: BTC)"),
    name STRING OPTIONS(description="Nome comercial"),
    supply STRING OPTIONS(description="Oferta circulante atual"),
    maxSupply STRING OPTIONS(description="Oferta maxima permitida"),
    marketCapUsd STRING OPTIONS(description="Capitalizacao de mercado"),
    volumeUsd24Hr STRING OPTIONS(description="Volume financeiro nas ultimas 24h"),
    priceUsd STRING OPTIONS(description="Preco atualizado"),
    changePercent24Hr STRING OPTIONS(description="Variacao percentual em 24h"),
    vwap24Hr STRING OPTIONS(description="Preco medio ponderado por volume"),
    explorer STRING OPTIONS(description="Link do blockchain explorer"),
    processado_em TIMESTAMP OPTIONS(description="Carimbo de tempo exato da ingestao no BigQuery")
)
PARTITION BY DATE(processado_em)
OPTIONS(
  description="Camada RAW: Dados brutos de ativos (Assets) ingeridos da API CoinCap.",
  labels=[("projeto", "crypto_arbitragem"), ("camada", "raw")]
);