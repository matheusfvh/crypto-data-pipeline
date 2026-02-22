CREATE TABLE `crypto-pipeline-teste.crypto_data.tb_markets`
(
    exchangeId STRING OPTIONS(description="Identificador unico da corretora"),
    baseId STRING OPTIONS(description="Identificador da moeda base (comprada)"),
    quoteId STRING OPTIONS(description="Identificador da moeda de cotacao (usada para pagar)"),
    baseSymbol STRING OPTIONS(description="Sigla da moeda base"),
    quoteSymbol STRING OPTIONS(description="Sigla da moeda de cotacao"),
    volumeUsd24Hr STRING OPTIONS(description="Volume negociado do par em 24h"),
    priceQuote STRING OPTIONS(description="Preco do ativo nesta corretora especifica"),
    volumePercent STRING OPTIONS(description="Percentual de representatividade do volume"),
    processado_em TIMESTAMP OPTIONS(description="Carimbo de tempo exato da ingestao no BigQuery")
)
PARTITION BY DATE(processado_em)
OPTIONS(
  description="Camada RAW: Dados brutos de pares de negociacao (Markets) por id de corretoras.",
  labels=[("projeto", "crypto_arbitragem"), ("camada", "raw")]
);