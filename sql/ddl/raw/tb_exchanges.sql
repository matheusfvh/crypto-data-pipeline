CREATE TABLE `crypto-pipeline-teste.crypto_data.tb_exchanges`
(
    exchangeId STRING OPTIONS(description="Identificador unico da corretora"),
    name STRING OPTIONS(description="Nome oficial da corretora"),
    rank STRING OPTIONS(description="Posicao no ranking global de volume"),
    percentTotalVolume STRING OPTIONS(description="Percentual do volume global que passa por ela"),
    volumeUsd STRING OPTIONS(description="Volume financeiro total em USD"),
    tradingPairs STRING OPTIONS(description="Quantidade de pares de moedas ativos"),
    socket STRING OPTIONS(description="Possui websocket ativo (boolean como string)"),
    exchangeUrl STRING OPTIONS(description="Site oficial da corretora"),
    updated STRING OPTIONS(description="Data da ultima atualizacao na fonte"),
    processado_em TIMESTAMP OPTIONS(description="Carimbo de tempo exato da ingestao no BigQuery")
)
PARTITION BY DATE(processado_em)
OPTIONS(
  description="Camada RAW: Dados brutos e cadastrais das corretoras (Exchanges).",
  labels=[("projeto", "crypto_arbitragem"), ("camada", "raw")]
);