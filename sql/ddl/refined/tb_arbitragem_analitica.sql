CREATE TABLE `crypto-pipeline-teste.crypto_data.tb_arbitragem_analitica`
(
    id_moeda STRING OPTIONS(description="Identificador unico da moeda (Join Key)"),
    nome_moeda STRING OPTIONS(description="Nome amigavel da moeda (Visualizacao)"),
    nome_corretora_compra STRING OPTIONS(description="Corretora com o menor preco (Onde Comprar)"),
    vlr_preco_compra FLOAT64 OPTIONS(description="Preco de compra na exchange mais barata em USD"),
    nome_corretora_venda STRING OPTIONS(description="Corretora com o maior preco (Onde Vender)"),
    vlr_preco_venda FLOAT64 OPTIONS(description="Preco de venda na exchange mais cara em USD"),
    vlr_margem_lucro_percentual FLOAT64 OPTIONS(description="Margem de lucro bruta da arbitragem. Formato decimal (Ex: 0.10 = 10%)"),
    vlr_volume_gargalo_usd FLOAT64 OPTIONS(description="O menor volume entre a compra e a venda (Define a liquidez real da operacao)"),
    dt_hr_processado TIMESTAMP OPTIONS(description="Timestamp de processamento do pipeline")
)
CLUSTER BY id_moeda, nome_moeda, nome_corretora_compra, nome_corretora_venda
OPTIONS(
  description="Tabela analitica de arbitragem contendo apenas a melhor rota de compra e venda por moeda (Snapshot da ultima execucao).",
  labels=[("projeto", "crypto_arbitragem"), ("camada", "refined")]
);