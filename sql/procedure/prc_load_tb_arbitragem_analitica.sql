/*
----------------------------------------------------------------------
DOCUMENTAÇÃO
Procedure: prc_load_tb_arbitragem_analitica
Objetivo: Carga Full (Snapshot) da Tabela Refined com Data Quality.
Estratégia: 
  1. Parametrização para evitar repetições de código.
  2. DQ Check 1: Freshness (Valida defasagem da origem).
  3. DQ Check 2: Volume Anomaly (Garante que há arbitragem calculada).
  4. Transação : TRUNCATE + INSERT.
----------------------------------------------------------------------
*/

CREATE OR REPLACE PROCEDURE `crypto-pipeline-teste.crypto_data.prc_load_tb_arbitragem_analitica`(
    p_project_id STRING,
    p_dataset_id STRING
)
BEGIN
    DECLARE v_ultima_coleta_source TIMESTAMP;
    DECLARE v_qtd_linhas_geradas INT64;

    -- ======================================================================
    -- CAPTURA DE ULTIMA COLETA DA TABELA DE ORIGEM (tb_assets)
    -- ======================================================================
    EXECUTE IMMEDIATE """
        SELECT MAX(TIMESTAMP_TRUNC(CAST(processado_em AS TIMESTAMP), SECOND)) 
        FROM `""" || p_project_id || """.""" || p_dataset_id || """.tb_assets`
    """ INTO v_ultima_coleta_source;

    -- ======================================================================
    -- DATA QUALITY CHECK 1: FRESHNESS
    -- ======================================================================
    IF v_ultima_coleta_source IS NULL THEN
        RAISE USING MESSAGE = 'DQ_ERROR_01: Nao ha dados na tabela de origem (tb_assets).';
    END IF;

    BEGIN TRANSACTION;

    ----------------------------------------------------------------------
    -- PASSO 1: Isolamento do Snapshot
    ----------------------------------------------------------------------
    EXECUTE IMMEDIATE """
        CREATE TEMP TABLE tmp_assets AS
        SELECT
            LOWER(TRIM(id)) AS id_join,
            name,
            TIMESTAMP_TRUNC(CAST(processado_em AS TIMESTAMP), SECOND) AS dt_hr_processado
        FROM `""" || p_project_id || """.""" || p_dataset_id || """.tb_assets`
        WHERE TIMESTAMP_TRUNC(CAST(processado_em AS TIMESTAMP), SECOND) = ?
    """ USING v_ultima_coleta_source;

    ----------------------------------------------------------------------
    -- PASSO 2: Base de Markets (Enriquecimento e Filtro)
    ----------------------------------------------------------------------
    EXECUTE IMMEDIATE """
        CREATE TEMP TABLE tmp_base_markets AS
        SELECT
            LOWER(TRIM(m.baseId)) AS id_moeda,
            INITCAP(COALESCE(a.name, m.baseId)) AS nome_moeda,
            INITCAP(COALESCE(e.name, REPLACE(REPLACE(m.exchangeId, '-', ' '), '_', ' '))) AS nome_corretora,
            CAST(m.priceQuote AS FLOAT64) AS preco,
            CAST(m.volumeUsd24Hr AS FLOAT64) AS volume,
            a.dt_hr_processado
        FROM `""" || p_project_id || """.""" || p_dataset_id || """.tb_markets` m
        INNER JOIN tmp_assets a
            ON LOWER(TRIM(m.baseId)) = a.id_join
            AND TIMESTAMP_TRUNC(CAST(m.processado_em AS TIMESTAMP), SECOND) = a.dt_hr_processado
        LEFT JOIN (
            SELECT exchangeId, name 
            FROM `""" || p_project_id || """.""" || p_dataset_id || """.tb_exchanges`
            QUALIFY ROW_NUMBER() OVER(PARTITION BY LOWER(TRIM(exchangeId)) ORDER BY processado_em DESC) = 1
        ) e
            ON LOWER(TRIM(m.exchangeId)) = LOWER(TRIM(e.exchangeId))
        WHERE LOWER(TRIM(m.quoteId)) IN (
            'united-states-dollar', 'tether', 'usd-coin', 'binance-usd', 
            'multi-collateral-dai', 'dai', 'usdd', 'united-stables', 
            'true-usd', 'first-digital-usd'
        )
    """;

    ----------------------------------------------------------------------
    -- PASSO 3: Rank de Markets
    ----------------------------------------------------------------------
    EXECUTE IMMEDIATE """
        CREATE TEMP TABLE tmp_ranked_markets AS
        SELECT
            id_moeda,
            nome_moeda,
            dt_hr_processado,
            FIRST_VALUE(nome_corretora) OVER(PARTITION BY id_moeda ORDER BY preco ASC) AS corretora_compra,
            FIRST_VALUE(preco) OVER(PARTITION BY id_moeda ORDER BY preco ASC) AS preco_compra,
            FIRST_VALUE(volume) OVER(PARTITION BY id_moeda ORDER BY preco ASC) AS volume_compra,
            FIRST_VALUE(nome_corretora) OVER(PARTITION BY id_moeda ORDER BY preco DESC) AS corretora_venda,
            FIRST_VALUE(preco) OVER(PARTITION BY id_moeda ORDER BY preco DESC) AS preco_venda,
            FIRST_VALUE(volume) OVER(PARTITION BY id_moeda ORDER BY preco DESC) AS volume_venda
        FROM tmp_base_markets
    """;

    ----------------------------------------------------------------------
    -- PASSO 4: Tabela Final
    ----------------------------------------------------------------------
    EXECUTE IMMEDIATE """
        CREATE TEMP TABLE tmp_final AS
        SELECT DISTINCT
            id_moeda,
            nome_moeda,
            corretora_compra AS nome_corretora_compra,
            preco_compra AS vlr_preco_compra,
            corretora_venda AS nome_corretora_venda,
            preco_venda AS vlr_preco_venda,
            (preco_venda / preco_compra) - 1 AS vlr_margem_lucro_percentual,
            LEAST(volume_compra, volume_venda) AS vlr_volume_gargalo_usd,
            dt_hr_processado
        FROM tmp_ranked_markets
        WHERE corretora_compra != corretora_venda 
            AND preco_compra > 0
    """;

    -- ======================================================================
    -- DATA QUALITY CHECK 2: VOLUME 
    -- ======================================================================
    EXECUTE IMMEDIATE """
        SELECT COUNT(1) FROM tmp_final
    """ INTO v_qtd_linhas_geradas;

    IF v_qtd_linhas_geradas = 0 THEN
        ROLLBACK TRANSACTION;
        RAISE USING MESSAGE = 'DQ_ERROR_03: O cruzamento resultou em 0 linhas de arbitragem. A tabela destino NAO foi truncada.';
    END IF;

    ----------------------------------------------------------------------
    -- PASSO 5.1: Limpeza da Tabela Destino (TRUNCATE)
    ----------------------------------------------------------------------
    EXECUTE IMMEDIATE """
        TRUNCATE TABLE `""" || p_project_id || """.""" || p_dataset_id || """.tb_arbitragem_analitica`
    """;

    ----------------------------------------------------------------------
    -- PASSO 5.2: Inserção do Novo Snapshot (INSERT)
    ----------------------------------------------------------------------
    EXECUTE IMMEDIATE """
        INSERT INTO `""" || p_project_id || """.""" || p_dataset_id || """.tb_arbitragem_analitica` (
            id_moeda, 
            nome_moeda, 
            nome_corretora_compra, 
            vlr_preco_compra,
            nome_corretora_venda, 
            vlr_preco_venda, 
            vlr_margem_lucro_percentual,
            vlr_volume_gargalo_usd, 
            dt_hr_processado
        )
        SELECT 
            id_moeda,
            nome_moeda,
            nome_corretora_compra,
            vlr_preco_compra,
            nome_corretora_venda,
            vlr_preco_venda,
            vlr_margem_lucro_percentual,
            vlr_volume_gargalo_usd,
            TIMESTAMP(DATETIME(dt_hr_processado, "America/Sao_Paulo"))
        FROM tmp_final
    """;

    COMMIT TRANSACTION;

END;