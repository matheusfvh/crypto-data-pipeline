/*
----------------------------------------------------------------------
DOCUMENTAÇÃO
Procedure: prc_load_tb_mercado_macro_analitica
Objetivo: Carga Full (Snapshot) da Tabela Refined (Visão Macro de Mercado).
Estratégia: 
  1. Parametrização via Dynamic SQL com concatenação nativa (||).
  2. DQ Check 1: Freshness (Valida defasagem da origem).
  3. Tratamento de tipagem e regras de negócio (Sentimento).
  4. DQ Check 2: Volume Anomaly (Garante que há moedas processadas).
  5. Transação: TRUNCATE + INSERT com colunas explícitas.
----------------------------------------------------------------------
*/
CREATE OR REPLACE PROCEDURE `crypto-pipeline-teste.crypto_data.prc_load_tb_mercado_macro_analitica`(
    p_project_id STRING,
    p_dataset_id STRING
)
BEGIN
    DECLARE v_ultima_coleta_source TIMESTAMP;
    DECLARE v_qtd_linhas_geradas INT64;

    -- ======================================================================
    -- CAPTURA DA ÚLTIMA COLETA DA TABELA DE ORIGEM (tb_assets)
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
    -- PASSO 1: Isolamento do Snapshot e Transformações (Tratamento)
    ----------------------------------------------------------------------
    EXECUTE IMMEDIATE """
        CREATE TEMP TABLE tmp_macro_assets AS
        SELECT
            LOWER(TRIM(id)) AS id_moeda,
            INITCAP(TRIM(name)) AS nome_moeda,
            UPPER(TRIM(symbol)) AS sigla_moeda,
            CAST(priceUsd AS FLOAT64) AS vlr_preco_usd,
            CAST(marketCapUsd AS FLOAT64) AS vlr_market_cap_usd,
            CAST(volumeUsd24Hr AS FLOAT64) AS vlr_volume_24h_usd,
            CAST(changePercent24Hr AS FLOAT64) AS vlr_variacao_24h_pct,
            CAST(vwap24Hr AS FLOAT64) AS vlr_vwap_24h,
            CASE 
                WHEN CAST(changePercent24Hr AS FLOAT64) > 0 THEN 'Alta'
                WHEN CAST(changePercent24Hr AS FLOAT64) < 0 THEN 'Baixa'
                ELSE 'Neutro' 
            END AS desc_sentimento_tendencia,
            TIMESTAMP_TRUNC(CAST(processado_em AS TIMESTAMP), SECOND) AS dt_hr_processado
        FROM `""" || p_project_id || """.""" || p_dataset_id || """.tb_assets`
        WHERE TIMESTAMP_TRUNC(CAST(processado_em AS TIMESTAMP), SECOND) = ?
    """ USING v_ultima_coleta_source;

    -- ======================================================================
    -- DATA QUALITY CHECK 2: VOLUME
    -- ======================================================================
    EXECUTE IMMEDIATE """
        SELECT COUNT(1) FROM tmp_macro_assets
    """ INTO v_qtd_linhas_geradas;

    IF v_qtd_linhas_geradas = 0 THEN
        ROLLBACK TRANSACTION;
        RAISE USING MESSAGE = 'DQ_ERROR_03: A transformacao resultou em 0 linhas. A tabela destino NAO foi truncada para proteger o dashboard.';
    END IF;

    ----------------------------------------------------------------------
    -- PASSO 2.1: Limpeza da Tabela Destino (TRUNCATE)
    ----------------------------------------------------------------------
    EXECUTE IMMEDIATE """
        TRUNCATE TABLE `""" || p_project_id || """.""" || p_dataset_id || """.tb_mercado_macro_analitica`
    """;

    ----------------------------------------------------------------------
    -- PASSO 2.2: Inserção do Novo Snapshot (INSERT)
    ----------------------------------------------------------------------
    EXECUTE IMMEDIATE """
        INSERT INTO `""" || p_project_id || """.""" || p_dataset_id || """.tb_mercado_macro_analitica` (
            id_moeda, 
            nome_moeda, 
            sigla_moeda, 
            vlr_preco_usd, 
            vlr_market_cap_usd,
            vlr_volume_24h_usd, 
            vlr_variacao_24h_pct, 
            vlr_vwap_24h,
            desc_sentimento_tendencia, 
            dt_hr_processado
        )
        SELECT 
            id_moeda, 
            nome_moeda, 
            sigla_moeda, 
            vlr_preco_usd, 
            vlr_market_cap_usd,
            vlr_volume_24h_usd, 
            vlr_variacao_24h_pct, 
            vlr_vwap_24h,
            desc_sentimento_tendencia, 
            dt_hr_processado
        FROM tmp_macro_assets
    """;

    COMMIT TRANSACTION;

END;