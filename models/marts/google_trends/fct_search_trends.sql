{{ config(materialized='table') }}

-- PURPOSE:
-- Fact table capturing weekly search-term observations at the grain
-- (week, DMA, term), based on the latest available snapshot per (week, DMA).
--
-- GRAIN:
-- One row per week_id, dma_id, term_id
--
-- DESIGN NOTES:
-- - Built from the curated intermediate layer after snapshot resolution
-- - Stores behavioral metrics focused on activity, spikes, and persistence
-- - Avoids over-reliance on rank-based trend metrics, since rank is relatively stable
-- - Keeps refresh_date for lineage and auditability, but this field is typically
--   not exposed directly in the BI-serving mart

with source_data as (

    -- Curated weekly search-term data enriched with analytical features
    select *
    from {{ ref('int_search_trends_enriched') }}

),

final as (

    select
        -- Week foreign key aligned to dim_week
        cast(format_date('%G%V', week_start_date) as {{ dbt.type_int() }}) as week_id,

        -- DMA foreign key aligned to dim_dma
        dma_id,

        -- Term surrogate key aligned to dim_term
        {{ dbt_utils.generate_surrogate_key(['term']) }} as term_id,

        -- Core source metrics
        rank,
        score,

        -- Score-based temporal comparison (used cautiously due to normalization)
        previous_score,
        score_change_wow,

        -- Behavioral indicators
        is_top_5_term,
        is_active_term,
        is_score_spike,
        term_entry_flag,

        -- Persistence metrics
        active_weeks_count,
        observed_weeks_count,
        term_persistence_ratio,

        -- Snapshot metadata for traceability
        refresh_date

    from source_data

)

select *
from final