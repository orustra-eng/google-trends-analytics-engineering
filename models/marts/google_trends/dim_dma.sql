{{ config(materialized='table') }}

-- PURPOSE:
-- Dimension table representing Designated Market Areas (DMAs).
-- Provides a stable lookup for regional analysis across all search trend data.
--
-- DESIGN NOTES:
-- - Built from the intermediate layer to ensure consistency with the curated dataset
-- - Uses DISTINCT to avoid duplicates introduced by multiple terms and weeks
-- - Assumes dma_id uniquely identifies a DMA

with dmas as (

    -- Extract unique DMA identifiers and names
    -- Source is already deduplicated at (week, dma, term) level,
    -- but multiple rows per DMA still exist across time and terms
    select distinct
        dma_id,
        dma_name
    from {{ ref('int_search_trends_enriched') }}

)

select
    -- Unique DMA identifier (used as primary key)
    dma_id,

    -- Human-readable DMA name (used for reporting and BI)
    dma_name

from dmas