{{ config(materialized='table') }}

-- PURPOSE:
-- Denormalized BI-serving mart for weekly search trend analysis.
-- Combines fact and dimension tables into a flat, analyst-friendly structure.
--
-- GRAIN:
-- One row per (week, DMA, term)
--
-- DESIGN NOTES:
-- - Built on top of the dimensional layer for consistency and reuse
-- - Exposes business-friendly weekly, regional, and term attributes
-- - Prioritizes behavioral metrics (activity, entry, spikes, persistence)
--   over rank-based trend metrics
-- - Excludes refresh_date to keep the BI layer focused on business-facing fields

with fct as (

    -- Core weekly search-term observations and behavioral metrics
    select *
    from {{ ref('fct_search_trends') }}

),

dim_week as (

    -- Weekly time dimension
    select *
    from {{ ref('dim_week') }}

),

dim_dma as (

    -- Regional DMA dimension
    select *
    from {{ ref('dim_dma') }}

),

dim_term as (

    -- Search-term dimension
    select *
    from {{ ref('dim_term') }}

)

select
    -- Time attributes
    dim_week.week_start_date,
    dim_week.iso_week_name,
    dim_week.iso_week_number,
    dim_week.iso_week_year,

    -- Regional attributes
    dim_dma.dma_id,
    dim_dma.dma_name,

    -- Term attribute
    dim_term.term,

    -- Core source metrics
    fct.rank,
    fct.score,

    -- Supporting temporal metric
    fct.previous_score,
    fct.score_change_wow,

    -- Behavioral indicators
    fct.is_top_5_term,
    fct.is_active_term,
    fct.is_score_spike,
    fct.term_entry_flag,

    -- Persistence metrics
    fct.active_weeks_count,
    fct.observed_weeks_count,
    fct.term_persistence_ratio

from fct
left join dim_week
    on fct.week_id = dim_week.week_id
left join dim_dma
    on fct.dma_id = dim_dma.dma_id
left join dim_term
    on fct.term_id = dim_term.term_id