{{ config(materialized='table') }}

-- PURPOSE:
-- Dimension table representing observed ISO weeks in the curated dataset.
-- Provides a reusable time dimension for weekly aggregation and reporting.
--
-- DESIGN NOTES:
-- - Built from the intermediate layer to ensure alignment with the curated weekly grain
-- - Uses one row per observed week_start_date
-- - week_id is derived from ISO year + ISO week for stable joins to the fact table

with weeks as (

    -- Extract unique observed weeks from the curated intermediate model
    -- DISTINCT is required because each week appears across many DMAs and terms
    select distinct
        week_start_date
    from {{ ref('int_search_trends_enriched') }}

)

select
    -- Surrogate-like business key for ISO week (e.g. 202612 for ISO week 12 of 2026)
    cast(format_date('%G%V', week_start_date) as {{ dbt.type_int() }}) as week_id,

    -- Canonical start date of the observed week
    week_start_date,

    -- Human-readable ISO week label for reporting
    format_date('%G-W%V', week_start_date) as iso_week_name,

    -- ISO week number within the ISO year
    extract(isoweek from week_start_date) as iso_week_number,

    -- ISO year associated with the ISO week
    extract(isoyear from week_start_date) as iso_week_year

from weeks