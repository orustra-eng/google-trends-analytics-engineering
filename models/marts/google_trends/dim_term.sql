{{ config(materialized='table') }}

-- PURPOSE:
-- Dimension table for search terms.
-- Provides a unique list of terms with a surrogate key for use in the fact table.
--
-- DESIGN NOTES:
-- - Built from the intermediate layer to ensure consistency with curated data
-- - Terms are high-cardinality and dynamic (new terms can appear over time)
-- - Surrogate key is used instead of natural key (term string) for performance and join stability

with terms as (

    -- Extract unique search terms across all weeks and DMAs
    -- DISTINCT is required because the same term appears multiple times over time and regions
    select distinct
        term
    from {{ ref('int_search_trends_enriched') }}

)

select
    -- Surrogate key generated from term string
    -- Ensures consistent joins and avoids repeated use of long string keys
    {{ dbt_utils.generate_surrogate_key(['term']) }} as term_id,

    -- Original search term (business-facing field)
    term

from terms