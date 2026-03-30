-- PURPOSE:
-- Validate that the curated BI mart does not expose more than 25 terms
-- for any (week_start_date, dma_id) combination.
--
-- WHY THIS TEST EXISTS:
-- - The Google Trends source is intended to represent a top-N view
-- - Source data contains repeated refresh snapshots, so bad deduplication
--   could accidentally inflate the number of terms in the final mart
-- - This test enforces a business-facing contract on the curated model,
--   rather than assuming the raw source is perfectly clean
--
-- PASS CONDITION:
-- - Each (week_start_date, dma_id) group contains at most 25 terms
--
-- FAILURE MEANING:
-- - Snapshot resolution may be incorrect
-- - Duplicate terms may be leaking into the mart
-- - The curated business view is no longer consistent with the intended top-N structure

with grouped as (

    select
        week_start_date,
        dma_id,
        count(*) as row_count
    from {{ ref('mart_search_trends') }}
    group by 1, 2

)

select *
from grouped
where row_count > 25