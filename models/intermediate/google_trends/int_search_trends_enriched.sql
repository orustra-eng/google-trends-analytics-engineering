{{ config(materialized='view') }}

-- PURPOSE:
-- This intermediate model prepares a clean, deduplicated weekly dataset
-- of Google Trends top terms by selecting the latest snapshot per (week, DMA)
-- and enriching it with analytical features for trend detection.
--
-- KEY DESIGN DECISIONS:
-- - Source data contains multiple refresh snapshots → must deduplicate
-- - Rank is not a reliable trend signal → focus on presence and score dynamics
-- - Score is sparse but useful for detecting spikes

with base as (

    -- Raw staging layer (already cleaned, typed, renamed)
    select *
    from {{ ref('stg_google_trends__top_terms') }}

),

latest_snapshot_per_dma_week as (

    -- Identify the latest available snapshot per (week, DMA)
    -- This ensures we do not mix multiple refreshes within the same time period
    select
        week_start_date,
        dma_id,
        max(refresh_date) as latest_refresh_date
    from base
    group by 1, 2

),

latest_snapshot as (

    -- Keep only rows from the latest snapshot
    -- This enforces a consistent analytical view of weekly data
    select
        b.*
    from base b
    inner join latest_snapshot_per_dma_week s
        on b.week_start_date = s.week_start_date
       and b.dma_id = s.dma_id
       and b.refresh_date = s.latest_refresh_date

),

enriched as (

    select
        refresh_date,
        week_start_date,
        dma_id,
        dma_name,
        term,
        rank,
        score,

        -- Time attributes for reporting and aggregation
        extract(isoweek from week_start_date) as iso_week_number,
        extract(isoyear from week_start_date) as iso_week_year,
        format_date('%G-W%V', week_start_date) as iso_week_name,

        -- Flag for top-ranked terms (useful for filtering, not core signal)
        case when rank <= 5 then true else false end as is_top_5_term,

        -- Presence indicator: term is considered "active" if it appears with score > 0
        case when score > 0 then true else false end as is_active_term,

        -- Detect strong spikes in relative popularity
        -- Threshold is heuristic (e.g., 80–100 often indicates peak interest)
        case when score >= 80 then true else false end as is_score_spike,

        -- Previous week presence (used to detect new entries into top results)
        lag(case when score > 0 then 1 else 0 end) over (
            partition by dma_id, term
            order by week_start_date
        ) as previous_week_active_flag,

        -- Previous week score (used for change calculations)
        lag(score) over (
            partition by dma_id, term
            order by week_start_date
        ) as previous_score,

        -- Number of weeks where term is active (score > 0)
        countif(score > 0) over (
            partition by dma_id, term
        ) as active_weeks_count,

        -- Total number of observed weeks for this term
        -- Note: dataset is truncated (top N), so this is not full history
        count(*) over (
            partition by dma_id, term
        ) as observed_weeks_count

    from latest_snapshot
)

select
    *,

    -- Entry detection:
    -- Term appears this week but was not active in the previous week
    case
        when is_active_term and coalesce(previous_week_active_flag, 0) = 0 then true
        else false
    end as term_entry_flag,

    -- Week-over-week score change (secondary signal, used cautiously)
    case
        when previous_score is not null and score is not null
        then score - previous_score
        else null
    end as score_change_wow,

    -- Persistence ratio:
    -- Measures how consistently a term appears in top results over time
    safe_divide(active_weeks_count, observed_weeks_count) as term_persistence_ratio

from enriched