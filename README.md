# Google Trends Analytics Engineering Case Study

A lightweight dbt project for the `bigquery-public-data.google_trends.top_terms` dataset.

## Goal
Build a scalable, testable analytics model for a marketing analytics team that needs to:
- track trending topics over time
- compare regions (DMA)
- identify rising trends early

## Modeling approach
This repo uses a layered dbt structure:
- `staging`: source cleanup and standardization
- `intermediate`: reusable enrichment logic and latest-snapshot deduplication
- `marts`: publishable analytical assets
  - dimensional core: `dim_week`, `dim_dma`, `dim_term`, and `fct_search_trends`
  - denormalized BI mart: `mart_search_trends`

## Source dataset
Source table:
- `bigquery-public-data.google_trends.top_terms`

Fields used here:
- `refresh_date`
- `week`
- `dma_id`
- `dma_name`
- `term`
- `rank`
- `score`

## Key design choices
- Model the business grain as **one row per `(week, dma_id, term)`**.
- Keep `refresh_date` as technical metadata in staging/intermediate/fact layers, but exclude it from the BI mart.
- Deduplicate to the **latest available snapshot** per business grain before publishing marts.
- Use a governed dimensional core plus a denormalized self-service BI table.
- Treat `score` as a nullable, relative index rather than an absolute measure.

## Proposed physical flow
1. Declare the BigQuery public dataset as a dbt `source` for the assignment.
2. Standardize and type the source in `stg_google_trends__top_terms`.
3. Build reusable enriched logic in `int_search_trends_enriched`.
4. Publish dimensions and a fact table in the `marts` layer.
5. Expose `mart_search_trends` as the self-service BI table.

## Suggested interview talking points
- Weekly grain aligned to the real source schema rather than the prompt wording.
- Logical dimensional model for governance and reuse.
- Denormalized serving mart for self-service BI in BigQuery.
- Direct dbt source for the assignment; scheduled raw copy in production.
- Tests at source, intermediate, mart, and BI-serving layers.

## Run notes
This is a project skeleton intended for the case-study deliverable. Update `profiles.yml` with your BigQuery project and target dataset before running.
