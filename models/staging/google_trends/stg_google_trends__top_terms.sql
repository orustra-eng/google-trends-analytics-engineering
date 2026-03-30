with source as (
    select *
    from {{ source('google_trends', 'top_terms') }}
),

renamed as (
    select
        cast(refresh_date as date) as refresh_date,
        cast(week as date) as week_start_date,
        cast(dma_id as {{ dbt.type_int() }}) as dma_id,
        cast(dma_name as {{ dbt.type_string() }}) as dma_name,
        trim(cast(term as {{ dbt.type_string() }})) as term,
        cast(rank as {{ dbt.type_int() }}) as rank,
        cast(score as {{ dbt.type_int() }}) as score
    from source
)

select *
from renamed
