with source as (
    select * from {{ ref('cities') }}
),

staged as (
    select
        id              as city_id,
        name            as city_name,
        country         as country_code,
        latitude,
        longitude,
        timezone
    from source
)

select * from staged