with stg as (
    select distinct
        city_name,
        latitude,
        longitude
    from {{ ref('stg_weather') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['city_name']) }} as hub_city_hk,
        city_name,
        latitude,
        longitude,
        current_timestamp() as load_date,
        'OPEN-METEO' as record_source
    from stg
)

select * from final