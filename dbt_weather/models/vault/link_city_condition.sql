with stg as (
    select distinct
        city_name,
        weather_code
    from {{ ref('stg_weather') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['city_name', 'weather_code']) }} as link_city_condition_hk,
        {{ dbt_utils.generate_surrogate_key(['city_name']) }} as hub_city_hk,
        {{ dbt_utils.generate_surrogate_key(['weather_code']) }} as hub_condition_hk,
        current_timestamp() as load_date,
        'OPEN-METEO' as record_source
    from stg
)

select * from final