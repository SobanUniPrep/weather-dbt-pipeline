with stg as (
    select distinct
        weather_code
    from {{ ref('stg_weather') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['weather_code']) }} as hub_condition_hk,
        weather_code,
        current_timestamp() as load_date,
        'OPEN-METEO' as record_source
    from stg
)

select * from final