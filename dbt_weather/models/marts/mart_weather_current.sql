{{
    config(
        materialized='incremental',
        unique_key='weather_id'
    )
}}

with stg as (
    select * from {{ ref('stg_weather') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['latitude', 'longitude', 'measured_at']) }} as weather_id,
        city_name,
        latitude,
        longitude,
        timezone,
        measured_at,
        temperature_c,
        relative_humidity_pct,
        precipitation_mm,
        weather_code,
        wind_speed_kmh,
        wind_direction_deg,
        loaded_at
    from stg

    {% if is_incremental() %}
        where measured_at > (select max(measured_at) from {{ this }})
    {% endif %}
)

select * from final