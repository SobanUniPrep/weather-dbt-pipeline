with base as (
    select
        city_name,
        AVG(temperature_c)      as avg_temperature_c,
        MAX(temperature_c)      as max_temperature_c,
        MIN(temperature_c)      as min_temperature_c,
        AVG(wind_speed_kmh)     as avg_wind_speed_kmh,
        MAX(wind_speed_kmh)     as max_wind_speed_kmh,
        AVG(precipitation_mm)   as avg_precipitation_mm,
        SUM(precipitation_mm)   as total_precipitation_mm
    from {{ ref('mart_weather_current') }}
    group by city_name
),

final as (
    select
        city_name,
        avg_temperature_c,
        max_temperature_c,
        min_temperature_c,
        avg_wind_speed_kmh,
        max_wind_speed_kmh,
        avg_precipitation_mm,
        total_precipitation_mm,

        DENSE_RANK() OVER (ORDER BY avg_temperature_c DESC)     as rank_warmest,
        DENSE_RANK() OVER (ORDER BY avg_temperature_c ASC)      as rank_coldest,
        DENSE_RANK() OVER (ORDER BY avg_wind_speed_kmh DESC)    as rank_windiest,
        DENSE_RANK() OVER (ORDER BY total_precipitation_mm DESC) as rank_wettest
    from base
)

select * from final