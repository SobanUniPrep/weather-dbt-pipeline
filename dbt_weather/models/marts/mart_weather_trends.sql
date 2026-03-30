with stg as (
    select * from {{ ref('mart_weather_current') }}
),

final as (
    select
        weather_id,
        city_name,
        measured_at,
        temperature_c,

        LAG(temperature_c) OVER (
            PARTITION BY city_name
            ORDER BY measured_at
        ) as temperature_previous_hour_c,

        temperature_c - LAG(temperature_c) OVER (
            PARTITION BY city_name
            ORDER BY measured_at
        ) as temperature_change_c,

        AVG(temperature_c) OVER (
            PARTITION BY city_name
            ORDER BY measured_at
            ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
        ) as temperature_24h_avg_c,

        AVG(temperature_c) OVER (
            PARTITION BY city_name
        ) as temperature_overall_avg_c,

        precipitation_mm,
        SUM(precipitation_mm) OVER (
            PARTITION BY city_name
            ORDER BY measured_at
            ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
        ) as precipitation_24h_sum_mm,

        wind_speed_kmh,
        MAX(wind_speed_kmh) OVER (
            PARTITION BY city_name
            ORDER BY measured_at
            ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
        ) as wind_speed_24h_max_kmh,

        relative_humidity_pct,

        AVG(temperature_c) OVER (
            PARTITION BY city_name
            ORDER BY measured_at
            ROWS BETWEEN 167 PRECEDING AND CURRENT ROW
        ) as temperature_7d_avg_c
    from stg
)

select * from final