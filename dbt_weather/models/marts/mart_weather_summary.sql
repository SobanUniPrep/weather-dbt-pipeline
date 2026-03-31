with base as (
    select
        city_name,
        measured_at::DATE as weather_date,
        AVG(temperature_c) as avg_temp,
        MAX(temperature_c) as max_temp,
        MIN(temperature_c) as min_temp,
        SUM(precipitation_mm) as total_precip,
        AVG(wind_speed_kmh) as avg_wind
    from {{ ref('mart_weather_current') }}
    group by city_name, measured_at::DATE
),

final as (
    select
        city_name,
        weather_date,
        avg_temp,
        max_temp,
        min_temp,
        total_precip,
        avg_wind,
        SNOWFLAKE.CORTEX.COMPLETE(
            'mistral-7b',
            'Summarize this weather in one sentence: City=' || city_name ||
            ', Date=' || weather_date ||
            ', AvgTemp=' || ROUND(avg_temp, 1) || 'C' ||
            ', MaxTemp=' || ROUND(max_temp, 1) || 'C' ||
            ', MinTemp=' || ROUND(min_temp, 1) || 'C' ||
            ', Precipitation=' || ROUND(total_precip, 1) || 'mm' ||
            ', AvgWind=' || ROUND(avg_wind, 1) || 'kmh'
        ) as ai_summary
    from base
)

select * from final