with source as (
    select * from {{ source('raw', 'raw_weather') }}
),

staged as (
    select
        city_name,
        TRY_CAST(latitude as FLOAT)                             as latitude,
        TRY_CAST(longitude as FLOAT)                            as longitude,
        timezone,
        TRY_CAST(REPLACE(timestamp, 'T', ' ') AS TIMESTAMP_NTZ) as measured_at,
        TRY_CAST(temperature_2m as FLOAT)                       as temperature_c,
        TRY_CAST(relative_humidity_2m as INTEGER)               as relative_humidity_pct,
        TRY_CAST(precipitation as FLOAT)                        as precipitation_mm,
        TRY_CAST(weather_code as INTEGER)                       as weather_code,
        TRY_CAST(wind_speed_10m as FLOAT)                       as wind_speed_kmh,
        TRY_CAST(wind_direction_10m as INTEGER)                 as wind_direction_deg,
        loaded_at
    from source
)

select * from staged