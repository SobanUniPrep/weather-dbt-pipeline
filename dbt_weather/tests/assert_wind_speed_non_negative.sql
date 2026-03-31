select *
from {{ ref('stg_weather') }}
where wind_speed_kmh < 0