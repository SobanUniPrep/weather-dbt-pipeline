select *
from {{ ref('stg_weather') }}
where temperature_c < -90 or temperature_c > 60