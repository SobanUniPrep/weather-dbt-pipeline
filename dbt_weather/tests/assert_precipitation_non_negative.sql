select *
from {{ ref('stg_weather') }}
where precipitation_mm < 0