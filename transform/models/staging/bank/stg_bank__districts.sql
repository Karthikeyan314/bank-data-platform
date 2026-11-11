with source as (
    select * from {{ source('bank', 'district') }}
)

select
    cast(district_id as integer) as district_id,
    trim(district_name) as district_name,
    trim(region) as region,
    cast(num_inhabitants as integer) as population,
    cast(num_cities as integer) as num_cities,
    cast(nullif(trim(ratio_urban), '') as decimal(5, 1)) as urban_population_pct,
    {{ to_amount('average_salary', 10, 2) }} as average_salary,
    -- District 69 (Jesenik) has no 1995 figures in the source: kept as NULL, not zero.
    cast(nullif(trim(unemployment_rate_95), '') as decimal(5, 2)) as unemployment_rate_95,
    cast(nullif(trim(unemployment_rate_96), '') as decimal(5, 2)) as unemployment_rate_96,
    cast(num_entrepreneurs_per_1000 as integer) as entrepreneurs_per_1000,
    cast(nullif(trim(num_crimes_95), '') as integer) as crimes_95,
    cast(nullif(trim(num_crimes_96), '') as integer) as crimes_96,
    _loaded_at
from source
