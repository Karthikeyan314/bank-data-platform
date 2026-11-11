select
    district_id,
    district_name,
    region,
    population,
    num_cities,
    urban_population_pct,
    average_salary,
    unemployment_rate_95,
    unemployment_rate_96,
    entrepreneurs_per_1000,
    crimes_95,
    crimes_96,
    -- Crime per 1,000 residents is comparable across districts; the raw count is not.
    round(1000.0 * crimes_96 / nullif(population, 0), 2) as crimes_per_1000_96,
    case
        when district_name = 'Hl.m. Praha' then 'capital'
        when urban_population_pct >= 70 then 'urban'
        else 'rural'
    end as district_type
from {{ ref('stg_bank__districts') }}
