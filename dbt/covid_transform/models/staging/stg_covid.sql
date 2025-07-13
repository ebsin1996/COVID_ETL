{{ config(materialized='view') }}

with source_data as (
  select *
  from {{ source('jhu','raw_covid_data') }}

),

normalized as (

  select
    fips,
    admin2,
    coalesce(province_state, "Province/State")       as province_state,
    coalesce(country_region, "Country/Region")       as country_region,
    coalesce(last_update, "Last Update")             as last_update,
    coalesce(lat, "Latitude")                        as lat,
    coalesce(long_, "Longitude")                     as long_,
    confirmed,
    deaths,
    recovered,
    active,
    file_date
  from source_data

)

select * from normalized
