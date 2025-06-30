{{ config(materialized='table') }}

select
  md5(province_state||'|'||country_region||'|'||last_update) as record_hash,
  file_date,
  province_state,
  country_region,
  confirmed,
  deaths,
  recovered,
  active
from {{ ref('stg_covid') }}


{% if is_incremental() %}
  -- only keep rows whose hash isn’t already in the target table
  where record_hash not in (
    select record_hash from {{ this }}
  )
{% endif %}

