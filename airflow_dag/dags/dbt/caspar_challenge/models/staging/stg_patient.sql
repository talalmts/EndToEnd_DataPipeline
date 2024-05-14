{{ config(materialized='table') }}

with remove_dups as (
    select
        patient_id,
        first_name,
        last_name,
        country,
        row_number() over (partition by patient_id) as row_num
    from {{source('caspar', 'patient') }}
)
select 
    *
from remove_dups
    where row_num = 1