{{ config(materialized='table') }}


with remove_dups as (
    select 
        id,
        external_id as patient_id,
        steps,
        (steps * 0.002)::float as generated_minutes,
        timezone('UTC', submission_time) as submission_time_utc,
        timezone('UTC', updated_at) as updated_at_utc,
        row_number() over (partition by id, external_id) as row_num
    from {{source('caspar', 'step') }}
)
SELECT
    *
FROM 
remove_dups
where row_num = 1