{{ config(materialized='table', schema='staging') }}


with remove_dups as (
    select 
        id,
        external_id as patient_id,
        minutes as generated_minutes,
        timezone('UTC', completed_at) as completed_at_utc,
        timezone('UTC', updated_at) as updated_at_utc,
        row_number() over (partition by id, external_id) as row_num
    from  {{source('caspar', 'exercise') }}

)
SELECT
    id, 
    patient_id,
    generated_minutes,
    completed_at_utc,
    updated_at_utc
FROM 
remove_dups
where row_num = 1
