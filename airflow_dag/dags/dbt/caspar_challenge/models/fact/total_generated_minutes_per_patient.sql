{{ config(materialized='table') }}


with generated_minutes_by_steps as (
    select 
        patient_id,
        round(sum(generated_minutes),2) as total_generated_by_steps
    from {{ref('stg_step')}}
    group by patient_id
),
generated_minutes_by_exercises as (
    select 
        patient_id,
        round(sum(generated_minutes),2) as total_generated_by_exercises
    from {{ref('stg_exercise')}}
    group by patient_id
)
select
    patient.patient_id,
    patient.first_name,
    patient.last_name,
    patient.country,
    round((generated_minutes_by_exercises.total_generated_by_exercises + generated_minutes_by_steps.total_generated_by_steps),0)::int as total_minutes
from {{ref('stg_patient')}} as patient
 left join generated_minutes_by_exercises on patient.patient_id = generated_minutes_by_exercises.patient_id
 left join generated_minutes_by_steps on patient.patient_id = generated_minutes_by_steps.patient_id
