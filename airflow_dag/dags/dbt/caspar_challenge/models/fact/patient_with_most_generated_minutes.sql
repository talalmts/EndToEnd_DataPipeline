{{ config(materialized='table') }}


WITH rank_generated_minutes AS (
    SELECT 
        patient_id, 
        first_name, 
        last_name,
        country,
        total_minutes,
        DENSE_RANK() OVER (ORDER BY total_minutes DESC) AS rank
    FROM 
         {{ref('total_generated_minutes_per_patient')}}
)
SELECT 
    patient_id, 
    first_name, 
    last_name,
    country,
    total_minutes
FROM 
    rank_generated_minutes
WHERE 
    rank = 1
