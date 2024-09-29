

WITH ja AS (
    SELECT * FROM {{ ref('src_job_ads') }}
),

jd AS (
    SELECT * FROM {{ ref('src_job_details') }}
),

e AS (
    SELECT * FROM {{ ref('src_employer') }}
),

aux AS (
    SELECT * FROM {{ ref('src_auxiliary') }} -- Reference the auxiliary dimension
)

SELECT 
    ja.id AS job_id,
    {{ dbt_utils.generate_surrogate_key(['jd.id', 'jd.headline']) }} AS job_details_key, -- Key for job details
    {{ dbt_utils.generate_surrogate_key(['e.id', 'e.employer_name']) }} AS employer_key, -- Key for employer
    {{ dbt_utils.generate_surrogate_key(['aux.experience_required', 'aux.driving_license_required']) }} AS auxiliary_key, -- Key for auxiliary attributes
    coalesce(ja.vacancies, 1) AS vacancies,
    coalesce(ja.relevance, 0) AS relevance,
    coalesce(ja.application_deadline, current_date) AS application_deadline -- Default to current date if null
FROM 
    ja 
LEFT JOIN 
    jd ON ja.id = jd.id
LEFT JOIN 
    e ON ja.id = e.id -- Ensure to join by the correct foreign key
LEFT JOIN 
    aux ON ja.id = aux.id -- Join auxiliary table
