WITH fct_job_ads AS (
    SELECT * FROM {{ ref('fct_job_ads') }}
),

job_details AS (
    SELECT * FROM {{ ref('dim_job_details') }}
),

employer AS (
    SELECT * FROM {{ ref('dim_employer') }}
),

auxiliary AS (
    SELECT * FROM {{ ref('dim_auxiliary') }}
)

SELECT
    jd.headline,                          -- Job headline
    f.vacancies,                          -- Number of vacancies
    jd.salary_type,                       -- Salary type
    f.relevance,                          -- Job relevance
    e.employer_name,                      -- Employer name
    e.workplace_city,                     -- Workplace city
    jd.description,                       -- Job description
    jd.description_html_formatted,        -- HTML formatted description
    jd.duration,                          -- Job duration
    jd.scope_of_work_min,                 -- Minimum scope of work
    jd.scope_of_work_max,                 -- Maximum scope of work
    f.application_deadline,               -- Application deadline
    aux.experience_required,              -- Experience required (from auxiliary dimension)
    aux.driving_license_required          -- Driving license required (from auxiliary dimension)
FROM fct_job_ads AS f
LEFT JOIN job_details AS jd
    ON f.job_details_key = jd.job_details_id
LEFT JOIN employer AS e
    ON f.employer_key = e.employer_id
LEFT JOIN auxiliary AS aux
    ON f.auxiliary_key = aux.auxiliary_id
