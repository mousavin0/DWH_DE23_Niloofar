{{
  config(
    materialized = 'ephemeral',
  )
}}

WITH stg_job_ads AS (
    SELECT * FROM {{ source('job_ads', 'stg_data_ads') }}
)

SELECT
    id,
    {{ capitalize_first_letter("coalesce(headline, 'Not specified')") }} AS headline,
    coalesce(description__text, 'No description provided.') AS description,
    coalesce(description__text_formatted, 'No description provided.') AS description_html_formatted,
    {{ capitalize_first_letter("coalesce(employment_type__label, 'Not specified')") }} AS employment_type,
    {{ capitalize_first_letter("coalesce(duration__label, 'Not specified')") }} AS duration,
    {{ capitalize_first_letter("coalesce(salary_type__label, 'Not specified')") }} AS salary_type,
    coalesce(scope_of_work__min, 0) AS scope_of_work_min,
    coalesce(scope_of_work__max, 0) AS scope_of_work_max
FROM stg_job_ads