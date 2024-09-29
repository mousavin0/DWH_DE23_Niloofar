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
    coalesce(experience_required, 'Not specified') AS experience_required, -- Fill nulls
    coalesce(driving_license_required, 'Not specified') AS driving_license_required -- Fill nulls
FROM stg_job_ads
