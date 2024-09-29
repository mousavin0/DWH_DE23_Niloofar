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
    {{ capitalize_first_letter("coalesce(employer__name, 'Not specified')") }}  AS employer_name,
    {{ capitalize_first_letter("coalesce(employer__workplace, 'Not specified')") }}  AS employer_workplace,
    coalesce(employer__organization_number, 'Not specified') AS employer_organization_number,
    {{ capitalize_first_letter("coalesce(workplace_address__street_address, 'Not specified')") }}  AS workplace_street_address,
    {{ capitalize_first_letter("coalesce(workplace_address__region, 'Not specified')") }} AS workplace_region,
    coalesce(workplace_address__postcode, 'Not specified') AS workplace_postcode,
    {{ capitalize_first_letter("coalesce(workplace_address__city, 'Not specified')") }}  AS workplace_city,
    {{ capitalize_first_letter("coalesce(workplace_address__country, 'Not specified')") }}  AS workplace_country, -- Assuming you have this field in your data
    coalesce(experience_required, 'Not specified') AS experience_required, -- Auxiliary attribute
    coalesce(driving_license_required, 'No') AS driving_license_required -- Auxiliary attribute
FROM stg_job_ads