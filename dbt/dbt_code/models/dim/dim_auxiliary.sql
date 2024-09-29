WITH src_auxiliary AS (
    SELECT * FROM {{ ref('src_auxiliary') }}
)

SELECT distinct
    {{ dbt_utils.generate_surrogate_key(['experience_required', 'driving_license_required']) }} AS auxiliary_id, -- Generate surrogate key
    experience_required,
    driving_license_required
FROM src_auxiliary
