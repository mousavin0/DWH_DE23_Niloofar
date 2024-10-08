
WITH src_employer AS (
    SELECT * FROM {{ ref('src_employer') }}
)

SELECT distinct
    {{ dbt_utils.generate_surrogate_key(['id', 'employer_name']) }} AS employer_id,
    employer_name,
    employer_workplace ,
    employer_organization_number ,
    workplace_street_address ,
    workplace_region ,
    workplace_postcode, 
    workplace_city,
    workplace_country 
FROM src_employer