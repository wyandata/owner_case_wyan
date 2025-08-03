WITH source AS (
    SELECT
        restaurant_id,
        TRY_PARSE_JSON(cuisines) AS cuisine_array
    FROM {{ ref('stg_restaurants') }}
),

cuisine_flattened AS (
    SELECT
        restaurant_id,
        value::string AS cuisine
    FROM source,
    LATERAL FLATTEN(input => cuisine_array)
),

has_cuisine AS (
    SELECT DISTINCT restaurant_id FROM cuisine_flattened
),

-- Only a small handful of restaurants don't have a cuisine when joined with gsc data, let's just call them unknown for now
no_cuisine AS (
    SELECT
        restaurant_id,
        'unknown' AS cuisine
    FROM source
    WHERE restaurant_id NOT IN (SELECT restaurant_id FROM has_cuisine)
),

final AS (
    SELECT * FROM cuisine_flattened
    UNION ALL
    SELECT * FROM no_cuisine
)

SELECT * FROM final