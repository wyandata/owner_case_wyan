WITH parsed AS (
    SELECT
        restaurant_id,
        domain,
        TRY_PARSE_JSON(data) AS parsed_data
    FROM {{ ref('stg_gsc_export') }} 
    WHERE status = 'success'
),

flattened AS (
    SELECT
        restaurant_id,
        domain,
        query.value
    FROM parsed,
    LATERAL FLATTEN(input => parsed_data:rows) AS query
),

final AS (
    SELECT
        restaurant_id,
        domain,
        -- Extract main domain name
        LOWER(
            REPLACE(
                REPLACE(
                    REPLACE(
                        SPLIT_PART(domain, '.', 1),
                        'www.', ''
                    ),
                    '.', ''
                ),
                '-', ''
            )
        ) AS domain_cleaned,
        value:"keys"[0]::string AS search_query,
        value:clicks::int AS clicks,
        value:impressions::int AS impressions,
        value:ctr::float AS ctr,
        value:position::float AS position
    FROM flattened
)

SELECT * FROM final
