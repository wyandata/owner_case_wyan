WITH gsc_base AS (
    SELECT * FROM {{ ref('int_gsc_export_flattened') }}
),

-- TODO: Create a mapping of regex patterns to domain names. I.e mcdo, mcdee, macdonalds, mcdonalds, bigmac, should all match McDonalds
step_1_regex AS (
    SELECT
        g.*,
        'regex' AS match_type,
        TRUE AS is_branded
    FROM gsc_base g
    WHERE REPLACE(LOWER(g.search_query), ' ', '') LIKE '%' || g.domain_cleaned || '%'
),

-- Saved significant time by using a stepwise fashion from least to most computational intensive
-- TODO: Tune JAROWINKLER_SIMILARITY score more
step_2_fuzzy AS (
    SELECT
        g.*,
        'jarowinkler' AS match_type,
        TRUE AS is_branded
    FROM gsc_base g
    WHERE JAROWINKLER_SIMILARITY(LOWER(g.search_query), g.domain_cleaned) > 80
      AND NOT EXISTS (
          SELECT 1 FROM step_1_regex r
          WHERE r.restaurant_id = g.restaurant_id AND r.search_query = g.search_query
      )
),

-- Can deal with different languages
-- TODO: Tune AI_SIMILARITY score more
step_3_ai AS (
    SELECT
        g.*,
        'ai' AS match_type,
        TRUE AS is_branded
    FROM gsc_base g
    WHERE AI_SIMILARITY(g.search_query, g.domain_cleaned) > 0.60
      AND NOT EXISTS (
          SELECT 1 FROM step_1_regex r
          WHERE r.restaurant_id = g.restaurant_id AND r.search_query = g.search_query
      )
      AND NOT EXISTS (
          SELECT 1 FROM step_2_fuzzy f
          WHERE f.restaurant_id = g.restaurant_id AND f.search_query = g.search_query
      )
),

-- If no matches, then it's unbranded
step_4_unmatched AS (
    SELECT
        g.*,
        NULL AS match_type,
        FALSE AS is_branded
    FROM gsc_base g
    WHERE NOT EXISTS (
        SELECT 1 FROM step_1_regex r
        WHERE r.restaurant_id = g.restaurant_id AND r.search_query = g.search_query
    )
    AND NOT EXISTS (
        SELECT 1 FROM step_2_fuzzy f
        WHERE f.restaurant_id = g.restaurant_id AND f.search_query = g.search_query
    )
    AND NOT EXISTS (
        SELECT 1 FROM step_3_ai a
        WHERE a.restaurant_id = g.restaurant_id AND a.search_query = g.search_query
    )
),

final AS (
SELECT * FROM step_1_regex
UNION ALL
SELECT * FROM step_2_fuzzy
UNION ALL
SELECT * FROM step_3_ai
UNION ALL
SELECT * FROM step_4_unmatched
)

SELECT * FROM final