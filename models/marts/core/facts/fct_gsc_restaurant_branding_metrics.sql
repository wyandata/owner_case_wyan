WITH gsc_classified AS (
  SELECT * FROM {{ ref('int_gsc_classified') }}
),

-- Aggregate metrics per restaurant and branded flag
aggregated AS (
  SELECT
    restaurant_id,
    is_branded,
    SUM(clicks) AS total_clicks,
    SUM(impressions) AS total_impressions,
    COUNT(*) AS query_count,
    -- Weighted average CTR by impressions - queries with more impressions have more influence
    -- This is more accurate than simple average since high-impression queries represent more real traffic
    ROUND(CASE WHEN SUM(impressions) > 0 THEN SUM(ctr * impressions) / SUM(impressions) ELSE 0 END, 4) AS avg_ctr,
    -- Weighted average position by impressions - same logic as CTR
    -- Lower position = better ranking, so this shows actual search performance
    ROUND(CASE WHEN SUM(impressions) > 0 THEN SUM(position * impressions) / SUM(impressions) ELSE NULL END, 2) AS avg_position
  FROM gsc_classified
  GROUP BY restaurant_id, is_branded
),

-- This gives us the baseline for calculating percentages
totals AS (
  SELECT
    restaurant_id,
    SUM(total_clicks) AS restaurant_total_clicks,
    SUM(total_impressions) AS restaurant_total_impressions,
    SUM(query_count) AS restaurant_total_queries
  FROM aggregated
  GROUP BY restaurant_id
),

-- Calculate % contribution of branded/unbranded to totals per restaurant
-- This shows the distribution of search performance between branded vs unbranded
-- Key business insight: how much of a restaurant's success comes from brand recognition vs organic discovery
final AS (
  SELECT
    a.*,
    t.restaurant_total_clicks,
    t.restaurant_total_impressions,
    t.restaurant_total_queries,
    -- % of total clicks from this search type - shows conversion effectiveness
    ROUND(CASE WHEN t.restaurant_total_clicks > 0 THEN a.total_clicks * 1.0 / t.restaurant_total_clicks ELSE 0 END, 4) AS pct_clicks,
    -- % of total impressions from this search type - shows visibility distribution
    ROUND(CASE WHEN t.restaurant_total_impressions > 0 THEN a.total_impressions * 1.0 / t.restaurant_total_impressions ELSE 0 END, 4) AS pct_impressions,
    -- % of total queries from this search type - shows search volume distribution
    ROUND(CASE WHEN t.restaurant_total_queries > 0 THEN a.query_count * 1.0 / t.restaurant_total_queries ELSE 0 END, 4) AS pct_queries
  FROM aggregated a
  INNER JOIN totals t ON a.restaurant_id = t.restaurant_id
)

SELECT * FROM final