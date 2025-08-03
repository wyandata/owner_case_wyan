WITH restaurant_metrics AS (
    SELECT * FROM {{ ref('fct_gsc_restaurant_branding_metrics') }}
),

cuisine_flat AS (
    SELECT * FROM {{ ref('int_restaurant_cuisines') }}
),

-- Join restaurant metrics with cuisine data to get cuisine-level performance
joined AS (
  SELECT
    r.restaurant_id,
    c.cuisine,
    r.is_branded,
    r.total_clicks,
    r.total_impressions,
    r.query_count,
    r.avg_ctr,
    r.avg_position
  FROM restaurant_metrics r
  INNER JOIN cuisine_flat c ON r.restaurant_id = c.restaurant_id
),

-- Aggregate metrics per cuisine and branded flag
cuisine_agg AS (
  SELECT
    cuisine,
    is_branded,
    SUM(total_clicks) AS total_clicks,
    SUM(total_impressions) AS total_impressions,
    SUM(query_count) AS query_count,
    -- Weighted average CTR by impressions - queries with more impressions have more influence
    -- This is more accurate than simple average since high-impression queries represent more real traffic
    ROUND(CASE WHEN SUM(total_impressions) > 0 THEN SUM(avg_ctr * total_impressions) / SUM(total_impressions) ELSE 0 END, 4) AS avg_ctr,
    -- Weighted average position by impressions - same logic as CTR
    -- Lower position = better ranking, so this shows actual search performance for this cuisine
    ROUND(CASE WHEN SUM(total_impressions) > 0 THEN SUM(avg_position * total_impressions) / SUM(total_impressions) ELSE NULL END, 2) AS avg_position
  FROM joined
  GROUP BY cuisine, is_branded
),

-- Calculate total clicks/impressions/queries per cuisine (all branded + unbranded combined)
cuisine_totals AS (
  SELECT
    cuisine,
    SUM(total_clicks) AS cuisine_total_clicks,
    SUM(total_impressions) AS cuisine_total_impressions,
    SUM(query_count) AS cuisine_total_queries
  FROM cuisine_agg
  GROUP BY cuisine
),

-- Calculate % contribution of branded/unbranded to totals per cuisine
-- This shows the distribution of search performance between branded vs unbranded at the cuisine level
-- Key business insight: which cuisines rely more on brand recognition vs organic discovery
final AS (
SELECT
  c.*,
  t.cuisine_total_clicks,
  t.cuisine_total_impressions,
  t.cuisine_total_queries,
  -- % of total clicks from this search type - shows conversion effectiveness for this cuisine
  ROUND(CASE WHEN t.cuisine_total_clicks > 0 THEN c.total_clicks * 1.0 / t.cuisine_total_clicks ELSE 0 END, 4) AS pct_clicks,
  -- % of total impressions from this search type - shows visibility distribution for this cuisine
  ROUND(CASE WHEN t.cuisine_total_impressions > 0 THEN c.total_impressions * 1.0 / t.cuisine_total_impressions ELSE 0 END, 4) AS pct_impressions,
  -- % of total queries from this search type - shows search volume distribution for this cuisine
  ROUND(CASE WHEN t.cuisine_total_queries > 0 THEN c.query_count * 1.0 / t.cuisine_total_queries ELSE 0 END, 4) AS pct_queries
FROM cuisine_agg c
INNER JOIN cuisine_totals t ON c.cuisine = t.cuisine
)

SELECT * FROM final