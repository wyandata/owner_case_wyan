{{
  config(
    materialized='snapshot',
    unique_key='restaurant_cuisine_sk',
    strategy='timestamp',
    updated_at='updated_at'
  )
}}

-- DEMONSTRATION: SCD Type 2 Implementation 
-- Tracks restaurant cuisine changes over time using dbt snapshots
-- Enables historical analysis of how cuisine changes impact search performance

WITH source AS (
    SELECT
        restaurant_id,
        cuisine,
        CURRENT_TIMESTAMP() AS updated_at
    FROM {{ ref('int_restaurant_cuisines') }}
)

SELECT
    -- Create a unique key for each restaurant-cuisine combo
    {{ dbt_utils.generate_surrogate_key(['restaurant_id', 'cuisine']) }} AS restaurant_cuisine_sk,
    restaurant_id,
    cuisine,
    updated_at
FROM source

-- Example of what will happen:
-- Initial snapshot: Restaurant A has ["Italian", "Pizza"]
-- - restaurant_cuisine_sk: abc123 (Italian), is_current: TRUE, valid_from: 2024-01-01, valid_to: NULL
-- - restaurant_cuisine_sk: def456 (Pizza), is_current: TRUE, valid_from: 2024-01-01, valid_to: NULL
--
-- After Restaurant A changes to ["Italian", "Pizza", "Mediterranean"]:
-- - restaurant_cuisine_sk: abc123 (Italian), is_current: FALSE, valid_from: 2024-01-01, valid_to: 2024-03-15
-- - restaurant_cuisine_sk: def456 (Pizza), is_current: FALSE, valid_from: 2024-01-01, valid_to: 2024-03-15
-- - restaurant_cuisine_sk: ghi789 (Italian), is_current: TRUE, valid_from: 2024-03-15, valid_to: NULL
-- - restaurant_cuisine_sk: jkl012 (Pizza), is_current: TRUE, valid_from: 2024-03-15, valid_to: NULL
-- - restaurant_cuisine_sk: mno345 (Mediterranean), is_current: TRUE, valid_from: 2024-03-15, valid_to: NULL
-- 
-- In fct_cuisine_branding_metrics, we can join GSC data with the cuisine
-- that was actually active when the search happened, not just the current one

