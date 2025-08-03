# Restaurant SEO Analysis

## Problem Statement
Owner optimizes SEO on behalf of restaurants to increase their sales volume by driving traffic with intent to order meals. The challenge is balancing two key search segments:

- **Branded searches** (e.g., "McDonald's near me"): Users already know they want to order from a specific restaurant. These are table-stakes - restaurants must show up properly, but there's limited upside for driving additional traffic.

- **Unbranded searches** (e.g., "pizza near me"): Users have high intent to order but haven't chosen a specific restaurant. This is where the real growth opportunity lies - helping restaurants rank higher in unbranded searches to generate new sales they wouldn't have otherwise.

**Key Business Question**: How do we track progress and identify improvement opportunities in unbranded searches to help restaurants acquire new customers through Google search?

## SCD Methodology for Restaurant Cuisine Changes

To handle restaurants that change their cuisine types over time, I implemented a **SCD Type 2 strategy** using dbt snapshots. This approach:

1. **Tracks cuisine changes over time** using dbt's snapshot functionality
2. **Preserves historical records** while creating new records for changes
3. **Enables temporal analysis** to see how cuisine changes impact search performance
4. **Ensures data integrity** by joining historical GSC data with the correct cuisine that was active at search time

**Implementation**: See `snapshots/snap_restaurant_cuisine.sql` for the SCD Type 2 demonstration using dbt snapshots.

## What this does
This analysis answers the core business question by classifying Google Search Console data into branded vs unbranded searches and measuring performance across both segments. It helps restaurant marketing teams understand:
- Which restaurants rely more on brand recognition vs organic discovery
- How well restaurants perform in unbranded searches (the growth opportunity)
- Where there's room for improvement in unbranded search rankings
- The balance between protecting branded search performance and growing unbranded search traffic

## How it works

### Data Sources
- **GSC Export**: 3-day aggregated Google Search Console data with search queries, clicks, impressions, CTR, and position
- **Restaurant Data**: Restaurant information including cuisine classifications

### Data Processing Pipeline
1. **Staging**: Clean raw data from Fivetran sources
2. **Intermediate**: 
   - `int_gsc_export_flattened`: Flatten JSON GSC data into tabular format
   - `int_gsc_classified`: Classify queries as branded/unbranded using cascading approach
   - `int_restaurant_cuisines`: Flatten restaurant cuisine arrays, handle missing cuisines as "unknown"
3. **Marts**: Final aggregated metrics tables

### Query Classification
We classify each search as branded/unbranded using:
1. **Regex matching** (fastest): Exact domain name matching
2. **Fuzzy matching**: Jaro-Winkler similarity > 80%
3. **AI similarity**: Semantic similarity > 60% for different languages
4. **Everything else** = unbranded

### Metrics Calculation
- **Clicks, impressions**: Summed by restaurant/cuisine + search type
- **CTR, position**: Weighted averages by impressions (more accurate than simple averages)
- **Percentages**: Distribution of clicks/impressions/queries between branded vs unbranded
- **SCD Type 2**: Cuisine dimension tracks changes over time for historical analysis
- **Multi-language support**: AI similarity handles different languages and variations

## Project Structure
```
snapshots/
└── snap_restaurant_cuisine.sql  # SCD Type 2 snapshot for cuisine changes
models/
├── source/           # Source table definitions
├── staging/          # Clean raw data
│   ├── stg_gsc_export.sql
│   └── stg_restaurants.sql
├── int/              # Intermediate transformations
│   ├── gsc/
│   │   ├── int_gsc_export_flattened.sql
│   │   └── int_gsc_classified.sql
│   └── restaurants/
│       └── int_restaurant_cuisines.sql
└── marts/            # Final metrics tables
    └── core/
        └── facts/
            ├── fct_gsc_restaurant_branding_metrics.sql
            └── fct_cuisine_branding_metrics.sql
```

## Key Models

### Intermediate Models
- **`int_gsc_export_flattened`**: Flattens JSON GSC data into tabular format with restaurant_id, domain, search_query, clicks, impressions, CTR, position
- **`int_gsc_classified`**: Classifies each query as branded/unbranded using multi-layered matching approach
- **`int_restaurant_cuisines`**: Flattens cuisine arrays and handles restaurants without cuisine data

### Final Models
- **`fct_gsc_restaurant_branding_metrics`**: Restaurant-level performance by branded/unbranded queries
- **`fct_cuisine_branding_metrics`**: Cuisine-level performance by branded/unbranded queries
- **`snap_restaurant_cuisine`**: SCD Type 2 snapshot tracking cuisine changes over time

## What you get
- **Restaurant-level insights**: Which restaurants rely more on brand recognition vs organic discovery
- **Cuisine-level insights**: Which cuisines perform better in branded vs unbranded searches
- **Historical analysis**: Track performance changes as restaurants update their cuisine classifications
- **Distribution analysis**: Understand the balance between branded and unbranded search performance

## Limitations
- 3-day data period (GSC aggregation window)
- Some restaurants marked as "unknown" cuisine when data is missing
- Lack of restaurant dimension with restaurant aliases or nicknames to further distinguish branded vs unbranded searches