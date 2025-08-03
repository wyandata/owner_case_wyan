with source as (
    select * from {{ source('fivetran_data_outputs', 'hex_brand_cuisine_export') }}
),

renamed as (
    select
        cuisines,
        restaurant_id
    from source
)

select * from renamed