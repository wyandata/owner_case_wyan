with source as (
    select * from {{ source('fivetran_data_outputs', 'hex_case_gsc_export') }}
),

renamed as (
    select
        data,
        domain,
        restaurant_id,
        status,
        _created_at,
        _updated_at
    from source
)

select * from renamed