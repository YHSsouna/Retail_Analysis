{{ config(materialized='table') }}

with all_products as (
    select distinct
        product_id,
        name,
        quantity,
        unit,
        NULL as marque,
        price_per_quantity,
        category,
        image_url as picture,
        store
    from {{ ref('inter_labellevie') }}

    union all

    select distinct
        product_id,
        name,
        quantity,
        unit,
        NULL as marque,
        price_per_quantity,
        category,
        image_url as picture,
        store
    from {{ ref('inter_biocoop') }}

    union all

    select distinct
        product_id,
        name,
        quantity,
        unit,
        NULL as marque,
        price_per_quantity,
        category,
        image_url as picture,
        store
    from {{ ref('inter_carrefour') }}

    union all

    select distinct
        product_id,
        name,
        quantity,
        unit,
        marque,
        price_per_quantity,
        category,
        image_url as picture,
        store
    from {{ ref('inter_auchan') }}
),

final_product as (
    select
        p.product_id,
        p.name,
        p.quantity,
        p.unit,
        p.marque,
        p.price_per_quantity,
        c.id_category,
        i.id_picture,
        s.id_store
    from all_products p
    left join {{ ref('dim_category') }} c
        on p.category = c.category
    left join {{ ref('dim_picture') }} i
        on p.picture = i.picture
    left join {{ ref('dim_store') }} s
        on p.store = s.store
)

select * from final_product
