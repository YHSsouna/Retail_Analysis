{{ config(materialized='table') }}

with inter_labellevie_sales as (
    {{ sales('inter_labellevie') }}
),

inter_biocoop_sales as (
    {{ sales('inter_biocoop') }}
),

inter_carrefour_sales as (
    {{ sales('inter_carrefour') }}
),

inter_auchan_sales as (
    {{ sales('inter_auchan') }}
),

-- Combine all the results
raw_union as (
    select * from inter_labellevie_sales
    union all
    select * from inter_biocoop_sales
    union all
    select * from inter_carrefour_sales
    union all
    select * from inter_auchan_sales
),

-- Deduplicate within raw_union before joining
raw_fact as (
    select *
    from (
        select *,
            row_number() over (
                partition by product_id, date
                order by stock desc nulls last
            ) as rn
        from raw_union
    ) sub
    where rn = 1
),

-- Final join with dimensions
final_fact as (
    select
        f.stock,
        f.sales,
        f.quantity,
        f.price_per_quantity,
        f.unit,
        p.product_id,
        d.date_id
    from raw_fact f
    left join {{ ref('dim_product') }} p
        on f.product_id = p.product_id
    left join {{ ref('dim_date') }} d
        on f.date = d.date
),

-- Remove duplicates based on product_id + date_id after join
deduplicated as (
    select *
    from (
        select *,
            row_number() over (
                partition by product_id, date_id
                order by stock desc nulls last
            ) as rn
        from final_fact
    ) sub
    where rn = 1
)

-- Final output with ID
select
    row_number() over (order by product_id, date_id) as id,
    stock,
    sales,
    quantity,
    price_per_quantity,
    unit,
    product_id,
    date_id
from deduplicated
