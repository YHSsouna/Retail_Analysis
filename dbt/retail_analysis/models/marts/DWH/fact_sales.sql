{{ config(materialized='table') }}

with raw_fact as (

    select
        product_id,
        date,
        stock
    from {{ ref('inter_labellevie') }}

    union all

    select
        product_id,
        date,
        stock
    from {{ ref('inter_biocoop') }}

    union all

    select
        product_id,
        date,
        stock
    from {{ ref('inter_carrefour') }}

    union all

    select
        product_id,
        date,
        stock
    from {{ ref('inter_auchan') }}

),

final_fact as (
    select
        row_number() over () as id,
        stock,
        p.product_id,
        d.date_id
    from raw_fact f
    left join {{ ref('dim_product') }} p
        on f.product_id = p.product_id
    left join {{ ref('dim_date') }} d
        on f.date = d.date
)

select * from final_fact
