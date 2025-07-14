{{ config(materialized='table') }}

with raw_pred as (
    select
        replace(product_id, 'df_f', '')::BIGINT as product_id,
        ds,
        predicted_y
    from {{ source('public_intermediate', 'predicted_stock_diff') }}
),

raw_prediction as (

    select
        row_number() OVER () AS id_prediction,
        p.product_id,
        d.date_id,
        predicted_y
    from raw_pred p
    left join {{ ref('dim_date') }} d
        on p.ds = d.date
    left join {{ ref('dim_product') }} dp
        on p.product_id = dp.product_id
)

select * from raw_prediction
