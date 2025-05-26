{{ config(materialized='table') }}


with raw_cat as (

    select distinct category
    from {{ ref('inter_labellevie') }}

    union

    select distinct category
    from {{ ref('inter_biocoop') }}

    union

    select distinct category
    from {{ ref('inter_carrefour') }}

    union

    select distinct category
    from {{ ref('inter_auchan') }}

),

dim_category as (

    select
        row_number() over(order by category) as id_category,
        category
    from raw_cat

)

select * from dim_category
