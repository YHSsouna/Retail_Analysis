{{ config(materialized='table') }}


with raw_picture as (

    select distinct image_url as picture
    from {{ ref('inter_labellevie') }}

    union

    select distinct image_url as picture
    from {{ ref('inter_biocoop') }}

    union

    select distinct image_url as picture
    from {{ ref('inter_carrefour') }}

    union

    select distinct image_url as picture
    from {{ ref('inter_auchan') }}

),


dim_picture as (

    select
        row_number() over (order by picture) as id_picture,
        picture
    from raw_picture

)

select * from dim_picture
