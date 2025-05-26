{{ config(materialized='table') }}


with raw_store as (

    select distinct store
    from {{ ref('inter_labellevie') }}

    union

    select distinct store
    from {{ ref('inter_biocoop') }}

    union

    select distinct store
    from {{ ref('inter_carrefour') }}

    union

    select distinct store
    from {{ ref('inter_auchan') }}

),


dim_store as (

    select
        row_number() over (order by store) as id_store,
        store
    from raw_store

)

select * from dim_store
