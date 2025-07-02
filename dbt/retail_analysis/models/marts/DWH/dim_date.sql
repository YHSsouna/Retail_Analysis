{{ config(materialized='table') }}


with raw_dates as (

    select distinct date::date as date
    from {{ ref('inter_labellevie') }}

    union

    select distinct date::date as date
    from {{ ref('inter_biocoop') }}

    union

    select distinct date::date as date
    from {{ ref('inter_carrefour') }}

    union

    select distinct date::date as date
    from {{ ref('inter_auchan') }}

),

dim_date as (

    select
        row_number() over (order by date) as date_id,
        extract(day from date) as day,
        extract(month from date) as month,
        extract(year from date) as year,
        date
    from raw_dates

)

select * from dim_date
