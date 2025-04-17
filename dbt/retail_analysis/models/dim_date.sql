with raw_dates as (

    select distinct date_trunc('day', date)::date as date
    from {{ source('public', 'labellevie') }}

    union

    select distinct date::date as date
    from {{ source('public', 'biocoop') }}

    union

    select distinct date::date as date
    from {{ source('public', 'carrefour') }}

    union

    select distinct date::date as date
    from {{ source('public', 'auchan') }}

),

dim_date as (

    select
        row_number() over (order by date) as date_id,
        extract(day from date) as day,
        extract(month from date) as month,
        extract(year from date) as year
    from raw_dates

)

select * from dim_date
