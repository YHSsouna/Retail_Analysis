
  
    

  create  table "airflow"."public_marts"."dim_date__dbt_tmp"
  
  
    as
  
  (
    


with raw_dates as (

    select distinct date::date as date
    from "airflow"."public_intermediate"."inter_labellevie"

    union

    select distinct date::date as date
    from "airflow"."public_intermediate"."inter_biocoop"

    union

    select distinct date::date as date
    from "airflow"."public_intermediate"."inter_carrefour"

    union

    select distinct date::date as date
    from "airflow"."public_intermediate"."inter_auchan"

    union

    select distinct ds::date as date
    from "airflow"."public_intermediate"."predicted_stock_diff"
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
  );
  