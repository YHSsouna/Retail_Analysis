
  
    

  create  table "airflow"."public_marts"."dim_store__dbt_tmp"
  
  
    as
  
  (
    

with raw_store as (

    select distinct store from "airflow"."public_intermediate"."inter_labellevie"
    union
    select distinct store from "airflow"."public_intermediate"."inter_biocoop"
    union
    select distinct store from "airflow"."public_intermediate"."inter_carrefour"
    union
    select distinct store from "airflow"."public_intermediate"."inter_auchan"

),

raw_category as (
    select distinct on (store)
        "Store" as store,
        image_str
    from "airflow"."public"."Store_img"
),

dim_store as (

    select
        row_number() over (order by rs.store) as id_store,
        rs.store,
        rc.image_str
    from raw_store rs
    left join raw_category rc
        on rs.store = rc.store

)

select * from dim_store
  );
  