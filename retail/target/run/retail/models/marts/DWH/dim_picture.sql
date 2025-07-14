
  
    

  create  table "airflow"."public_marts"."dim_picture__dbt_tmp"
  
  
    as
  
  (
    


with raw_picture as (

    select distinct image_url as picture
    from "airflow"."public_intermediate"."inter_labellevie"

    union

    select distinct image_url as picture
    from "airflow"."public_intermediate"."inter_biocoop"

    union

    select distinct image_url as picture
    from "airflow"."public_intermediate"."inter_carrefour"

    union

    select distinct image_url as picture
    from "airflow"."public_intermediate"."inter_auchan"

),


dim_picture as (

    select
        row_number() over (order by picture) as id_picture,
        picture
    from raw_picture

)

select * from dim_picture
  );
  