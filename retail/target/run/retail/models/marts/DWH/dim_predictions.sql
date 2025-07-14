
  
    

  create  table "airflow"."public_marts"."dim_predictions__dbt_tmp"
  
  
    as
  
  (
    

with raw_pred as (
    select
        replace(product_id, 'df_f', '')::BIGINT as product_id,
        ds,
        predicted_y
    from "airflow"."public_intermediate"."predicted_stock_diff"
),

raw_prediction as (
    select
        row_number() OVER () AS id_prediction,
        product_id,
        d.date_id,
        predicted_y
    from raw_pred p
    left join "airflow"."public_marts"."dim_date" d
        on p.ds = d.date
)

select * from raw_prediction
  );
  