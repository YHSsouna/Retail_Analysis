
  create view "airflow"."public_marts"."time_series_data__dbt_tmp"
    
    
  as (
    with stock_diff as (
    select
        *,
        stock - lag(stock) over (partition by image_url, name order by date) as raw_stock_diff
    from "airflow"."public_intermediate"."inter_auchan"
),

mean_neg_stock_diff as (
    select
        image_url,
        name,
        avg(abs(raw_stock_diff)) as mean_negative_stock_diff
    from stock_diff
    where raw_stock_diff <= 0
    group by image_url, name
),

processed as (
    select
        sd.*,
        case
            when sd.raw_stock_diff > 0 then mnsd.mean_negative_stock_diff
            when sd.raw_stock_diff is null then 0
            else abs(sd.raw_stock_diff)
        end as stock_diff_hors_restock
    from stock_diff sd
    left join mean_neg_stock_diff mnsd
        on sd.image_url = mnsd.image_url and sd.name = mnsd.name
)

select
    product_id,
    name,
    date,
    stock,
    stock_diff_hors_restock,
    image_url
from processed
  );