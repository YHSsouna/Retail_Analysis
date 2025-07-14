
  
    

  create  table "airflow"."public_marts"."fact_sales__dbt_tmp"
  
  
    as
  
  (
    

with inter_labellevie_sales as (
    

with base as (
    select
        product_id as product_id,
        image_url as image_url,
        date as date,
        price as price,
        stock as stock,
        quantity as quantity,
        price_per_quantity as price_per_quantity,
        unit as unit,
        lag(stock) over (partition by image_url order by date) as prev_stock
    from "airflow"."public_intermediate"."inter_labellevie"
),

diffs as (
    select
        *,
        stock - prev_stock as raw_stock_diff
    from base
),

mean_neg as (
    select
        image_url,
        avg(abs(raw_stock_diff)) as mean_negative_stock_diff
    from diffs
    where raw_stock_diff <= 0
    group by image_url
),

final as (
    select
        product_id,
        date,
        price,
        stock,
        quantity,
        price_per_quantity,
        unit,
        
    case
        when raw_stock_diff > 0 then mean_negative_stock_diff
        when raw_stock_diff is null then 0
        else abs(raw_stock_diff)
    end
 as sales
    from diffs d
    left join mean_neg m on d.image_url = m.image_url
)

select * from final


),

inter_biocoop_sales as (
    

with base as (
    select
        product_id as product_id,
        image_url as image_url,
        date as date,
        price as price,
        stock as stock,
        quantity as quantity,
        price_per_quantity as price_per_quantity,
        unit as unit,
        lag(stock) over (partition by image_url order by date) as prev_stock
    from "airflow"."public_intermediate"."inter_biocoop"
),

diffs as (
    select
        *,
        stock - prev_stock as raw_stock_diff
    from base
),

mean_neg as (
    select
        image_url,
        avg(abs(raw_stock_diff)) as mean_negative_stock_diff
    from diffs
    where raw_stock_diff <= 0
    group by image_url
),

final as (
    select
        product_id,
        date,
        price,
        stock,
        quantity,
        price_per_quantity,
        unit,
        
    case
        when raw_stock_diff > 0 then mean_negative_stock_diff
        when raw_stock_diff is null then 0
        else abs(raw_stock_diff)
    end
 as sales
    from diffs d
    left join mean_neg m on d.image_url = m.image_url
)

select * from final


),

inter_carrefour_sales as (
    

with base as (
    select
        product_id as product_id,
        image_url as image_url,
        date as date,
        price as price,
        stock as stock,
        quantity as quantity,
        price_per_quantity as price_per_quantity,
        unit as unit,
        lag(stock) over (partition by image_url order by date) as prev_stock
    from "airflow"."public_intermediate"."inter_carrefour"
),

diffs as (
    select
        *,
        stock - prev_stock as raw_stock_diff
    from base
),

mean_neg as (
    select
        image_url,
        avg(abs(raw_stock_diff)) as mean_negative_stock_diff
    from diffs
    where raw_stock_diff <= 0
    group by image_url
),

final as (
    select
        product_id,
        date,
        price,
        stock,
        quantity,
        price_per_quantity,
        unit,
        
    case
        when raw_stock_diff > 0 then mean_negative_stock_diff
        when raw_stock_diff is null then 0
        else abs(raw_stock_diff)
    end
 as sales
    from diffs d
    left join mean_neg m on d.image_url = m.image_url
)

select * from final


),

inter_auchan_sales as (
    

with base as (
    select
        product_id as product_id,
        image_url as image_url,
        date as date,
        price as price,
        stock as stock,
        quantity as quantity,
        price_per_quantity as price_per_quantity,
        unit as unit,
        lag(stock) over (partition by image_url order by date) as prev_stock
    from "airflow"."public_intermediate"."inter_auchan"
),

diffs as (
    select
        *,
        stock - prev_stock as raw_stock_diff
    from base
),

mean_neg as (
    select
        image_url,
        avg(abs(raw_stock_diff)) as mean_negative_stock_diff
    from diffs
    where raw_stock_diff <= 0
    group by image_url
),

final as (
    select
        product_id,
        date,
        price,
        stock,
        quantity,
        price_per_quantity,
        unit,
        
    case
        when raw_stock_diff > 0 then mean_negative_stock_diff
        when raw_stock_diff is null then 0
        else abs(raw_stock_diff)
    end
 as sales
    from diffs d
    left join mean_neg m on d.image_url = m.image_url
)

select * from final


),

-- Combine all the results
raw_union as (
    select * from inter_labellevie_sales
    union all
    select * from inter_biocoop_sales
    union all
    select * from inter_carrefour_sales
    union all
    select * from inter_auchan_sales
),

-- Deduplicate within raw_union before joining
raw_fact as (
    select *
    from (
        select *,
            row_number() over (
                partition by product_id, date
                order by stock desc nulls last
            ) as rn
        from raw_union
    ) sub
    where rn = 1
),

-- Final join with dimensions
final_fact as (
    select
        f.stock,
        case
            when sales between 9850 and 10000 then 1
            else sales
        end as sales,
        f.price,
        f.quantity,
        f.price_per_quantity,
        f.unit,
        p.product_id,
        d.date_id
    from raw_fact f
    left join "airflow"."public_marts"."dim_product" p
        on f.product_id = p.product_id
    left join "airflow"."public_marts"."dim_date" d
        on f.date = d.date
),

-- Remove duplicates based on product_id + date_id after join
deduplicated as (
    select *
    from (
        select *,
            row_number() over (
                partition by product_id, date_id
                order by stock desc nulls last
            ) as rn
        from final_fact
    ) sub
    where rn = 1
)

-- Final output with ID
select
    row_number() over (order by product_id, date_id) as id,
    stock,
    sales,
    price,
    quantity,
    price_per_quantity,
    unit,
    product_id,
    date_id
from deduplicated
  );
  