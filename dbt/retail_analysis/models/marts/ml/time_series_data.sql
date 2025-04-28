with stock_diff as (

    select
        *,
        md5(image_url) as product_id,  -- <--- Generate product_id based on image_url
        stock - lag(stock) over (partition by name order by date) as raw_stock_diff
    from {{ source('public_staging', 'stg_auchan') }}


),

mean_neg_stock_diff as (

    select
        name,
        avg(abs(raw_stock_diff)) as mean_negative_stock_diff
    from stock_diff
    where raw_stock_diff <= 0
    group by name

),

processed as (

    select
        sd.*,
        case
            when sd.raw_stock_diff > 0 then mnsd.mean_negative_stock_diff
            when sd.raw_stock_diff is null then 0 -- optional: handle NULLs
            else abs(sd.raw_stock_diff)
        end as stock_diff_hors_restock
    from stock_diff sd
    left join mean_neg_stock_diff mnsd
        on sd.name = mnsd.name

)

select
    product_id,
    name,
    date,
    stock,
    stock_diff_hors_restock,
    image_url

from processed
