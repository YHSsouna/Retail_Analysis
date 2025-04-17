

with raw_auchan as (
    select
        row_number() over (order by date) as id,
        date::date as date,
        name,
        quantity_stock::numeric as stock,
        replace(replace(replace(replace(price,'€',''),' ',''),',','.'),'Àpartirde','0')::numeric as cleaned_price,

        replace(replace(regexp_replace(price_per_quantity, '[^a-zA-Z\s]+', '', 'g'),'€',''),'x','') as unit,
        replace(replace(regexp_replace(quantity, '[^a-zA-Z\s]+', '', 'g'),'€',''),'x','') as ss,
        regexp_replace(quantity, '[^0-9\.]+', '', 'g') AS quantity,
        regexp_replace(replace(price_per_quantity,',','.'), '[^0-9\.]+', '', 'g') AS price_per_quantity,

        marque,



        store,
        image_url



    from {{ source('public', 'auchan') }}
),

raw_category as (
    select
        name,
        mapped_category as category
    from {{ source('public', 'auchan_categories') }}
)



select
    id,
    l.name,
    cleaned_price as price,
    stock,
    quantity,
    price_per_quantity,
    unit,
    c.category,
    date,
    marque,
    store,
    image_url
from raw_auchan as l
join raw_category as c
on c.name = l.name