

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
),

raw_norm as (
    select
        name,
        unit,
        CASE
            WHEN quantity::numeric = 0 THEN NULL
            ELSE quantity::numeric
        END AS quantity
    from {{ source('public', 'auchan_norm') }}
)



select
    id,
    l.name,
    cleaned_price as price,
    stock,
    n.quantity,
    n.unit,
    CASE
        WHEN n.quantity::numeric IS NULL THEN NULL
        ELSE cleaned_price::numeric / n.quantity::numeric
    END AS price_per_quantity,
    c.category,
    date,
    marque,
    store,
    image_url
from raw_auchan as l
join raw_category as c
on c.name = l.name
join raw_norm as n
on n.name = l.name