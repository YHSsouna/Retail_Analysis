with raw_biocoop as (
    select
        row_number() over (order by date) as id,
        date::date as date,
        name,
        CASE
            WHEN stock ~ '^[0-9]+(\.[0-9]+)?$' THEN stock::numeric
            WHEN lower(stock) IN ('in stock', 'en stock', 'disponible') THEN -1
            WHEN stock IS NULL OR trim(stock) = '' THEN 0
            ELSE 0
        END AS stock,

        replace(replace(price,'2150','2.15'),'1975','1.975')::numeric as cleaned_price,

        replace(regexp_replace(price_per_quantity, '[^a-zA-Z\s]+', '', 'g'),'€','') as unit,
        regexp_replace(replace(price_per_quantity,',','.'), '[^0-9\.]+', '', 'g')::numeric AS price_per_quantity,
        CASE
            WHEN NULLIF(
                regexp_replace(replace(price_per_quantity, ',', '.'), '[^0-9\.]+', '', 'g'),
                ''
            )::numeric IS NULL
            THEN NULL
            ELSE ROUND(
                replace(replace(price,'2150','2.15'),'1975','1.975')::numeric
                / regexp_replace(replace(price_per_quantity, ',', '.'), '[^0-9\.]+', '', 'g')::numeric,
                3
            )
        END AS quantity,



        store,
        img as image_url



    from {{ source('public', 'biocoop') }}
),

raw_category as (
    select
        name,
        category
    from {{ source('public', 'biocoop_categories') }}
)



select
    id,
    l.name,
    cleaned_price as price,
    stock,
    price_per_quantity,
    quantity,
    unit,
    c.category,
    date,
    store,
    image_url
from raw_biocoop as l
join raw_category as c
on c.name = l.name
