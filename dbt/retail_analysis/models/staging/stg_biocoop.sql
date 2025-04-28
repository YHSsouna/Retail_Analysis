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

        replace(replace(regexp_replace(price_per_quantity, '[^a-zA-Z\s]+', '', 'g'),'€',''),' ','') as unit,
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
    select distinct on (name)
        name,
        category
    from {{ source('public', 'biocoop_categories') }}
    order by name, category
)



select
    id,
    l.name,
    cleaned_price as price,
    stock,
    CASE
        WHEN l.unit = 'kg' THEN l.quantity * 1000
        ELSE l.quantity
    END AS quantity,
    replace(l.unit,'kg','g') as unit,
    CASE
        WHEN l.quantity::numeric IS NULL THEN NULL
        WHEN l.unit = 'kg' THEN (cleaned_price / (l.quantity * 1000))::numeric
        ELSE cleaned_price::numeric / l.quantity::numeric
    END AS price_per_quantity,
    c.category,
    date,
    store,
    image_url
from raw_biocoop as l
join raw_category as c
on c.name = l.name
