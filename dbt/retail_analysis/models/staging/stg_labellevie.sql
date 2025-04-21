with raw_labellevie as (
    select
        row_number() over (order by date) as id,
        date_trunc('day', date)::date as date_cleaned,
        name,
        regexp_replace(quantity_stock, '[^0-9.]', '', 'g')::numeric as stock,
        nullif(replace(promotion, '%', ''), '')::numeric / 100 as promotion,
        CASE
            WHEN nullif(replace(promotion, '%', ''), '')::numeric IS NOT NULL THEN
                replace(replace(replace(price, '€', ''), 'au lieu de', ''), '+', '')::numeric
                * (1 - (nullif(replace(promotion, '%', ''), '')::numeric / 100))
            WHEN promotion = '' THEN
                replace(replace(replace(price, '€', ''), 'au lieu de', ''), '+', '')::numeric
        END as price_cleaned,
        regexp_replace(name, '.*\((.*)\).*', '\1') AS extracted_content,
        image_url,
        store
    from {{ source('public', 'labellevie') }}
),

raw_category as (
    select
        name,
        category
    from {{ source('public', 'labellevie_categories') }}
),

raw_norm as (
        select
            name,
            CASE
                WHEN quantity::numeric = 0 THEN NULL
                ELSE quantity::numeric
            END AS quantity,
            unit
        from {{ source('public', 'labellevie_norm') }}
)

select
    id,
    l.name,
    price_cleaned as price,
    stock,
    n.quantity,
    n.unit,
    CASE
        WHEN n.quantity::numeric IS NULL THEN NULL
        ELSE price_cleaned::numeric / n.quantity::numeric
    END AS price_per_quantity,
    c.category,
    date_cleaned as date,
    promotion,
    store,
    image_url

from raw_labellevie as l
join raw_category as c
on c.name = l.name
join raw_norm as n
on n.name = l.name
