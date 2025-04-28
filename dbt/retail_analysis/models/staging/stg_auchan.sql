

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
    select distinct on (name)
        name,
        mapped_category as category
    from {{ source('public', 'auchan_categories') }}
    order by name, mapped_category
),

raw_norm as (
    select distinct on (name)
        name,
        unit,
        CASE
            WHEN quantity::numeric = 0 THEN NULL
            ELSE quantity::numeric
        END AS quantity
    from {{ source('public', 'auchan_norm') }}
    order by name, quantity desc nulls last
),




final as (
    select
        *,
        row_number() over (partition by image_url, date order by id) as dedup_rank
    from (
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
        left join raw_category as c
            on c.name = l.name
        left join raw_norm as n
            on n.name = l.name
        -- filter bad image_url here
        where l.image_url is not null
          and l.image_url != ''
          and lower(l.image_url) != 'nan'
    ) as subquery
)

select
    id,
    name,
    price,
    stock,
    quantity,
    unit,
    price_per_quantity,
    category,
    date,
    marque,
    store,
    image_url
from final
where dedup_rank = 1
