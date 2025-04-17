with raw_carrefour as(
    select
        row_number() over (order by date) as id,
        date::date as date,
        name,
        quantity_stock::numeric as stock,
        replace(replace(replace(price,'€',''),' ',''),',','.')::numeric as cleaned_price,
        CASE
            -- If promotion is not null, calculate the discount rate (1 - promo_price / original_price)
            WHEN nullif(replace(replace(replace(promotion, '€', ''), ' ', ''), ',', '.'), '')::numeric IS NOT NULL THEN
                1 - (
                    replace(replace(replace(promotion, '€', ''), ' ', ''), ',', '.')::numeric /
                    replace(replace(replace(price, '€', ''), ' ', ''), ',', '.')::numeric
                )

            -- If promotion is an empty string, just return the cleaned numeric price
            ELSE
                replace(replace(replace(promotion,'€',''),' ',''),',','.')::numeric
        END AS cleaned_promotion,
        replace(replace(regexp_replace(price_per_quantity, '.*/\s*([a-zA-Z]+)', '\1'),'Kilogramme','Kg'),'le flacon de 250ml','ml') as unit,
        nullif(
            replace(
                replace(
                    regexp_replace(price_per_quantity, '^([0-9]+(?:\.[0-9]+)?)\s.*', '\1'),
                    '1,067.50 € / KG', ''
                ),
                'le flacon de 250ml', ''
            ),
            ''
        )::numeric as price_per_unit,
        CASE
            WHEN NULLIF(
                regexp_replace(
                    replace(
                        replace(price_per_quantity, '1,067.50 € / KG', ''),
                        'le flacon de 250ml', ''
                    ),
                    '^([0-9]+(?:[.,][0-9]+)?)\s.*',
                    '\1'
                ),
                ''
            )::numeric IS NULL
                OR NULLIF(
                    regexp_replace(
                        replace(
                            replace(price_per_quantity, '1,067.50 € / KG', ''),
                            'le flacon de 250ml', ''
                        ),
                        '^([0-9]+(?:[.,][0-9]+)?)\s.*',
                        '\1'
                    ),
                    ''
                )::numeric = 0
            THEN NULL
            ELSE replace(replace(replace(price,'€',''),' ',''),',','.')::numeric / (
                NULLIF(
                    regexp_replace(
                        replace(
                            replace(price_per_quantity, '1,067.50 € / KG', ''),
                            'le flacon de 250ml', ''
                        ),
                        '^([0-9]+(?:[.,][0-9]+)?)\s.*',
                        '\1'
                    ),
                    ''
                )::numeric
            )
        END AS quantity,
        store,
        image_url



    from {{ source('public', 'carrefour') }}
),

raw_category as (
    select
        name,
        category
    from {{ source('public', 'carrefour_categories') }}
)


select
    id,
    l.name,
    cleaned_price as price,
    stock,
    cleaned_promotion as promotion,
    price_per_unit,
    unit,
    quantity,
    c.category,
    date,
    store,
    image_url
from raw_carrefour as l
join raw_category as c
on c.name = l.name