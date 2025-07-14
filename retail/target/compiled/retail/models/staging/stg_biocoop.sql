WITH raw_biocoop AS (
    SELECT
        row_number() OVER (ORDER BY date) AS id,
        date::date AS date,
        name,
        CASE
            WHEN stock ~ '^[0-9]+(\.[0-9]+)?$' THEN stock::numeric
            WHEN lower(stock) IN ('in stock', 'en stock', 'disponible') THEN -1
            WHEN stock IS NULL OR trim(stock) = '' THEN 0
            ELSE 0
        END AS stock,
        replace(replace(price,'2150','2.15'),'1975','1.975')::numeric AS cleaned_price,
        replace(replace(regexp_replace(price_per_quantity, '[^a-zA-Z\s]+', '', 'g'),'€',''),' ','') AS unit,
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
        img AS image_url
    FROM "airflow"."public"."biocoop"
),

raw_category AS (
    SELECT DISTINCT ON (product_name)
        product_name,
        category
    FROM "airflow"."public"."biocoop_cat"
    ORDER BY product_name, category
),

raw_section AS (
    SELECT DISTINCT ON (product_name)
        product_name,
        section
    FROM "airflow"."public"."biocoop_section"
    ORDER BY product_name, section
),

final_data AS (
    SELECT
        id,
        l.name,
        cleaned_price AS price,
        stock,
        CASE
            WHEN l.unit = 'kg' THEN l.quantity * 1000
            ELSE l.quantity
        END AS quantity,
        replace(l.unit,'kg','g') AS unit,
        CASE
            WHEN l.quantity::numeric IS NULL THEN NULL
            WHEN l.unit = 'kg' THEN (cleaned_price / (l.quantity * 1000))::numeric
            ELSE cleaned_price::numeric / l.quantity::numeric
        END AS price_per_quantity,
        c.category,
        s.section,
        date,
        store,
        image_url,
        ROW_NUMBER() OVER (PARTITION BY image_url,date ORDER BY id ) AS dedup_rank  -- Dedup rank per image_url
    FROM raw_biocoop AS l
    JOIN raw_category AS c
      ON c.product_name = l.name
    JOIN raw_section AS s
      ON s.product_name = l.name

)

SELECT
    id,
    name,
    price,
    stock,
    quantity,
    unit,
    price_per_quantity,
    category,
    section,
    date,
    store,
    image_url
FROM final_data
WHERE dedup_rank = 1  -- Keep only one row per image_url (latest date)