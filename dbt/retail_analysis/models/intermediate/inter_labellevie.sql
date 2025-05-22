{{ config(materialized='table') }}

WITH RECURSIVE

-- Get min and max date per image_url
image_url_dates AS (
    SELECT
        image_url,
        MIN(date) AS min_date,
        MAX(date) AS max_date
    FROM {{ ref('stg_labellevie') }}
    WHERE image_url IS NOT NULL
    GROUP BY image_url
),

-- Generate date range per image_url
image_date_matrix AS (
    SELECT
        i.image_url,
        generate_series(i.min_date, i.max_date, INTERVAL '1 day')::date AS date_day
    FROM image_url_dates i
),

-- Join with actual data
joined AS (
    SELECT
        m.image_url,
        m.date_day,
        a.name,
        a.price,
        a.stock,
        a.quantity,
        a.unit,
        a.price_per_quantity,
        a.category,
        a.store
    FROM image_date_matrix m
    LEFT JOIN {{ ref('stg_labellevie') }} a
      ON a.image_url = m.image_url
     AND a.date = m.date_day
),

-- Order the data
ordered AS (
    SELECT *
    FROM joined
    ORDER BY image_url, date_day
),

-- Base cases: first and last known records per product
base_case AS (
    (
        SELECT DISTINCT ON (image_url)
            image_url,
            date_day,
            name,
            price,
            stock,
            quantity,
            unit,
            price_per_quantity,
            category,
            store
        FROM ordered
        WHERE name IS NOT NULL OR price IS NOT NULL OR stock IS NOT NULL
        ORDER BY image_url, date_day  -- first record
    )

    UNION

    (
        SELECT DISTINCT ON (image_url)
            image_url,
            date_day,
            name,
            price,
            stock,
            quantity,
            unit,
            price_per_quantity,
            category,
            store
        FROM ordered
        WHERE name IS NOT NULL OR price IS NOT NULL OR stock IS NOT NULL
        ORDER BY image_url, date_day DESC  -- last record
    )
),


-- Recursive forward fill
recursive_fill AS (
    SELECT * FROM base_case

    UNION ALL

    SELECT
        o.image_url,
        o.date_day,
        COALESCE(o.name, r.name) AS name,
        COALESCE(o.price, r.price) AS price,
        COALESCE(o.stock, r.stock) AS stock,
        COALESCE(o.quantity, r.quantity) AS quantity,
        COALESCE(o.unit, r.unit) AS unit,
        COALESCE(o.price_per_quantity, r.price_per_quantity) AS price_per_quantity,
        COALESCE(o.category, r.category) AS category,
        COALESCE(o.store, r.store) AS store
    FROM ordered o
    JOIN recursive_fill r
      ON o.image_url = r.image_url
     AND o.date_day = r.date_day + INTERVAL '1 day'
)

-- Final result
SELECT
    ROW_NUMBER() OVER (ORDER BY image_url, date_day) AS id,
    CAST(('x' || substr(md5(image_url), 1, 16))::bit(64) AS BIGINT) as product_id,  -- <--- Generate product_id based on image_url
    image_url,
    date_day as date,
    name,
    price,
    stock,
    quantity,
    unit,
    price_per_quantity,
    category,
    store
FROM recursive_fill
ORDER BY image_url, date_day
