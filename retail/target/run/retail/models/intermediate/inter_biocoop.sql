
  
    

  create  table "airflow"."public_intermediate"."inter_biocoop__dbt_tmp"
  
  
    as
  
  (
    

WITH RECURSIVE

-- Get min and max date per image_url and name
image_url_dates AS (
    SELECT
        image_url,
        name,
        MIN(date) AS min_date,
        MAX(date) AS max_date
    FROM "airflow"."public_staging"."stg_biocoop"
    WHERE image_url IS NOT NULL AND name IS NOT NULL
    GROUP BY image_url, name
),

-- Generate date range per image_url + name
image_date_matrix AS (
    SELECT
        i.image_url,
        i.name,
        generate_series(i.min_date, i.max_date, INTERVAL '1 day')::date AS date_day
    FROM image_url_dates i
),

-- Join with actual data
joined AS (
    SELECT
        m.image_url,
        m.name,
        m.date_day,
        a.price,
        a.stock,
        a.quantity,
        a.unit,
        a.price_per_quantity,
        a.category,
        a.section,
        a.store
    FROM image_date_matrix m
    LEFT JOIN "airflow"."public_staging"."stg_biocoop" a
      ON a.image_url = m.image_url
     AND a.name = m.name
     AND a.date = m.date_day
),

-- Order the data
ordered AS (
    SELECT *
    FROM joined
    ORDER BY image_url, name, date_day
),

-- Base cases: first and last known records per product
base_case AS (
    (
        SELECT DISTINCT ON (image_url, name)
            image_url,
            name,
            date_day,
            price,
            stock,
            quantity,
            unit,
            price_per_quantity,
            category,
            section,
            store
        FROM ordered
        WHERE name IS NOT NULL OR price IS NOT NULL OR stock IS NOT NULL
        ORDER BY image_url, name, date_day  -- first record
    )
    UNION
    (
        SELECT DISTINCT ON (image_url, name)
            image_url,
            name,
            date_day,
            price,
            stock,
            quantity,
            unit,
            price_per_quantity,
            category,
            section,
            store
        FROM ordered
        WHERE name IS NOT NULL OR price IS NOT NULL OR stock IS NOT NULL
        ORDER BY image_url, name, date_day DESC  -- last record
    )
),

-- Recursive forward fill
recursive_fill AS (
    SELECT * FROM base_case

    UNION ALL

    SELECT
        o.image_url,
        o.name,
        o.date_day,
        COALESCE(o.price, r.price) AS price,
        COALESCE(o.stock, r.stock) AS stock,
        COALESCE(o.quantity, r.quantity) AS quantity,
        COALESCE(o.unit, r.unit) AS unit,
        COALESCE(o.price_per_quantity, r.price_per_quantity) AS price_per_quantity,
        COALESCE(o.category, r.category) AS category,
        COALESCE(o.section, r.section) AS section,
        COALESCE(o.store, r.store) AS store
    FROM ordered o
    JOIN recursive_fill r
      ON o.image_url = r.image_url
     AND o.name = r.name
     AND o.date_day = r.date_day + INTERVAL '1 day'
),

-- Deduplicate rows by keeping only one per (image_url, name, date_day)
final_deduplicated AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY image_url, name, date_day ORDER BY image_url, name, date_day) AS rn
    FROM recursive_fill
)

-- Final result
SELECT
    ROW_NUMBER() OVER (ORDER BY image_url, name, date_day) AS id,
    CAST(('x' || substr(md5(image_url || name), 1, 16))::bit(64) AS BIGINT) AS product_id,
    image_url,
    date_day AS date,
    name,
    price,
    stock,
    quantity,
    unit,
    price_per_quantity,
    category,
    section,
    store
FROM final_deduplicated
WHERE rn = 1
ORDER BY image_url, name, date_day
  );
  