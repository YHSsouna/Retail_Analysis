

with all_products as (
    select distinct
        product_id,
        name,
        NULL as marque,
        category,
        image_url as picture,
        store
    from "airflow"."public_intermediate"."inter_labellevie"

    union all

    select distinct
        product_id,
        name,
        NULL as marque,
        category,
        image_url as picture,
        store
    from "airflow"."public_intermediate"."inter_biocoop"

    union all

    select distinct
        product_id,
        name,
        NULL as marque,
        category,
        image_url as picture,
        store
    from "airflow"."public_intermediate"."inter_carrefour"

    union all

    select distinct
        product_id,
        name,
        marque,
        category,
        image_url as picture,
        store
    from "airflow"."public_intermediate"."inter_auchan"
),

final_product as (
    select
        p.product_id,
        p.name,
        p.marque,
        c.id_category,
        i.id_picture,
        s.id_store
    from all_products p
    left join "airflow"."public_marts"."dim_category" c
        on p.category = c.category
    left join "airflow"."public_marts"."dim_picture" i
        on p.picture = i.picture
    left join "airflow"."public_marts"."dim_store" s
        on p.store = s.store
),

deduplicated as (
    select *
    from (
        select *,
            row_number() over (
                partition by product_id
                order by id_category, id_store, id_picture
            ) as rn
        from final_product
    ) sub
    where rn = 1
)

select * from deduplicated