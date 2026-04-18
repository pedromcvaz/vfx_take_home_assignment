{{
    config(
        materialized='table',
        unique_key='product_master_id'
    )
}}

with product_base as (
    select
        product_id,
        category,
    from {{ ref('stg_ecommerce__transactions') }}
    group by product_id, category_raw
),

-- Normalize category values
category_normalization as (
    select
        product_id,
        category_raw,
        -- Clean and standardize category names
        case
            when category_raw ilike '%electronic%' or category_raw ilike '%gadget%' then 'ELECTRONICS'
            when category_raw ilike '%cloth%' or category_raw ilike '%apparel%' or category_raw ilike '%fashion%' then 'CLOTHING'
            when category_raw ilike '%home%' or category_raw ilike '%furniture%' then 'HOME_GOODS'
            when category_raw ilike '%book%' then 'BOOKS'
            when category_raw ilike '%sport%' or category_raw ilike '%fitness%' then 'SPORTS'
            when category_raw ilike '%beauty%' or category_raw ilike '%cosmetic%' then 'BEAUTY'
            when category_raw ilike '%food%' or category_raw ilike '%grocery%' then 'FOOD'
            else 'OTHER'
        end as category_normalized,
        last_seen_at
    from product_base
),

product_metrics as (
    select
        p.product_id,
        p.category_raw,
        p.category_normalized,
        count(distinct s.transaction_id) as times_sold,
        count(distinct s.user_id) as unique_customers,
        avg(s.price_inr) as avg_price_inr,
        avg(s.discount_percentage) as avg_discount_pct,
        min(s.purchase_date) as first_sold_date,
        max(s.purchase_date) as last_sold_date,
        p.last_seen_at
    from category_normalization p
    left join {{ ref('stg_ecommerce__transactions') }} s
        on p.product_id = s.product_id
    group by
        p.product_id,
        p.category_raw,
        p.category_normalized,
        p.last_seen_at
),

product_master as (
    select
        -- Deterministic master key
        {{ generate_deterministic_guid(['product_id']) }} as product_master_id,

        -- Natural key
        product_id as source_product_id,

        -- Product attributes (SCD2 candidates)
        category_raw as category_original,
        category_normalized as category,

        -- Product metrics
        times_sold,
        unique_customers,
        round(avg_price_inr, 2) as avg_price_inr,
        round(avg_discount_pct, 2) as avg_discount_pct,
        first_sold_date,
        last_sold_date,

        -- Product classification
        case
            when times_sold >= 50 then 'BEST_SELLER'
            when times_sold >= 20 then 'POPULAR'
            when times_sold >= 5 then 'REGULAR'
            else 'LOW_VOLUME'
        end as product_tier,

        -- Metadata
        'RAW_ECOMMERCE_DATA' as source_system,
        last_seen_at,
        current_timestamp() as dbt_valid_from,
        null::timestamp as dbt_valid_to,
        true as is_current

        -- Note: For SCD2 implementation, I would've add track changes to:
        -- - category (if product gets recategorized)
        -- - avg_price_inr (if pricing strategy changes significantly)
        -- - product_tier (if sales velocity changes)

    from product_metrics
)

select * from product_master
