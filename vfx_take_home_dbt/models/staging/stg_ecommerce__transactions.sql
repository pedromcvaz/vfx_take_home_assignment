with source_data as (
    select
        USER_ID,
        PRODUCT_ID,
        PURCHASE_DATE,
        CATEGORY,
        PRICE_RS,
        DISCOUNT,
        FINAL_PRICE_RS,
        PAYMENT_METHOD,
        LOADED_AT
    from {{ source('raw_ecommerce', 'RAW_ECOMMERCE_DATA') }}
    -- Filter out only truly invalid records (defensive barrier)
    where USER_ID is not null
      and PRODUCT_ID is not null
      and PURCHASE_DATE is not null
      and FINAL_PRICE_RS is not null  -- Can't have a transaction without a price
),

cleaned as (
    select
        -- Generate deterministic transaction ID
        -- I found this at https://docs.getdbt.com/blog/sql-surrogate-keys?version=1.12 which neatly handles
        -- null values and type casting
        {{ dbt_utils.generate_surrogate_key([
            'USER_ID',
            'PRODUCT_ID',
            'PURCHASE_DATE',
            'FINAL_PRICE_RS'
        ]) }} as transaction_id,

        -- User attributes
        trim(upper(USER_ID)) as user_id,

        -- Product attributes
        trim(upper(PRODUCT_ID)) as product_id,
        trim(upper(CATEGORY)) as category_raw,

        -- Transaction attributes
        to_date(PURCHASE_DATE, 'DD-MM-YYYY') as purchase_date,
        trim(upper(PAYMENT_METHOD)) as payment_method,

        -- Monetary values (in USD as per challenge instructions)
        -- I chose not to mention the currency in the column name as in a real scenario we might want to add support for
        -- multiple currencies. The currency might be something that would also arrive in the raw data and we would
        -- process the conversion in our marts model(s)
        PRICE_RS as price,
        DISCOUNT as discount_percentage,
        FINAL_PRICE_RS as final_price,

        -- Metadata
        to_timestamp(LOADED_AT) as loaded_at,
        current_timestamp() as dbt_processed_at

    from source_data
)

select * from cleaned
