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
      and PRICE_RS is not null
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
        -- Standardize category values
        case
            when CATEGORY = 'Beauty' then 'BEAUTY'
            when CATEGORY = 'Books' then 'BOOKS'
            when CATEGORY = 'Clothing' then 'CLOTHING'
            when CATEGORY = 'Electronics' then 'ELECTRONICS'
            when CATEGORY = 'Home & Kitchen' then 'HOME_AND_KITCHEN'
            when CATEGORY = 'Sports' then 'SPORTS'
            when CATEGORY = 'Toys' then 'TOYS'
            else 'OTHER'
        end as category,

        -- Transaction attributes
        try_to_date(PURCHASE_DATE, 'DD-MM-YYYY') as purchase_date, -- found this at https://docs.snowflake.com/en/sql-reference/functions/try_to_date
        -- Standardize payment method values
        case
            when PAYMENT_METHOD = 'Cash on Delivery' then 'CASH_ON_DELIVERY'
            when PAYMENT_METHOD = 'Credit Card' then 'CREDIT_CARD'
            when PAYMENT_METHOD = 'Debit Card' then 'DEBIT_CARD'
            when PAYMENT_METHOD = 'Net Banking' then 'NET_BANKING'
            when PAYMENT_METHOD = 'UPI' then 'UPI'
            else trim(upper(PAYMENT_METHOD))
        end as payment_method,

        -- Monetary values (in USD as per challenge instructions)
        -- I chose not to mention the currency in the column name as in a real scenario we might want to add support for
        -- multiple currencies. The currency might be something that would also arrive in the raw data and we would
        -- process the conversion in our marts model(s)
        PRICE_RS as price,
        DISCOUNT as discount_percentage,
        FINAL_PRICE_RS as final_price,

        -- Metadata
        to_timestamp(LOADED_AT) as loaded_at,
        current_timestamp()::timestamp_ntz as dbt_processed_at

    from source_data
)

select * from cleaned
