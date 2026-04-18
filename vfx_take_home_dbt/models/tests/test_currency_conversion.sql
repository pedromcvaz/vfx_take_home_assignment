-- Test macro functionality
-- depends_on: {{ ref('exchange_rates') }}
select
    'USD' as from_currency,
    'GBP' as to_currency,
    100.00 as amount_usd,
    {{ convert_amount('100.00', get_conversion_rate()) }} as amount_gbp,
    {{ get_conversion_rate() }} as rate_used
