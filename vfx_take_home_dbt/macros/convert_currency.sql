-- I made this very basic macro that only takes the most recent conversion rate from the seed csv file
-- In a real world scenario this could support multiple currencies and match the effective date with the purchase date

{% macro get_conversion_rate(from_currency='USD', to_currency='GBP') %}
    (
        select rate
        from {{ ref('exchange_rates') }}
        where currency_from = '{{ from_currency }}'
          and currency_to = '{{ to_currency }}'
        order by effective_date desc
        limit 1
    )
{% endmacro %}

{% macro convert_amount(amount_column, rate_column) %}
    round({{ amount_column }} * {{ rate_column }}, 2)::float
{% endmacro %}
