{{
    config(
        materialized ='incremental',
        unique_key = 'payment_id'
    )
}}
{% set cols = ['payment_id', 'trip_id', 'customer_id', 'payment_method', 'payment_status', 'amount', 'transaction_time', 'last_updated_timestamp'] %}

SELECT
    {% for col in cols%}
        {{ col }}
        {% if not loop.last %}
            ,
        {% endif %}
    {% endfor %}
FROM
    {{source('source_silver','payments') }}
{% if is_incremental() %}
WHERE 
    last_updated_timestamp > (SELECT COALESCE(MAX(last_updated_timestamp),'1990-01-01') FROM {{ this }})
{% endif %}
