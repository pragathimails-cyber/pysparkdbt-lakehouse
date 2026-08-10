{{
    config(
        materialized ='incremental',
        unique_key = 'trip_id'
    )
}}
{% set cols = ['trip_id', 'vehicle_id', 'customer_id', 'driver_id', 'trip_start_time','trip_end_time','distance_km','fare_amount','trip_status','last_updated_timestamp'] %}

SELECT
    {% for col in cols%}
        {{ col }}
        {% if not loop.last %}
            ,
        {% endif %}
    {% endfor %}
FROM
    {{source('source_silver','trips') }}
{% if is_incremental() %}
WHERE 
    last_updated_timestamp > (SELECT COALESCE(MAX(last_updated_timestamp),'1990-01-01') FROM {{ this }})
{% endif %}
