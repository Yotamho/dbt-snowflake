{{
    config(
        materialized = "incremental",
        unique_key = "id",
        merge_condition = 'DBT_INTERNAL_SOURCE.ip_address != DBT_INTERNAL_DEST.ip_address'
    )
}}


select *
from {{ ref('seed') }}

{% if is_incremental() %}

    where load_date > (select max(load_date) from {{this}})

{% endif %}
