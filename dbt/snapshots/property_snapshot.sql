{% snapshot property_snapshot %}

{{ 
config(
  target_schema='snapshots',
  unique_key='property_type',
  strategy='timestamp',
  updated_at='snapshot_ts'
) 
}}

SELECT * FROM {{ ref('dim_property') }}

{% endsnapshot %}
