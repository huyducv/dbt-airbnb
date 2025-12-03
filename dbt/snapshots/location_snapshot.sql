{% snapshot location_snapshot %}

{{ 
config(
  target_schema='snapshots',
  unique_key='host_neighbourhood',
  strategy='timestamp',
  updated_at='snapshot_ts'
)
}}

SELECT * FROM {{ ref('dim_location') }}

{% endsnapshot %}
