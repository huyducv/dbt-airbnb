{% snapshot host_snapshot %}

{{ 
config(
  target_schema='snapshots',
  unique_key='host_id',
  strategy='timestamp',
  updated_at='snapshot_ts'
) 
}}

SELECT * FROM {{ ref('dim_hosts') }}

{% endsnapshot %}
