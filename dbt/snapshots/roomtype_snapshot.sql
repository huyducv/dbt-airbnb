{% snapshot roomtype_snapshot %}

{{ 
config(
  target_schema='snapshots',
  unique_key='room_type',
  strategy='timestamp',
  updated_at='snapshot_ts'
) 
}}

SELECT * FROM {{ ref('dim_roomtype') }}

{% endsnapshot %}
