CREATE OR REPLACE TABLE `<PROJECT_ID>.<DATASET>.<TABLE_NAME>`
PARTITION BY event_date
OPTIONS(
  #partition_expiration_days=365,
  description="Partitioned table with general data for GA4"
) AS
SELECT

  # property_id*
  '<PROPERTY_ID>' AS property_id,

  # property_name*
  'PROPERTY_NAME' AS property_name,

  # Date
  PARSE_DATE('%Y%m%d',event_date) AS event_date,

  # event_timestamp*
  event_timestamp,

  # Event name
  event_name,

  # Campaign
  traffic_source.name AS campaign,

  # User Pseudo ID*
  user_pseudo_id, 

  # New users
  IF(event_name IN ('first_visit', 'first_open'),
  1, 0) AS is_new_user, 

  # Active users
  (CASE 
  WHEN
    # engagement_time_msec parameter from a website.
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'engagement_time_msec') > 0
    OR event_name = 'first_visit'
    OR event_name = 'first_open'
    OR (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'session_engaged') >= 1
    OR (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'engaged_session_event')>=1
    OR (SELECT value.int_value from unnest(event_params) where key = 'ga_session_number') = 1
  THEN user_pseudo_id ELSE NULL END)
  AS active_user,

  # event_deepen*
  (SELECT item.value.string_value
    FROM UNNEST(event_params) AS item
    WHERE item.key = 'event_deepen') AS event_key_string_value,

  # ad_content*
  (SELECT item.value.string_value
    FROM UNNEST(event_params) AS item
    WHERE item.key = 'content') AS ad_content,

  # Session ID [Not unique]*
  (SELECT item.value.int_value
    FROM UNNEST(event_params) AS item
    WHERE item.key = 'ga_session_id') AS ga_session_id, 
  
  # Session ID
  CONCAT(
  user_pseudo_id,
  (SELECT item.value.int_value
    FROM UNNEST(event_params) AS item
    WHERE item.key = 'ga_session_id' ) ) AS session_id,

  # Transaction ID
  ecommerce.transaction_id AS transaction_id, 

  # Currency
  user_ltv.currency AS currency,

  # Source
  traffic_source.source AS source, 

  # Medium
  traffic_source.medium AS medium,

  # Source / medium
  CONCAT(COALESCE(traffic_source.source,'(direct)'),' / ',
          COALESCE(traffic_source.medium,'(none)')
  ) AS source_medium,

  # Hostname
  device.web_info.hostname AS hostname,

  # search_category*
  (SELECT item.value.string_value
    FROM UNNEST(event_params) AS item
    WHERE item.key = 'search_category') AS search_category,

  # search_label*
  (SELECT item.value.string_value
  FROM UNNEST(event_params) AS item
  WHERE item.key = 'search_label'
  ) AS search_label,

  # Page location
  (SELECT item.value.string_value
    FROM UNNEST(event_params) AS item
    WHERE item.key = 'page_location'
  ) AS page_location,

  # Page title
  (SELECT item.value.string_value
  FROM UNNEST(event_params) AS item
  WHERE item.key = 'page_title'
  ) AS page_title,

  # link_paths*
  (SELECT item.value.string_value
  FROM UNNEST(event_params) AS item
  WHERE item.key = 'links_paths'
  ) AS link_paths,

  # Region
  geo.region AS region,

  # Country
  geo.Country AS country,

  # Device category
  device.category AS device_category,

  # Conversion default channel grouping
  CASE
    WHEN REGEXP_CONTAINS(traffic_source.source,".*direct.*")
      THEN "Direct"
    WHEN REGEXP_CONTAINS(traffic_source.source,
      ".*facebook.*|.*Facebook.*|.*Linkedin.*|.*Twitter.*|.*twitter.*|.*instagram.*|.*pinterest.*|.*linkedin.*|.*reddit.*|.*youtube.*")
      THEN "Social"
    WHEN traffic_source.medium=("organic")
      THEN "Organic"
    WHEN traffic_source.medium=("referral")
      THEN "Referral"
    WHEN traffic_source.medium=("email")
      THEN "Email"
    WHEN traffic_source.medium=("blog")
      THEN "Display"
    WHEN traffic_source.medium=("cpc")
      THEN"Organic Search"
    ELSE ("(Others)")
  END AS default_channel_grouping,

  # Event count
  COUNT(*) AS event_count,

  # Engaged sessions
  SUM((SELECT item.value.int_value 
      FROM UNNEST(event_params) AS item
      WHERE item.key = 'session_engaged'
      AND item.value.int_value=1)
  ) AS sessions_engaged,

  # Event value
  SUM(
  (
    SELECT COALESCE(value.int_value, value.float_value, value.double_value)
    FROM UNNEST(event_params)
    WHERE key = 'value'
  ))
  AS event_value,

  # User engagement
  SUM(
    (
      SELECT item.value.int_value 
      FROM UNNEST(event_params) AS item
      WHERE item.key = 'engagement_time_msec' 
    )
  ) AS user_engagement,

  # Items*
  SUM(ARRAY_LENGTH(items)) AS n_items,

  # Purchase revenue (USD)
  SUM(ecommerce.purchase_revenue_in_usd) AS purchase_revenue_in_usd, 

  # Conversions
  COUNTIF(event_name
    IN
      (
      'purchase',
      'formstack_formularioexitoso_completarpag'
      )
    ) AS conversions,

FROM `<PROJECT_ID>.<DATASET>.events_*`

GROUP BY
  event_date,
  event_timestamp,
  event_name,
  campaign,
  user_pseudo_id,
  is_new_user,
  active_user,
  event_key_string_value,
  ad_content,
  ga_session_id,
  session_id,
  transaction_id,
  currency,
  source,
  medium,
  source_medium,
  hostname,
  search_category,
  search_label,
  page_location,
  page_title,
  link_paths,
  region,
  country,
  device_category,
  default_channel_grouping
;
