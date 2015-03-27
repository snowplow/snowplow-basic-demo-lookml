# Copyright (c) 2013-2015 Snowplow Analytics Ltd. All rights reserved.
#
# This program is licensed to you under the Apache License Version 2.0,
# and you may not use this file except in compliance with the Apache License Version 2.0.
# You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the Apache License Version 2.0 is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
#
# Version: 3-0-0
#
# Authors: Yali Sassoon, Christophe Bogaert
# Copyright: Copyright (c) 2013-2015 Snowplow Analytics Ltd
# License: Apache License Version 2.0

- view: page_views
  derived_table:
    sql: |
      SELECT
        domain_userid,
        domain_sessionidx,
        page_urlhost,
        page_urlpath,
        MIN(collector_tstamp) AS first_touch_tstamp,
        MAX(collector_tstamp) AS last_touch_tstamp,
        MIN(dvce_tstamp) AS dvce_min_tstamp,
        MAX(dvce_tstamp) AS dvce_max_tstamp,
        COUNT(*) AS event_count,
        SUM(CASE WHEN event = 'page_view' THEN 1 ELSE 0 END) AS page_view_count,
        SUM(CASE WHEN event = 'page_ping' THEN 1 ELSE 0 END) AS page_ping_count,
        COUNT(DISTINCT(FLOOR(EXTRACT (EPOCH FROM dvce_tstamp)/30)))/2::FLOAT AS time_engaged_with_minutes
      FROM atomic.events
      WHERE page_urlhost IS NOT NULL -- Remove incorrect page views
        AND page_urlpath IS NOT NULL -- Remove incorrect page views
        AND domain_sessionidx IS NOT NULL
        AND domain_userid IS NOT NULL
        AND domain_userid != ''
        AND dvce_tstamp IS NOT NULL
        AND dvce_tstamp > '2000-01-01' -- Prevent SQL errors
        AND dvce_tstamp < '2030-01-01' -- Prevent SQL errors
        -- if dev -- AND collector_tstamp > '2015-03-20'
      GROUP BY 1,2,3,4
        
    sql_trigger_value: SELECT COUNT(*) FROM ${visitors.SQL_TABLE_NAME} # Generate this table after visitors
    distkey: domain_userid
    sortkeys: [domain_userid, domain_sessionidx, first_touch_tstamp]
  
  fields:
  
  # DIMENSIONS # 
  
  # Basic dimensions
  
  - dimension: user_id
    sql: ${TABLE}.domain_userid
    
  - dimension: session_index
    type: int
    sql: ${TABLE}.domain_sessionidx
  
  - dimension: session_id
    sql: ${TABLE}.domain_userid || '-' || ${TABLE}.domain_sessionidx
  
  - dimension: page_host
    sql: ${TABLE}.page_urlhost
  
  - dimension: page_path
    sql: ${TABLE}.page_urlpath
  
  - dimension: page
    sql: ${TABLE}.page_urlhost ||${TABLE}.page_urlpath
  
  - dimension: start
    sql: ${TABLE}.first_touch_tstamp
  
  - dimension_group: start
    type: time
    timeframes: [time, hour, date, week, month]
    sql: ${TABLE}.first_touch_tstamp
    
  - dimension: end
    sql: ${TABLE}.last_touch_tstamp
  
  - dimension_group: end
    type: time
    timeframes: [time, hour, date, week, month]
    sql: ${TABLE}.last_touch_tstamp
  
  - dimension: events_during_page_view
    type: int
    sql: ${TABLE}.event_count
  
  - dimension: events_during_page_view_tiered
    type: tier
    tiers: [1,5,10,25,50,100,1000,10000,100000]
    sql: ${TABLE}.event_count
  
  - dimension: bounce
    type: yesno
    sql: ${TABLE}.event_count = 1
  
  - dimension: number_of_page_views
    type: int
    sql: ${TABLE}.page_view_count
  
  - dimension: number_of_page_views_tiered
    type: tier
    tiers: [1,2,5,10,25,50,100,1000]
    sql: ${number_of_page_views}
  
  - dimension: number_of_page_pings
    type: int
    sql: ${TABLE}.page_view_count
  
  - dimension: number_of_page_pings_tiered
    type: tier
    tiers: [1,2,5,10,25,50,100,1000]
    sql: ${number_of_page_views}
  
  - dimension: event_stream
    sql: ${user_id}
    html: |
      <a href=events?fields=events.event_detail*&f[events.user_id]={{value}}>Event stream</a>
  
  # MEASURES #
  
  - measure: count
    type: count_distinct
    sql: ${user_id}
    drill_fields: individual_detail*
  
  - measure: bounced_visitor_count
    type: count_distinct
    sql: ${user_id}
    filter:
      bounce: yes
    drill_fields: detail*
  
  - measure: bounce_rate
    type: number
    decimals: 2
    sql: ${bounced_visitor_count}/NULLIF(${count},0)::REAL
  
  - measure: event_count
    type: sum
    sql: ${TABLE}.event_count
  
  - measure: events_per_visitor
    type: number
    decimals: 2
    sql: ${event_count}/NULLIF(${count},0)::REAL
  
  - measure: session_count
    type: sum
    sql: ${TABLE}.session_count
    drill_fields: details*
  
  - measure: sessions_per_visitor
    type: number
    decimals: 2
    sql: ${session_count}/NULLIF(${count},0)::REAL
  
  # Landing page measures
  
  - measure: landing_page_count
    type: count_distinct
    sql: ${landing_page}
    drill_fields:
    - landing_page
    - detail*
  
  # Marketing measures (paid acquisition channels)
  
  - measure: campaign_medium_count
    type: count_distinct
    sql: ${campaign_medium}
    drill_fields: 
    - campaign_medium
    - detail*
    
  - measure: campaign_source_count
    type: count_distinct
    sql: ${campaign_source}
    drill_fields: 
    - campaign_medium
    - campaign_source
    - detail*
    
  - measure: campaign_term_count
    type: count_distinct
    sql: ${campaign_term}
    drill_fields: 
    - campaign_medium
    - campaign_source
    - campaign_term
    - detail*
      
  - measure: campaign_count
    type: count_distinct
    sql: ${campaign_name}
    drill_fields: 
    - campaign_medium
    - campaign_source
    - campaign_term
    - detail*
  
  # Referer measures (all acquisition channels)
  
  - measure: referer_medium_count
    type: count_distinct
    sql: ${referer_medium}
    drill_fields:
    - referer_medium
    - detail*
  
  - measure: referer_source_count
    type: count_distinct
    sql: ${referer_source}
    drill_fields:
    - referer_medium
    - referer_source
    - detail*
  
  - measure: referer_term_count
    type: count_distinct
    sql: ${referer_term}
    drill_fields:
    - referer_medium
    - referer_source
    - referer_term
    - detail*
  
  # DRILL FIELDS #
  
  sets:    
    detail:
      - count
      - bounce_rate
      - sessions_per_visitor
      - events_per_visitor
      - campaign_medium_count
      - campaign_source_count
      - campaign_term_count
      - campaign_count
      - referer_medium_count
      - referer_source_count
      - referer_term_count
    
    individual_detail:
      - user_id
      - first_touch
      - last_touch
      - referer_medium
      - referer_source
      - referer_host
      - referer_url_host
      - referer_url_path
      - campaign_medium
      - campaign_source
      - campaign_name
      - landing_page
      - number_of_sessions
      - number_of_events
      - session_stream
      - event_stream
    