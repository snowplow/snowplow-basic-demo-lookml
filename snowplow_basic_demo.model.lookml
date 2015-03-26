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
# Author(s): Yali Sassoon, Christophe
# Copyright: Copyright (c) 2013-2015 Snowplow Analytics Ltd
# License: Apache License Version 2.0

- connection: snowplow_demo

- scoping: true                   # for backward compatibility
- include: "*.view.lookml"        # include all the views
- include: "*.dashboard.lookml"   # include all the dashboards

#- base_view: signup_form_submitted
#- base_view: trial_form_submitted

#- base_view: events  
#  joins:
#  - join: sessions
#    sql_on: |
#      events.domain_userid = sessions.domain_userid AND
#      events.domain_sessionidx = sessions.domain_sessionidx
#  - join: visitors
#    sql_on: |
#      events.domain_userid = visitors.domain_userid

## Views to support events
#- base_view: atomic_events

- explore: sessions
#  joins: 
#  - join: visitors
#    sql_on: |
#      sessions.domain_userid = visitors.domain_userid

#- base_view: visitors
