create or replace table bronze_subscriptions as
select *
from bronze_apfel_subscriptions
union all
select *
from bronze_fenster_subscriptions
order by event_id, event_timestamp desc
;
