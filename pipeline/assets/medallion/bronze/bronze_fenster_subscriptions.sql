create or replace table bronze_fenster_subscriptions as
select
    id as event_id,
    ts as event_timestamp,
    case
        upper(type)
        when 'NEW'
        then 'SUBSCRIPTION_STARTED'
        when 'RENEW'
        then 'SUBSCRIPTION_RENEWED'
        when 'CANCEL'
        then 'SUBSCRIPTION_CANCELLED'
        else upper(type)
    end as event_type,
    cid as customer_uuid,
    mail as customer_email,
    signup_ts as customer_created_at,
    coalesce(nullif(ctry, ''), 'unknown') as country_code,
    state as region,
    null as city,
    zip as postal_code,
    case
        when plan like '%premium%'
        then 'premium'
        when plan like '%standard%'
        then 'standard'
        else 'unknown'
    end as subscription_type,
    case
        when plan like '%monthly%'
        then 'monthly'
        when plan like '%yearly%'
        then 'yearly'
        else 'unknown'
    end as renewal_period,
    upper(ccy) as currency,
    price as amount,
    tax as tax_amount,
    null as discount_code,
    null as affiliate_id,
    browser as device_type,
    null as app_version,
    null as session_id,
    null as internal_ref,
    'completed' as processing_status,
    'fenster' as platform
from raw_fenster_subscriptions
qualify row_number() over (partition by id order by ts desc) = 1
order by id, ts desc
;
