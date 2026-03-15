create or replace table bronze_apfel_subscriptions as
select
    event_id,
    event_timestamp,
    upper(event_type) as event_type,
    customer_uuid,
    customer_email,
    customer_created_at,
    coalesce(nullif(country_code, ''), 'unknown') as country_code,
    region,
    city,
    postal_code,
    lower(subscription_type) as subscription_type,
    lower(renewal_period) as renewal_period,
    case 
        when currency = 'Euro' 
        then 'EUR' 
        else upper(currency) 
    end as currency,
    amount,
    tax_amount,
    discount_code,
    affiliate_id,
    device_type,
    app_version,
    session_id,
    internal_ref,
    upper(processing_status) as processing_status,
    'apfel' as platform
from raw_apfel_subscriptions
where upper(processing_status) = 'COMPLETED'
qualify row_number() over (partition by event_id order by event_timestamp desc) = 1
order by event_id, event_timestamp desc
;
