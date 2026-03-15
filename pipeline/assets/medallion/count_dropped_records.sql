-- Count records dropped due to missing exchange rates
-- Used for logging/alerting purposes
select count(*) as dropped_count
from bronze_subscriptions s
left join
    bronze_exchange_rates r
    on upper(s.currency) = upper(r.currency)
    and date_trunc('month', s.event_timestamp) = date_trunc('month', r.date)
where s.currency != 'EUR' and r.rate_to_eur is null
;
