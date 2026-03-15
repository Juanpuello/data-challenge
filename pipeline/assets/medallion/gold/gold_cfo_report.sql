create or replace view gold_cfo_report as
with
    subscriptions_with_rates as (
        select
            sub.*,
            date_trunc('month', sub.event_timestamp) as report_month,
            case
                when sub.currency = 'EUR'
                then 1.0
                else exc.rate_to_eur
            end as effective_rate
        from bronze_subscriptions sub
        left join
            bronze_exchange_rates exc
            on sub.currency = exc.currency
            and date_trunc('month', sub.event_timestamp) = date_trunc('month', exc.date)
    ),

    aggregated as (
        select
            report_month,
            platform,
            subscription_type,
            country_code as country,
            count(
                case when event_type = 'SUBSCRIPTION_STARTED' then 1 end
            ) as acquisitions,
            count(case when event_type = 'SUBSCRIPTION_RENEWED' then 1 end) as renewals,
            count(
                case when event_type = 'SUBSCRIPTION_CANCELLED' then 1 end
            ) as cancellations,
            sum(
                case
                    when event_type = 'SUBSCRIPTION_CANCELLED'
                    then 0
                    when renewal_period = 'yearly'
                    then (amount / 12.0) * effective_rate
                    else amount * effective_rate
                end
            ) as mrr_eur
        from subscriptions_with_rates
        where effective_rate is not null
        group by report_month, platform, subscription_type, country_code
    )

select
    report_month,
    platform,
    subscription_type,
    country,
    acquisitions,
    renewals,
    cancellations,
    round(mrr_eur, 2) as mrr_eur
from aggregated
order by report_month, platform, country, subscription_type
;
