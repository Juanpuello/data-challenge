create or replace table bronze_exchange_rates as
select date, upper(currency) as currency, rate_to_eur
from raw_exchange_rates
qualify row_number() over (partition by date, currency order by date desc) = 1
order by date desc, currency
;
