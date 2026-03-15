create table if not exists raw_exchange_rates (
    date timestamptz,
    currency varchar,
    rate_to_eur double
);
