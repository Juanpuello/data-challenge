## Running the Infrastructure

### 1. Start the stack

From the repo root:

```bash
docker compose up --build
```

Output files are written directly to `pipeline/data/` on your host machine (bind mount), so they are available immediately after the run.

### 2. Re-run the pipeline

The pipeline container exits after one successful run. To trigger it again (e.g. after the source data changes):

```bash
docker compose run --rm pipeline
```

To run without a full refresh (append-only raw load):

```bash
docker compose run --rm -e FULL_REFRESH=false pipeline
```

### 3. Query the results

The pipeline writes directly to `pipeline/data/` on your host machine. Open the DuckDB file with the [DuckDB CLI](https://duckdb.org/docs/installation/) or any DuckDB-compatible tool like DBeaver or DataGrip:

```bash
duckdb pipeline/data/warehouse.duckdb
```

---

## EDA (Took like 30 minutes)

Using jupyter notebook for exploring the datasets. Plain pandas.

### 1. Apfel Subscriptions findings:
- Needs to be handled for `''` strings from the json, as well as numerics
- Country code should be filled for nulls to `'unknown'`
- Unique key is `event_id` so we should implement an index on this one and on `event_timestamp` to deduplicate the raw data if necessary

| Column | Values | Notes |
|--------|--------|-------|
| `subscription_type` | `'standard'`, `'premium'` | |
| `event_type` | `'SUBSCRIPTION_STARTED'`, `'SUBSCRIPTION_RENEWED'`, `'SUBSCRIPTION_CANCELLED'`, `'subscription_started'`, `'subscription_renewed'`, `'subscription_cancelled'` | should be normalized |
| `renewal_period` | `'monthly'`, `'yearly'` | to calculate MRR |
| `currency` | `'EUR'`, `'USD'`, `'GBP'`, `'Euro'`, `'eur'` | we have no GBP available in endpoint so these wouldn't appear in the final report |
| `processing_status` | `'completed'`, `'COMPLETED'`, `'pending'`, `'processing'`, `'failed'` | these should be considered in the final report |

### 2. Fenster Subscriptions
- Needs to be handled for `''` strings, as well as numeric
- Timestamps seem to be timezone aware so we need to convert to UTC to match Apfel
- Country code should be filled for nulls to `'unknown'`

| Column | Values | Notes |
|--------|--------|-------|
| `plan` | `'premium_monthly'`, `'standard_yearly'`, `'standard_monthly'`, `'premium_yearly'` | includes both subscription type and renewal interval |
| `type` | `'new'`, `'renew'`, `'cancel'`, `'RENEW'`, `'NEW'` | should be normalized |
| `ccy` | `'USD'` | only USD available |

### 3. Exchange Rates

Just needed a slight modification for date, I'm assuming dates are UTC

---

## Pipeline Architecture (~1 hour)

I wanted to understand clickhouse but decided to go with DuckDB for simplicity.

### Package Management

uv as a package manager for portability:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### Processing Model

This will be a batch pipeline, no real time needed as the analysis is monthly.

### Storage

For this data size parquet files with a well defined schema and with the correct partitioning would work perfectly. For this I'll use pyarrow parquet, create a parquet writter from pandas dataframe in a batch fashion.

### Analytical Engine

DuckDB running in a docker container for an analytical engine that can run locally by reading the parquet files and that is queryable.

### Data Modeling

For modelling lets use dbt for medallion architecture on top of duckdb.

**Medallion layers:**
- **Bronze:** normalization and deduplication
- **Silver:** star schema where we have a fact table of subscription events
- **Gold:** the final report needed for the CFO

Each layer will have its tests. We should have built in dbt utils tests or dbt expectation tests for all columns. Maybe great expectations. Maybe setting both of those frameworks wouldn't fit into the time constrain.

### Star Schema Design

**Dimension tables:**
- customers
- subscription type + renewal frequency
- country
- currency

**Fact tables:**
- subscription event fact table
- currency evolution table

Both fact tables would be connected by the currency dimension so we can properly calculate the report MRR in EUR. Missing currencies should trigger an alert and would be dropped from the final report until fixed. We only have USD/EUR exchange rate so any other currency would fail.

### Idempotency

The pipeline should be idempotent, so I prefer to implement this within the warehouse or databse I'll use. That way I can keep the whole raw data.

### Docker Setup

The pipeline should be running in a docker container that runs both the pipeline and duckdb. This docker container starts the pipeline which will extract data from endpoints and dump to parquet files. Parquet files are loaded into DuckDB landing tables using COPY command. Add docker file in the docker compose file so both are in the same network.

Example docker file
```
FROM python:3.13.11-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

WORKDIR /code
ENV PATH="/code/.venv/bin:$PATH"

COPY pyproject.toml .python-version uv.lock ./
RUN uv sync --locked


COPY pipeline.py .
ENTRYPOINT ["uv", "run", "python", "pipeline.py"]
```

Rest of the time went to actual coding, I didn't keep count but it was more than 3 hours for sure.

---

## Implementation Decisions (Time-Boxed)

*Important Note: This chunk was generated with Claude after working in the above sections. At first I intended to create a more complete solution with a star schema and dbt. Time wasn't enough so I lowered the bar, and decided to follow Pablo instructions of not fully leveraging AI. Bellow is an artifact from my convo with Cluade and the intial scope of my solution*

Given the 3-hour time constraint, I intended to deliver a working end-to-end solution while documenting what I would do differently in production.

### Business Rules

| Rule | Decision | Rationale |
|------|----------|-----------|
| **Processing Status (Apfel)** | Only include events with `processing_status` in `['completed', 'COMPLETED']` | Failed/pending/processing events should not count toward financial metrics. Revenue isn't recognized until processing completes. |
| **Processing Status (Fenster)** | Include all records | Fenster dataset has no status column. Assumption: all events are successfully processed. This should be validated with the Fenster team in production. |
| **GBP Currency** | Dropped from report with logged warning | Only ~few records have GBP. No GBP→EUR exchange rate available in the `/exchange-rates` endpoint. Time constraint prevents implementing a fallback. Documented for transparency. |
| **MRR Calculation** | `MRR = amount / 12` for yearly subscriptions, `amount` for monthly | Yearly revenue normalized to monthly equivalent. Cancelled subscriptions excluded from MRR per requirements. |
| **Country Nulls** | Filled with `'unknown'` | Preserves data for aggregation while flagging data quality issue. |

### Idempotency Strategy

**Approach:** Full table replace with deduplication via `ROW_NUMBER()` within DuckDB. 

```sql
with ranked as (
    select *,
        row_number() over (
            partition by event_id 
            order by event_timestamp desc
        ) as rn
    from raw_apfel_subscriptions
)

select * from ranked where rn = 1
```

**Why this approach:**
- Simple to implement and reason about
- No complex merge/upsert logic required, which is not beneficial for OLAP databases anyway
- Raw data preserved; deduplication happens at query time. As one of the datasets contains a batch_id column it seems better to save the whole raw dataset. In the future we might need to revert to a previous batch id or app version if a bug was introduced in the current release.

Obviously this means we have full table scans on each run. Acceptable for this data volume; would use incremental models in production. If data is sorted before being inserted performance for full scans would be better.

### Simplified Architecture (Implemented)

```
API Endpoints
    ↓
Python ingest (requests + pandas)
    ↓
Parquet files (pyarrow + raw landing)
    ↓
DuckDB:
  └── bronze: UNION sources, normalize case, deduplicate, business logic for maps
  └── gold: CFO report aggregation with MRR in EUR
```

**What I skipped (time constraint):**
- Silver layer with star schema
- Dimension tables: customer, subscription_type(plan + renewal interval), country, currency
- Fact tables: subscirption events, exhchange rate evolution
- dbt project structure: dbt utils would be used for testing in all medallion layers

### Production Recommendations

If building this for production, I would add:

1. **dbt for transformation orchestration**
    - Modular SQL models with dependency management
    - Built-in `dbt_utils` tests: `unique`, `not_null`, `accepted_values`, `relationships`
    - Custom tests for MRR reconciliation

2. **Data quality framework** (one of):
    - `dbt-expectations` for GX-style assertions in dbt
    - `great_expectations` standalone for profiling and validation
    - `soda-core` for lightweight checks

3. **Missing currency alerting**
    - Slack/email notification when records dropped due to missing exchange rates
    - Dashboard showing % of revenue excluded

4. **Incremental processing**
    - `dbt incremental` models with `event_timestamp` watermark
    - Incremental retrieval if APIs support it

5. **Observability**: this could be implemented by adding a proper orchestration tool like Dagster or Prefect
    - Row counts per layer logged
    - Data freshness monitoring
    - Lineage tracking <- `dbt docs` already provides lineage graphs