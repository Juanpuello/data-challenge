import logging
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)

ALLOWED_TABLE_NAMES = frozenset(
    [
        "raw_apfel_subscriptions",
        "raw_fenster_subscriptions",
        "raw_exchange_rates",
        "bronze_apfel_subscriptions",
        "bronze_fenster_subscriptions",
        "bronze_exchange_rates",
        "bronze_subscriptions",
        "gold_cfo_report",
    ]
)


def validate_table_name(table_name: str) -> str:
    """
    Validate table name against whitelist.

    Args:
        table_name: The table name to validate.

    Returns:
        The validated table name.

    Raises:
        ValueError: If table name is not in whitelist.
    """
    if table_name not in ALLOWED_TABLE_NAMES:
        raise ValueError(f"Table name '{table_name}' is not in allowed list")
    return table_name


class SubscriptionTransformer:
    """
    Transforms subscription data through medallion architecture layers.

    Parameters:
        db_path (str): Path to DuckDB database file.
        data_dir (Path): Directory containing parquet files.
        assets_dir (Path): Directory containing SQL asset files.
    """

    def __init__(
        self,
        db_path: str = "data/warehouse.duckdb",
        data_dir: Path | None = None,
        assets_dir: Path | None = None,
        full_refresh: bool = False,
    ):
        self.db_path = db_path
        self.data_dir = data_dir or Path("data/raw")
        self.assets_dir = assets_dir or Path("assets")
        self.full_refresh = full_refresh
        self.conn: duckdb.DuckDBPyConnection = duckdb.connect(self.db_path)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()
            self.conn = None  # type: ignore[assignment]
        return False

    def _load_sql(self, subdir: str, filename: str) -> str:
        """
        Load SQL from asset file.

        Args:
            subdir: Subdirectory in assets (ddl, loading, medallion).
            filename: Name of the SQL file.

        Returns:
            SQL content as string.
        """
        sql_path = self.assets_dir / subdir / filename
        if not sql_path.exists():
            raise FileNotFoundError(f"SQL asset not found: {sql_path}")
        return sql_path.read_text()

    def load_raw_tables(self):
        """
        Load parquet files into DuckDB raw tables.

        Creates tables using DDL scripts, then loads data using COPY commands.
        Uses validated table names.
        """
        tables = [
            ("raw_apfel_subscriptions", "apfel_subscriptions.parquet"),
            ("raw_fenster_subscriptions", "fenster_subscriptions.parquet"),
            ("raw_exchange_rates", "exchange_rates.parquet"),
        ]

        self.conn.begin()
        try:
            for table_name, parquet_file in tables:
                parquet_path = self.data_dir / parquet_file
                if not parquet_path.exists():
                    raise FileNotFoundError(f"Parquet file not found: {parquet_path}")

                validated_table = validate_table_name(table_name)
                ddl_sql = self._load_sql("ddl", f"{validated_table}.sql")
                self.conn.execute(ddl_sql)

                if self.full_refresh:
                    self.conn.execute(f"truncate {validated_table};")
                    logger.info(f"Truncated {validated_table} for full refresh")

                copy_sql = self._load_sql("loading", "generic_copy.sql").format(
                    table_name=validated_table
                )
                row = self.conn.execute(
                    copy_sql, {"file_path": str(parquet_path.resolve())}
                ).fetchone()
                count = row[0]
                logger.info(f"Loaded {count} rows into {validated_table}")
            self.conn.commit()
        except Exception as exc:
            self.conn.rollback()
            raise Exception(f"Error loading raw tables: {exc}") from exc

    def create_bronze_layer(self):
        """
        Create bronze layer with normalized and deduplicated data.
        """
        self.conn.begin()
        try:
            sql = self._load_sql("medallion/bronze", "bronze_apfel_subscriptions.sql")
            self.conn.execute(sql)
            logger.info("Created bronze_apfel_subscriptions table")

            sql = self._load_sql("medallion/bronze", "bronze_fenster_subscriptions.sql")
            self.conn.execute(sql)
            logger.info("Created bronze_fenster_subscriptions table")

            sql = self._load_sql("medallion/bronze", "bronze_exchange_rates.sql")
            self.conn.execute(sql)
            logger.info("Created bronze_exchange_rates table")

            sql = self._load_sql("medallion/bronze", "bronze_subscriptions.sql")
            self.conn.execute(sql)
            logger.info("Created bronze_subscriptions table")
            self.conn.commit()
        except Exception as exc:
            self.conn.rollback()
            raise Exception(f"Error creating bronze layer: {exc}") from exc

    def create_gold_layer(self):
        """
        Create gold layer with CFO report.

        Defined as a view — always reflects current bronze layer state. Would need
        to be promoted to a table if data size is too large.
        """
        self.conn.begin()
        try:
            sql = self._load_sql("medallion/gold", "gold_cfo_report.sql")
            self.conn.execute(sql)
            logger.info("Created gold_cfo_report view")

            sql = self._load_sql("medallion", "count_dropped_records.sql")
            row = self.conn.execute(sql).fetchone()
            dropped_count = row[0]
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

        if dropped_count > 0:
            logger.warning(
                f"Dropped {dropped_count} records due to missing exchange rates "
                "(likely GBP - no GBP→EUR rate available)"
            )  # To-do: Implement alerting thorugh Slack or email.

    def run_transformations(self):
        """Run all transformation layers in sequence."""
        logger.info("Starting transformations")

        self.load_raw_tables()
        self.create_bronze_layer()
        self.create_gold_layer()

        row = self.conn.execute("select count(*) from gold_cfo_report;").fetchone()
        assert row is not None
        report_count = row[0]
        logger.info(f"Transformation complete. Gold report has {report_count} rows")

    def query_report(self, limit: int = 100) -> list:
        """
        Query the CFO report.

        Parameters:
            limit (int): Maximum rows to return.

        Returns:
            list: Report rows as dictionaries.
        """
        result = self.conn.execute(
            "select * from gold_cfo_report limit ?;",
            [limit],
        ).fetchdf()
        return result.to_dict(orient="records")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    with SubscriptionTransformer(
        db_path="data/warehouse.duckdb",
        data_dir=Path("data/raw"),
        full_refresh=True,
    ) as transformer:
        transformer.run_transformations()

        logger.info("CFO Report Preview:")
        report = transformer.query_report(limit=10)
        for row in report:
            print(row)
