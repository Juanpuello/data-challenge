import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterator, Optional

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

logger = logging.getLogger(__name__)

APFEL_SCHEMA = pa.schema(
    [
        ("event_id", pa.string()),
        ("affiliate_id", pa.string()),
        ("event_timestamp", pa.timestamp("us", tz="UTC")),
        ("event_type", pa.string()),
        ("customer_uuid", pa.string()),
        ("customer_email", pa.string()),
        ("customer_created_at", pa.timestamp("us", tz="UTC")),
        ("country_code", pa.string()),
        ("region", pa.string()),
        ("city", pa.string()),
        ("postal_code", pa.string()),
        ("subscription_type", pa.string()),
        ("renewal_period", pa.string()),
        ("currency", pa.string()),
        ("amount", pa.float64()),
        ("tax_amount", pa.float64()),
        ("discount_code", pa.string()),
        ("device_type", pa.string()),
        ("app_version", pa.string()),
        ("session_id", pa.string()),
        ("internal_ref", pa.string()),
        ("processing_status", pa.string()),
    ]
)

FENSTER_SCHEMA = pa.schema(
    [
        ("id", pa.string()),
        ("ts", pa.timestamp("us", tz="UTC")),
        ("type", pa.string()),
        ("cid", pa.string()),
        ("mail", pa.string()),
        ("signup_ts", pa.timestamp("us", tz="UTC")),
        ("ctry", pa.string()),
        ("state", pa.string()),
        ("zip", pa.string()),
        ("plan", pa.string()),
        ("ccy", pa.string()),
        ("price", pa.float64()),
        ("tax", pa.float64()),
        ("vat_id", pa.string()),
        ("campaign_src", pa.string()),
        ("utm_medium", pa.string()),
        ("utm_campaign", pa.string()),
        ("browser", pa.string()),
        ("os", pa.string()),
        ("screen_res", pa.string()),
        ("lang", pa.string()),
        ("tz", pa.string()),
        ("legacy_flag", pa.bool_()),
        ("migrated_from", pa.string()),
        ("batch_id", pa.string()),
        ("row_hash", pa.string()),
    ]
)

EXCHANGE_RATES_SCHEMA = pa.schema(
    [
        ("date", pa.timestamp("us", tz="UTC")),
        ("currency", pa.string()),
        ("rate_to_eur", pa.float64()),
    ]
)

TZ_MAP = {
    "AEST": "Australia/Sydney",
    "GMT": "UTC",
    "EST": "America/New_York",
    "CET": "Europe/Berlin",
    "PST": "America/Los_Angeles",
    "CST": "America/Chicago",
    "JST": "Asia/Tokyo",
}


class SubscriptionExtractor:
    """
    Extracts subscription data from API endpoints and writes to Parquet files.

    Parameters:
        base_url (str): Base URL for the API endpoints.
        output_dir (Path): Directory to write Parquet files.
        chunk_size (int): Number of rows to process per chunk for CSV files.
    """

    def __init__(
        self,
        base_url: str = "http://localhost:5050",
        output_dir: Optional[Path] = None,
        chunk_size: int = 500,
    ):
        self.base_url = base_url
        self.output_dir = output_dir or Path("data/raw")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.chunk_size = chunk_size

    def _make_utc(self, df: pd.DataFrame, column: str, tz_column: str) -> pd.Series:
        """
        Converts a datetime column to UTC using timezone information from another column.

        Parameters:
            df (pd.DataFrame): DataFrame containing the columns.
            column (str): Name of the datetime column to convert.
            tz_column (str): Name of the column containing timezone information.

        Returns:
            pd.Series: Datetime series converted to UTC.
        """
        result = []
        for _, row in df.iterrows():
            ts = pd.Timestamp(row[column])
            tz = row[tz_column]
            if pd.notna(tz):
                ts = ts.tz_localize(tz).tz_convert("UTC")
            else:
                raise ValueError(f"Missing timezone for row with {column}: {ts}")
            result.append(ts)
        return pd.Series(result, index=df.index)

    def extract_apfel_subscriptions(self) -> pd.DataFrame:
        """
        Extracts Apfel subscription data from API and applies transformations.

        Returns:
            pd.DataFrame: Transformed Apfel subscription data.

        Raises:
            Exception: If API request fails.
        """
        url = f"{self.base_url}/apfel/subscriptions"
        response = requests.get(url, timeout=30)

        if response.status_code != 200:
            raise Exception(
                f"Apfel API request failed with status code: {response.status_code}"
            )

        df = pd.DataFrame(response.json()["events"])
        logger.info(f"Apfel subscriptions extracted: {df.shape[0]} rows")

        df = df.replace("", np.nan)

        date_columns = ["customer_created_at", "event_timestamp"]
        for col in date_columns:
            df[col] = pd.to_datetime(df[col], errors="raise", utc=True)

        numeric_columns = ["amount", "tax_amount"]
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors="raise")

        return df

    def _transform_fenster_chunk(self, chunk: pd.DataFrame) -> pd.DataFrame:
        """
        Applies transformations to a Fenster subscription data chunk.

        Parameters:
            chunk (pd.DataFrame): Raw chunk from CSV reader.

        Returns:
            pd.DataFrame: Transformed chunk.
        """
        chunk = chunk.replace("", np.nan)

        chunk["tz_clean"] = chunk["tz"].map(TZ_MAP)
        date_columns = ["ts", "signup_ts"]
        for col in date_columns:
            chunk[col] = pd.to_datetime(chunk[col], errors="raise")
            chunk[col] = self._make_utc(chunk, col, "tz_clean")

        chunk = chunk.drop(columns=["tz_clean"])

        numeric_columns = ["price", "tax"]
        for col in numeric_columns:
            chunk[col] = pd.to_numeric(chunk[col], errors="raise")

        chunk["legacy_flag"] = chunk["legacy_flag"].map(
            {"true": True, "false": False, True: True, False: False}
        )

        return chunk

    def extract_fenster_subscriptions_chunked(self) -> Iterator[pd.DataFrame]:
        """
        Extracts Fenster subscription data from API in chunks.

        Yields:
            pd.DataFrame: Transformed Fenster subscription data chunks.
        """
        url = f"{self.base_url}/fenster/subscriptions"

        for chunk in pd.read_csv(url, chunksize=self.chunk_size):
            yield self._transform_fenster_chunk(chunk)

    def _transform_exchange_rates_chunk(self, chunk: pd.DataFrame) -> pd.DataFrame:
        """
        Applies transformations to an exchange rates data chunk.

        Parameters:
            chunk (pd.DataFrame): Raw chunk from CSV reader.

        Returns:
            pd.DataFrame: Transformed chunk.
        """
        chunk["date"] = pd.to_datetime(chunk["date"], errors="raise", utc=True)
        return chunk

    def extract_exchange_rates_chunked(self) -> Iterator[pd.DataFrame]:
        """
        Extracts exchange rate data from API in chunks.

        Yields:
            pd.DataFrame: Transformed exchange rate data chunks.
        """
        url = f"{self.base_url}/exchange-rates"

        for chunk in pd.read_csv(url, chunksize=self.chunk_size):
            yield self._transform_exchange_rates_chunk(chunk)

    def write_parquet(
        self,
        df: pd.DataFrame,
        filename: str,
        schema: pa.Schema,
    ) -> Path:
        """
        Writes DataFrame to Parquet file with predefined schema.

        Parameters:
            df (pd.DataFrame): DataFrame to write.
            filename (str): Output filename (without extension).
            schema (pa.Schema): PyArrow schema for the Parquet file.

        Returns:
            Path: Path to the written Parquet file.
        """
        output_path = self.output_dir / f"{filename}.parquet"

        table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)

        pq.write_table(table, output_path)
        logger.info(f"Written {len(df)} rows to {output_path}")

        return output_path

    def write_parquet_chunked(
        self,
        chunks: Iterator[pd.DataFrame],
        filename: str,
        schema: pa.Schema,
    ) -> Path:
        """
        Writes DataFrame chunks to Parquet file incrementally.

        Parameters:
            chunks (Iterator[pd.DataFrame]): Iterator yielding DataFrame chunks.
            filename (str): Output filename (without extension).
            schema (pa.Schema): PyArrow schema for the Parquet file.

        Returns:
            Path: Path to the written Parquet file.
        """
        output_path = self.output_dir / f"{filename}.parquet"
        writer = None
        total_rows = 0

        for chunk in chunks:
            table = pa.Table.from_pandas(chunk, schema=schema, preserve_index=False)

            if writer is None:
                writer = pq.ParquetWriter(output_path, schema)

            writer.write_table(table)
            total_rows += len(chunk)

            logger.info(
                f"Written chunk of {len(chunk)} rows to {output_path} (total so far: {total_rows})"
            )

        if writer is not None:
            writer.close()

        logger.info(f"Written {total_rows} rows to {output_path}")
        return output_path

    def extract_all(self) -> Dict[str, Path]:
        """
        Extracts all data sources and writes to Parquet files.

        Returns:
            Dict[str, Path]: Dictionary mapping source names to output file paths.
        """
        output_files = {}

        df_apfel = self.extract_apfel_subscriptions()
        output_files["apfel_subscriptions"] = self.write_parquet(
            df_apfel, "apfel_subscriptions", APFEL_SCHEMA
        )

        fenster_chunks = self.extract_fenster_subscriptions_chunked()
        output_files["fenster_subscriptions"] = self.write_parquet_chunked(
            fenster_chunks, "fenster_subscriptions", FENSTER_SCHEMA
        )

        rates_chunks = self.extract_exchange_rates_chunked()
        output_files["exchange_rates"] = self.write_parquet_chunked(
            rates_chunks, "exchange_rates", EXCHANGE_RATES_SCHEMA
        )

        return output_files


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    extractor = SubscriptionExtractor(
        base_url="http://localhost:5050",
        output_dir=Path("data/raw"),
    )

    logger.info(f"Starting extraction at {datetime.now()}")
    output_files = extractor.extract_all()
    logger.info(f"Extraction completed. Output files: {output_files}")
