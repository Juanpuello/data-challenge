import logging
import os
from datetime import datetime
from pathlib import Path

from src.extraction_load import SubscriptionExtractor
from src.transformation import SubscriptionTransformer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    base_url = os.getenv("API_BASE_URL", "http://localhost:5050")
    data_dir = Path("data/raw")
    db_path = "data/warehouse.duckdb"
    assets_dir = Path("assets")
    full_refresh = os.getenv("FULL_REFRESH", "true").lower() == "true"

    extractor = SubscriptionExtractor(base_url=base_url, output_dir=data_dir)
    output_files = extractor.extract_all()
    logger.info(f"Extraction complete. Output files: {output_files}")

    with SubscriptionTransformer(
        db_path=db_path,
        data_dir=data_dir,
        assets_dir=assets_dir,
        full_refresh=full_refresh,
    ) as transformer:
        transformer.run_transformations()

        logger.info("CFO Report Preview (first 5 rows):")
        report = transformer.query_report(limit=5)
        for row in report:
            logger.info(row)

    logger.info(f"Pipeline completed at {datetime.now()}")


if __name__ == "__main__":
    main()
