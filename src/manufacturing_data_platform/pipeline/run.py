from __future__ import annotations

import argparse
import json

from manufacturing_data_platform.db import get_database
from manufacturing_data_platform.pipeline.lakehouse import result_to_dict, run_lakehouse_pipeline


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the mini lakehouse pipeline.")
    parser.add_argument("--raw-path", default="data/raw/manufacturing_events.csv")
    parser.add_argument("--output-dir", default="data/lakehouse")
    parser.add_argument("--business-date", default=None)
    parser.add_argument(
        "--catalog-backend",
        choices=["mongo", "json"],
        default="mongo",
        help="Use mongo for the real catalog, or json for offline demos/tests.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    db = get_database() if args.catalog_backend == "mongo" else None
    result = run_lakehouse_pipeline(
        raw_path=args.raw_path,
        output_dir=args.output_dir,
        business_date=args.business_date,
        db=db,
        catalog_backend=args.catalog_backend,
    )
    print(json.dumps(result_to_dict(result), indent=2))


if __name__ == "__main__":
    main()

