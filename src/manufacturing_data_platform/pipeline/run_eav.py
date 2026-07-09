from __future__ import annotations

import argparse
import json

from manufacturing_data_platform.db import get_database
from manufacturing_data_platform.pipeline.eav import result_to_dict, run_eav_pipeline


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the EAV mini pipeline (wide -> EAV -> gold).")
    parser.add_argument("--raw-dir", default="data/raw/eav")
    parser.add_argument("--mapping-dir", default="config/eav_mappings")
    parser.add_argument("--output-dir", default="data/lakehouse_eav")
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
    result = run_eav_pipeline(
        raw_dir=args.raw_dir,
        mapping_dir=args.mapping_dir,
        output_dir=args.output_dir,
        business_date=args.business_date,
        db=db,
        catalog_backend=args.catalog_backend,
    )
    print(json.dumps(result_to_dict(result), indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
