import argparse
from pathlib import Path
from src.pipelines.extract import run as extract_raw
from src.pipelines.transform import transform as transform_raw
from src.pipelines.load import load as load_processed

def step_extract():
    """Extract raw data from MovieLens + OMDb"""
    print("Starting extract step...")
    extract_raw()
    print("Extract step completed.\n")

def step_transform():
    """Transform raw CSVs into clean processed data"""
    print(" Starting transform step...")
    root = Path(__file__).resolve().parents[0]
    raw_dir = root / "data" / "raw"
    out_dir = root / "data" / "processed"
    transform_raw(raw_dir, out_dir)
    print("Transform step completed.\n")

def step_load():
    """Apply schema (idempotent) and load processed data into the database"""
    root = Path(__file__).resolve().parents[0]
    processed = root / "data" / "processed"
    load_processed(processed)
    print("Load step completed.\n")

def main():
    parser = argparse.ArgumentParser(description="Run ETL pipeline steps")
    parser.add_argument(
        "--steps",
        nargs="+",
        default=["extract", "transform", "load"],
        help="Steps to run: extract, transform, load"
    )
    args = parser.parse_args()

    for step in args.steps:
        if step == "extract":
            step_extract()
        elif step == "transform":
            step_transform()
        elif step == "load":
            step_load()
        else:
            print(f"[unknown] Step '{step}' not recognized.")

if __name__ == "__main__":
    main()
