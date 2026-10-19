import io
import json
import os
from datetime import datetime, timezone

import pandas as pd
import requests
from dotenv import load_dotenv
from google.cloud import storage


# ============================================================
# CONFIGURATION
# ============================================================

load_dotenv()

EIA_API_KEY = os.getenv("EIA_API_KEY")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
GCS_RAW_PREFIX = os.getenv("GCS_RAW_PREFIX", "raw/eia")

EIA_API_URL = "https://api.eia.gov/v2/petroleum/crd/crpdn/data/"


# ============================================================
# VALIDATION
# ============================================================

def validate_config():
    """Validate required environment variables."""

    required = {
        "EIA_API_KEY": EIA_API_KEY,
    }

    missing = [
        name
        for name, value in required.items()
        if not value
    ]

    if missing:
        raise ValueError(
            f"Missing required environment variables: {', '.join(missing)}"
        )


# ============================================================
# TIMESTAMP
# ============================================================

def get_ingestion_timestamp():
    """Return current UTC timestamp."""

    return datetime.now(timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


# ============================================================
# EIA API EXTRACTION
# ============================================================

def fetch_eia_data():
    """Extract annual petroleum data from the EIA API."""

    params = [
        ("api_key", EIA_API_KEY),
        ("frequency", "annual"),
        ("data[0]", "value"),
        ("start", "2020"),
        ("end", "2026"),
        ("sort[0][column]", "period"),
        ("sort[0][direction]", "desc"),
        ("offset", "0"),
        ("length", "5000"),
    ]

    print("Requesting data from EIA API...")

    response = requests.get(
        EIA_API_URL,
        params=params,
        timeout=60,
    )

    response.raise_for_status()

    payload = response.json()

    if "response" not in payload:
        raise ValueError(
            "Unexpected EIA API response format."
        )

    print("EIA API extraction successful.")

    return payload


# ============================================================
# LOCAL TRANSFORMATION
# ============================================================

def transform_eia_data(payload):
    """
    Transform the EIA API response locally using Pandas.
    """

    response_data = payload["response"]

    records = response_data.get("data", [])

    if not records:
        raise ValueError(
            "EIA API returned no records."
        )

    df = pd.DataFrame(records)

    print(f"Records extracted: {len(df)}")

    # Add ingestion metadata
    ingestion_timestamp = get_ingestion_timestamp()

    df["ingestion_timestamp"] = ingestion_timestamp
    df["source"] = "EIA API"

    # Convert value to numeric
    if "value" in df.columns:
        df["value"] = pd.to_numeric(
            df["value"],
            errors="coerce"
        )

    # Convert period to integer where possible
    if "period" in df.columns:
        df["period"] = pd.to_numeric(
            df["period"],
            errors="coerce"
        ).astype("Int64")

    # Remove completely empty rows
    df = df.dropna(
        how="all"
    ).reset_index(drop=True)

    print(
        f"Records after transformation: {len(df)}"
    )

    return df


# ============================================================
# SAVE LOCALLY
# ============================================================

def save_local_files(payload, df):
    """Save raw JSON and transformed CSV/Parquet locally."""

    timestamp = datetime.now(
        timezone.utc
    ).strftime("%Y%m%d_%H%M%S")

    raw_directory = "data/raw/eia"
    processed_directory = "data/processed/eia"

    os.makedirs(
        raw_directory,
        exist_ok=True
    )

    os.makedirs(
        processed_directory,
        exist_ok=True
    )

    # --------------------------------------------------------
    # Raw JSON
    # --------------------------------------------------------

    raw_path = (
        f"{raw_directory}/"
        f"eia_petroleum_{timestamp}.json"
    )

    with open(
        raw_path,
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            payload,
            file,
            indent=2
        )

    # --------------------------------------------------------
    # Processed CSV
    # --------------------------------------------------------

    csv_path = (
        f"{processed_directory}/"
        f"eia_petroleum_{timestamp}.csv"
    )

    df.to_csv(
        csv_path,
        index=False
    )

    # --------------------------------------------------------
    # Processed Parquet
    # --------------------------------------------------------

    parquet_path = (
        f"{processed_directory}/"
        f"eia_petroleum_{timestamp}.parquet"
    )

    df.to_parquet(
        parquet_path,
        index=False
    )

    print(f"Raw JSON saved: {raw_path}")
    print(f"CSV saved: {csv_path}")
    print(f"Parquet saved: {parquet_path}")

    return {
        "raw": raw_path,
        "csv": csv_path,
        "parquet": parquet_path,
    }


# ============================================================
# GCS UPLOAD
# ============================================================

def upload_to_gcs(
    local_files,
    ingestion_date
):
    """Upload raw and processed files to Google Cloud Storage."""

    print("Connecting to Google Cloud Storage...")

    client = storage.Client(
        project=GCP_PROJECT_ID
    )

    bucket = client.bucket(
        GCS_BUCKET_NAME
    )

    year = ingestion_date.strftime("%Y")
    month = ingestion_date.strftime("%m")
    day = ingestion_date.strftime("%d")

    # --------------------------------------------------------
    # Raw JSON
    # --------------------------------------------------------

    raw_blob_name = (
        f"{GCS_RAW_PREFIX}/"
        f"petroleum/"
        f"year={year}/"
        f"month={month}/"
        f"day={day}/"
        f"{os.path.basename(local_files['raw'])}"
    )

    raw_blob = bucket.blob(
        raw_blob_name
    )

    raw_blob.upload_from_filename(
        local_files["raw"],
        content_type="application/json"
    )

    print(
        f"Uploaded raw JSON: gs://"
        f"{GCS_BUCKET_NAME}/"
        f"{raw_blob_name}"
    )

    # --------------------------------------------------------
    # Processed CSV
    # --------------------------------------------------------

    csv_blob_name = (
        f"processed/eia/"
        f"petroleum/"
        f"year={year}/"
        f"month={month}/"
        f"day={day}/"
        f"{os.path.basename(local_files['csv'])}"
    )

    csv_blob = bucket.blob(
        csv_blob_name
    )

    csv_blob.upload_from_filename(
        local_files["csv"],
        content_type="text/csv"
    )

    print(
        f"Uploaded CSV: gs://"
        f"{GCS_BUCKET_NAME}/"
        f"{csv_blob_name}"
    )

    # --------------------------------------------------------
    # Processed Parquet
    # --------------------------------------------------------

    parquet_blob_name = (
        f"processed/eia/"
        f"petroleum/"
        f"year={year}/"
        f"month={month}/"
        f"day={day}/"
        f"{os.path.basename(local_files['parquet'])}"
    )

    parquet_blob = bucket.blob(
        parquet_blob_name
    )

    parquet_blob.upload_from_filename(
        local_files["parquet"],
        content_type="application/octet-stream"
    )

    print(
        f"Uploaded Parquet: gs://"
        f"{GCS_BUCKET_NAME}/"
        f"{parquet_blob_name}"
    )


# ============================================================
# MAIN PIPELINE
# ============================================================

def main():

    print("=" * 60)
    print("GCP ENERGY LAKEHOUSE - EIA INGESTION")
    print("=" * 60)

    # 1. Validate configuration
    validate_config()

    # 2. Extract from EIA
    payload = fetch_eia_data()

    # 3. Transform locally
    df = transform_eia_data(
        payload
    )

    # 4. Save locally
    local_files = save_local_files(
        payload,
        df
    )

    # 5. Upload to GCS
    ingestion_date = datetime.now(
        timezone.utc
    )

    upload_to_gcs(
        local_files,
        ingestion_date
    )

    print("=" * 60)
    print("INGESTION COMPLETED SUCCESSFULLY")
    print("=" * 60)


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()