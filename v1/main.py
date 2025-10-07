#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Export a single MONTH_ID shard from BigQuery to S3 in CSV format (optionally gzipped)
using pandas and the BigQuery Storage API.

Workflow:
  1. Query BigQuery (filtered by MONTH_ID) into a temporary table.
  2. Stream that table via the BigQuery Storage API into pandas DataFrames.
  3. Concatenate and write a CSV or CSV.GZ file with pandas.
  4. Upload to S3 (temp -> final rename) and write a manifest.

Author: YourName
"""

import argparse
import hashlib
import json
import logging
import os
import sys
import tempfile
import time
import boto3
import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig, WriteDisposition
from google.cloud.bigquery_storage import BigQueryReadClient
from google.cloud.bigquery_storage import types

# ---------- Logging ----------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("bq-month-to-s3")


def sha256_file(path: str) -> str:
    """Return sha256 hex digest of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def s3_atomic_upload(local_path: str, bucket: str, tmp_key: str, final_key: str, manifest: dict):
    """Upload local file to S3 via .tmp -> finalize rename and manifest write."""
    s3 = boto3.client("s3")

    # Upload to .tmp
    s3.upload_file(local_path, bucket, tmp_key)
    head = s3.head_object(Bucket=bucket, Key=tmp_key)
    size = head["ContentLength"]

    # Upload manifest
    s3.put_object(
        Bucket=bucket,
        Key=os.path.join(os.path.dirname(final_key), "_manifest.json"),
        Body=json.dumps(manifest, indent=2).encode("utf-8"),
        ContentType="application/json",
    )

    # Atomic finalize: copy -> delete tmp
    s3.copy({"Bucket": bucket, "Key": tmp_key}, bucket, final_key)
    s3.delete_object(Bucket=bucket, Key=tmp_key)
    return size


def export_month_to_s3(
    month_id: str,
    base_query: str,
    bq_project: str,
    bq_location: str,
    scratch_dataset: str,
    scratch_table_prefix: str,
    s3_bucket: str,
    s3_prefix: str,
    execution_id: str,
    gzip_output: bool = True,
    delete_temp_table: bool = False,
):
    """Run query, export results with pandas, upload to S3."""
    bq_client = bigquery.Client(project=bq_project, location=bq_location)
    bq_storage_client = BigQueryReadClient()

    # Compose query
    sql = f"SELECT * FROM ({base_query}) WHERE MONTH_ID = '{month_id}'"
    query_hash = hashlib.sha256(sql.encode("utf-8")).hexdigest()

    # Temporary destination table
    scratch_table = f"{scratch_table_prefix}{execution_id.replace(':', '_').replace('-', '_')}_{month_id}"
    dest_ref = f"{bq_project}.{scratch_dataset}.{scratch_table}"

    # Run query into temp table
    logger.info("Executing query for MONTH_ID=%s into %s", month_id, dest_ref)
    job_cfg = QueryJobConfig(
        destination=dest_ref,
        write_disposition=WriteDisposition.WRITE_TRUNCATE,
        use_query_cache=True,
    )
    job = bq_client.query(sql, job_config=job_cfg, location=bq_location)
    job.result()  # wait for completion
    logger.info("Query finished; exporting via BQ Storage API")

    # Stream table via BigQuery Storage API into pandas
    dfs = []
    session = bq_storage_client.create_read_session(
        parent=f"projects/{bq_project}",
        read_session=types.ReadSession(
            table=dest_ref,
            data_format=types.DataFormat.ARROW,
        ),
        max_stream_count=4,  # parallel streams
    )

    for stream in session.streams:
        reader = bq_storage_client.read_rows(stream.name)
        for page in reader.rows().pages:
            dfs.append(page.to_dataframe())

    df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    row_count = len(df)
    logger.info("Read %d rows into pandas DataFrame", row_count)

    # Local file
    with tempfile.TemporaryDirectory() as tdir:
        ext = ".csv.gz" if gzip_output else ".csv"
        local_path = os.path.join(tdir, f"part-0000{ext}")
        df.to_csv(local_path, index=False, compression="gzip" if gzip_output else None)
        bytes_local = os.path.getsize(local_path)
        sha = sha256_file(local_path)

        # S3 keys
        out_dir = f"{s3_prefix.rstrip('/')}/month_id={month_id}"
        tmp_key = f"{out_dir}/part-0000{ext}.tmp"
        final_key = f"{out_dir}/part-0000{ext}"

        # Manifest
        manifest = {
            "execution_id": execution_id,
            "month_id": month_id,
            "row_count": row_count,
            "bytes": bytes_local,
            "sha256": sha,
            "query_hash": query_hash,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "bq_project": bq_project,
            "scratch_table": dest_ref,
        }

        # Upload
        size_s3 = s3_atomic_upload(local_path, s3_bucket, tmp_key, final_key, manifest)
        logger.info("Uploaded %s (%d bytes)", final_key, size_s3)

    # Optionally clean up
    if delete_temp_table:
        try:
            bq_client.delete_table(dest_ref, not_found_ok=True)
            logger.info("Deleted temp table %s", dest_ref)
        except Exception as e:
            logger.warning("Failed to delete temp table: %s", e)

    return {
        "month_id": month_id,
        "row_count": row_count,
        "s3_key": final_key,
        "manifest_key": os.path.join(out_dir, "_manifest.json"),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--month-id", required=True)
    parser.add_argument("--base-query", required=True)
    parser.add_argument("--bq-project", required=True)
    parser.add_argument("--bq-location", required=True)
    parser.add_argument("--scratch-dataset", required=True)
    parser.add_argument("--scratch-table-prefix", default="__bq_export_")
    parser.add_argument("--s3-bucket", required=True)
    parser.add_argument("--s3-prefix", required=True)
    parser.add_argument("--execution-id", required=True)
    parser.add_argument("--gzip", action="store_true")
    parser.add_argument("--delete-temp-table", action="store_true")
    args = parser.parse_args()

    result = export_month_to_s3(
        month_id=args.month_id,
        base_query=args.base_query,
        bq_project=args.bq_project,
        bq_location=args.bq_location,
        scratch_dataset=args.scratch_dataset,
        scratch_table_prefix=args.scratch_table_prefix,
        s3_bucket=args.s3_bucket,
        s3_prefix=args.s3_prefix,
        execution_id=args.execution_id,
        gzip_output=args.gzip,
        delete_temp_table=args.delete_temp_table,
    )
    print(json.dumps(result))


if __name__ == "__main__":
    sys.exit(main())
