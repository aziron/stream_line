#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import hashlib
import json
import logging
import os
import sys
import tempfile
import time
from typing import List, Optional

import boto3
import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery_storage_v1 import BigQueryReadClient, types as bqs_types

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("bq-export")

def sha256_file(path: str) -> str:
    import hashlib
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def s3_finalize(local_path: str, bucket: str, tmp_key: str, final_key: str, manifest: dict):
    s3 = boto3.client("s3")
    # Upload to .tmp
    s3.upload_file(local_path, bucket, tmp_key)
    head = s3.head_object(Bucket=bucket, Key=tmp_key)
    size = head["ContentLength"]
    # Manifest
    s3.put_object(
        Bucket=bucket,
        Key=os.path.join(os.path.dirname(final_key), "_manifest.json"),
        Body=json.dumps(manifest).encode("utf-8"),
        ContentType="application/json",
    )
    # Atomic finalize
    s3.copy({"Bucket": bucket, "Key": tmp_key}, bucket, final_key)
    s3.delete_object(Bucket=bucket, Key=tmp_key)
    return size

def write_csv_incremental(pages: List[pd.DataFrame], csv_path: str, header_written: bool) -> bool:
    """
    Append a list of dataframes to a CSV on disk.
    Returns True if header is now written.
    """
    for df in pages:
        if df is None or df.empty:
            continue
        df.to_csv(csv_path, index=False, mode="a" if header_written else "w", header=not header_written)
        header_written = True
    return header_written

def export_with_storage_api(
    project: str,
    dataset: str,
    table: str,
    month_id: str,
    selected_fields: Optional[List[str]],
    max_streams: int,
    csv_path: str,
) -> int:
    """
    Reads directly from a table (or view) using the BigQuery Storage Read API with a row_restriction.
    Requires NO tables.create permission.
    """
    read_client = BigQueryReadClient()

    table_ref = f"projects/{project}/datasets/{dataset}/tables/{table}"
    opts = bqs_types.ReadSession.TableReadOptions()
    if selected_fields:
        opts.selected_fields = selected_fields
    opts.row_restriction = f"MONTH_ID = '{month_id}'"

    session = read_client.create_read_session(
        parent=f"projects/{project}",
        read_session=bqs_types.ReadSession(
            table=table_ref,
            data_format=bqs_types.DataFormat.ARROW,
            read_options=opts,
        ),
        max_stream_count=max_streams,
    )

    header_written = False
    total_rows = 0
    for stream in session.streams:
        reader = read_client.read_rows(stream.name)
        for page in reader.rows().pages:
            df = page.to_dataframe()
            total_rows += len(df)
            header_written = write_csv_incremental([df], csv_path, header_written)

    # If no rows at all, still write an empty header if we know the schema.
    if not header_written:
        cols = [f.name for f in session.avro_schema.schema.fields] if session.avro_schema.schema.fields else []
        pd.DataFrame(columns=cols).to_csv(csv_path, index=False)
        header_written = True

    return total_rows

def export_with_jobs_api(
    sql: str,
    location: str,
    page_size: int,
    csv_path: str,
) -> int:
    """
    Runs an arbitrary SQL query and streams results page-by-page via the Jobs API
    into pandas DataFrames, appending to a CSV. No destination table, no tables.create.
    """
    bq = bigquery.Client(location=location)
    job = bq.query(sql, location=location)
    it = job.result(page_size=page_size)

    # Stream pages into pandas → CSV
    header_written = False
    total_rows = 0
    for page in it.pages:
        df = page.to_dataframe()  # uses tabledata.list internally (no Storage API needed)
        total_rows += len(df)
        header_written = write_csv_incremental([df], csv_path, header_written)

    # If completely empty, write header from schema (if available)
    if not header_written:
        cols = [f.name for f in it.schema] if it.schema else []
        pd.DataFrame(columns=cols).to_csv(csv_path, index=False)
    return total_rows

def main():
    p = argparse.ArgumentParser(description="Export MONTH_ID shard to S3 without tables.create")
    p.add_argument("--month-id", required=True)
    p.add_argument("--bq-project", required=True)
    p.add_argument("--bq-location", required=True)
    # Path A (Storage API, no temp table): provide source table
    p.add_argument("--source-table", help="Format: dataset.table (uses Storage API with row_restriction)")
    p.add_argument("--selected-fields", nargs="*", help="Optional list of columns to project")
    p.add_argument("--storage-max-streams", type=int, default=4)
    # Path B (Jobs API fallback): provide base SQL (arbitrary)
    p.add_argument("--base-query", help="Arbitrary SQL; we'll append WHERE MONTH_ID = '<month>'")
    p.add_argument("--jobs-page-size", type=int, default=100000)
    # Output/S3
    p.add_argument("--s3-bucket", required=True)
    p.add_argument("--s3-prefix", required=True)
    p.add_argument("--execution-id", required=True)
    p.add_argument("--gzip", action="store_true")
    args = p.parse_args()

    if not args.source_table and not args.base_query:
        print("Provide either --source-table or --base-query", file=sys.stderr)
        sys.exit(2)

    # Build SQL if needed (no destination table)
    if args.base_query:
        sql = f"SELECT * FROM ({args.base_query}) WHERE MONTH_ID = '{args.month_id}'"
    else:
        sql = None

    query_hash = hashlib.sha256((sql or f"{args.source_table}|{args.selected_fields}|MONTH_ID={args.month_id}").encode("utf-8")).hexdigest()

    # Prepare local paths
    with tempfile.TemporaryDirectory() as tdir:
        csv_path = os.path.join(tdir, "part-0000.csv")
        gz_path = csv_path + ".gz"

        # Export
        if args.source_table:
            dataset, table = args.source_table.split(".", 1)
            log.info("Reading from table %s.%s via Storage API with row_restriction", dataset, table)
            rows = export_with_storage_api(
                project=args.bq_project,
                dataset=dataset,
                table=table,
                month_id=args.month_id,
                selected_fields=args.selected_fields,
                max_streams=args.storage_max_streams,
                csv_path=csv_path,
            )
        else:
            log.info("Running arbitrary SQL via Jobs API; streaming pages to pandas")
            rows = export_with_jobs_api(
                sql=sql,
                location=args.bq_location,
                page_size=args.jobs_page_size,
                csv_path=csv_path,
            )

        # Optionally gzip
        final_local = csv_path
        if args.gzip:
            import gzip, shutil
            with open(csv_path, "rb") as fin, gzip.open(gz_path, "wb") as fout:
                shutil.copyfileobj(fin, fout, length=1024 * 1024)
            final_local = gz_path

        # S3 keys
        ext = ".csv.gz" if args.gzip else ".csv"
        out_dir = f"{args.s3_prefix.rstrip('/')}/month_id={args.month_id}"
        tmp_key = f"{out_dir}/part-0000{ext}.tmp"
        final_key = f"{out_dir}/part-0000{ext}"

        # Manifest
        bytes_local = os.path.getsize(final_local)
        sha = sha256_file(final_local)
        manifest = {
            "execution_id": args.execution_id,
            "month_id": args.month_id,
            "row_count": rows,
            "bytes": bytes_local,
            "sha256": sha,
            "query_hash": query_hash,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "bq_project": args.bq_project,
            "bq_location": args.bq_location,
            "mode": "storage_api" if args.source_table else "jobs_api",
            "source_table": args.source_table or "",
        }

        # Upload with temp→final + manifest
        size_s3 = s3_finalize(final_local, args.s3_bucket, tmp_key, final_key, manifest)
        log.info("Uploaded %s (%d bytes, %d rows)", final_key, size_s3, rows)

        print(json.dumps({
            "status": "ok",
            "month_id": args.month_id,
            "rows": rows,
            "s3_key": final_key,
            "manifest_key": f"{out_dir}/_manifest.json",
            "mode": manifest["mode"],
        }))

if __name__ == "__main__":
    sys.exit(main())
