#!/usr/bin/env python3
import argparse
import csv
import io
import os
import sys
import time
import threading
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from google.cloud import bigquery
from google.oauth2 import service_account

# ---------- Arguments ----------
parser = argparse.ArgumentParser(description="Parallel BigQuery to S3 exporter by MONTH_ID (1..12)")
parser.add_argument("--gcp-sa-json-path", required=True, help="Path to GCP service account JSON on disk")
parser.add_argument("--query-template", required=True,
                    help="SQL with '{month_id}' token, e.g. SELECT * FROM X WHERE MONTH_ID = {month_id}")
parser.add_argument("--bq-location", default=None, help="Optional BigQuery job location (e.g. EU, US)")
parser.add_argument("--s3-bucket", required=True)
parser.add_argument("--s3-prefix", required=True,
                    help="Prefix base, e.g. DATA_SOURCE_2025_10_08 (script adds _part_01.csv ... _part_12.csv)")
parser.add_argument("--max-workers", type=int, default=12)
parser.add_argument("--retries", type=int, default=3)
parser.add_argument("--retry-backoff-seconds", type=int, default=20)
parser.add_argument("--month-column", default="MONTH_ID", help="(Doc: for clarity) Filtered by the query template")
parser.add_argument("--aws-region", default=os.getenv("AWS_REGION", "eu-central-1"))
args = parser.parse_args()

# ---------- Clients ----------
session = boto3.session.Session(region_name=args.aws-region if hasattr(args, 'aws-region') else args.aws_region)
s3 = session.client("s3")

credentials = service_account.Credentials.from_service_account_file(args.gcp_sa_json_path)
bq_client = bigquery.Client(credentials=credentials, project=credentials.project_id, location=args.bq_location)

# ---------- Globals for progress ----------
TOTAL_PARTS = 12
completed_parts = 0
completed_lock = threading.Lock()
stop_progress = threading.Event()

def percent_done():
    with completed_lock:
        return int((completed_parts / TOTAL_PARTS) * 100)

def progress_reporter():
    # Print once per minute until stop flag set
    while not stop_progress.is_set():
        print(f"[{datetime.now(timezone.utc).isoformat()}] progress={percent_done()}%", flush=True)
        stop_progress.wait(60)

def stream_query_to_csv_s3(month_id: int):
    """
    Execute the query for one month_id, stream results to CSV in memory and upload to S3.
    Retries on transient failures.
    """
    key = f"{args.s3_prefix}_part_{month_id:02d}.csv"
    sql = args.query_template.format(month_id=month_id)

    for attempt in range(1, args.retries + 1):
        try:
            job = bq_client.query(sql)
            # Fetch schema early
            result_iter = job.result(page_size=50000)
            schema = [field.name for field in result_iter.schema]

            # Stream rows to CSV in memory (use SpooledTemporaryFile if you expect huge outputs)
            buf = io.StringIO()
            writer = csv.writer(buf, lineterminator="\n")
            writer.writerow(schema)
            for row in result_iter:
                writer.writerow([row.get(field) for field in schema])

            # Upload to S3 with managed transfer
            data_bytes = buf.getvalue().encode("utf-8")
            s3.upload_fileobj(Fileobj=io.BytesIO(data_bytes), Bucket=args.s3_bucket, Key=key)
            print(f"month_id={month_id} -> uploaded s3://{args.s3_bucket}/{key} (rows including header)", flush=True)

            with completed_lock:
                global completed_parts
                completed_parts += 1
            return key

        except Exception as e:
            print(f"ERROR month_id={month_id} attempt={attempt}/{args.retries}: {e}", file=sys.stderr, flush=True)
            if attempt < args.retries:
                time.sleep(args.retry_backoff_seconds)
            else:
                raise

def verify_all_parts(keys):
    # Ensure all 12 keys exist with nonzero size
    missing = []
    for k in keys:
        try:
            head = s3.head_object(Bucket=args.s3_bucket, Key=k)
            if head.get("ContentLength", 0) == 0:
                missing.append(k)
        except Exception:
            missing.append(k)
    return missing

def main():
    # Kick off periodic progress logs
    t = threading.Thread(target=progress_reporter, daemon=True)
    t.start()

    month_ids = list(range(1, 13))
    keys_expected = [f"{args.s3_prefix}_part_{m:02d}.csv" for m in month_ids]

    futures = {}
    with ThreadPoolExecutor(max_workers=args.max_workers) as ex:
        for m in month_ids:
            futures[ex.submit(stream_query_to_csv_s3, m)] = m

        failures = []
        try:
            for f in as_completed(futures):
                m = futures[f]
                try:
                    f.result()
                except Exception as e:
                    failures.append((m, str(e)))
        finally:
            stop_progress.set()  # stop the reporter
            t.join(timeout=2)

    if failures:
        print(f"FAILED months: {failures}", file=sys.stderr, flush=True)
        sys.exit(2)

    # Final verification
    missing = verify_all_parts(keys_expected)
    if missing:
        print(f"MISSING/EMPTY files: {missing}", file=sys.stderr, flush=True)
        sys.exit(3)

    print("All 12 parts present in S3. progress=100%", flush=True)

if __name__ == "__main__":
    main()
