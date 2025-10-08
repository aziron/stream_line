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

import tempfile
import gzip
from boto3.s3.transfer import TransferConfig

# add/replace args
parser.add_argument("--multipart-chunk-mib", type=int, default=64,
                    help="S3 multipart chunk size in MiB (64–128 recommended for large files)")
parser.add_argument("--multipart-max-concurrency", type=int, default=8,
                    help="Concurrent multipart threads per upload")
parser.add_argument("--gzip", action="store_true",
                    help="Write .csv.gz instead of .csv (recommended for 2GB chunks)")


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

transfer_cfg = TransferConfig(
    multipart_threshold=8*1024*1024,   # trigger multipart above 8 MiB
    multipart_chunksize=args.multipart_chunk_mib * 1024 * 1024,
    max_concurrency=args.multipart_max_concurrency,
    use_threads=True
)


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
    Execute query for one month_id, stream rows to a temp file (csv or csv.gz),
    then multipart-upload to S3. Retries on transient failures.
    """
    ext = ".csv.gz" if args.gzip else ".csv"
    key = f"{args.s3_prefix}_part_{month_id:02d}{ext}"
    sql = args.query_template.format(month_id=month_id)

    for attempt in range(1, args.retries + 1):
        tmp_path = None
        try:
            # Run query
            job = bq_client.query(sql)
            result_iter = job.result(page_size=200_000)  # large pages → fewer API calls
            schema = [field.name for field in result_iter.schema]

            # Create temp file on disk
            with tempfile.NamedTemporaryFile(prefix=f"bq_m{month_id:02d}_", suffix=ext, delete=False) as tmp:
                tmp_path = tmp.name

            # Stream rows into the temp file (optionally gzip)
            if args.gzip:
                f_open = lambda p: gzip.open(p, mode="wt", newline="", compresslevel=6)
            else:
                f_open = lambda p: open(p, mode="w", newline="")

            with f_open(tmp_path) as fh:
                writer = csv.writer(fh, lineterminator="\n")
                writer.writerow(schema)
                # Iterate page by page to keep memory flat
                for page in result_iter.pages:
                    for row in page:
                        writer.writerow([row.get(col) for col in schema])

            # Multipart upload to S3
            s3.upload_file(
                Filename=tmp_path,
                Bucket=args.s3_bucket,
                Key=key,
                Config=transfer_cfg
            )

            # Verify non-zero object size
            head = s3.head_object(Bucket=args.s3_bucket, Key=key)
            if head.get("ContentLength", 0) == 0:
                raise RuntimeError("Uploaded object has zero size")

            print(f"month_id={month_id} -> uploaded s3://{args.s3_bucket}/{key} size={head['ContentLength']}", flush=True)

            # Mark progress
            with completed_lock:
                global completed_parts
                completed_parts += 1

            # Cleanup temp file
            try:
                os.remove(tmp_path)
            except Exception:
                pass

            return key

        except Exception as e:
            print(f"ERROR month_id={month_id} attempt={attempt}/{args.retries}: {e}", file=sys.stderr, flush=True)
            # Cleanup partial temp
            if tmp_path and os.path.exists(tmp_path):
                try: os.remove(tmp_path)
                except Exception: pass
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
