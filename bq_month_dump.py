import os, json, hashlib, tempfile, time
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from google.cloud import bigquery
from google.cloud import bigquery_storage
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from botocore.config import Config as BotoConfig
from boto3.s3.transfer import TransferConfig

# ---- Inputs (from SSM parameters / env) ----
PROJECT      = os.environ["BQ_PROJECT"]
DATASET      = os.environ["BQ_DATASET"]
TABLE        = os.environ["BQ_TABLE"]              # e.g. "events"
MONTH_ID     = os.environ["MONTH_ID"]              # e.g. "2025-01"
S3_BUCKET    = os.environ["S3_BUCKET"]
S3_PREFIX    = os.environ["S3_PREFIX"]             # e.g. "bq-export/events/"
TMPDIR       = os.environ.get("TMPDIR", "/mnt/ebs/tmp")
PARQUET_ROWS_PER_FILE = int(os.environ.get("PARQUET_ROWS_PER_FILE", "5_000_000"))
MAX_THREADS  = int(os.environ.get("MAX_THREADS", "16"))

# ---- AWS clients with aggressive retries ----
boto_cfg = BotoConfig(retries={"max_attempts": 10, "mode": "adaptive"})
s3 = boto3.client("s3", config=boto_cfg)
transfer_cfg = TransferConfig(multipart_threshold=64*1024*1024,
                              multipart_chunksize=64*1024*1024,
                              max_concurrency=MAX_THREADS,
                              use_threads=True)

# ---- GCP clients ----
bq = bigquery.Client(project=PROJECT)
bqs = bigquery_storage.BigQueryReadClient()

# ---- Helpers ----
def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024*1024), b""):
            h.update(chunk)
    return h.hexdigest()

def s3_put_with_meta(local_path, bucket, key, metadata):
    s3.upload_file(local_path, bucket, key, ExtraArgs={"Metadata": metadata}, Config=transfer_cfg)

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=30))
def run_query(sql, params):
    job = bq.query(sql, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter(k, "STRING", v) for k, v in params.items()]
    ))
    return job.result()

def main():
    os.makedirs(TMPDIR, exist_ok=True)
    start = time.time()
    manifest = {
        "project": PROJECT, "dataset": DATASET, "table": TABLE,
        "month_id": MONTH_ID, "status": "STARTED", "started_at": datetime.utcnow().isoformat()+"Z"
    }

    # 1) Count rows (for validation)
    count_sql = f"""
      SELECT COUNT(*) AS c
      FROM `{PROJECT}.{DATASET}.{TABLE}`
      WHERE month_id = @m
    """
    count = int(next(run_query(count_sql, {"m": MONTH_ID})).c)
    manifest["expected_rows"] = count

    # Short-circuit: nothing to export
    if count == 0:
        manifest.update({"status":"SUCCESS","files":[],"row_count":0,"bytes":0,
                         "finished_at": datetime.utcnow().isoformat()+"Z"})
        print(json.dumps(manifest, ensure_ascii=False))
        return

    # 2) Pull data efficiently via Storage API → Arrow → Parquet
    # NOTE: Use a SELECT list (avoid SELECT *) for stable schemas in prod.
    sql = f"""
      SELECT *
      FROM `{PROJECT}.{DATASET}.{TABLE}`
      WHERE month_id = @m
      ORDER BY _PARTITIONTIME, 1
    """
    rows = run_query(sql, {"m": MONTH_ID})
    # Convert to Arrow in one go (Storage API used under the hood when provided)
    arrow_tbl = rows.to_arrow(bqstorage_client=bqs)  # fast + memory-efficient vs Python objects

    # Optional: split into multiple Parquet files for huge months
    files = []
    if arrow_tbl.num_rows > PARQUET_ROWS_PER_FILE:
        for i in range(0, arrow_tbl.num_rows, PARQUET_ROWS_PER_FILE):
            part = arrow_tbl.slice(i, min(PARQUET_ROWS_PER_FILE, arrow_tbl.num_rows - i))
            local_path = os.path.join(TMPDIR, f"{TABLE}_{MONTH_ID}_{i//PARQUET_ROWS_PER_FILE:03d}.parquet")
            pq.write_table(part, local_path, compression="snappy")
            files.append(local_path)
    else:
        local_path = os.path.join(TMPDIR, f"{TABLE}_{MONTH_ID}.parquet")
        pq.write_table(arrow_tbl, local_path, compression="snappy")
        files.append(local_path)

    # 3) Upload to S3 with checksum metadata + retries
    s3_files = []
    total_bytes = 0
    for p in files:
        sha = sha256_file(p)
        size = os.path.getsize(p)
        key = f"{S3_PREFIX.rstrip('/')}/{TABLE}/month_id={MONTH_ID}/{os.path.basename(p)}"
        meta = {"sha256": sha, "rowcount": str(arrow_tbl.num_rows), "month_id": MONTH_ID}
        s3_put_with_meta(p, S3_BUCKET, key, meta)
        # (Optional) HEAD to confirm it exists
        head = s3.head_object(Bucket=S3_BUCKET, Key=key)
        total_bytes += head["ContentLength"]
        s3_files.append({"s3_uri": f"s3://{S3_BUCKET}/{key}", "bytes": head["ContentLength"], "sha256": sha})

    # 4) Validate row counts (basic end-to-end assurance)
    # (Row count in Parquet is not stored natively; re-read quickly via Arrow meta if needed. Here we trust rows.to_arrow.)
    actual_rows = arrow_tbl.num_rows
    if actual_rows != count:
        raise RuntimeError(f"Row count mismatch: expected {count}, got {actual_rows}")

    # 5) Emit manifest JSON to stdout (Step Functions will capture) and also to S3
    manifest.update({
        "status": "SUCCESS",
        "row_count": actual_rows,
        "bytes": total_bytes,
        "files": s3_files,
        "finished_at": datetime.utcnow().isoformat()+"Z"
    })
    manifest_local = os.path.join(TMPDIR, f"{TABLE}_{MONTH_ID}_manifest.json")
    with open(manifest_local, "w") as f:
        json.dump(manifest, f, ensure_ascii=False)
    manifest_key = f"{S3_PREFIX.rstrip('/')}/{TABLE}/month_id={MONTH_ID}/_manifest.json"
    s3.upload_file(manifest_local, S3_BUCKET, manifest_key, Config=transfer_cfg)

    print(json.dumps(manifest, ensure_ascii=False))

if __name__ == "__main__":
    main()