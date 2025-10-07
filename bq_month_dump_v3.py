# pip install google-cloud-bigquery pyarrow boto3 botocore tenacity
import os, json, time, hashlib, decimal
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from botocore.config import Config as BotoConfig
from boto3.s3.transfer import TransferConfig
from tenacity import retry, stop_after_attempt, wait_exponential
from google.cloud import bigquery

# ---- Inputs (env/SSM) ----
BQ_PROJECT=os.environ["BQ_PROJECT"]
BQ_DATASET=os.environ["BQ_DATASET"]
BQ_TABLE  =os.environ["BQ_TABLE"]
MONTH_ID  =os.environ["MONTH_ID"]
S3_BUCKET =os.environ["S3_BUCKET"]
S3_PREFIX =os.environ["S3_PREFIX"]
TMPDIR    =os.environ.get("TMPDIR","/mnt/ebs/tmp")
PAGE_SIZE =int(os.environ.get("BQ_PAGE_SIZE","100000"))
COERCE_FLOATS = os.environ.get("COERCE_FLOATS_TO_STRING","false").lower() == "true"  # set to "true" if you also want floats as strings

os.makedirs(TMPDIR, exist_ok=True)

bq = bigquery.Client(project=BQ_PROJECT)

s3 = boto3.client("s3", config=BotoConfig(retries={"max_attempts":10,"mode":"adaptive"}))
tcfg = TransferConfig(multipart_threshold=64*1024*1024,
                      multipart_chunksize=64*1024*1024,
                      max_concurrency=16,
                      use_threads=True)

def sha256f(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024*1024), b""):
            h.update(chunk)
    return h.hexdigest()

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=30))
def run_query(sql, params):
    return bq.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("m", "STRING", params["m"])]
        )
    )

def to_string_if_decimal_or_float(v):
    if v is None:
        return None
    # BigQuery NUMERIC/DECIMAL -> decimal.Decimal in Python
    if isinstance(v, decimal.Decimal):
        # str() preserves exact textual form returned by client
        return str(v)
    if COERCE_FLOATS and isinstance(v, float):
        # Use repr() to keep precision; or format(v, ".17g")
        return repr(v)
    return v

def build_table_from_page(page, schema_fields):
    """
    Convert a BigQuery page (iterable of Row) into a PyArrow Table,
    coercing Decimal (and optionally float) values to string so Arrow infers 'string' for those columns.
    """
    cols = {f.name: [] for f in schema_fields}
    for row in page:
        # row.get(name) is safer for missing values
        for f in schema_fields:
            cols[f.name].append(to_string_if_decimal_or_float(row.get(f.name)))
    # Let Arrow infer; decimal/float columns become string (with nulls)
    return pa.table(cols)

def main():
    # 1) Count rows for this month (optional but nice for validation/manifest)
    count_res = run_query(
        f"SELECT COUNT(*) AS c FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}` WHERE month_id=@m",
        {"m": MONTH_ID}
    ).result()
    expected = list(count_res)[0].c

    if expected == 0:
        print(json.dumps({
            "month_id": MONTH_ID, "status": "SUCCESS",
            "row_count": 0, "files": [], "note": "No rows to export"
        }))
        return

    # 2) Start query (no Storage API required)
    job = run_query(
        f"SELECT * FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}` WHERE month_id=@m ORDER BY 1",
        {"m": MONTH_ID}
    )
    it = job.result(page_size=PAGE_SIZE)

    # 3) Stream pages -> Arrow Table (with decimals coerced to string) -> single Parquet file
    out_path = os.path.join(TMPDIR, f"{BQ_TABLE}_{MONTH_ID}.parquet")
    writer = None
    total_rows = 0
    page_idx = 0

    for page in it.pages:
        tbl = build_table_from_page(page, it.schema)
        if writer is None:
            # First page defines the Parquet file schema (now stable, because decimals are strings)
            writer = pq.ParquetWriter(out_path, tbl.schema, compression="snappy")
        else:
            # (Optional) enforce identical schema anyway
            if tbl.schema != writer.schema:
                tbl = tbl.cast(writer.schema)
        writer.write_table(tbl)
        total_rows += tbl.num_rows
        page_idx += 1

    if writer:
        writer.close()

    # 4) Upload to S3 (+checksum metadata) and verify
    key = f"{S3_PREFIX.rstrip('/')}/{BQ_TABLE}/month_id={MONTH_ID}/{os.path.basename(out_path)}"
    sha = sha256f(out_path)
    s3.upload_file(
        out_path, S3_BUCKET, key,
        ExtraArgs={"Metadata": {
            "sha256": sha,
            "month_id": MONTH_ID,
            "rowcount": str(total_rows),
            "coerced": "decimals" + ("+floats" if COERCE_FLOATS else "")
        }},
        Config=tcfg
    )
    s3.head_object(Bucket=S3_BUCKET, Key=key)  # existence check

    # 5) Simple manifest to stdout (Step Functions can parse this)
    status = "SUCCESS" if total_rows == expected else "WARNING_MISMATCH_COUNT"
    print(json.dumps({
        "table": f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}",
        "month_id": MONTH_ID,
        "status": status,
        "expected_rows": int(expected),
        "row_count": int(total_rows),
        "files": [{"s3_uri": f"s3://{S3_BUCKET}/{key}", "sha256": sha}],
        "coercion": {"decimals_to_string": True, "floats_to_string": COERCE_FLOATS}
    }))
    # Optional: also write a sidecar JSON next to the parquet

if __name__ == "__main__":
    main()