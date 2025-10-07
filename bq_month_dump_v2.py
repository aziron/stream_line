# pip install google-cloud-bigquery pyarrow boto3 botocore tenacity
import os, json, hashlib, time, pyarrow as pa, pyarrow.parquet as pq
from tenacity import retry, stop_after_attempt, wait_exponential
from google.cloud import bigquery
import boto3
from botocore.config import Config as BotoConfig
from boto3.s3.transfer import TransferConfig

BQ_PROJECT=os.environ["BQ_PROJECT"]; DATASET=os.environ["BQ_DATASET"]; TABLE=os.environ["BQ_TABLE"]
MONTH_ID=os.environ["MONTH_ID"]; S3_BUCKET=os.environ["S3_BUCKET"]; S3_PREFIX=os.environ["S3_PREFIX"]
TMPDIR=os.environ.get("TMPDIR","/mnt/ebs/tmp"); os.makedirs(TMPDIR, exist_ok=True)

bq = bigquery.Client(project=BQ_PROJECT)
s3 = boto3.client("s3", config=BotoConfig(retries={"max_attempts":10,"mode":"adaptive"}))
tcfg = TransferConfig(multipart_threshold=64*1024*1024, multipart_chunksize=64*1024*1024, max_concurrency=16, use_threads=True)

def sha256f(p):
    import hashlib; h=hashlib.sha256()
    with open(p,'rb') as f:
        for c in iter(lambda:f.read(1024*1024), b""): h.update(c)
    return h.hexdigest()

@retry(stop=stop_after_attempt(5), wait=wait_exponential())
def run_query(sql, params):
    return bq.query(sql, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("m","STRING",params["m"])]
    ))

def main():
    count = list(run_query(
        f"SELECT COUNT(*) c FROM `{BQ_PROJECT}.{DATASET}.{TABLE}` WHERE month_id=@m", {"m":MONTH_ID}
    ).result())[0].c
    if count == 0:
        print(json.dumps({"month_id":MONTH_ID,"status":"SUCCESS","row_count":0,"files":[]}))
        return

    # Start query job (REST path; no Storage API)
    job = run_query(
        f"SELECT * FROM `{BQ_PROJECT}.{DATASET}.{TABLE}` WHERE month_id=@m ORDER BY 1",
        {"m": MONTH_ID}
    )
    it = job.result(page_size=100000)  # page through results
    schema = [pa.field(f.name, pa.string()) for f in it.schema]  # keep simple; map types if desired

    # Stream pages -> Arrow chunks -> Parquet writer (no huge RAM)
    out_path = os.path.join(TMPDIR, f"{TABLE}_{MONTH_ID}.parquet")
    writer = None; rows_total = 0
    for page in it.pages:
        # Convert page rows (RowIterator) -> Arrow Table
        batch_cols = {f.name: [] for f in it.schema}
        for row in page:
            for f in it.schema: batch_cols[f.name].append(row.get(f.name))
        table = pa.table(batch_cols)
        if writer is None:
            writer = pq.ParquetWriter(out_path, table.schema, compression="snappy")
        writer.write_table(table)
        rows_total += table.num_rows
    if writer: writer.close()

    key = f"{S3_PREFIX.rstrip('/')}/{TABLE}/month_id={MONTH_ID}/{os.path.basename(out_path)}"
    sha = sha256f(out_path)
    s3.upload_file(out_path, S3_BUCKET, key, ExtraArgs={"Metadata":{"sha256":sha,"month_id":MONTH_ID,"rowcount":str(rows_total)}}, Config=tcfg)
    s3.head_object(Bucket=S3_BUCKET, Key=key)  # confirm

    print(json.dumps({"month_id":MONTH_ID,"status":"SUCCESS","row_count":rows_total,"files":[{"s3_uri":f"s3://{S3_BUCKET}/{key}","sha256":sha}]}))

if __name__=="__main__": main()