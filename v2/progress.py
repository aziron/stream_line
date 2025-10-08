# --- add near imports ---
import re
import subprocess
from datetime import datetime, timedelta

# --- helpers ---
def _s3_put_json(uri: str, payload: dict):
    # uri like s3://bucket/key.json
    assert uri.startswith("s3://")
    bucket_key = uri[5:]
    bucket, key = bucket_key.split("/", 1)
    boto3.client("s3").put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
        ContentType="application/json",
    )

def _send_sfn_heartbeat(task_token: str):
    subprocess.run(
        ["aws", "stepfunctions", "send-task-heartbeat", "--task-token", task_token],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

class Progressor:
    def __init__(self, total_estimate_rows: int | None, s3_uri: str | None,
                 task_token: str | None, interval_rows: int, interval_seconds: int):
        self.total_estimate_rows = total_estimate_rows
        self.s3_uri = s3_uri
        self.task_token = task_token
        self.interval_rows = max(1, interval_rows)
        self.interval_seconds = max(1, interval_seconds)
        self.rows = 0
        self.last_emit = datetime.utcnow()

    def add(self, n: int, context: dict):
        self.rows += n
        now = datetime.utcnow()
        if (self.rows % self.interval_rows == 0) or ((now - self.last_emit).total_seconds() >= self.interval_seconds):
            self.emit(context)
            self.last_emit = now

    def emit(self, context: dict):
        pct = None
        if self.total_estimate_rows and self.total_estimate_rows > 0:
            pct = round(min(100.0, self.rows * 100.0 / self.total_estimate_rows), 2)
        # 1) print to stdout (SSM -> Step Functions can read)
        print(f"PROGRESS {pct if pct is not None else 'NA'} {self.rows}", flush=True)
        # 2) write to S3
        if self.s3_uri:
            payload = {
                "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                "rows_processed": self.rows,
                "percent": pct,
                **context,
            }
            _s3_put_json(self.s3_uri, payload)
        # 3) optional SFN heartbeat
        if self.task_token:
            _send_sfn_heartbeat(self.task_token)

# --- args: add these in argparse ---
# parser.add_argument("--progress-s3", help="s3://bucket/path/progress.json")
# parser.add_argument("--progress-interval-rows", type=int, default=200000)
# parser.add_argument("--progress-interval-seconds", type=int, default=15)
# parser.add_argument("--task-token", help="(optional) Step Functions task token for send-task-heartbeat")

# --- inside export_with_jobs_api(...) replace the loop ---
def export_with_jobs_api(sql: str, location: str, page_size: int, csv_path: str,
                         progress: Progressor | None, progress_ctx: dict) -> int:
    bq = bigquery.Client(location=location)
    job = bq.query(sql, location=location)
    row_iter = job.result(page_size=page_size)

    header_written = False
    total_rows = 0
    for df in row_iter.to_dataframe_iterable(bqstorage_client=None, progress_bar_type=None):
        if df is None or df.empty:
            continue
        total_rows += len(df)
        df.to_csv(csv_path, index=False, mode="a" if header_written else "w", header=not header_written)
        header_written = True
        if progress:
            progress.add(len(df), progress_ctx)

    if not header_written:
        cols = [f.name for f in row_iter.schema] if row_iter.schema else []
        pd.DataFrame(columns=cols).to_csv(csv_path, index=False)

    # final emit
    if progress:
        progress.emit(progress_ctx)
    return total_rows

# --- in main(), construct the Progressor and pass it down ---
progress = Progressor(
    total_estimate_rows=None,  # or pass a pre-count if you have one
    s3_uri=args.progress_s3,
    task_token=args.task_token,
    interval_rows=args.progress_interval_rows,
    interval_seconds=args.progress_interval_seconds,
)
progress_ctx = {
    "execution_id": args.execution_id,
    "month_id": args.month_id,
    "mode": "jobs_api",
}
rows = export_with_jobs_api(sql, args.bq_location, args.jobs_page_size, csv_path, progress, progress_ctx)