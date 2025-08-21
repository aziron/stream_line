#!/usr/bin/env python3
import os
import math
import boto3
import tempfile
from contextlib import contextmanager
from typing import Optional

s3 = boto3.client("s3")

@contextmanager
def temp_outfile(dir_: Optional[str] = None, suffix: str = ".csv"):
    """
    A NamedTemporaryFile that works with boto3.upload_file (needs a real path),
    and is cleanly removed afterwards.
    """
    fp = tempfile.NamedTemporaryFile(delete=False, dir=dir_, suffix=suffix, mode="w", buffering=1024*1024*16)  # 16MB buffer
    path = fp.name
    try:
        yield fp, path
    finally:
        try:
            fp.close()
        except Exception:
            pass
        if os.path.exists(path):
            os.remove(path)

def split_and_upload_csv(
    input_path: str,
    bucket: str,
    key_prefix: str,
    parts: int = 10,
    include_header_in_each: bool = True,
    known_total_lines: Optional[int] = None,
    temp_dir: Optional[str] = None,
    sse_s3: bool = False,               # set True to use AES256 server-side encryption
    sse_kms_key_id: Optional[str] = None,  # or give a KMS key id
):
    """
    Split a large CSV into N parts by line count and upload each to S3.
    Only one part is stored on disk at any time.

    Args:
        input_path: path to the large CSV (already on EBS).
        bucket: target S3 bucket name.
        key_prefix: S3 key prefix (e.g., 'exports/run-2025-08-21/data_part').
        parts: how many output files.
        include_header_in_each: replicate header row in every part.
        known_total_lines: provide if you already know it (to avoid a counting pass).
        temp_dir: optional dir for temp files (e.g., '/mnt' or another EBS mount).
        sse_s3: use S3-managed encryption (AES256) when uploading.
        sse_kms_key_id: use KMS CMK if provided.
    """
    # Determine total lines if not known (single sequential pass; memory-safe)
    total_lines = known_total_lines
    if total_lines is None:
        with open(input_path, "r", buffering=1024*1024*32, newline="") as f:  # 32MB read buffer
            total_lines = sum(1 for _ in f)

    # If we have a header, account for it in per-part distribution
    header = None
    with open(input_path, "r", buffering=1024*1024*32, newline="") as f:
        first_line = f.readline()
        # Heuristic: treat first line as header if it contains commas and not purely digits.
        # If you know definitively, set include_header_in_each accordingly and skip this check.
        header = first_line if include_header_in_each else None

    # Compute lines per part (excluding header lines that we’ll re-add if requested)
    data_lines = total_lines - (1 if header else 0)
    if data_lines < 0:
        data_lines = 0
    lines_per_part = math.ceil(data_lines / parts) if parts > 0 else data_lines

    def upload_temp_file(path: str, part_idx: int):
        key = f"{key_prefix}_{part_idx:02d}.csv"
        extra_args = {}
        if sse_kms_key_id:
            extra_args["ServerSideEncryption"] = "aws:kms"
            extra_args["SSEKMSKeyId"] = sse_kms_key_id
        elif sse_s3:
            extra_args["ServerSideEncryption"] = "AES256"
        # Upload
        if extra_args:
            s3.upload_file(path, bucket, key, ExtraArgs=extra_args)
        else:
            s3.upload_file(path, bucket, key)
        print(f"Uploaded s3://{bucket}/{key}")

    # Second pass: stream and write out parts
    with open(input_path, "r", buffering=1024*1024*32, newline="") as f:
        # Skip header if present (we’ll add to each part)
        if header is not None:
            # we already read the header earlier with a different handle,
            # so here we need to read and discard the first line again.
            _ = f.readline()

        part_idx = 1
        written_in_part = 0
        outfile_fp = None
        outfile_path = None

        def start_new_part():
            nonlocal outfile_fp, outfile_path, written_in_part, part_idx
            if part_idx > parts:
                return False
            # create a fresh temp file
            nonlocal temp_dir
            tmp = tempfile.NamedTemporaryFile(delete=False, dir=temp_dir, suffix=".csv", mode="w", buffering=1024*1024*16)
            outfile_fp = tmp
            outfile_path = tmp.name
            written_in_part = 0
            if header is not None:
                outfile_fp.write(header)
            return True

        def close_and_upload_part():
            nonlocal outfile_fp, outfile_path, part_idx
            if outfile_fp:
                outfile_fp.flush()
                os.fsync(outfile_fp.fileno())
                outfile_fp.close()
                upload_temp_file(outfile_path, part_idx)
                # Remove temp after successful upload
                os.remove(outfile_path)
                outfile_fp = None
                part_idx += 1

        # Begin first part
        if not start_new_part():
            return

        for line in f:
            # write the line
            outfile_fp.write(line)
            written_in_part += 1

            if written_in_part >= lines_per_part and part_idx < parts:
                # Finish this part and start next
                close_and_upload_part()
                if not start_new_part():
                    break

        # Finish the last part (even if smaller)
        if outfile_fp:
            close_and_upload_part()

    print("Done.")

if __name__ == "__main__":
    # Example usage
    split_and_upload_csv(
        input_path="/mnt/ebs/big_export.csv",
        bucket="my-target-bucket",
        key_prefix="exports/run-2025-08-21/data_part",
        parts=10,
        include_header_in_each=True,
        known_total_lines=9_000_000,  # you already know this; skips counting pass
        temp_dir="/mnt/ebs/tmp",      # point to your EBS mount (create the dir)
        sse_s3=True                   # optional encryption
        # sse_kms_key_id="arn:aws:kms:..."  # if you prefer KMS
    )
