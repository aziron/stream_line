python3 bq_month_to_s3.py \
  --month-id "{{ MonthId }}" \
  --base-query "{{ BaseQuery }}" \
  --bq-project "{{ BqProject }}" \
  --bq-location "{{ BqLocation }}" \
  --scratch-dataset "{{ ScratchDataset }}" \
  --s3-bucket "{{ S3Bucket }}" \
  --s3-prefix "{{ S3Prefix }}" \
  --execution-id "{{ ExecutionId }}" \
  --delete-temp-table
