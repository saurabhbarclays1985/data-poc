import os, boto3, datetime

s3 = boto3.client("s3")
sns = boto3.client("sns")

S3_BUCKET = os.environ.get("S3_BUCKET", "my-cfn-artifacts-ap-south-1")
SNS_ARN = os.environ.get("SNS_ARN")  # optional

def lambda_handler(event, context):
    ts = datetime.datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    key = f"raw/employees_{ts}.csv"
    body = "employee_id,name,dept\n1,Alice,IT\n2,Bob,Finance\n"
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body.encode("utf-8"))
    if SNS_ARN:
      sns.publish(TopicArn=SNS_ARN, Message=f"New file: {key}")
    return {"ok": True, "object": key}
