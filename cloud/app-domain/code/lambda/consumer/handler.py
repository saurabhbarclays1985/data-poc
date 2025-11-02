def lambda_handler(event, context):
    for rec in event.get("Records", []):
        print("SQS message:", rec.get("body", ""))
    return {"ok": True, "count": len(event.get("Records", []))}
