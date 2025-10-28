import os, json, boto3, datetime

sns = boto3.client("sns")
TOPIC_ARN = os.environ["ALERT_TOPIC_ARN"]
ALERT_FN_NAME = os.environ.get("ALERT_FN_NAME")
lambda_client = boto3.client("lambda")

def handler(event, context):
    # event could be your IoT-processed payload:
    # { "sensorId": "sensor-kitchen", "levelPct": 8.4, "ts": "2025-10-25T00:00:00Z" }
    msg = event if isinstance(event, dict) else json.loads(event)
    sensor_id = msg["sensorId"]
    level = float(msg["levelPct"])
    ts = msg.get("ts", datetime.datetime.utcnow().isoformat() + "Z")

def maybe_alert(device: str, level_pct: float, ts_iso: str):
    if level_pct < 10.0 and ALERT_FN_NAME:
        payload = {"sensorId": device, "levelPct": level_pct, "ts": ts_iso}
        lambda_client.invoke(
            FunctionName=ALERT_FN_NAME,
            InvocationType="Event",  # async
            Payload=json.dumps(payload).encode("utf-8"),
        )

    # only publish when you've applied hysteresis/cooldown upstream
    subject = f"[Salt Alert] {sensor_id} below 10% ({level:.1f}%)"
    body = (
        f"Brine tank level is low.\n\n"
        f"Sensor: {sensor_id}\n"
        f"Level:  {level:.1f}%\n"
        f"Time:   {ts}\n\n"
        f"Action: Schedule a refill."
    )

    sns.publish(
        TopicArn=TOPIC_ARN,
        Subject=subject,
        Message=body,
        MessageAttributes={
            "sensorId": {"DataType": "String", "StringValue": sensor_id},
            "type": {"DataType": "String", "StringValue": "LOW_LEVEL"}
        }
    )

    return {"ok": True}
