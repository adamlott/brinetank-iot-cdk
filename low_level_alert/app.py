import os, json, boto3, datetime, time
from typing import List

ses = boto3.client("sesv2")
dynamo = boto3.client("dynamodb")

SES_FROM = os.environ["SES_FROM"]
CONFIG_TABLE = os.environ["CONFIG_TABLE"]

# simple in-memory cache to reduce DDB calls during bursts
_cache = {"ts": 0.0, "sensorId": None, "recipients": None}
CACHE_TTL_SECONDS = 30

def _get_recipients(sensor_id: str) -> List[str]:
    now = time.time()
    if _cache["sensorId"] == sensor_id and (now - _cache["ts"]) < CACHE_TTL_SECONDS:
        return _cache["recipients"] or []

    resp = dynamo.get_item(
        TableName=CONFIG_TABLE,
        Key={"sensorId": {"S": sensor_id}},
        ConsistentRead=True,
    )
    recipients: List[str] = []
    if "Item" in resp:
        item = resp["Item"]
        # Expect attribute "emails": L of S, or "channels": M with type=email
        if "emails" in item and "L" in item["emails"]:
            recipients = [v["S"] for v in item["emails"]["L"] if "S" in v]
        elif "channels" in item and "M" in item["channels"]:
            ch = item["channels"]["M"]
            if "email" in ch and "L" in ch["email"]:
                recipients = [v["S"] for v in ch["email"]["L"] if "S" in v]

    _cache.update({"ts": now, "sensorId": sensor_id, "recipients": recipients})
    return recipients

def handler(event, context):
    msg = event if isinstance(event, dict) else json.loads(event)
    sensor_id = msg["sensorId"]
    level = float(msg["levelPct"])
    ts = msg.get("ts", datetime.datetime.utcnow().isoformat() + "Z")

    # 1) explicit "to" override if provided in event (for ad-hoc tests)
    if "to" in msg:
        if isinstance(msg["to"], str): to_addrs = [msg["to"]]
        else: to_addrs = list(msg["to"])
    else:
        # 2) load from DynamoDB config
        to_addrs = _get_recipients(sensor_id)

    if not to_addrs:
        # nothing configured, fail gracefully
        return {"ok": False, "reason": f"No recipients configured for {sensor_id}"}

    subject = f"[Salt Alert] {sensor_id} below 10% ({level:.1f}%)"
    body_text = (
        f"Brine tank level is low.\n\n"
        f"Sensor: {sensor_id}\n"
        f"Level:  {level:.1f}%\n"
        f"Time:   {ts}\n\n"
        f"Action: Time to schedule a refill!  https://salty-water.com"
    )

    ses.send_email(
        FromEmailAddress=SES_FROM,
        Destination={"ToAddresses": to_addrs},
        Content={"Simple": {
            "Subject": {"Data": subject},
            "Body": {"Text": {"Data": body_text}}
        }},
    )
    return {"ok": True, "sent": to_addrs}
