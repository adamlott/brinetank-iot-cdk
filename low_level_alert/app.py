import os, json, boto3, datetime

ses = boto3.client("sesv2")

SES_FROM = os.environ["SES_FROM"]  # e.g., alerts@salty-water.com
# JSON string mapping sensorId -> list of emails
RECIPIENTS_JSON = os.environ.get("RECIPIENTS_JSON", "{}")
RECIPIENTS = json.loads(RECIPIENTS_JSON)

def handler(event, context):
    msg = event if isinstance(event, dict) else json.loads(event)
    sensor_id = msg["sensorId"]
    level = float(msg["levelPct"])
    ts = msg.get("ts", datetime.datetime.utcnow().isoformat() + "Z")

    # Choose recipients:
    # 1) allow explicit "to" in event (list or string), else
    # 2) look up by sensor_id in env mapping
    to_arg = msg.get("to")
    if isinstance(to_arg, str):
        to_addrs = [to_arg]
    elif isinstance(to_arg, list):
        to_addrs = to_arg
    else:
        to_addrs = RECIPIENTS.get(sensor_id, [])

    if not to_addrs:
        # No recipients configured; nothing to send
        return {"ok": False, "reason": f"No recipients for {sensor_id}"}

    subject = f"[Salt Alert] {sensor_id} below 10% ({level:.1f}%)"
    body_text = (
        f"Brine tank level is low.\n\n"
        f"Sensor: {sensor_id}\n"
        f"Level:  {level:.1f}%\n"
        f"Time:   {ts}\n\n"
        f"Action: Schedule a refill."
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
