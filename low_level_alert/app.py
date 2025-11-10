import os, json, boto3, datetime, time
from typing import List, Tuple

ses = boto3.client("sesv2")
dynamo = boto3.client("dynamodb")

SES_FROM = os.environ["SES_FROM"]
CONFIG_TABLE = os.environ["CONFIG_TABLE"]

# defaults if not set in the item
DEFAULT_THRESHOLD = 10.0
DEFAULT_HYSTERESIS = 2.0
DEFAULT_COOLDOWN = 6 * 3600  # 6 hours

def _now_iso() -> str:
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def _n(val) -> float:
    try:
        return float(val)
    except Exception:
        return 0.0

def _load_config(sensor_id: str):
    resp = dynamo.get_item(
        TableName=CONFIG_TABLE,
        Key={"sensorId": {"S": sensor_id}},
        ConsistentRead=True,
    )
    item = resp.get("Item", {}) or {}
    # emails
    emails: List[str] = []
    if "emails" in item and "L" in item["emails"]:
        emails = [x["S"] for x in item["emails"]["L"] if "S" in x]
    elif "channels" in item and "M" in item["channels"]:
        ch = item["channels"]["M"]
        if "email" in ch and "L" in ch["email"]:
            emails = [x["S"] for x in ch["email"]["L"] if "S" in x]

    threshold = _n(item.get("thresholdPercent", {}).get("N", DEFAULT_THRESHOLD))
    hysteresis = _n(item.get("hysteresisPercent", {}).get("N", DEFAULT_HYSTERESIS))
    cooldown = int(_n(item.get("cooldownSeconds", {}).get("N", DEFAULT_COOLDOWN)))

    last_state = (item.get("lastState", {}).get("S") or "normal").lower()
    last_alert_ts = item.get("lastAlertTs", {}).get("S") or ""
    last_seen_ts = item.get("lastSeenTs", {}).get("S") or ""
    last_level = _n(item.get("lastLevel", {}).get("N", 0))

    return {
        "emails": emails,
        "threshold": threshold,
        "hysteresis": hysteresis,
        "cooldown": cooldown,
        "lastState": last_state,
        "lastAlertTs": last_alert_ts,
        "lastSeenTs": last_seen_ts,
        "lastLevel": last_level,
    }

def _should_alert(prev_state: str, last_alert_ts: str, cooldown_s: int, level: float, threshold: float) -> bool:
    # Only alert on transitions from normal -> low and if cooldown has elapsed
    is_low = level < threshold
    if not is_low:
        return False
    if prev_state == "low":
        return False
    # cooldown check
    if not last_alert_ts:
        return True
    try:
        last = datetime.datetime.fromisoformat(last_alert_ts.replace("Z",""))
        delta = datetime.datetime.utcnow() - last
        return delta.total_seconds() >= cooldown_s
    except Exception:
        return True

def _compute_new_state(prev_state: str, level: float, threshold: float, hysteresis: float) -> str:
    # Enter LOW if below threshold
    if level < threshold:
        return "low"
    # Recover to NORMAL only after we climb above threshold + hysteresis
    if level >= (threshold + hysteresis):
        return "normal"
    # inside the hysteresis band: keep prior state
    return prev_state

def _update_state(sensor_id: str, new_state: str, level: float, ts: str, alert_sent: bool, threshold: float, hysteresis: float, cooldown: int):
    # Write back latest state/levels; set lastAlertTs only when alert sent
    expr = [
        "SET lastState = :state",
        "lastLevel = :lvl",
        "lastSeenTs = :ts",
        "thresholdPercent = :thr",
        "hysteresisPercent = :hys",
        "cooldownSeconds = :cd"
    ]
    vals = {
        ":state": {"S": new_state},
        ":lvl": {"N": str(level)},
        ":ts": {"S": ts},
        ":thr": {"N": str(threshold)},
        ":hys": {"N": str(hysteresis)},
        ":cd": {"N": str(cooldown)},
    }
    if alert_sent:
        expr.append("lastAlertTs = :alertTs")
        vals[":alertTs"] = {"S": ts}

    dynamo.update_item(
        TableName=CONFIG_TABLE,
        Key={"sensorId": {"S": sensor_id}},
        UpdateExpression=", ".join(expr),
        ExpressionAttributeValues=vals,
    )

def handler(event, context):
    msg = event if isinstance(event, dict) else json.loads(event)
    sensor_id = msg["sensorId"]
    level = float(msg["levelPct"])
    ts = msg.get("ts") or _now_iso()

    cfg = _load_config(sensor_id)

    # Allow direct "to" override in test events
    to_addrs = []
    if "to" in msg:
        to_addrs = [msg["to"]] if isinstance(msg["to"], str) else list(msg["to"])
    if not to_addrs:
        to_addrs = cfg["emails"]

    # Always update state, but only SEND on crossing + cooldown
    prev_state = cfg["lastState"]
    threshold = cfg["threshold"] or DEFAULT_THRESHOLD
    hysteresis = cfg["hysteresis"] or DEFAULT_HYSTERESIS
    cooldown = cfg["cooldown"] or DEFAULT_COOLDOWN

    new_state = _compute_new_state(prev_state, level, threshold, hysteresis)
    send = _should_alert(prev_state, cfg["lastAlertTs"], cooldown, level, threshold)

    if send and to_addrs:
        subject = f"[Salt Alert] {sensor_id} below {threshold:.0f}% ({level:.1f}%)"
        body_text = (
            f"Brine tank level is low.\n\n"
            f"Sensor: {sensor_id}\n"
            f"Level:  {level:.1f}%\n"
            f"Time:   {ts}\n"
            f"Threshold: < {threshold:.0f}% (hysteresis {hysteresis:.0f}%)\n\n"
            f"Action: Lets schedule a refill!  https://salty-water.com"
        )
        ses.send_email(
            FromEmailAddress=SES_FROM,
            Destination={"ToAddresses": to_addrs},
            Content={"Simple": {
                "Subject": {"Data": subject},
                "Body": {"Text": {"Data": body_text}}
            }},
        )

    _update_state(
        sensor_id=sensor_id,
        new_state=new_state,
        level=level,
        ts=ts,
        alert_sent=bool(send and to_addrs),
        threshold=threshold,
        hysteresis=hysteresis,
        cooldown=cooldown,
    )

    return {"ok": True, "state": {"prev": prev_state, "new": new_state}, "sent": to_addrs if send else []}
