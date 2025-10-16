import os, json, time, boto3, logging
from decimal import Decimal, InvalidOperation

dynamo = boto3.resource("dynamodb")
log = logging.getLogger()
log.setLevel(logging.INFO)

HIST_TABLE = os.getenv("TABLE_NAME", "BrineTankReadings")
LATEST_TABLE = os.getenv("LATEST_TABLE_NAME", "BrineTankLatest")

hist = dynamo.Table(HIST_TABLE)
latest = dynamo.Table(LATEST_TABLE)

def to_decimal(x):
    if x is None: return None
    if isinstance(x, Decimal): return x
    if isinstance(x, (int,)): return Decimal(x)
    try:
        return Decimal(str(x))
    except (InvalidOperation, ValueError, TypeError):
        return None

def handler(event, context):
    # Allow either dict or string
    if isinstance(event, str):
        event = json.loads(event)

    device  = event.get("device")
    ts      = event.get("ts") or time.strftime("%Y-%m-%dT%H:%M:%S")
    sensor  = event.get("sensor", "A02YYUW")
    unit    = event.get("unit", "cm")
    status  = int(event.get("status", 0)) if event.get("status") is not None else 0

    dist    = to_decimal(event.get("distance_cm"))
    filt    = to_decimal(event.get("distance_cm_filtered"))

    if not device:
        raise ValueError("Missing 'device' in event")

    # 1) Write to history table
    hist_item = {
        "device": device,
        "ts": ts,
        "sensor": sensor,
        "unit": unit,
        "status": status,
    }
    if dist is not None: hist_item["distance_cm"] = dist
    if filt is not None: hist_item["distance_cm_filtered"] = filt

    hist.put_item(Item=hist_item)

    # 2) Upsert latest table (1 item per device)
    latest_item = {
        "device": device,
        "ts": ts,              # last updated
        "sensor": sensor,
        "unit": unit,
        "status": status,
    }
    if dist is not None: latest_item["distance_cm"] = dist
    if filt is not None: latest_item["distance_cm_filtered"] = filt

    latest.put_item(Item=latest_item)

    return {"ok": True, "history": hist_item, "latest": latest_item}
