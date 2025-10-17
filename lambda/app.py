import os, json, time, boto3, logging
from decimal import Decimal, InvalidOperation

dynamo = boto3.resource("dynamodb")
log = logging.getLogger()
log.setLevel(logging.INFO)

HIST_TABLE = os.getenv("TABLE_NAME", "BrineTankReadings")
LATEST_TABLE = os.getenv("LATEST_TABLE_NAME", "BrineTankLatest")

hist = dynamo.Table(HIST_TABLE)
latest = dynamo.Table(LATEST_TABLE)

# ----- Fill % config -----
EMPTY_DISTANCE = float(os.getenv("EMPTY_DISTANCE", "70"))  # cm
FULL_DISTANCE  = float(os.getenv("FULL_DISTANCE",  "6"))   # cm
RANGE = max(0.0001, EMPTY_DISTANCE - FULL_DISTANCE)

def calculate_fill_percentage(distance_cm: float):
    # Clamp to [FULL_DISTANCE, EMPTY_DISTANCE]
    d = max(FULL_DISTANCE, min(EMPTY_DISTANCE, distance_cm))
    percent_full = ((EMPTY_DISTANCE - d) / RANGE) * 100.0
    return round(percent_full, 1)

def to_decimal(x):
    if x is None: return None
    if isinstance(x, Decimal): return x
    if isinstance(x, (int,)): return Decimal(x)
    try:
        return Decimal(str(x))
    except (InvalidOperation, ValueError, TypeError):
        return None

def handler(event, context):
    if isinstance(event, str):
        event = json.loads(event)

    device  = event.get("device")
    ts      = event.get("ts") or time.strftime("%Y-%m-%dT%H:%M:%S")
    sensor  = event.get("sensor", "A02YYUW")
    unit    = event.get("unit", "cm")
    status  = int(event.get("status", 0)) if event.get("status") is not None else 0

    dist    = event.get("distance_cm")
    dist_f  = event.get("distance_cm_filtered")
    temp_c  = event.get("temperature_c")  # from Pi payload (optional)

    if not device:
        raise ValueError("Missing 'device' in event")

    # Compute percent_full if we have a distance
    percent_full = None
    if dist is not None:
        try:
            percent_full = calculate_fill_percentage(float(dist))
        except Exception:
            percent_full = None

    # Convert numerics to Decimal for DynamoDB
    dist_dec   = to_decimal(dist)
    distf_dec  = to_decimal(dist_f)
    temp_dec   = to_decimal(temp_c)
    pct_dec    = to_decimal(percent_full)

    # TTL: now + 90 days
    ttl_epoch = int(time.time()) + 90 * 24 * 3600

    # 1) History item
    hist_item = {
        "device": device,
        "ts": ts,
        "sensor": sensor,
        "unit": unit,
        "status": status,
        "ttl_epoch": ttl_epoch,  # enables 90-day retention
    }
    if dist_dec   is not None: hist_item["distance_cm"] = dist_dec
    if distf_dec  is not None: hist_item["distance_cm_filtered"] = distf_dec
    if pct_dec    is not None: hist_item["percent_full"] = pct_dec
    if temp_dec   is not None: hist_item["temperature_c"] = temp_dec

    hist.put_item(Item=hist_item)

    # 2) Latest item (no TTL so it always exists)
    latest_item = {
        "device": device,
        "ts": ts,
        "sensor": sensor,
        "unit": unit,
        "status": status,
    }
    if dist_dec   is not None: latest_item["distance_cm"] = dist_dec
    if distf_dec  is not None: latest_item["distance_cm_filtered"] = distf_dec
    if pct_dec    is not None: latest_item["percent_full"] = pct_dec
    if temp_dec   is not None: latest_item["temperature_c"] = temp_dec

    latest.put_item(Item=latest_item)

    return {"ok": True, "history": hist_item, "latest": latest_item}
