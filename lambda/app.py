import os, json, boto3, time, datetime, logging
from decimal import Decimal, InvalidOperation  # <-- needed for to_decimal()

lambda_client = boto3.client("lambda")
ALERT_FN_NAME = os.environ.get("ALERT_FN_NAME")

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
    ts      = event.get("ts") or time.strftime("%Y-%m-%dT%H:%M:%S")  # ISO-ish; fine for our use
    sensor  = event.get("sensor", "A02YYUW")
    unit    = event.get("unit", "cm")
    status  = int(event.get("status", 0)) if event.get("status") is not None else 0

    dist    = event.get("distance_cm")
    dist_f  = event.get("distance_cm_filtered")
    temp_c  = event.get("temperature_c")  # optional

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

    # TTL: now + N days
    TTL_DAYS = int(os.getenv("TTL_DAYS", "7"))  # default 7 days
    ttl_epoch = int(time.time()) + TTL_DAYS * 24 * 3600

    # 1) History item
    hist_item = {
        "device": device,
        "ts": ts,
        "sensor": sensor,
        "unit": unit,
        "status": status,
        "ttl_epoch": ttl_epoch,
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

    # 3) 🔔 Invoke the alert lambda for every reading (it will manage hysteresis/cooldown)
    if ALERT_FN_NAME and percent_full is not None and percent_full < 10:
        try:
            lambda_client.invoke(
                FunctionName=ALERT_FN_NAME,
                InvocationType="Event",  # async
                Payload=json.dumps({
                    "sensorId": device,              # expected by LowLevelAlert
                    "levelPct": float(percent_full), # use % full
                    "ts": ts                         # pass through your reading timestamp
                }).encode("utf-8"),
            )
            log.info(f"Low level alert triggered for {device}: {percent_full:.1f}% full")
        except Exception as e:
            log.warning(f"Failed to invoke {ALERT_FN_NAME}: {e}")
    else:
        log.debug(f"No alert sent — {device} at {percent_full:.1f}% full")

    return {"ok": True, "history": hist_item, "latest": latest_item}
