import os, json, boto3, logging
from decimal import Decimal
from boto3.dynamodb.conditions import Key

dynamo = boto3.resource("dynamodb")
log = logging.getLogger()
log.setLevel(logging.INFO)

CUSTOMER_DEVICES_TABLE = os.getenv("CUSTOMER_DEVICES_TABLE", "CustomerDevices")
LATEST_TABLE = os.getenv("LATEST_TABLE_NAME", "BrineTankLatest")
HIST_TABLE = os.getenv("HIST_TABLE_NAME", "BrineTankReadings")

customer_devices = dynamo.Table(CUSTOMER_DEVICES_TABLE)
latest = dynamo.Table(LATEST_TABLE)
hist = dynamo.Table(HIST_TABLE)

HISTORY_LIMIT = 200


def _decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def _response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body, default=_decimal_default),
    }


def _caller_email(event):
    claims = event.get("requestContext", {}).get("authorizer", {}).get("jwt", {}).get("claims", {})
    email = claims.get("email")
    return email.lower() if email else None


def _list_devices(email):
    resp = customer_devices.query(KeyConditionExpression=Key("email").eq(email))
    sensor_ids = [item["sensorId"] for item in resp.get("Items", [])]

    devices = []
    for sensor_id in sensor_ids:
        item = latest.get_item(Key={"device": sensor_id}).get("Item")
        devices.append(item or {"device": sensor_id, "status": "no_data"})
    return devices


def _device_history(email, sensor_id):
    owned = customer_devices.get_item(Key={"email": email, "sensorId": sensor_id}).get("Item")
    if not owned:
        return None  # caller does not own this device

    resp = hist.query(
        KeyConditionExpression=Key("device").eq(sensor_id),
        ScanIndexForward=False,
        Limit=HISTORY_LIMIT,
    )
    return resp.get("Items", [])


def handler(event, context):
    route = event.get("routeKey", "")
    email = _caller_email(event)
    if not email:
        log.warning("Request missing email claim")
        return _response(401, {"error": "unauthorized"})

    try:
        if route == "GET /devices":
            return _response(200, {"devices": _list_devices(email)})

        if route == "GET /devices/{sensorId}/history":
            sensor_id = event.get("pathParameters", {}).get("sensorId")
            readings = _device_history(email, sensor_id)
            if readings is None:
                return _response(403, {"error": "forbidden"})
            return _response(200, {"sensorId": sensor_id, "readings": readings})

        return _response(404, {"error": "not_found"})
    except Exception as e:
        log.error(f"Failed to handle {route} for {email}: {e}")
        return _response(500, {"error": "internal_error"})
