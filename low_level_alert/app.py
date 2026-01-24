import os, json, boto3, datetime, time, logging
from typing import List, Tuple

# Initialize logging
log = logging.getLogger()
log.setLevel(logging.INFO)

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
    log.info(f"Loading configuration for sensor: {sensor_id}")
    
    resp = dynamo.get_item(
        TableName=CONFIG_TABLE,
        Key={"sensorId": {"S": sensor_id}},
        ConsistentRead=True,
    )
    item = resp.get("Item", {}) or {}
    log.info(f"DynamoDB response for {sensor_id}: {json.dumps(item, default=str)}")
    
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

    config = {
        "emails": emails,
        "threshold": threshold,
        "hysteresis": hysteresis,
        "cooldown": cooldown,
        "lastState": last_state,
        "lastAlertTs": last_alert_ts,
        "lastSeenTs": last_seen_ts,
        "lastLevel": last_level,
    }
    
    log.info(f"Parsed config for {sensor_id}: {json.dumps(config, default=str)}")
    return config

def _should_alert(prev_state: str, last_alert_ts: str, cooldown_s: int, level: float, threshold: float) -> bool:
    """Determine if we should send an alert based on state transition and cooldown."""
    log.info(f"Checking alert conditions: prev_state={prev_state}, level={level:.1f}%, threshold={threshold:.1f}%")
    
    # Only alert on transitions from normal -> low and if cooldown has elapsed
    is_low = level < threshold
    log.info(f"Level check: is_low={is_low} (level {level:.1f}% < threshold {threshold:.1f}%)")
    
    if not is_low:
        log.info("No alert: level is not below threshold")
        return False
        
    if prev_state == "low":
        log.info("No alert: already in low state (no state transition)")
        return False
    
    # cooldown check
    if not last_alert_ts:
        log.info("Alert allowed: no previous alert timestamp found")
        return True
        
    try:
        last = datetime.datetime.fromisoformat(last_alert_ts.replace("Z",""))
        delta = datetime.datetime.utcnow() - last
        elapsed_seconds = delta.total_seconds()
        elapsed_hours = elapsed_seconds / 3600
        cooldown_hours = cooldown_s / 3600
        
        log.info(f"Cooldown check: last_alert={last_alert_ts}, elapsed={elapsed_hours:.1f}h, cooldown={cooldown_hours:.1f}h")
        
        if elapsed_seconds >= cooldown_s:
            log.info("Alert allowed: cooldown period has elapsed")
            return True
        else:
            log.info(f"No alert: still in cooldown period ({elapsed_hours:.1f}h < {cooldown_hours:.1f}h)")
            return False
            
    except Exception as e:
        log.error(f"Error parsing last alert timestamp '{last_alert_ts}': {e}")
        log.info("Alert allowed: timestamp parsing failed, assuming cooldown elapsed")
        return True

def _compute_new_state(prev_state: str, level: float, threshold: float, hysteresis: float) -> str:
    """Compute new state based on level and hysteresis logic."""
    log.info(f"Computing state: prev_state={prev_state}, level={level:.1f}%, threshold={threshold:.1f}%, hysteresis={hysteresis:.1f}%")
    
    # Enter LOW if below threshold
    if level < threshold:
        log.info(f"New state: LOW (level {level:.1f}% < threshold {threshold:.1f}%)")
        return "low"
        
    # Recover to NORMAL only after we climb above threshold + hysteresis
    recovery_threshold = threshold + hysteresis
    if level >= recovery_threshold:
        log.info(f"New state: NORMAL (level {level:.1f}% >= recovery threshold {recovery_threshold:.1f}%)")
        return "normal"
        
    # inside the hysteresis band: keep prior state
    log.info(f"New state: {prev_state.upper()} (in hysteresis band: {threshold:.1f}% <= {level:.1f}% < {recovery_threshold:.1f}%)")
    return prev_state

def _update_state(sensor_id: str, new_state: str, level: float, ts: str, alert_sent: bool, threshold: float, hysteresis: float, cooldown: int):
    """Write back latest state/levels; set lastAlertTs only when alert sent."""
    log.info(f"Updating state for {sensor_id}: new_state={new_state}, level={level:.1f}%, alert_sent={alert_sent}")
    
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
        log.info(f"Setting lastAlertTs to {ts} for {sensor_id}")

    try:
        dynamo.update_item(
            TableName=CONFIG_TABLE,
            Key={"sensorId": {"S": sensor_id}},
            UpdateExpression=", ".join(expr),
            ExpressionAttributeValues=vals,
        )
        log.info(f"Successfully updated DynamoDB state for {sensor_id}")
    except Exception as e:
        log.error(f"Failed to update DynamoDB state for {sensor_id}: {e}")
        raise

def handler(event, context):
    """Lambda entry point for processing brine tank level alerts."""
    log.info(f"Processing alert event: {json.dumps(event, default=str)}")
    
    try:
        msg = event if isinstance(event, dict) else json.loads(event)
        sensor_id = msg["sensorId"]
        level = float(msg["levelPct"])
        ts = msg.get("ts") or _now_iso()
        
        log.info(f"Alert request: sensor_id={sensor_id}, level={level:.1f}%, timestamp={ts}")

        cfg = _load_config(sensor_id)

        # Allow direct "to" override in test events
        to_addrs = []
        if "to" in msg:
            to_addrs = [msg["to"]] if isinstance(msg["to"], str) else list(msg["to"])
            log.info(f"Using override email addresses from event: {to_addrs}")
        if not to_addrs:
            to_addrs = cfg["emails"]
            log.info(f"Using configured email addresses: {to_addrs}")

        if not to_addrs:
            log.warning(f"No email addresses configured for sensor {sensor_id}")

        # Always update state, but only SEND on crossing + cooldown
        prev_state = cfg["lastState"]
        threshold = cfg["threshold"] or DEFAULT_THRESHOLD
        hysteresis = cfg["hysteresis"] or DEFAULT_HYSTERESIS
        cooldown = cfg["cooldown"] or DEFAULT_COOLDOWN

        log.info(f"Alert parameters: threshold={threshold:.1f}%, hysteresis={hysteresis:.1f}%, cooldown={cooldown/3600:.1f}h")

        new_state = _compute_new_state(prev_state, level, threshold, hysteresis)
        send = _should_alert(prev_state, cfg["lastAlertTs"], cooldown, level, threshold)

        log.info(f"Decision: prev_state={prev_state} -> new_state={new_state}, should_send_alert={send}")

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
            
            log.info(f"Sending email alert to {to_addrs}")
            log.info(f"Email subject: {subject}")
            log.info(f"SES sender: {SES_FROM}")
            
            try:
                ses_response = ses.send_email(
                    FromEmailAddress=SES_FROM,
                    Destination={"ToAddresses": to_addrs},
                    Content={"Simple": {
                        "Subject": {"Data": subject},
                        "Body": {"Text": {"Data": body_text}}
                    }},
                )
                log.info(f"Email sent successfully: {json.dumps(ses_response, default=str)}")
                
            except Exception as e:
                log.error(f"Failed to send email via SES: {e}")
                log.error(f"SES error details: FromEmailAddress={SES_FROM}, ToAddresses={to_addrs}")
                raise
                
        elif send and not to_addrs:
            log.warning(f"Alert should be sent but no email addresses available for sensor {sensor_id}")
        elif not send:
            log.info("No alert sent - conditions not met")

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

        result = {"ok": True, "state": {"prev": prev_state, "new": new_state}, "sent": to_addrs if send else []}
        log.info(f"Handler completed successfully: {json.dumps(result, default=str)}")
        return result
        
    except Exception as e:
        log.error(f"Handler failed with error: {e}")
        log.error(f"Event that caused error: {json.dumps(event, default=str)}")
        raise
