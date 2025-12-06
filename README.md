# Brine Tank IoT Monitoring System

An AWS CDK-based IoT solution for monitoring water softener brine tank levels using ultrasonic sensors. The system ingests telemetry data from IoT devices, calculates fill percentages, stores historical data, and sends email alerts when levels drop below configurable thresholds.

## Overview

This system monitors brine tank levels in real-time using ultrasonic distance sensors connected to IoT devices (e.g., Raspberry Pi). When the sensor detects low salt levels, the system automatically sends email notifications to configured recipients.

### Key Features

- Real-time telemetry ingestion from AWS IoT Core
- Automatic fill percentage calculation based on distance measurements
- Historical data storage with automatic TTL-based cleanup
- Smart alerting with configurable thresholds, hysteresis, and cooldown periods
- Email notifications via Amazon SES
- Per-sensor notification configuration
- Latest reading cache for quick status checks

## Architecture

### AWS Resources Created

**DynamoDB Tables:**
- `BrineTankReadings` - Historical sensor readings (PK: device, SK: ts) with 7-day TTL
- `BrineTankLatest` - Current state per device (PK: device)
- `SensorNotificationConfig` - Per-sensor alert configuration (PK: sensorId)

**Lambda Functions:**
- `BrineTankIngest` - Processes IoT telemetry, calculates fill %, stores data, triggers alerts
- `LowLevelAlert` - Manages alert state machine and sends SES emails

**AWS IoT Core:**
- IoT Rule: `BrineTankIngestRule` - Routes messages from `pi/+/telemetry` topic to ingest Lambda

### Data Flow

1. IoT device publishes telemetry to `pi/{device-id}/telemetry`
2. IoT Rule triggers `BrineTankIngest` Lambda
3. Lambda calculates fill percentage from distance measurement
4. Data written to both history and latest tables
5. If level < 10%, `LowLevelAlert` Lambda invoked asynchronously
6. Alert Lambda checks state/cooldown and sends email if needed

## Prerequisites

- Python 3.11+
- AWS CLI configured with appropriate credentials
- AWS CDK CLI (`npm install -g aws-cdk`)
- Amazon SES verified sender email address
- IoT devices configured to publish to AWS IoT Core

## Installation

### 1. Clone and Setup Virtual Environment

```bash
# Clone the repository
git clone <repository-url>
cd brinetank-iot-cdk

# Create virtual environment (Windows)
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# Install dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

### 2. Configure Deployment

Edit `app.py` to configure your deployment:

```python
processing = BrinetankIotCdkStack(
    app,
    "BrinetankIotCdkStack",
    env_name="prod",
    ses_from="alerts@yourdomain.com",  # Must be verified in SES
    sensor_email_map={
        "sensor-garage": ["you@example.com"],
        "sensor-basement": ["family@example.com"],
    },
)
```

### 3. Deploy to AWS

```bash
# Bootstrap CDK (first time only, per account/region)
cdk bootstrap

# Deploy the stack
cdk deploy

# Watch mode for development
cdk watch
```

## Configuration

### Sensor Calibration

The ingest Lambda uses environment variables to calculate fill percentage:

- `EMPTY_DISTANCE`: Distance in cm when tank is empty (default: 70)
- `FULL_DISTANCE`: Distance in cm when tank is full (default: 6)
- `TTL_DAYS`: Days to retain historical data (default: 7)

Adjust these in `brinetank_iot_cdk_stack.py` based on your tank dimensions.

### Alert Configuration

Alerts are configured per sensor in the `SensorNotificationConfig` DynamoDB table:

**Table Schema:**
```
sensorId (String, PK)
emails (List<String>)
thresholdPercent (Number) - Alert when level drops below this % (default: 10)
hysteresisPercent (Number) - Recovery buffer to prevent flapping (default: 2)
cooldownSeconds (Number) - Minimum time between alerts (default: 21600 = 6 hours)
lastState (String) - Current state: "normal" or "low"
lastAlertTs (String) - ISO timestamp of last alert sent
lastSeenTs (String) - ISO timestamp of last reading
lastLevel (Number) - Last observed fill percentage
```

**Example DynamoDB Item:**
```json
{
  "sensorId": "sensor-garage",
  "emails": ["you@example.com", "family@example.com"],
  "thresholdPercent": 15,
  "hysteresisPercent": 3,
  "cooldownSeconds": 21600,
  "lastState": "normal"
}
```

### Alert State Machine

The alert system uses hysteresis to prevent notification spam:

1. **Normal → Low**: Alert sent when level drops below `thresholdPercent`
2. **Low → Normal**: State recovers only when level rises above `thresholdPercent + hysteresisPercent`
3. **Cooldown**: Minimum time between alerts even if level fluctuates

## IoT Device Integration

### MQTT Topic Format

Devices should publish to: `pi/{device-id}/telemetry`

### Message Payload

```json
{
  "device": "sensor-garage",
  "sensor": "A02YYUW",
  "unit": "cm",
  "distance_cm": 45.2,
  "distance_cm_filtered": 44.8,
  "temperature_c": 22.5,
  "status": 0,
  "ts": "2025-12-05T10:30:00Z"
}
```

**Required Fields:**
- `device` - Unique sensor identifier
- `distance_cm` - Raw distance measurement

**Optional Fields:**
- `distance_cm_filtered` - Smoothed/filtered distance
- `temperature_c` - Ambient temperature
- `sensor` - Sensor model
- `unit` - Measurement unit
- `status` - Sensor status code
- `ts` - ISO timestamp (auto-generated if missing)

## Testing

### Run Unit Tests

```bash
pytest tests/
```

### Test Alert Lambda Directly

```bash
aws lambda invoke \
  --function-name LowLevelAlert-prod \
  --payload '{"sensorId":"sensor-garage","levelPct":8.5,"to":"test@example.com"}' \
  response.json
```

### Monitor IoT Messages

```bash
# Subscribe to telemetry topic
aws iot-data publish \
  --topic "pi/sensor-garage/telemetry" \
  --payload '{"device":"sensor-garage","distance_cm":65}'
```

## Monitoring and Troubleshooting

### CloudWatch Logs

- `/aws/lambda/BrineTankIngest-prod` - Ingest processing logs
- `/aws/lambda/LowLevelAlert-prod` - Alert delivery logs

### Common Issues

**No alerts received:**
- Verify SES sender email is verified in AWS Console
- Check recipient emails are verified (if SES is in sandbox mode)
- Review CloudWatch logs for Lambda errors
- Verify sensor configuration exists in `SensorNotificationConfig` table

**Incorrect fill percentage:**
- Verify `EMPTY_DISTANCE` and `FULL_DISTANCE` calibration values
- Check sensor mounting position hasn't changed
- Review raw `distance_cm` values in DynamoDB

**Too many alerts:**
- Increase `cooldownSeconds` in sensor configuration
- Adjust `hysteresisPercent` to widen the recovery band

## Development

### Project Structure

```
brinetank-iot-cdk/
├── app.py                          # CDK app entry point
├── brinetank_iot_cdk/
│   └── brinetank_iot_cdk_stack.py  # Infrastructure definition
├── lambda/
│   └── app.py                      # Ingest Lambda handler
├── low_level_alert/
│   └── app.py                      # Alert Lambda handler
├── tests/
│   └── unit/                       # Unit tests
├── cdk.json                        # CDK configuration
├── requirements.txt                # Python dependencies
└── requirements-dev.txt            # Dev dependencies
```

### Making Changes

```bash
# Activate virtual environment
.\.venv\Scripts\Activate.ps1

# Make code changes
# ...

# Run tests
pytest

# Deploy changes
cdk deploy

# Or use watch mode for rapid iteration
cdk watch
```

### Adding New Sensors

1. Configure IoT device to publish to `pi/{sensor-id}/telemetry`
2. Add sensor to `sensor_email_map` in `app.py` (optional, for initial setup)
3. Create configuration item in `SensorNotificationConfig` table
4. Deploy: `cdk deploy`

## Cost Considerations

- DynamoDB: Pay-per-request billing (minimal cost for typical usage)
- Lambda: Free tier covers most small deployments
- IoT Core: First 250K messages/month free
- SES: $0.10 per 1,000 emails (after free tier)
- Data transfer: Minimal for typical sensor data volumes

Estimated monthly cost for 1-5 sensors with hourly readings: < $5/month

## Security

- Lambda functions use least-privilege IAM roles
- DynamoDB tables use encryption at rest (AWS managed keys)
- IoT devices should use X.509 certificates for authentication
- SES sender identity must be verified to prevent abuse

## License

[Add your license here]

## Support

For issues or questions, please open an issue in the repository.
