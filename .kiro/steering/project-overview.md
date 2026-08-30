---
inclusion: always
---

# Brine Tank IoT Project Overview

## Project Purpose
This is an AWS CDK-based IoT monitoring system for water softener brine tanks. It uses ultrasonic distance sensors to measure salt levels and sends email alerts when levels are low.

## Technology Stack
- **Infrastructure**: AWS CDK (Python)
- **Compute**: AWS Lambda (Python 3.11-3.12)
- **Storage**: Amazon DynamoDB
- **Messaging**: AWS IoT Core (MQTT)
- **Notifications**: Amazon SES (Simple Email Service)

## Core Components

### 1. IoT Ingestion Pipeline
- **Topic Pattern**: `pi/+/telemetry`
- **IoT Rule**: Routes MQTT messages to Lambda
- **Ingest Lambda**: Processes telemetry, calculates fill %, stores data

### 2. Data Storage
- **BrineTankReadings**: Historical time-series data with 7-day TTL
- **BrineTankLatest**: Current state snapshot per device
- **SensorNotificationConfig**: Per-sensor alert settings

### 3. Alert System
- **LowLevelAlert Lambda**: State machine for smart alerting
- **Features**: Hysteresis, cooldown periods, per-sensor configuration
- **Delivery**: Email via Amazon SES

## Key Design Patterns

### Fill Percentage Calculation
Distance measurements are converted to fill percentage using calibration values:
- `EMPTY_DISTANCE`: Distance when tank is empty (default: 70cm)
- `FULL_DISTANCE`: Distance when tank is full (default: 6cm)
- Formula: `((EMPTY_DISTANCE - measured) / RANGE) * 100`

### Alert State Machine
Prevents notification spam using:
- **Threshold**: Alert when level drops below X%
- **Hysteresis**: Recover only when level rises above threshold + hysteresis
- **Cooldown**: Minimum time between alerts (default: 6 hours)

### Data Retention
- Historical readings auto-expire after 45 days (configurable TTL via `TTL_DAYS` env var on the ingest Lambda)
- The customer portal's history view shows the most recent 30 days (configurable via `HISTORY_DAYS` env var on `CustomerPortalApi`)
- The customer portal also shows salt-delivery order history (`GET /orders`), read from the `SaltDeliveryAppStack` orders table imported by name via the `ORDERS_TABLE_NAME` env var — read-only, filtered to the signed-in customer's email
- Latest readings persist indefinitely for status checks
- Alert state persists in configuration table
- TTL changes only apply to newly-written readings — existing items keep whatever `ttl_epoch` was computed at write time

## Development Workflow

### Local Development
1. Activate virtual environment: `.\.venv\Scripts\Activate.ps1`
2. Make changes to Lambda code or CDK stack
3. Run tests: `pytest`
4. Deploy: `cdk deploy` or `cdk watch` for rapid iteration

### Testing
- Unit tests in `tests/unit/`
- Direct Lambda invocation for integration testing
- IoT message simulation via AWS CLI

### Deployment
- Environment configured in `app.py`
- Stack name: `BrinetankIotCdkStack`
- Supports multiple environments via `env_name` parameter

## Common Tasks

### Adding a New Sensor
1. Configure IoT device with unique device ID
2. Add to `sensor_email_map` in `app.py` (optional)
3. Create DynamoDB item in `SensorNotificationConfig`
4. Deploy stack

### Adjusting Alert Thresholds
Update DynamoDB item in `SensorNotificationConfig` table:
- `thresholdPercent`: When to alert
- `hysteresisPercent`: Recovery buffer
- `cooldownSeconds`: Time between alerts

### Calibrating Fill Percentage
Adjust environment variables in CDK stack:
- Measure distance when tank is full → `FULL_DISTANCE`
- Measure distance when tank is empty → `EMPTY_DISTANCE`
- Redeploy stack

## Security Considerations
- Lambda functions use least-privilege IAM roles
- IoT devices should use X.509 certificates
- SES sender must be verified
- DynamoDB encryption at rest enabled
- No hardcoded credentials in code

## Cost Optimization
- DynamoDB: Pay-per-request (no provisioned capacity)
- Lambda: Minimal invocations (per reading + alerts)
- TTL-based cleanup reduces storage costs
- Async alert invocation prevents blocking
