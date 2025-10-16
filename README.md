# brinetank-iot-cdk

CDK (Python, v2) stack for ingesting ultrasonic sensor telemetry from AWS IoT Core into DynamoDB.

## What it creates
- **DynamoDB**
  - `BrineTankReadings` (PK: `device`, SK: `ts`) – history
  - `BrineTankLatest`   (PK: `device`) – current state per device
- **Lambda**
  - `BrineTankIngest` – invoked by IoT Rule, writes to both tables
- **AWS IoT Core**
  - Rule `BrineTankIngestRule` (`SELECT ... FROM 'pi/+/telemetry'`) → Lambda

## Deploy
```bash
# Windows PowerShell
.\.venv\Scripts\Activate.ps1
cdk bootstrap   # once per account/region
cdk deploy
