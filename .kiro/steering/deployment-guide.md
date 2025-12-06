---
inclusion: manual
---

# Deployment Guide for Brine Tank IoT System

## Prerequisites Checklist

### AWS Account Setup
- [ ] AWS account with appropriate permissions
- [ ] AWS CLI installed and configured
- [ ] CDK CLI installed: `npm install -g aws-cdk`
- [ ] Python 3.11+ installed
- [ ] Git installed

### AWS Service Configuration
- [ ] Amazon SES sender email verified
- [ ] If in SES sandbox, recipient emails verified
- [ ] IoT Core endpoint identified for your region
- [ ] IAM permissions for CDK deployment

## Initial Setup

### 1. Clone and Configure Environment

```powershell
# Clone repository
git clone <repository-url>
cd brinetank-iot-cdk

# Create virtual environment
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# Install dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

### 2. Configure Deployment Parameters

Edit `app.py`:

```python
processing = BrinetankIotCdkStack(
    app,
    "BrinetankIotCdkStack",
    env_name="prod",                    # Environment name
    ses_from="alerts@yourdomain.com",   # Verified SES sender
    sensor_email_map={                  # Initial sensor config
        "sensor-garage": ["you@example.com"],
    },
)
```

### 3. Verify SES Configuration

```powershell
# Check verified identities
aws ses list-verified-email-addresses

# If not verified, verify sender email
aws ses verify-email-identity --email-address alerts@yourdomain.com
```

### 4. Bootstrap CDK (First Time Only)

```powershell
# Bootstrap for your account/region
cdk bootstrap

# Verify bootstrap stack exists
aws cloudformation describe-stacks --stack-name CDKToolkit
```

## Deployment Process

### Standard Deployment

```powershell
# Activate virtual environment
.\.venv\Scripts\Activate.ps1

# Synthesize CloudFormation template (optional, for review)
cdk synth

# Deploy stack
cdk deploy

# Confirm changes when prompted
# Review IAM changes and type 'y' to proceed
```

### Watch Mode (Development)

```powershell
# Auto-deploy on file changes
cdk watch

# Excludes: tests, README, __pycache__, etc. (see cdk.json)
```

### Deployment Output

After successful deployment, note the outputs:
- Lambda function ARNs
- DynamoDB table names
- IoT Rule name

## Post-Deployment Configuration

### 1. Configure Sensor Notifications

Create items in `SensorNotificationConfig` table:

```powershell
# Using AWS CLI
aws dynamodb put-item \
  --table-name SensorNotificationConfig \
  --item '{
    "sensorId": {"S": "sensor-garage"},
    "emails": {"L": [{"S": "you@example.com"}]},
    "thresholdPercent": {"N": "15"},
    "hysteresisPercent": {"N": "3"},
    "cooldownSeconds": {"N": "21600"}
  }'
```

Or use AWS Console:
1. Navigate to DynamoDB → Tables → SensorNotificationConfig
2. Create item with required attributes
3. Repeat for each sensor

### 2. Configure IoT Devices

Update device configuration to publish to:
- **Topic**: `pi/{device-id}/telemetry`
- **Endpoint**: Your AWS IoT Core endpoint
- **Authentication**: X.509 certificate

Get IoT endpoint:
```powershell
aws iot describe-endpoint --endpoint-type iot:Data-ATS
```

### 3. Test the Pipeline

Send test message:
```powershell
aws iot-data publish \
  --topic "pi/sensor-garage/telemetry" \
  --payload '{"device":"sensor-garage","distance_cm":65}'
```

Verify in DynamoDB:
```powershell
# Check latest reading
aws dynamodb get-item \
  --table-name BrineTankLatest \
  --key '{"device":{"S":"sensor-garage"}}'
```

## Environment-Specific Deployments

### Multiple Environments

Create separate stacks for dev/staging/prod:

```python
# app.py
dev_stack = BrinetankIotCdkStack(
    app, "BrinetankIotCdkStack-Dev",
    env_name="dev",
    ses_from="alerts-dev@yourdomain.com",
    sensor_email_map={"sensor-test": ["dev@example.com"]},
)

prod_stack = BrinetankIotCdkStack(
    app, "BrinetankIotCdkStack-Prod",
    env_name="prod",
    ses_from="alerts@yourdomain.com",
    sensor_email_map={"sensor-garage": ["you@example.com"]},
)
```

Deploy specific stack:
```powershell
cdk deploy BrinetankIotCdkStack-Dev
cdk deploy BrinetankIotCdkStack-Prod
```

## Updating Existing Deployment

### Code Changes

```powershell
# Make changes to Lambda code or CDK stack
# ...

# Run tests
pytest

# Deploy updates
cdk deploy

# CDK will show diff of changes
# Review and confirm
```

### Configuration Changes

For Lambda environment variables:
1. Update values in `brinetank_iot_cdk_stack.py`
2. Run `cdk deploy`
3. Lambda will be updated automatically

For sensor configurations:
1. Update DynamoDB items directly (no deployment needed)
2. Changes take effect immediately

## Rollback Procedures

### Rollback to Previous Version

```powershell
# List CloudFormation stacks
aws cloudformation describe-stacks --stack-name BrinetankIotCdkStack

# Rollback via CloudFormation
aws cloudformation rollback-stack --stack-name BrinetankIotCdkStack
```

### Emergency Disable

Disable IoT Rule:
```powershell
aws iot disable-topic-rule --rule-name BrineTankIngestRule
```

Re-enable:
```powershell
aws iot enable-topic-rule --rule-name BrineTankIngestRule
```

## Monitoring Deployment

### CloudWatch Logs

```powershell
# Tail ingest Lambda logs
aws logs tail /aws/lambda/BrineTankIngest-prod --follow

# Tail alert Lambda logs
aws logs tail /aws/lambda/LowLevelAlert-prod --follow
```

### CloudWatch Metrics

Monitor in AWS Console:
- Lambda invocations, errors, duration
- DynamoDB consumed capacity
- IoT Rule success/failure

### Alarms (Optional)

Create CloudWatch alarms for:
- Lambda error rate > threshold
- Lambda throttles
- DynamoDB throttles

## Troubleshooting Deployment Issues

### CDK Bootstrap Errors
```
Error: This stack uses assets, so the toolkit stack must be deployed
```
**Solution**: Run `cdk bootstrap`

### IAM Permission Errors
```
User is not authorized to perform: cloudformation:CreateStack
```
**Solution**: Ensure AWS credentials have sufficient permissions

### SES Errors
```
Email address is not verified
```
**Solution**: Verify sender email in SES console or via CLI

### Lambda Deployment Errors
```
Code size exceeds maximum
```
**Solution**: Check Lambda code size, remove unnecessary dependencies

## Cleanup / Teardown

### Delete Stack

```powershell
# Delete stack (keeps RETAIN resources)
cdk destroy

# Confirm deletion when prompted
```

### Manual Cleanup

After `cdk destroy`, manually delete:
- DynamoDB tables (if RETAIN policy)
- CloudWatch log groups
- IoT certificates and policies

```powershell
# Delete DynamoDB tables
aws dynamodb delete-table --table-name BrineTankReadings
aws dynamodb delete-table --table-name BrineTankLatest
aws dynamodb delete-table --table-name SensorNotificationConfig

# Delete log groups
aws logs delete-log-group --log-group-name /aws/lambda/BrineTankIngest-prod
aws logs delete-log-group --log-group-name /aws/lambda/LowLevelAlert-prod
```

## Best Practices

### Pre-Deployment
- Review `cdk diff` output before deploying
- Test changes in dev environment first
- Backup DynamoDB tables if needed
- Document configuration changes

### During Deployment
- Monitor CloudWatch logs during deployment
- Test with sample IoT message immediately after
- Verify alert emails are received

### Post-Deployment
- Update documentation with any config changes
- Tag deployment in git: `git tag v1.0.0`
- Update runbook with any new procedures
- Notify team of changes

## Deployment Checklist

- [ ] Virtual environment activated
- [ ] Dependencies installed
- [ ] `app.py` configured correctly
- [ ] SES sender email verified
- [ ] Tests passing: `pytest`
- [ ] CDK diff reviewed: `cdk diff`
- [ ] Deployment successful: `cdk deploy`
- [ ] Sensor configs created in DynamoDB
- [ ] Test message sent and processed
- [ ] CloudWatch logs verified
- [ ] Alert email received (if triggered)
- [ ] Documentation updated
- [ ] Git commit and tag created
