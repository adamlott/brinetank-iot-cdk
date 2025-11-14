from pathlib import Path
from aws_cdk import (
    Stack, Duration, RemovalPolicy,
    aws_lambda as _lambda,
    aws_dynamodb as dynamodb,
    aws_iam as iam,
    aws_iot as iot,
)
from constructs import Construct
import json

PROJECT_ROOT = Path(__file__).parent.parent
LOW_LEVEL_ALERT_DIR = (PROJECT_ROOT / "low_level_alert").as_posix()
INGEST_DIR = (PROJECT_ROOT / "lambda").as_posix()

class BrinetankIotCdkStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        env_name: str = "prod",
        ses_from: str,                     # NEW: e.g. "alerts@salty-water.com"
        sensor_email_map: dict,            # NEW: {"sensor-garage": ["you@..."], ...}
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Tables
        hist_table = dynamodb.Table(
            self, "BrineTankReadings",
            table_name="BrineTankReadings",
            partition_key=dynamodb.Attribute(name="device", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="ts", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            time_to_live_attribute="ttl_epoch",
            removal_policy=RemovalPolicy.RETAIN
        )
        latest_table = dynamodb.Table(
            self, "BrineTankLatest",
            table_name="BrineTankLatest",
            partition_key=dynamodb.Attribute(name="device", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN
        )
        notif_table = dynamodb.Table(
            self, "SensorNotificationConfig",
            table_name="SensorNotificationConfig",
            partition_key=dynamodb.Attribute(name="sensorId", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN,
        )

        # ALERT LAMBDA (SES)
        alert_fn = _lambda.Function(
            self, "LowLevelAlert",
            function_name=f"LowLevelAlert-{env_name}",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="app.handler",
            code=_lambda.Code.from_asset(LOW_LEVEL_ALERT_DIR),
            timeout=Duration.seconds(10),
            environment={
                "SES_FROM": ses_from,
                "CONFIG_TABLE": notif_table.table_name,
            },
        )

        # Allow SES send
        notif_table.grant_read_write_data(alert_fn)
        alert_fn.add_to_role_policy(iam.PolicyStatement(
            actions=["ses:SendEmail", "ses:SendRawEmail"],
            resources=["*"]
        ))

        # INGEST LAMBDA
        ingest_fn = _lambda.Function(
            self, "BrineTankIngest",
            function_name=f"BrineTankIngest-{env_name}",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="app.handler",
            code=_lambda.Code.from_asset(INGEST_DIR),
            timeout=Duration.seconds(30),
            environment={
                "TABLE_NAME": hist_table.table_name,
                "LATEST_TABLE_NAME": latest_table.table_name,
                "EMPTY_DISTANCE": "70",
                "FULL_DISTANCE": "6",
                "TTL_DAYS": "7",
                "ALERT_FN_NAME": f"LowLevelAlert-{env_name}",  # invoke by name
            },
        )
        hist_table.grant_write_data(ingest_fn)
        latest_table.grant_write_data(ingest_fn)

        # Allow IoT → ingest
        ingest_fn.add_permission(
            "AllowIotInvoke",
            principal=iam.ServicePrincipal("iot.amazonaws.com"),
            action="lambda:InvokeFunction",
        )

        # Allow ingest → alert
        alert_fn.grant_invoke(ingest_fn)

        # IoT rule
        sql = "SELECT device, sensor, unit, distance_cm, distance_cm_filtered, status, temperature_c, ts FROM 'pi/+/telemetry'"
        iot.CfnTopicRule(
            self, "BrineTankIngestRule",
            rule_name=f"BrineTankIngestRule",
            topic_rule_payload=iot.CfnTopicRule.TopicRulePayloadProperty(
                sql=sql,
                aws_iot_sql_version="2016-03-23",
                rule_disabled=False,
                actions=[iot.CfnTopicRule.ActionProperty(
                    lambda_=iot.CfnTopicRule.LambdaActionProperty(
                        function_arn=ingest_fn.function_arn
                    )
                )],
            ),
        )
