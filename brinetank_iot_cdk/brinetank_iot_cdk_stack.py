from pathlib import Path
from aws_cdk import (
    Stack, Duration, RemovalPolicy,
    aws_lambda as _lambda,
    aws_lambda_event_sources as lambda_event_sources,
    aws_dynamodb as dynamodb,
    aws_iam as iam,
    aws_iot as iot,
    aws_sqs as sqs,
)
from constructs import Construct
import json

PROJECT_ROOT = Path(__file__).parent.parent
LOW_LEVEL_ALERT_DIR = (PROJECT_ROOT / "low_level_alert").as_posix()
INGEST_DIR = (PROJECT_ROOT / "lambda").as_posix()
EMAIL_SENDER_DIR = (PROJECT_ROOT / "email_sender").as_posix()


class BrinetankIotCdkStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        env_name: str = "prod",
        ses_from: str,           # e.g. "alerts@salty-water.com"
        sensor_email_map: dict,  # {"sensor-garage": ["you@..."], ...}
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ── DynamoDB Tables ──────────────────────────────────────────────────
        hist_table = dynamodb.Table(
            self, "BrineTankReadings",
            table_name="BrineTankReadings",
            partition_key=dynamodb.Attribute(name="device", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="ts", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            time_to_live_attribute="ttl_epoch",
            removal_policy=RemovalPolicy.RETAIN,
        )
        latest_table = dynamodb.Table(
            self, "BrineTankLatest",
            table_name="BrineTankLatest",
            partition_key=dynamodb.Attribute(name="device", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN,
        )
        notif_table = dynamodb.Table(
            self, "SensorNotificationConfig",
            table_name="SensorNotificationConfig",
            partition_key=dynamodb.Attribute(name="sensorId", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN,
        )

        # ── SQS Queue (email notifications) ─────────────────────────────────
        # Dead-letter queue captures messages that fail after max_receive_count attempts
        email_dlq = sqs.Queue(
            self, "EmailNotificationDLQ",
            queue_name=f"EmailNotificationDLQ-{env_name}",
            retention_period=Duration.days(14),
        )
        email_queue = sqs.Queue(
            self, "EmailNotificationQueue",
            queue_name=f"EmailNotificationQueue-{env_name}",
            visibility_timeout=Duration.seconds(30),   # matches email_sender_fn timeout
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=3,
                queue=email_dlq,
            ),
        )

        # ── Email Sender Lambda ──────────────────────────────────────────────
        # Sole function responsible for all SES sends; triggered by SQS
        email_sender_fn = _lambda.Function(
            self, "EmailSender",
            function_name=f"EmailSender-{env_name}",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="app.handler",
            code=_lambda.Code.from_asset(EMAIL_SENDER_DIR),
            timeout=Duration.seconds(30),
            environment={
                "SES_FROM": ses_from,
            },
        )

        # Allow EmailSender to call SES
        email_sender_fn.add_to_role_policy(iam.PolicyStatement(
            actions=["ses:SendEmail", "ses:SendRawEmail"],
            resources=["*"],
        ))

        # Wire SQS → EmailSender (batch size 10, partial batch failure reporting)
        email_sender_fn.add_event_source(
            lambda_event_sources.SqsEventSource(
                email_queue,
                batch_size=10,
                report_batch_item_failures=True,
            )
        )

        # ── Alert Lambda (state machine; publishes to SQS) ───────────────────
        alert_fn = _lambda.Function(
            self, "LowLevelAlert",
            function_name=f"LowLevelAlert-{env_name}",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="app.handler",
            code=_lambda.Code.from_asset(LOW_LEVEL_ALERT_DIR),
            timeout=Duration.seconds(10),
            environment={
                "SQS_QUEUE_URL": email_queue.queue_url,
                "CONFIG_TABLE": notif_table.table_name,
            },
        )

        # Alert Lambda needs DynamoDB read/write and SQS send
        notif_table.grant_read_write_data(alert_fn)
        email_queue.grant_send_messages(alert_fn)

        # ── Ingest Lambda ────────────────────────────────────────────────────
        ingest_fn = _lambda.Function(
            self, "BrineTankIngest",
            function_name=f"BrineTankIngest-{env_name}",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="app.handler",
            code=_lambda.Code.from_asset(INGEST_DIR),
            timeout=Duration.seconds(30),
            environment={
                "TABLE_NAME": hist_table.table_name,
                "LATEST_TABLE_NAME": latest_table.table_name,
                "EMPTY_DISTANCE": "70",
                "FULL_DISTANCE": "6",
                "TTL_DAYS": "7",
                "ALERT_FN_NAME": alert_fn.function_name,
            },
        )
        hist_table.grant_write_data(ingest_fn)
        latest_table.grant_write_data(ingest_fn)

        # Allow IoT Core → Ingest
        ingest_fn.add_permission(
            "AllowIotInvoke",
            principal=iam.ServicePrincipal("iot.amazonaws.com"),
            action="lambda:InvokeFunction",
        )

        # Allow Ingest → Alert (async invoke)
        alert_fn.grant_invoke(ingest_fn)

        # ── IoT Rule ─────────────────────────────────────────────────────────
        sql = (
            "SELECT device, sensor, unit, distance_cm, distance_cm_filtered, "
            "status, temperature_c, ts FROM 'pi/+/telemetry'"
        )
        iot.CfnTopicRule(
            self, "BrineTankIngestRule",
            rule_name="BrineTankIngestRule",
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
