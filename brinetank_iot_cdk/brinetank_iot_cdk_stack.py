from aws_cdk import (
    Stack,
    CfnOutput,
    aws_sns as sns,
    aws_sns_subscriptions as subs,
    aws_dynamodb as dynamodb,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_iot as iot,
    RemovalPolicy,
    Duration,
)
from constructs import Construct


class BrinetankIotCdkStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        notification_topic: sns.ITopic,   # <-- accept the topic from app.py
        env_name: str = "prod",
        **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # 1) DynamoDB: history table (device, ts)
        hist_table = dynamodb.Table(
            self, "BrineTankReadings",
            table_name="BrineTankReadings",
            partition_key=dynamodb.Attribute(name="device", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="ts", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            time_to_live_attribute="ttl_epoch",
            removal_policy=RemovalPolicy.RETAIN
        )

        # 2) DynamoDB: latest table (one item per device)
        latest_table = dynamodb.Table(
            self, "BrineTankLatest",
            table_name="BrineTankLatest",
            partition_key=dynamodb.Attribute(name="device", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN
        )

        # === NEW: Low-level alert Lambda that publishes to SNS ===
        alert_fn = _lambda.Function(
            self, "LowLevelAlert",
            function_name=f"LowLevelAlert-{env_name}",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="app.handler",
            code=_lambda.Code.from_asset("low_level_alert"),  # your /low_level_alert/app.py
            timeout=Duration.seconds(10),
            environment={
                "ALERT_TOPIC_ARN": notification_topic.topic_arn
            }
        )
        # Allow this Lambda to publish to the topic
        notification_topic.grant_publish(alert_fn)

        # 3) Ingest Lambda
        ingest_fn = _lambda.Function(
            self, "BrineTankIngest",
            function_name=f"BrineTankIngest-{env_name}",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="app.handler",
            code=_lambda.Code.from_asset("lambda"),           # your existing ingest code folder
            timeout=Duration.seconds(30),
            environment={
                "TABLE_NAME": hist_table.table_name,
                "LATEST_TABLE_NAME": latest_table.table_name,
                "EMPTY_DISTANCE": "70",
                "FULL_DISTANCE": "6",
                "TTL_DAYS": "7",
                # let ingest invoke the alert lambda when needed
                "ALERT_FN_NAME": alert_fn.function_name
            },
        )
        hist_table.grant_write_data(ingest_fn)
        latest_table.grant_write_data(ingest_fn)

        # Allow IoT Core to invoke this Lambda
        ingest_fn.add_permission(
            "AllowIotInvoke",
            principal=iam.ServicePrincipal("iot.amazonaws.com"),
            action="lambda:InvokeFunction",
        )

        # Allow ingest to invoke the alert function
        alert_fn.grant_invoke(ingest_fn)

        # 4) IoT Rule: route telemetry to the Ingest Lambda
        sql = "SELECT device, sensor, unit, distance_cm, distance_cm_filtered, status, ts FROM 'pi/+/telemetry'"

        iot.CfnTopicRule(
            self, "BrineTankIngestRule",
            rule_name=f"BrineTankIngestRule-{env_name}",
            topic_rule_payload=iot.CfnTopicRule.TopicRulePayloadProperty(
                sql=sql,
                aws_iot_sql_version="2016-03-23",
                rule_disabled=False,
                actions=[
                    iot.CfnTopicRule.ActionProperty(
                        lambda_=iot.CfnTopicRule.LambdaActionProperty(
                            function_arn=ingest_fn.function_arn
                        )
                    )
                ],
            ),
        )
