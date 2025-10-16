from aws_cdk import (
    Stack,
    aws_dynamodb as dynamodb,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_iot as iot,
    RemovalPolicy,
    Duration,
)
from constructs import Construct

class BrinetankIotCdkStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # 1) DynamoDB: history table (device, ts)
        hist_table = dynamodb.Table(
            self, "BrineTankReadings",
            table_name="BrineTankReadings",
            partition_key=dynamodb.Attribute(name="device", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="ts", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
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

        # 3) Lambda: writes to history + latest
        ingest_fn = _lambda.Function(
            self, "BrineTankIngest",
            function_name="BrineTankIngest",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="app.handler",
            code=_lambda.Code.from_asset("lambda"),
            timeout=Duration.seconds(30),
            environment={
                "TABLE_NAME": hist_table.table_name,
                "LATEST_TABLE_NAME": latest_table.table_name,
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

        # 4) IoT Rule: route telemetry to the Lambda
        sql = "SELECT device, sensor, unit, distance_cm, distance_cm_filtered, status, ts FROM 'pi/+/telemetry'"

        iot.CfnTopicRule(
            self, "BrineTankIngestRule",
            rule_name="BrineTankIngestRule",
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
