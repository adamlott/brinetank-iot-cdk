#!/usr/bin/env python3
import aws_cdk as cdk
from brinetank_iot_cdk.brinetank_iot_cdk_stack import BrinetankIotCdkStack
from brinetank_iot_cdk.customer_portal_stack import CustomerPortalStack

app = cdk.App()

processing = BrinetankIotCdkStack(
    app,
    "BrinetankIotCdkStack",
    env_name="prod",
    ses_from="alerts@salty-water.com",
    sensor_email_map={
        "sensor-garage": ["you@example.com"],
        "sensor-basement": ["family@example.com"],
    },
)

# Separate stack: read-only customer portal. Table names are passed as plain
# strings (not the `processing` stack's Table objects) so this stack has no
# CloudFormation cross-stack reference to BrinetankIotCdkStack and both stacks
# can be deployed/destroyed independently.
portal = CustomerPortalStack(
    app,
    "CustomerPortalStack",
    env_name="prod",
    latest_table_name="BrineTankLatest",
    readings_table_name="BrineTankReadings",
    domain_name="portal.salty-water.com",
    hosted_zone_id="Z0371086XJXG1EVE52RU",
    hosted_zone_name="salty-water.com",
    cors_allowed_origins=[
        "https://portal.salty-water.com",
        "http://localhost:5173",  # Vite dev server, for local dev against the deployed API
    ],
)

app.synth()
