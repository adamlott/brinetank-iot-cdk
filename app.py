#!/usr/bin/env python3
import aws_cdk as cdk
from brinetank_iot_cdk.brinetank_iot_cdk_stack import BrinetankIotCdkStack

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

app.synth()
