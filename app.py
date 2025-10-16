#!/usr/bin/env python3
import aws_cdk as cdk
from brinetank_iot_cdk.brinetank_iot_cdk_stack import BrinetankIotCdkStack

app = cdk.App()
BrinetankIotCdkStack(app, "BrinetankIotCdkStack")
app.synth()
