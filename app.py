# app.py
import aws_cdk as cdk
from brinetank_iot_cdk import BrinetankIotCdkStack, NotificationsStack

app = cdk.App()

notifications = NotificationsStack(
    app,
    "NotificationsStack",
    env_name="prod",
    sensor_email_map={
        "sensor-garage": ["you@example.com"],
        "sensor-basement": ["family@example.com"],
    },
)

processing = BrinetankIotCdkStack(
    app,
    "ProcessingStack",
    notification_topic=notifications.topic,
    env_name="prod",
)

app.synth()
