from aws_cdk import (
    Stack,
    CfnOutput,
    aws_sns as sns,
    aws_iam as iam,
)
from constructs import Construct


class NotificationsStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        env_name: str,
        sensor_email_map: dict | None = None,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.topic = sns.Topic(
            self,
            "SaltAlerterTopic",
            topic_name=f"salt-alerter-{env_name}",
            display_name=f"Salt Alerter ({env_name})",
        )

        # Allow Lambdas to publish
        self.topic.grant_publish(iam.ServicePrincipal("lambda.amazonaws.com"))

        # Optional email subs with filter policy
        if sensor_email_map:
            for sensor_id, emails in sensor_email_map.items():
                for email in emails:
                    sns.CfnSubscription(
                        self,
                        f"EmailSub_{sensor_id}_{email.replace('@','_').replace('.','_')}",
                        topic_arn=self.topic.topic_arn,
                        protocol="email",
                        endpoint=email,
                        # Simple exact-match policy
                        filter_policy={
                            "sensorId": [sensor_id],
                            "type": ["LOW_LEVEL"],
                        },
                    )

        CfnOutput(self, "SaltAlerterTopicArn", value=self.topic.topic_arn)
