import aws_cdk as core
import aws_cdk.assertions as assertions

from brinetank_iot_cdk.brinetank_iot_cdk_stack import BrinetankIotCdkStack

# example tests. To run these tests, uncomment this file along with the example
# resource in brinetank_iot_cdk/brinetank_iot_cdk_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = BrinetankIotCdkStack(app, "brinetank-iot-cdk")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
