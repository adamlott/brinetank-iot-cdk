import aws_cdk as core
from aws_cdk import assertions

from brinetank_iot_cdk.customer_portal_stack import CustomerPortalStack

WRITE_ACTIONS = {
    "dynamodb:PutItem",
    "dynamodb:UpdateItem",
    "dynamodb:DeleteItem",
    "dynamodb:BatchWriteItem",
}


def _synth():
    app = core.App()
    stack = CustomerPortalStack(app, "CustomerPortalStackTest")
    return assertions.Template.from_stack(stack)


def test_creates_cognito_user_pool():
    template = _synth()
    template.resource_count_is("AWS::Cognito::UserPool", 1)


def test_creates_http_api():
    template = _synth()
    template.resource_count_is("AWS::ApiGatewayV2::Api", 1)


def test_api_cors_is_not_wildcard():
    """A logged-in customer's ID token must not be replayable from any origin."""
    template = _synth()
    apis = template.find_resources("AWS::ApiGatewayV2::Api")
    (api,) = apis.values()
    origins = api["Properties"]["CorsConfiguration"]["AllowOrigins"]
    assert "*" not in origins
    assert origins == ["https://portal.salty-water.com"]


def test_serves_from_custom_domain():
    """CloudFront distribution carries the custom domain + ACM cert, and a
    Route53 alias points portal.salty-water.com at it."""
    template = _synth()
    template.resource_count_is("AWS::CertificateManager::Certificate", 1)
    template.has_resource_properties(
        "AWS::CloudFront::Distribution",
        {
            "DistributionConfig": assertions.Match.object_like(
                {"Aliases": ["portal.salty-water.com"]}
            )
        },
    )
    template.has_resource_properties(
        "AWS::Route53::RecordSet",
        {"Name": "portal.salty-water.com.", "Type": "A"},
    )


def test_portal_lambda_role_has_no_write_actions():
    """The whole point of a separate stack is that it can never write to
    backend data. Assert the synthesized IAM policy grants no write actions."""
    template = _synth()
    policies = template.find_resources("AWS::IAM::Policy")

    found_actions = set()
    for policy in policies.values():
        statements = policy["Properties"]["PolicyDocument"]["Statement"]
        for statement in statements:
            actions = statement.get("Action", [])
            if isinstance(actions, str):
                actions = [actions]
            found_actions.update(actions)

    write_actions_found = found_actions & WRITE_ACTIONS
    assert not write_actions_found, f"Portal stack must not grant write actions, found: {write_actions_found}"
