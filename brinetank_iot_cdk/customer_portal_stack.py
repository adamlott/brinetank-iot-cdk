from pathlib import Path
from aws_cdk import (
    Stack, Duration, RemovalPolicy, CfnOutput,
    aws_lambda as _lambda,
    aws_dynamodb as dynamodb,
    aws_cognito as cognito,
    aws_apigatewayv2 as apigwv2,
    aws_apigatewayv2_authorizers as apigwv2_authorizers,
    aws_apigatewayv2_integrations as apigwv2_integrations,
    aws_s3 as s3,
    aws_s3_deployment as s3_deployment,
    aws_cloudfront as cloudfront,
    aws_cloudfront_origins as origins,
    aws_certificatemanager as acm,
    aws_route53 as route53,
    aws_route53_targets as route53_targets,
)
from constructs import Construct

PROJECT_ROOT = Path(__file__).parent.parent
PORTAL_API_DIR = (PROJECT_ROOT / "customer_portal_api").as_posix()
FRONTEND_DIST_DIR = (PROJECT_ROOT / "frontend" / "dist").as_posix()


class CustomerPortalStack(Stack):
    """Read-only customer-facing portal: Cognito auth + HTTP API + S3/CloudFront frontend.

    Only ever reads BrineTankLatest/BrineTankReadings (imported by name, not by
    construct, so this stack has zero CloudFormation coupling to
    BrinetankIotCdkStack and both stacks can be deployed/destroyed independently).
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        env_name: str = "prod",
        latest_table_name: str = "BrineTankLatest",
        readings_table_name: str = "BrineTankReadings",
        domain_name: str = "portal.salty-water.com",
        hosted_zone_id: str = "Z0371086XJXG1EVE52RU",
        hosted_zone_name: str = "salty-water.com",
        cors_allowed_origins: list | None = None,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Browser origins allowed to call the API. Defaults to the portal's own
        # domain only — never "*", so a logged-in customer's ID token can't be
        # replayed from an arbitrary site.
        cors_allowed_origins = cors_allowed_origins or [f"https://{domain_name}"]

        # ── Read-only handles to the backend stack's tables (import by name) ──
        latest_table = dynamodb.Table.from_table_name(self, "LatestTableRef", latest_table_name)
        readings_table = dynamodb.Table.from_table_name(self, "ReadingsTableRef", readings_table_name)

        # ── Tenant → device mapping (owned by this stack) ─────────────────────
        customer_devices = dynamodb.Table(
            self, "CustomerDevices",
            table_name="CustomerDevices",
            partition_key=dynamodb.Attribute(name="email", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="sensorId", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN,
        )

        # ── Cognito ──────────────────────────────────────────────────────────
        # email is required+immutable+auto-verified so it can be trusted as the
        # CustomerDevices partition key (self-service email changes are blocked;
        # unconfirmed accounts can never obtain a token).
        user_pool = cognito.UserPool(
            self, "CustomerUserPool",
            user_pool_name=f"BrineTankCustomers-{env_name}",
            self_sign_up_enabled=True,
            sign_in_aliases=cognito.SignInAliases(email=True, username=False),
            sign_in_case_sensitive=False,
            auto_verify=cognito.AutoVerifiedAttrs(email=True),
            standard_attributes=cognito.StandardAttributes(
                email=cognito.StandardAttribute(required=True, mutable=False),
            ),
            account_recovery=cognito.AccountRecovery.EMAIL_ONLY,
            removal_policy=RemovalPolicy.RETAIN,
        )
        user_pool_client = user_pool.add_client(
            "CustomerPortalClient",
            generate_secret=False,
            auth_flows=cognito.AuthFlow(user_srp=True, user_password=True),
        )

        # ── API Lambda (read-only) ──────────────────────────────────────────
        portal_api_fn = _lambda.Function(
            self, "CustomerPortalApi",
            function_name=f"CustomerPortalApi-{env_name}",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="app.handler",
            code=_lambda.Code.from_asset(PORTAL_API_DIR),
            timeout=Duration.seconds(15),
            environment={
                "CUSTOMER_DEVICES_TABLE": customer_devices.table_name,
                "LATEST_TABLE_NAME": latest_table.table_name,
                "HIST_TABLE_NAME": readings_table.table_name,
                "HISTORY_DAYS": "30",
            },
        )

        # Read-only grants only — never grant write access to backend data from here.
        latest_table.grant_read_data(portal_api_fn)
        readings_table.grant_read_data(portal_api_fn)
        customer_devices.grant_read_data(portal_api_fn)

        # ── HTTP API + Cognito authorizer ───────────────────────────────────
        authorizer = apigwv2_authorizers.HttpUserPoolAuthorizer(
            "CustomerPortalAuthorizer",
            user_pool,
            user_pool_clients=[user_pool_client],
        )
        integration = apigwv2_integrations.HttpLambdaIntegration(
            "CustomerPortalIntegration", portal_api_fn,
        )
        http_api = apigwv2.HttpApi(
            self, "CustomerPortalHttpApi",
            api_name=f"CustomerPortalApi-{env_name}",
            cors_preflight=apigwv2.CorsPreflightOptions(
                allow_origins=cors_allowed_origins,
                allow_methods=[apigwv2.CorsHttpMethod.GET],
                allow_headers=["Authorization", "Content-Type"],
            ),
        )
        http_api.add_routes(
            path="/devices",
            methods=[apigwv2.HttpMethod.GET],
            integration=integration,
            authorizer=authorizer,
        )
        http_api.add_routes(
            path="/devices/{sensorId}/history",
            methods=[apigwv2.HttpMethod.GET],
            integration=integration,
            authorizer=authorizer,
        )

        # ── Custom domain (portal.salty-water.com) ─────────────────────────
        # Zone is imported by id+name (no context lookup) so the stack stays
        # environment-agnostic for `cdk synth`/tests.
        hosted_zone = route53.HostedZone.from_hosted_zone_attributes(
            self, "PortalHostedZone",
            hosted_zone_id=hosted_zone_id,
            zone_name=hosted_zone_name,
        )
        # CloudFront requires its ACM cert in us-east-1; this stack deploys
        # there. DNS validation records are written into the zone above.
        certificate = acm.Certificate(
            self, "PortalCertificate",
            domain_name=domain_name,
            validation=acm.CertificateValidation.from_dns(hosted_zone),
        )

        # ── Frontend hosting (S3 + CloudFront, private bucket via OAC) ──────
        site_bucket = s3.Bucket(
            self, "PortalSiteBucket",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.RETAIN,
        )
        distribution = cloudfront.Distribution(
            self, "PortalDistribution",
            default_root_object="index.html",
            domain_names=[domain_name],
            certificate=certificate,
            default_behavior=cloudfront.BehaviorOptions(
                origin=origins.S3BucketOrigin.with_origin_access_control(site_bucket),
                viewer_protocol_policy=cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
            ),
        )
        s3_deployment.BucketDeployment(
            self, "PortalSiteDeployment",
            sources=[s3_deployment.Source.asset(FRONTEND_DIST_DIR)],
            destination_bucket=site_bucket,
            distribution=distribution,
            distribution_paths=["/*"],
        )

        # ── DNS: portal.salty-water.com → CloudFront (A + AAAA aliases) ─────
        cf_target = route53.RecordTarget.from_alias(
            route53_targets.CloudFrontTarget(distribution)
        )
        route53.ARecord(
            self, "PortalAliasRecord",
            zone=hosted_zone,
            record_name=domain_name,
            target=cf_target,
        )
        route53.AaaaRecord(
            self, "PortalAliasRecordV6",
            zone=hosted_zone,
            record_name=domain_name,
            target=cf_target,
        )

        # ── Outputs (paste into frontend/.env before `npm run build`) ───────
        CfnOutput(self, "UserPoolId", value=user_pool.user_pool_id)
        CfnOutput(self, "UserPoolClientId", value=user_pool_client.user_pool_client_id)
        CfnOutput(self, "ApiUrl", value=http_api.api_endpoint)
        CfnOutput(self, "PortalUrl", value=f"https://{domain_name}")
        CfnOutput(self, "CloudFrontUrl", value=f"https://{distribution.distribution_domain_name}")
