import os
import shutil

from aws_cdk import CfnOutput, Duration, RemovalPolicy, Stack
from aws_cdk import aws_apigateway as apigateway
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_sqs as sqs
from constructs import Construct


class MainStack(Stack):
    """Single stack combining data and API resources (avoids cross-stack Fn::ImportValue)."""

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ── Data resources ─────────────────────────────────────────────────────
        sessions_table = dynamodb.Table(
            self,
            "SessionsTable",
            table_name="f1_sessions",
            partition_key=dynamodb.Attribute(name="session_key", type=dynamodb.AttributeType.NUMBER),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
        )

        driver_stats_table = dynamodb.Table(
            self,
            "DriverStatsTable",
            table_name="f1_driver_stats",
            partition_key=dynamodb.Attribute(name="session_key", type=dynamodb.AttributeType.NUMBER),
            sort_key=dynamodb.Attribute(name="driver_number", type=dynamodb.AttributeType.NUMBER),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
        )

        laps_table = dynamodb.Table(
            self,
            "LapsTable",
            table_name="f1_laps",
            partition_key=dynamodb.Attribute(name="session_driver", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="lap_number", type=dynamodb.AttributeType.NUMBER),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
        )

        raw_bucket = s3.Bucket(
            self,
            "RawDataBucket",
            removal_policy=RemovalPolicy.DESTROY,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
        )

        simulation_queue = sqs.Queue(
            self,
            "SimulationEventsQueue",
            queue_name="f1_simulation_events",
            removal_policy=RemovalPolicy.DESTROY,
        )

        lambdas_dir = os.path.join(os.path.dirname(__file__), "..", "lambdas")
        layer_python_dir = os.path.join(lambdas_dir, "layer", "python")
        build_dir = os.path.join(lambdas_dir, ".build")

        # ── Common environment variables ────────────────────────────────────────
        common_env = {
            "SESSIONS_TABLE": sessions_table.table_name,
            "DRIVER_STATS_TABLE": driver_stats_table.table_name,
            "LAPS_TABLE": laps_table.table_name,
            "RAW_BUCKET": raw_bucket.bucket_name,
            "OPENF1_BASE_URL": "https://api.openf1.org",
            "SIMULATION_QUEUE_URL": simulation_queue.queue_url,
        }

        def _bundle(handler_dir: str) -> str:
            """Merge handler + layer files into .build/<handler_dir> so LocalStack can find shared modules."""
            out = os.path.join(build_dir, handler_dir)
            if os.path.exists(out):
                shutil.rmtree(out)
            shutil.copytree(os.path.join(lambdas_dir, handler_dir), out)
            for item in os.listdir(layer_python_dir):
                src = os.path.join(layer_python_dir, item)
                dst = os.path.join(out, item)
                if os.path.isfile(src):
                    shutil.copy2(src, dst)
                else:
                    shutil.copytree(src, dst)
            return out

        def make_function(name: str, handler_dir: str, timeout_seconds: int = 30, memory: int = 256) -> lambda_.Function:
            return lambda_.Function(
                self,
                name,
                runtime=lambda_.Runtime.PYTHON_3_12,
                handler="handler.handler",
                code=lambda_.Code.from_asset(_bundle(handler_dir)),
                timeout=Duration.seconds(timeout_seconds),
                memory_size=memory,
                environment=common_env,
            )

        # ── Lambda functions ────────────────────────────────────────────────────
        ingest_fn = make_function("IngestSessionFunction", "ingest_session", timeout_seconds=120, memory=512)
        list_sessions_fn = make_function("ListSessionsFunction", "list_sessions")
        list_drivers_fn = make_function("ListDriversFunction", "list_drivers")
        get_summary_fn = make_function("GetDriverSummaryFunction", "get_driver_summary")
        get_laps_fn = make_function("GetDriverLapsFunction", "get_driver_laps")
        start_simulation_fn = make_function("StartSimulationFunction", "start_simulation", timeout_seconds=120, memory=512)

        # ── Permissions ─────────────────────────────────────────────────────────
        sessions_table.grant_read_write_data(ingest_fn)
        driver_stats_table.grant_read_write_data(ingest_fn)
        laps_table.grant_read_write_data(ingest_fn)
        raw_bucket.grant_put(ingest_fn)

        ingest_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=["lambda:InvokeFunction"],
                resources=["*"],
            )
        )

        sessions_table.grant_read_data(list_sessions_fn)
        driver_stats_table.grant_read_data(list_drivers_fn)
        driver_stats_table.grant_read_data(get_summary_fn)
        laps_table.grant_read_data(get_laps_fn)

        sessions_table.grant_read_data(start_simulation_fn)
        driver_stats_table.grant_read_data(start_simulation_fn)
        laps_table.grant_read_data(start_simulation_fn)
        simulation_queue.grant_send_messages(start_simulation_fn)

        start_simulation_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=["lambda:InvokeFunction"],
                resources=["*"],
            )
        )

        # ── API Gateway ─────────────────────────────────────────────────────────
        api = apigateway.RestApi(
            self,
            "F1TelemetryRestApi",
            rest_api_name="f1-telemetry",
            description="F1 telemetry API",
            default_cors_preflight_options=apigateway.CorsOptions(
                allow_origins=apigateway.Cors.ALL_ORIGINS,
                allow_methods=apigateway.Cors.ALL_METHODS,
            ),
        )

        sessions = api.root.add_resource("sessions")
        sessions.add_method("GET", apigateway.LambdaIntegration(list_sessions_fn))

        session = sessions.add_resource("{session_key}")

        ingest = session.add_resource("ingest")
        ingest.add_method("POST", apigateway.LambdaIntegration(ingest_fn))

        drivers = session.add_resource("drivers")
        drivers.add_method("GET", apigateway.LambdaIntegration(list_drivers_fn))

        driver = drivers.add_resource("{driver_number}")

        summary = driver.add_resource("summary")
        summary.add_method("GET", apigateway.LambdaIntegration(get_summary_fn))

        laps = driver.add_resource("laps")
        laps.add_method("GET", apigateway.LambdaIntegration(get_laps_fn))

        start_simulation = api.root.add_resource("start-simulation")
        start_simulation.add_method("POST", apigateway.LambdaIntegration(start_simulation_fn))

        # ── Outputs ─────────────────────────────────────────────────────────────
        CfnOutput(self, "ApiUrl", value=api.url)
        CfnOutput(self, "SessionsTableName", value=sessions_table.table_name)
        CfnOutput(self, "DriverStatsTableName", value=driver_stats_table.table_name)
        CfnOutput(self, "LapsTableName", value=laps_table.table_name)
        CfnOutput(self, "RawBucketName", value=raw_bucket.bucket_name)
        CfnOutput(self, "SimulationQueueUrl", value=simulation_queue.queue_url)
