import os

from aws_cdk import CfnOutput, Duration, Stack
from aws_cdk import aws_apigateway as apigateway
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_s3 as s3
from constructs import Construct


class ApiStack(Stack):
    """API Gateway + Lambda functions for F1 telemetry endpoints."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        sessions_table: dynamodb.ITable,
        driver_stats_table: dynamodb.ITable,
        laps_table: dynamodb.ITable,
        raw_bucket: s3.IBucket,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        lambdas_dir = os.path.join(os.path.dirname(__file__), "..", "lambdas")

        # ── Lambda Layer (shared repositories + openf1_client) ────────────────
        layer = lambda_.LayerVersion(
            self,
            "F1TelemetryLayer",
            code=lambda_.Code.from_asset(os.path.join(lambdas_dir, "layer")),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_12],
            description="Shared repositories, OpenF1 client, and utilities",
        )

        # ── Common environment variables ───────────────────────────────────────
        common_env = {
            "SESSIONS_TABLE": sessions_table.table_name,
            "DRIVER_STATS_TABLE": driver_stats_table.table_name,
            "LAPS_TABLE": laps_table.table_name,
            "RAW_BUCKET": raw_bucket.bucket_name,
            "OPENF1_BASE_URL": "https://api.openf1.org",
        }

        def make_function(name: str, handler_dir: str, timeout_seconds: int = 30, memory: int = 256) -> lambda_.Function:
            return lambda_.Function(
                self,
                name,
                runtime=lambda_.Runtime.PYTHON_3_12,
                handler="handler.handler",
                code=lambda_.Code.from_asset(os.path.join(lambdas_dir, handler_dir)),
                timeout=Duration.seconds(timeout_seconds),
                memory_size=memory,
                layers=[layer],
                environment=common_env,
            )

        # ── Lambda functions ───────────────────────────────────────────────────
        ingest_fn = make_function("IngestSessionFunction", "ingest_session", timeout_seconds=120, memory=512)
        list_sessions_fn = make_function("ListSessionsFunction", "list_sessions")
        list_drivers_fn = make_function("ListDriversFunction", "list_drivers")
        get_summary_fn = make_function("GetDriverSummaryFunction", "get_driver_summary")
        get_laps_fn = make_function("GetDriverLapsFunction", "get_driver_laps")

        # ── Permissions ────────────────────────────────────────────────────────
        sessions_table.grant_read_write_data(ingest_fn)
        driver_stats_table.grant_read_write_data(ingest_fn)
        laps_table.grant_read_write_data(ingest_fn)
        raw_bucket.grant_put(ingest_fn)

        sessions_table.grant_read_data(list_sessions_fn)
        driver_stats_table.grant_read_data(list_drivers_fn)
        driver_stats_table.grant_read_data(get_summary_fn)
        laps_table.grant_read_data(get_laps_fn)

        # ── API Gateway ────────────────────────────────────────────────────────
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

        # /sessions
        sessions = api.root.add_resource("sessions")
        sessions.add_method("GET", apigateway.LambdaIntegration(list_sessions_fn))

        # /sessions/{session_key}
        session = sessions.add_resource("{session_key}")

        # /sessions/{session_key}/ingest
        ingest = session.add_resource("ingest")
        ingest.add_method("POST", apigateway.LambdaIntegration(ingest_fn))

        # /sessions/{session_key}/drivers
        drivers = session.add_resource("drivers")
        drivers.add_method("GET", apigateway.LambdaIntegration(list_drivers_fn))

        # /sessions/{session_key}/drivers/{driver_number}
        driver = drivers.add_resource("{driver_number}")

        # /sessions/{session_key}/drivers/{driver_number}/summary
        summary = driver.add_resource("summary")
        summary.add_method("GET", apigateway.LambdaIntegration(get_summary_fn))

        # /sessions/{session_key}/drivers/{driver_number}/laps
        laps = driver.add_resource("laps")
        laps.add_method("GET", apigateway.LambdaIntegration(get_laps_fn))

        CfnOutput(self, "ApiUrl", value=api.url)
