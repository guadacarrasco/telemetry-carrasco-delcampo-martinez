#!/usr/bin/env python3
import aws_cdk as cdk

from stacks.data_stack import DataStack
from stacks.api_stack import ApiStack

app = cdk.App()

data = DataStack(app, "F1TelemetryData")

ApiStack(
    app,
    "F1TelemetryApi",
    sessions_table=data.sessions_table,
    driver_stats_table=data.driver_stats_table,
    laps_table=data.laps_table,
    raw_bucket=data.raw_bucket,
)

app.synth()
