#!/usr/bin/env python3
import aws_cdk as cdk

from stacks.data_stack import DataStack
from stacks.messaging_stack import MessagingStack

app = cdk.App()
data = DataStack(app, "F1TelemetryData")
MessagingStack(
    app,
    "F1TelemetryMessaging",
    sessions_table=data.sessions_table,
    driver_stats_table=data.driver_stats_table,
    raw_bucket=data.raw_bucket,
)
app.synth()
