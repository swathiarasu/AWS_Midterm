from pathlib import Path

from aws_cdk import (
    Stack,
    Duration,
    aws_lambda as _lambda,
    aws_events as events,
    aws_events_targets as targets,
    aws_logs as logs,
)
from constructs import Construct


class CleanerStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        config,
        destination_bucket,
        mapping_table,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        lambda_root = Path(__file__).resolve().parent.parent / "lambda_src"

        self.cleaner_fn = _lambda.Function(
            self,
            "CleanerFunction",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="cleaner.handler.lambda_handler",
            code=_lambda.Code.from_asset(str(lambda_root)),
            timeout=Duration.seconds(60),
            memory_size=256,
            log_retention=logs.RetentionDays.ONE_WEEK,
            environment={
                "DST_BUCKET_NAME": destination_bucket.bucket_name,
                "TABLE_NAME": mapping_table.table_name,
                "DISOWNED_THRESHOLD_SECONDS": str(config.cleaner_threshold_seconds),
            },
        )

        destination_bucket.grant_read_write(self.cleaner_fn)
        mapping_table.grant_read_write_data(self.cleaner_fn)

        rule = events.Rule(
            self,
            "CleanerScheduleRule",
            schedule=events.Schedule.rate(
                Duration.minutes(config.cleaner_schedule_minutes)
            ),
        )

        rule.add_target(targets.LambdaFunction(self.cleaner_fn))