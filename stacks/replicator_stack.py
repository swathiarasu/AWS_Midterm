from pathlib import Path

from aws_cdk import (
    Stack,
    Duration,
    aws_lambda as _lambda,
    aws_logs as logs,
    aws_s3 as s3,
    aws_events as events,
    aws_events_targets as targets,
)
from constructs import Construct


class ReplicatorStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        config,
        source_bucket: s3.Bucket,
        destination_bucket: s3.Bucket,
        mapping_table,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        lambda_root = Path(__file__).resolve().parent.parent / "lambda_src"

        self.replicator_fn = _lambda.Function(
            self,
            "ReplicatorFunction",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="replicator.handler.lambda_handler",
            code=_lambda.Code.from_asset(str(lambda_root)),
            timeout=Duration.seconds(60),
            memory_size=256,
            log_retention=logs.RetentionDays.ONE_WEEK,
            environment={
                "SRC_BUCKET_NAME": source_bucket.bucket_name,
                "DST_BUCKET_NAME": destination_bucket.bucket_name,
                "TABLE_NAME": mapping_table.table_name,
                "MAX_COPIES": str(config.max_copies),
            },
        )

        source_bucket.grant_read(self.replicator_fn)
        destination_bucket.grant_read_write(self.replicator_fn)
        mapping_table.grant_read_write_data(self.replicator_fn)

        events.Rule(
            self,
            "SourceBucketObjectEventsRule",
            event_pattern=events.EventPattern(
                source=["aws.s3"],
                detail_type=["Object Created", "Object Deleted"],
                detail={
                    "bucket": {
                        "name": [source_bucket.bucket_name]
                    }
                },
            ),
            targets=[targets.LambdaFunction(self.replicator_fn)],
        )