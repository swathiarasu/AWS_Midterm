#!/usr/bin/env python3
import aws_cdk as cdk

from config.settings import AppConfig
from stacks.storage_stack import StorageStack
from stacks.replicator_stack import ReplicatorStack
from stacks.cleaner_stack import CleanerStack

app = cdk.App()
config = AppConfig()

storage_stack = StorageStack(
    app,
    "StorageStack",
    config=config,
)

ReplicatorStack(
    app,
    "ReplicatorStack",
    config=config,
    source_bucket=storage_stack.source_bucket,
    destination_bucket=storage_stack.destination_bucket,
    mapping_table=storage_stack.mapping_table,
)

CleanerStack(
    app,
    "CleanerStack",
    config=config,
    destination_bucket=storage_stack.destination_bucket,
    mapping_table=storage_stack.mapping_table,
)

app.synth()