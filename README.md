# AWS Midterm

## Overview
This project builds an object backup system using:
- 2 S3 buckets
- 1 DynamoDB table
- 2 Lambda functions
- 2 EventBridge rule

## Components
- **Replicator Lambda**
  - Triggered by S3 events in source bucket
  - On PUT: creates a copy in destination bucket and keeps only the latest 3 copies
  - On DELETE: marks copies as disowned in DynamoDB

- **Cleaner Lambda**
  - Triggered every 1 minute by EventBridge
  - Deletes destination copies that have been disowned for more than 10 seconds

## Deploy
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cdk bootstrap
cdk deploy --all
