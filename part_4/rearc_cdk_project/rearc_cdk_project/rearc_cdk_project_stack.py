import aws_cdk as cdk
from aws_cdk import (
    aws_s3 as s3,
    aws_sqs as sqs,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_s3_notifications as s3_notifications,
    aws_events as events,
    aws_events_targets as targets
)
import os


class RearcCdkProjectStack(cdk.Stack):

    def __init__(self, scope: cdk.App, id: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        # ✅ 1. S3 Bucket for storing CSV & JSON files
        self.s3_bucket = s3.Bucket(self, "DataBucket",
                                   bucket_name="abh-de-rearc",
                                   removal_policy=cdk.RemovalPolicy.RETAIN,  # Avoid accidental deletion
                                   auto_delete_objects=False
                                   )

        # ✅ 2. SQS Queue (Triggered when JSON is uploaded)
        self.sqs_queue = sqs.Queue(self, "DataProcessingQueue",
                                   visibility_timeout=cdk.Duration.seconds(300)
                                   )

        # ✅ 3. IAM Role for Lambda Functions
        lambda_role = iam.Role(self, "LambdaExecutionRole",
                               assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
                               managed_policies=[
                                   iam.ManagedPolicy.from_aws_managed_policy_name(
                                       "service-role/AWSLambdaBasicExecutionRole"),
                                   iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess"),
                                   iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSQSFullAccess")
                               ]
                               )

        # ✅ 4. Lambda Function for Fetching Data (Part 1 & Part 2)
        self.data_fetch_lambda = _lambda.Function(self, "DataFetchLambda",
                                                  runtime=_lambda.Runtime.PYTHON_3_9,
                                                  handler="lambda_functions.data_fetch_lambda.lambda_handler",
                                                  code=_lambda.Code.from_asset("rearc_cdk_project/lambda_functions"),
                                                  timeout=cdk.Duration.minutes(5),
                                                  memory_size=512,
                                                  role=lambda_role
                                                  )

        # ✅ 5. Lambda Function for Processing Reports (Part 3)
        self.report_lambda = _lambda.Function(self, "ReportLambda",
                                              runtime=_lambda.Runtime.PYTHON_3_9,
                                              handler="lambda_functions.report_lambda.process_reports",
                                              code=_lambda.Code.from_asset("rearc_cdk_project/lambda_functions"),
                                              timeout=cdk.Duration.minutes(5),
                                              memory_size=512,
                                              role=lambda_role
                                              )

        # ✅ 6. Connect S3 Bucket to SQS (Triggers Report Lambda when JSON is uploaded)
        self.s3_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3_notifications.SqsDestination(self.sqs_queue),
            s3.NotificationKeyFilter(prefix="api-data/", suffix=".json")  # Trigger on JSON updates
        )

        # ✅ 7. Connect SQS to Report Lambda (Invokes Report Lambda on new message)
        self.report_lambda.add_event_source_mapping("SQSTrigger",
                                                    event_source_arn=self.sqs_queue.queue_arn,
                                                    batch_size=1
                                                    )

        # ✅ 8. EventBridge Rule to Run Data Fetch Lambda Daily
        self.event_rule = events.Rule(self, "DailyTriggerRule",
                                      schedule=events.Schedule.rate(cdk.Duration.days(1))
                                      )
        self.event_rule.add_target(targets.LambdaFunction(self.data_fetch_lambda))

        # ✅ 9. Output the important resource names
        cdk.CfnOutput(self, "S3BucketName", value=self.s3_bucket.bucket_name)
        cdk.CfnOutput(self, "SQSQueueName", value=self.sqs_queue.queue_name)
        cdk.CfnOutput(self, "DataFetchLambdaName", value=self.data_fetch_lambda.function_name)
        cdk.CfnOutput(self, "ReportLambdaName", value=self.report_lambda.function_name)
