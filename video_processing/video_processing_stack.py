from aws_cdk import (
    aws_lambda as lambda_,
    aws_iam as iam,
    aws_s3 as s3,
    aws_sqs as sqs,
    aws_events as events,
    aws_events_targets as targets,
    aws_lambda_event_sources as sources,
    Duration,
    Stack
)
from constructs import Construct
from aws_cdk import aws_s3_notifications as s3_notifications
from aws_cdk.aws_lambda import EphemeralStorage , Size


class VideoProcessingStack(Stack):

    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Reference the existing S3 bucket in CDK
        bucket = s3.Bucket.from_bucket_arn(self, "VideoProcessingBucket",
                                           bucket_arn="arn:aws:s3:::chop-video-processing-bucket")


        # IAM Role for Lambda
        lambda_role = iam.Role(self, "LambdaExecutionRole",
                               assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"))

        lambda_role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess"))
        lambda_role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name("CloudWatchLogsFullAccess"))
        lambda_role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSNSFullAccess"))
        lambda_role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSQSFullAccess"))
        lambda_role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name("AmazonRDSDataFullAccess"))   



        workflow_queue = sqs.Queue(
            self, "WorkflowQueue",
            visibility_timeout=Duration.seconds(320),
            retention_period=Duration.days(1),
            delivery_delay=Duration.seconds(120)
        )

        # Define DLQs for each queue
        metadata_queue_dlq = sqs.Queue(self, "MetadataQueueDLQ", visibility_timeout=Duration.seconds(300))
        quality_check_queue_dlq = sqs.Queue(self, "QualityCheckQueueDLQ", visibility_timeout=Duration.seconds(300))
        processing_queue_dlq = sqs.Queue(self, "ProcessingQueueDLQ", visibility_timeout=Duration.seconds(300))

        # Define SQS Queues
        metadata_queue = sqs.Queue(
            self, "MetadataCheckQueue",
            visibility_timeout=Duration.seconds(620),
            dead_letter_queue=sqs.DeadLetterQueue(
                queue=metadata_queue_dlq,
                max_receive_count=5
            )
        )

        quality_check_queue = sqs.Queue(
            self, "QualityCheckQueue",
            visibility_timeout=Duration.seconds(620),
            dead_letter_queue=sqs.DeadLetterQueue(
                queue=quality_check_queue_dlq,
                max_receive_count=5
            )
        )

        processing_queue = sqs.Queue(
            self, "ProcessingQueue",
            visibility_timeout=Duration.seconds(620),
            dead_letter_queue=sqs.DeadLetterQueue(
                queue=processing_queue_dlq,
                max_receive_count=5
            )
        )


        layer_psycopg2 = lambda_.LayerVersion.from_layer_version_arn(
            self, "Psycopg2Layer",
            layer_version_arn="arn:aws:lambda:us-east-1:050451400714:layer:psycopg2:3"
        )

        layer_db_utils = lambda_.LayerVersion.from_layer_version_arn(
            self, "DbUtilsLayer",
            layer_version_arn="arn:aws:lambda:us-east-1:050451400714:layer:db_utils:1"
        )

        layer_ffmpeg = lambda_.LayerVersion.from_layer_version_arn(
            self, "FFmpegLayer",
            layer_version_arn="arn:aws:lambda:us-east-1:050451400714:layer:ffmpeg:1"
        )

        layer_ffprobe = lambda_.LayerVersion.from_layer_version_arn(
            self, "FFprobeLayer",
            layer_version_arn="arn:aws:lambda:us-east-1:050451400714:layer:ffprobe:1"
        )

        

        # Environment variables for the bucket and database
        environment_vars = {
            'S3_BUCKET_NAME': "chop-video-processing-bucket",
            'FAILED_FOLDER': "failed/",
            'UPLOAD_FOLDER': "uploads/",
            'PROCESSED_FOLDER': "processed/",
            'SNS_TOPIC_ARN': "arn:aws:sns:us-east-1:050451400714:VideoProcessingNotifications",
            "DB_HOST": "database-2.cq74ikq28hgi.us-east-1.rds.amazonaws.com",
            "DB_NAME": "videodb",
            "DB_USER": "postgres",
            "DB_PASSWORD": "postgres",
            "DB_PORT": "5432",
            "WORKFLOW_QUEUE_URL": workflow_queue.queue_url,
            "METADATA_QUEUE_URL": metadata_queue.queue_url,
            "QUALITY_CHECK_QUEUE_URL": quality_check_queue.queue_url,
            "PROCESSING_QUEUE_URL": processing_queue.queue_url,
            "METADATA_QUEUE_DLQ_URL": metadata_queue_dlq.queue_url,
            "QUALITY_CHECK_QUEUE_DLQ_URL": quality_check_queue_dlq.queue_url,
            "PROCESSING_QUEUE_DLQ_URL": processing_queue_dlq.queue_url
        }

        # Metadata Check Lambda
        metadata_check_lambda = lambda_.Function(
            self, "MetadataCheckLambda",
            runtime=lambda_.Runtime.PYTHON_3_9,
            handler="metadata_check.lambda_handler",
            code=lambda_.Code.from_asset("lambdas/metadata_check"),
            layers=[layer_psycopg2, layer_db_utils, layer_ffmpeg, layer_ffprobe], 
            role=lambda_role,
            timeout=Duration.seconds(300),
            environment= environment_vars

        )
        metadata_check_lambda.add_event_source(sources.SqsEventSource(
            workflow_queue,
            batch_size=1  # Process one file at a time
        ))

        # Quality Check Lambda
        quality_check_lambda = lambda_.Function(
            self, "QualityCheckLambda",
            runtime=lambda_.Runtime.PYTHON_3_9,
            handler="quality_check.lambda_handler",
            code=lambda_.Code.from_asset("lambdas/quality_check"),
            layers=[layer_psycopg2, layer_db_utils, layer_ffmpeg, layer_ffprobe], 
            role=lambda_role,
            timeout=Duration.seconds(900),
            environment= environment_vars
        )
        quality_check_lambda.add_event_source(sources.SqsEventSource(
            metadata_queue,
            batch_size=1  # Process one file at a time
        ))

        # Video Processing Lambda
        video_processing_lambda = lambda_.Function(
            self, "VideoProcessingLambda",
            runtime=lambda_.Runtime.PYTHON_3_9,
            handler="video_processing.lambda_handler",
            code=lambda_.Code.from_asset("lambdas/video_processing"),
            layers=[layer_psycopg2, layer_db_utils, layer_ffmpeg, layer_ffprobe], 
            role=lambda_role,
            timeout=Duration.seconds(900),
            memory_size=3000,  # Increase memory to 3 GB
            ephemeral_storage_size=EphemeralStorage.size(Size.gibibytes(10)),  
            environment= environment_vars
        )
        video_processing_lambda.add_event_source(sources.SqsEventSource(
            quality_check_queue,
            batch_size=1  # Process one file at a time
        ))

        # Notification Lambda
            # Notification Lambda corrected with closing parenthesis
        notification_lambda = lambda_.Function(
            self, "NotificationLambda",
            runtime=lambda_.Runtime.PYTHON_3_9,
            handler="notification.lambda_handler",
            code=lambda_.Code.from_asset("lambdas/notification"),
            layers=[layer_psycopg2, layer_db_utils, layer_ffmpeg, layer_ffprobe], 
            role=lambda_role,
            timeout=Duration.seconds(600),
            environment=environment_vars
        )
        notification_lambda.add_event_source(sources.SqsEventSource(
            processing_queue,
            batch_size=1  # Process one file at a time
        ))

        # Retry Lambda for Missing Metadata
        retry_lambda = lambda_.Function(
            self, "RetryLambda",
            runtime=lambda_.Runtime.PYTHON_3_9,
            handler="Retry_failed.lambda_handler",
            code=lambda_.Code.from_asset("lambdas/Retry_failed"),
            layers=[layer_psycopg2, layer_db_utils, layer_ffmpeg, layer_ffprobe], 
            role=lambda_role,
            timeout=Duration.seconds(300),
            environment= environment_vars
        )


        # S3 Event Notification to Trigger Workflow Queue
        bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3_notifications.SqsDestination(workflow_queue),
            s3.NotificationKeyFilter(prefix="uploads/", suffix=".mp4")
        )

        # EventBridge Rule for Retry Lambda
        retry_rule = events.Rule(
            self, "RetryScheduleRule",
            schedule=events.Schedule.cron(minute="*/30")  # Every 5 minutes
        )

        retry_rule.add_target(targets.LambdaFunction(retry_lambda))

