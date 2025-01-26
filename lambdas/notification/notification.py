import boto3
import os
import json
from db_utils import connect_to_postgres # Import the modular database connection

# Initialize AWS clients
sns_client = boto3.client('sns')
sqs_client = boto3.client('sqs')

# Environment variables
SNS_TOPIC_ARN = os.getenv('SNS_TOPIC_ARN')  # ARN of the SNS topic for notifications
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')  # Name of the S3 bucket



def send_annotation_notification(processed_file_key):
    """
    Sends an SNS notification to inform that the file is ready for annotation.
    """
    try:
        message = (
            f"The processed video is ready for annotation:\n"
            f"File Location: s3://{S3_BUCKET_NAME}/{processed_file_key}"
        )
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=message,
            Subject="Video Ready for Annotation"
        )
        print(f"Notification sent for processed file: {processed_file_key}")
    except Exception as e:
        print(f"Error sending notification for {processed_file_key}: {e}")
        raise


def update_notification_status(video_key, s3_arrival_time):
    """
    Updates the notification status in the database.
    """
    connection = connect_to_postgres()
    try:
        normalized_video_key = os.path.splitext(os.path.basename(video_key).replace("processed_", ""))[0]

        with connection.cursor() as cursor:
            sql = """
            UPDATE video_processing_status
            SET notification_sent = TRUE, last_updated = CURRENT_TIMESTAMP
            WHERE video_key = %s AND s3_arrival_time = %s;
            """
            cursor.execute(sql, (normalized_video_key, s3_arrival_time))
            connection.commit()
            print(f"Notification status updated for {normalized_video_key}, Arrival Time: {s3_arrival_time}.")
    except Exception as e:
        print(f"Error updating notification status for {normalized_video_key}: {e}")
        raise
    finally:
        connection.close()



def process_notification(message):
    """
    Processes a single SQS message to send a notification.
    """
    try:
        # Extract processed file key from the message
        processed_file_key = message.get('processed_file_key')
        video_key = os.path.basename(processed_file_key)
        s3_arrival_time = message.get('s3_arrival_time')

        # Send annotation notification
        send_annotation_notification(processed_file_key)

        # Update the database to log notification status
        update_notification_status(video_key, s3_arrival_time)

        print(f"Successfully processed notification for {video_key}.")
        return {"notification_sent": True, "processed_file_key": processed_file_key}

    except Exception as e:
        print(f"Error processing notification for message: {message}: {e}")
        return {"notification_sent": False, "error": str(e)}


def lambda_handler(event, context):
    """
    Lambda handler for sending annotation notifications.
    """
    try:
        # Process all messages from the SQS queue
        for record in event['Records']:
            message_body = json.loads(record['body'])
            result = process_notification(message_body)
            print(f"Processed message result: {result}")

        return {"status": "success"}

    except Exception as e:
        print(f"Error processing SQS messages: {e}")
        return {"status": "failure", "error": str(e)}
