import boto3
import os
import json
from db_utils import connect_to_postgres

# Initialize AWS clients
sqs_client = boto3.client('sqs')
sns_client = boto3.client('sns')
s3_client = boto3.client('s3')

# Load environment variables
SNS_TOPIC_ARN = os.getenv('SNS_TOPIC_ARN')  # ARN of the SNS topic for notifications
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')  # Name of the S3 bucket
metadata_queue_url = os.getenv("METADATA_QUEUE_URL")


def lambda_handler(event, context):
    """
    Lambda handler for processing S3 notifications from the workflow queue.
    """
    for record in event['Records']:
        try:
            # Directly parse the SQS message body (it contains the S3 event notification)
            message_body = json.loads(record['body'])

            # Extract bucket name and object key from the S3 event notification
            bucket_name = message_body['Records'][0]['s3']['bucket']['name']
            video_file_key = message_body['Records'][0]['s3']['object']['key']
            s3_arrival_time = message_body['Records'][0]['eventTime']

            if not video_file_key:
                print("No video file key found in the message. Skipping...")
                continue

            print(f"Processing video file: {video_file_key}")

            # Call process_metadata with the video file key
            process_metadata(video_file_key, s3_arrival_time)

        except Exception as e:
            print(f"Error processing message: {record}. Error: {e}")
            move_to_failed_folder(video_file_key)
            continue

    return {"status": "success"}


def process_metadata(video_file_key, s3_arrival_time):
    """
    Process metadata for a given video file.
    """
    print(f"Processing metadata for {video_file_key}...")
    

    # Check if metadata exists
    metadata_exists = check_metadata_exists(S3_BUCKET_NAME, video_file_key)

    if metadata_exists:
        print(f"Metadata exists for {video_file_key}. Extracting and storing in the database.")

        # Update processing status
        update_metadata_status(video_file_key,s3_arrival_time, metadata_present=True)
        
        # Download metadata JSON file
        metadata_file_key = os.path.splitext(video_file_key)[0] + '.json'
        metadata = download_metadata(S3_BUCKET_NAME, metadata_file_key)

        # Store metadata in the database
        store_metadata_in_db(video_file_key,s3_arrival_time, metadata)

        # Forward to the next step in the pipeline
        forward_to_next_queue(video_file_key, s3_arrival_time)
    else:
        print(f"Metadata missing for {video_file_key}. Sending notification and moving to failed folder.")
        send_metadata_missing_notification(video_file_key)
        log_missing_metadata_in_db(video_file_key, s3_arrival_time)
        move_to_failed_folder(video_file_key)


def move_to_failed_folder(video_file_key):
    """
    Moves the video file with missing metadata to the failed folder in the S3 bucket.
    """
    failed_key = video_file_key.replace("uploads/", "failed/")
    metadata_file_key = os.path.splitext(video_file_key)[0] + '.json'
    failed_metadata_key = metadata_file_key.replace("uploads/", "failed/")
    try:
        s3_client.copy_object(
            Bucket=S3_BUCKET_NAME,
            CopySource={'Bucket': S3_BUCKET_NAME, 'Key': video_file_key},
            Key=failed_key
        )
        s3_client.delete_object(Bucket=S3_BUCKET_NAME, Key=video_file_key)
        print(f"File moved to failed folder: s3://{S3_BUCKET_NAME}/{failed_key}")
    
        try:
            s3_client.copy_object(
                Bucket=S3_BUCKET_NAME,
                CopySource={'Bucket': S3_BUCKET_NAME, 'Key': metadata_file_key},
                Key=failed_metadata_key
            )
            s3_client.delete_object(Bucket=S3_BUCKET_NAME, Key=metadata_file_key)
            print(f"Metadata file moved to failed folder: s3://{S3_BUCKET_NAME}/{failed_metadata_key}")
        except s3_client.exceptions.NoSuchKey:
            print(f"Metadata file {metadata_file_key} does not exist. Skipping metadata move.")

    except Exception as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            print(f"No metadata file found for {video_file_key}.")
        else:
            print(f"Error moving files to failed folder: {e}")
            raise
        


def check_metadata_exists(bucket_name, video_file_key):
    """
    Checks if the metadata file exists in the specified S3 bucket.
    """
    metadata_file_key = os.path.splitext(video_file_key)[0] + '.json'
    try:
        s3_client.head_object(Bucket=bucket_name, Key=metadata_file_key)
        print(f"Metadata file found: {metadata_file_key}")
        return True
    except s3_client.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            print(f"Metadata file missing for: {video_file_key}")
            return False
        else:
            print(f"Error checking metadata: {e}")
            raise


def send_metadata_missing_notification(video_file_key):
    """
    Sends an SNS notification if the metadata file is missing.
    """
    message = (
        f"Metadata file is missing for the video file: {video_file_key}.\n"
        f"Please upload the metadata file to proceed.\n"
        f"File Location: s3://{S3_BUCKET_NAME}/{video_file_key}"
    )
    try:
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=message,
            Subject="Metadata Missing Notification"
        )
        print(f"Notification sent for missing metadata: {video_file_key}")
    except Exception as e:
        print(f"Error sending notification: {e}")


def update_metadata_status(video_key, s3_arrival_time, metadata_present):
    """
    Updates the metadata status in the database.
    """
    connection = connect_to_postgres()
    try:
        normalized_video_key = os.path.splitext(os.path.basename(video_key))[0]

        with connection.cursor() as cursor:
            sql = """
            INSERT INTO video_processing_status (video_key, s3_arrival_time, metadata_present, arrival_time, last_updated)
            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (video_key, s3_arrival_time)
            DO UPDATE SET metadata_present = EXCLUDED.metadata_present, last_updated = CURRENT_TIMESTAMP;
            """
            cursor.execute(sql, (normalized_video_key, s3_arrival_time, 'true' if metadata_present else 'false', s3_arrival_time))
            connection.commit()
            print(f"Metadata status updated for {normalized_video_key}, Arrival Time: {s3_arrival_time}.")
    except Exception as e:
        print(f"Error updating metadata status for {video_key}: {e}")
        raise
    finally:
        connection.close()



def forward_to_next_queue(video_file_key, s3_arrival_time):
    """
    Forwards the video file to the next FIFO SQS queue (Quality Check).
    """

    metadata_queue_url = os.getenv("METADATA_QUEUE_URL")
    try:
        sqs_client.send_message(
            QueueUrl=metadata_queue_url,
            MessageBody=json.dumps({"video_key": video_file_key, "s3_arrival_time": s3_arrival_time})
        )
        print(f"Message forwarded to Quality Check Queue: {video_file_key}")
    except Exception as e:
        print(f"Error forwarding to next queue: {e}")



def log_missing_metadata_in_db(video_file_key, s3_arrival_time):
    """
    Logs the missing metadata event in the database.
    """
    connection = connect_to_postgres()
    try:
        normalized_video_key = os.path.splitext(os.path.basename(video_file_key))[0]

        with connection.cursor() as cursor:
            sql = """
            INSERT INTO video_processing_status (
                video_key, s3_arrival_time, metadata_present, arrival_time, last_updated
            )
            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP);
            """
            cursor.execute(sql, (normalized_video_key, s3_arrival_time, 'false', s3_arrival_time))
            connection.commit()
            print(f"Logged missing metadata for {normalized_video_key}, Arrival Time: {s3_arrival_time}.")
    except Exception as e:
        print(f"Error logging missing metadata for {video_file_key}: {e}")
        raise
    finally:
        connection.close()



def store_metadata_in_db(video_file_key, s3_arrival_time, metadata):
    """
    Stores metadata information into the video_metadata table.
    """
    connection = connect_to_postgres()
    try:
        normalized_video_key = os.path.splitext(os.path.basename(video_file_key))[0]

        with connection.cursor() as cursor:
            # Insert metadata
            sql = """
            INSERT INTO video_metadata (
                video_key, s3_arrival_time, duration, file_size, codec, resolution,
                frame_rate, bitrate, audio_channels, title, description, tags,
                creation_date, creator_author, copyright_info, file_format,
                encoding_type, access_rights, version_info, created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP);
            """
            cursor.execute(sql, (
                normalized_video_key, s3_arrival_time,
                metadata.get('Duration'), metadata.get('File size'), metadata.get('Codec'),
                metadata.get('Resolution'), metadata.get('Frame rate'), metadata.get('Bitrate'),
                metadata.get('Audio channels'), metadata.get('Title'), metadata.get('Description'),
                metadata.get('Tags'), metadata.get('Creation date'), metadata.get('Creator/Author'),
                metadata.get('Copyright information'), metadata.get('File format'),
                metadata.get('Encoding type'), metadata.get('Access rights'), metadata.get('Version information')
            ))
            connection.commit()
            print(f"Metadata for {normalized_video_key}, Arrival Time: {s3_arrival_time}, stored successfully in the database.")
    except Exception as e:
        print(f"Error storing metadata for {video_file_key}: {e}")
        raise
    finally:
        connection.close()




def download_metadata(bucket_name, metadata_file_key):
    """
    Downloads the metadata JSON file from S3 and parses it.
    """
    try:
        metadata_object = s3_client.get_object(Bucket=bucket_name, Key=metadata_file_key)
        metadata_content = metadata_object['Body'].read().decode('utf-8')
        metadata = json.loads(metadata_content)

        # Validate metadata for required keys
        required_keys = ['Duration', 'File size', 'Codec', 'Resolution']
        missing_keys = [key for key in required_keys if key not in metadata]
        if missing_keys:
            raise ValueError(f"Metadata is missing required keys: {missing_keys}")

        print(f"Metadata for {metadata_file_key} downloaded successfully.")
        return metadata
    except json.JSONDecodeError as e:
        print(f"Error parsing metadata JSON for {metadata_file_key}: {e}")
        raise
    except Exception as e:
        print(f"Error downloading metadata for {metadata_file_key}: {e}")
        raise

