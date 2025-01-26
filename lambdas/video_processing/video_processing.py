import boto3
import os
import subprocess
import json
from db_utils import connect_to_postgres  # Import the modular database connection

# Initialize AWS clients
s3_client = boto3.client('s3')
sqs_client = boto3.client('sqs')

# Environment variables
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')  # Name of the S3 bucket
UPLOAD_FOLDER_PREFIX = os.getenv('UPLOAD_FOLDER')  # Prefix for the upload folder
PROCESSED_FOLDER_PREFIX = os.getenv('PROCESSED_FOLDER')  # Prefix for processed videos
PROCESSING_QUEUE_URL = os.getenv('PROCESSING_QUEUE_URL')  # URL of the notification queue


if not S3_BUCKET_NAME or not UPLOAD_FOLDER_PREFIX or not PROCESSED_FOLDER_PREFIX or not PROCESSING_QUEUE_URL:
    raise ValueError("Missing required environment variables.")


def process_video(input_path, output_path):
    """
    Processes the video using FFmpeg to create a derivative file (e.g., rotated and cropped).
    """
    try:
        command = [
            '/opt/ffmpeglib/ffmpeg', '-i', input_path, '-vf', 'transpose=1, crop=240:240',
             output_path
        ]
        subprocess.run(command, check=True)
        print(f"Video processed successfully: {output_path}")
    except subprocess.CalledProcessError as e:
        print(f"Error processing video: {e}")
        raise


def update_processing_status(video_key, s3_arrival_time, status):
    """
    Updates the processing status of the video in the database.
    """
    connection = connect_to_postgres()
    try:
        normalized_video_key = os.path.splitext(os.path.basename(video_key))[0]
        
        with connection.cursor() as cursor:
            sql = """
            UPDATE video_processing_status
            SET processing_status = %s, last_updated = CURRENT_TIMESTAMP
            WHERE video_key = %s AND s3_arrival_time = %s;
            """
            cursor.execute(sql, (status, normalized_video_key, s3_arrival_time))
            connection.commit()
            print(f"Processing status updated for {normalized_video_key}, Arrival Time: {s3_arrival_time}, Status: {status}.")
    except Exception as e:
        print(f"Error updating processing status for {video_key}: {e}")
        raise
    finally:
        connection.close()



def forward_to_notification_queue(processed_key,s3_arrival_time ):
    """
    Forwards the processed video file key to the notification queue.
    """
    try:
        sqs_client.send_message(
            QueueUrl=PROCESSING_QUEUE_URL,
            MessageBody=json.dumps({"processed_file_key": processed_key, "s3_arrival_time": s3_arrival_time})
        )

        print(f"Message forwarded to the notification queue for {processed_key}.")
    except Exception as e:
        print(f"Error forwarding message to the notification queue: {e}")
        raise


def lambda_handler(event, context):
    """
    Lambda handler for video processing.
    
    """
    for record in event['Records']:
        # Parse the SQS message
        message_body = json.loads(record['body'])
        video_file_key = message_body.get('video_key')
        s3_arrival_time = message_body.get('s3_arrival_time')

        if not video_file_key:
            print("No video file key found in message. Skipping...")
            continue

        print(f"Processing video file: {video_file_key}")

    # Ensure the file is in the correct folder prefix
    if not video_file_key.startswith(UPLOAD_FOLDER_PREFIX):
        print(f"File {video_file_key} is outside the upload folder. Skipping...")
        update_processing_status(video_file_key, status="Skipped")
        return {"processed": False}

    # Download video file locally
    local_input_path = f"/tmp/{os.path.basename(video_file_key)}"
    try:
        s3_client.download_file(S3_BUCKET_NAME, video_file_key, local_input_path)
        print(f"Downloaded file from S3: {local_input_path}")
    except s3_client.exceptions.NoSuchKey:
        print(f"File {video_file_key} does not exist in S3.")
        update_processing_status(video_file_key, status="Failed - Missing File")
    except Exception as e:
        print(f"Error downloading video file {video_file_key}: {e}")
        update_processing_status(video_file_key, status="Failed")

    
    # Define the metadata file key and local path
    metadata_file_key = os.path.splitext(video_file_key)[0] + '.json'
    local_metadata_path = f"/tmp/{os.path.basename(metadata_file_key)}"

    try:
        s3_client.download_file(S3_BUCKET_NAME, metadata_file_key, local_metadata_path)
        print(f"Downloaded metadata file from S3: {local_metadata_path}")
    except s3_client.exceptions.NoSuchKey:
        print(f"Metadata file {metadata_file_key} does not exist. Proceeding without it.")
        local_metadata_path = None

    # Define the output file path
    local_output_path = f"/tmp/processed_{os.path.basename(video_file_key)}"

    try:
        # Process the video
        process_video(local_input_path, local_output_path)

        # Upload the processed video to the processed folder in S3
        processed_key = f"{PROCESSED_FOLDER_PREFIX}{os.path.basename(local_output_path)}"
        try:
            s3_client.upload_file(local_output_path, S3_BUCKET_NAME, processed_key)
        except Exception as e:
            print(f"Error uploading processed file {processed_key}: {e}")
            update_processing_status(video_file_key, status="Failed - Upload Error")
            raise

        # Move the metadata file to the processed folder, if it exists
        if local_metadata_path:
            processed_metadata_key = f"{PROCESSED_FOLDER_PREFIX}{os.path.basename(metadata_file_key)}"
            try:
                s3_client.upload_file(local_metadata_path, S3_BUCKET_NAME, processed_metadata_key)
                print(f"Metadata file moved to processed folder: s3://{S3_BUCKET_NAME}/{processed_metadata_key}")
            except Exception as e:
                print(f"Error moving metadata file {processed_metadata_key}: {e}")
                raise


        # Update database with completion status
        update_processing_status(video_file_key,s3_arrival_time, status="Completed")

        # Forward the processed video key to the notification queue
        forward_to_notification_queue(processed_key, s3_arrival_time)

        return {"processed": True, "processed_file_key": processed_key}

    except Exception as e:
        # Update database with failure status
        update_processing_status(video_file_key,s3_arrival_time, status="Failed")
        print(f"Error processing video: {e}")
        return {"processed": False, "error": str(e)}
    
    finally:
        for file_path in [local_input_path, local_output_path]:
            if os.path.exists(file_path):
                os.remove(file_path)
                print(f"Temporary file removed: {file_path}")
