import boto3
import os
import json
from db_utils import connect_to_postgres

# Initialize AWS clients
s3_client = boto3.client('s3')

# Environment variables
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')  # Name of the S3 bucket
FAILED_FOLDER = "failed/"
UPLOADS_FOLDER = "uploads/"
UNABLE_TO_PROCESS_FOLDER = "unable_to_process/"
MAX_RETRY_COUNT = 5  # Maximum retry attempts for a file

def lambda_handler(event, context):
    """
    Lambda handler to retry processing files in the failed folder.
    """
    try:
        # List objects in the failed folder
        failed_objects = s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=FAILED_FOLDER)
        if "Contents" not in failed_objects:
            print("No files in the failed folder.")
            return

        for obj in failed_objects["Contents"]:
            file_key = obj["Key"]

            # Skip directories
            if file_key.endswith("/"):
                continue

            # Process the video file if it is a video
            if file_key.endswith(".mp4"):
                process_failed_video(file_key)

        print("Retry process completed successfully.")
    except Exception as e:
        print(f"Error during retry process: {e}")


def process_failed_video(video_file_key):
    """
    Processes a single video file in the failed folder.
    """
    metadata_file_key = os.path.splitext(video_file_key)[0] + ".json"

    # Check if metadata exists in the failed folder
    metadata_in_failed = file_exists(S3_BUCKET_NAME, metadata_file_key)

    if metadata_in_failed:
        # Move both video and metadata to uploads
        move_file(S3_BUCKET_NAME, video_file_key, UPLOADS_FOLDER + os.path.basename(video_file_key))
        move_file(S3_BUCKET_NAME, metadata_file_key, UPLOADS_FOLDER + os.path.basename(metadata_file_key))
        increment_retry_count(video_file_key)
        print(f"Moved {video_file_key} and its metadata to uploads.")
        return

    # Check if metadata exists in uploads
    metadata_in_uploads = file_exists(S3_BUCKET_NAME, UPLOADS_FOLDER + os.path.basename(metadata_file_key))

    if metadata_in_uploads:
        # Move video to uploads
        move_file(S3_BUCKET_NAME, video_file_key, UPLOADS_FOLDER + os.path.basename(video_file_key))
        increment_retry_count(video_file_key)
        print(f"Moved {video_file_key} to uploads. Metadata already in uploads.")
        return

    retry_count = get_retry_count(video_file_key)

    if retry_count > MAX_RETRY_COUNT:
        # Move video and metadata to unable_to_process if both exist
        if metadata_in_failed:
            move_file(S3_BUCKET_NAME, video_file_key, UNABLE_TO_PROCESS_FOLDER + os.path.basename(video_file_key))
            move_file(S3_BUCKET_NAME, metadata_file_key, UNABLE_TO_PROCESS_FOLDER + os.path.basename(metadata_file_key))
            print(f"Moved {video_file_key} and its metadata to unable_to_process.")
        else:
            print(f"Metadata missing for {video_file_key}. File remains in failed folder.")
        return

    print(f"Retry count for {video_file_key} incremented to {retry_count}.")


def file_exists(bucket_name, file_key):
    """
    Checks if a file exists in the S3 bucket.
    """
    try:
        s3_client.head_object(Bucket=bucket_name, Key=file_key)
        return True
    except s3_client.exceptions.ClientError:
        return False


def move_file(bucket_name, source_key, destination_key):
    """
    Moves a file from one location to another in the S3 bucket.
    """
    try:
        s3_client.copy_object(Bucket=bucket_name, CopySource={"Bucket": bucket_name, "Key": source_key}, Key=destination_key)
        s3_client.delete_object(Bucket=bucket_name, Key=source_key)
        print(f"Moved {source_key} to {destination_key}.")
    except Exception as e:
        print(f"Error moving file {source_key} to {destination_key}: {e}")


def increment_retry_count(video_file_key):
    """
    Increments the retry count for a video file in the retry_status table.
    """
    connection = connect_to_postgres()
    try:
        normalized_video_key = os.path.splitext(os.path.basename(video_file_key))[0]

        with connection.cursor() as cursor:
            # Check if the video_key exists in retry_status
            sql_check = """
            SELECT retry_count FROM retry_status WHERE video_key = %s;
            """
            cursor.execute(sql_check, (normalized_video_key,))
            result = cursor.fetchone()

            if result:
                # Increment retry count
                sql_update = """
                UPDATE retry_status
                SET retry_count = retry_count + 1, last_retry_time = CURRENT_TIMESTAMP
                WHERE video_key = %s;
                """
                cursor.execute(sql_update, (normalized_video_key,))
            else:
                # Insert a new record for retry
                sql_insert = """
                INSERT INTO retry_status (video_key, retry_count, last_retry_time)
                VALUES (%s, 1, CURRENT_TIMESTAMP);
                """
                cursor.execute(sql_insert, (normalized_video_key,))

            connection.commit()
    except Exception as e:
        print(f"Error incrementing retry count for {video_file_key}: {e}")
    finally:
        connection.close()


def get_retry_count(video_file_key):
    """
    Retrieves the retry count for a video file from the retry_status table.
    """
    connection = connect_to_postgres()
    try:
        normalized_video_key = os.path.splitext(os.path.basename(video_file_key))[0]

        with connection.cursor() as cursor:
            sql = """
            SELECT COALESCE(retry_count, 0)
            FROM retry_status
            WHERE video_key = %s;
            """
            cursor.execute(sql, (normalized_video_key,))
            result = cursor.fetchone()
            return result[0] if result else 0
    except Exception as e:
        print(f"Error fetching retry count for {video_file_key}: {e}")
        return 0
    finally:
        connection.close()

