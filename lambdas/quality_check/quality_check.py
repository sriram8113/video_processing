import boto3
import os
import subprocess
import json
from db_utils import connect_to_postgres # Import the modular database connection

# Initialize AWS clients
s3_client = boto3.client('s3')
sqs_client = boto3.client('sqs')

# Environment variables
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')  # Name of the S3 bucket
UPLOAD_FOLDER_PREFIX = os.getenv('UPLOAD_FOLDER')  # Prefix for the upload folder
FILE_SIZE_THRESHOLD_MB = int(os.getenv('FILE_SIZE_THRESHOLD_MB', 1))  # File size threshold in MB
QUALITY_CHECK_QUEUE_URL = os.getenv("QUALITY_CHECK_QUEUE_URL")

def lambda_handler(event, context):
    """
    Lambda handler for processing messages from the SQS queue for quality checks.
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

        try:
            # Process the video for quality check
            process_quality_check(video_file_key, s3_arrival_time)
        except Exception as e:
            print(f"Error processing video file {video_file_key}: {e}")
            move_to_failed_folder(video_file_key)

    return {"status": "success"}


def process_quality_check(video_file_key, s3_arrival_time):
    if not video_file_key.startswith(UPLOAD_FOLDER_PREFIX):
        print(f"File {video_file_key} is outside the upload folder. Skipping...")
        return

    local_video_path = f"/tmp/{os.path.basename(video_file_key)}"
    try:
        s3_client.download_file(S3_BUCKET_NAME, video_file_key, local_video_path)
        print(f"File downloaded successfully: {local_video_path}")
    except s3_client.exceptions.NoSuchKey:
        print(f"File {video_file_key} does not exist in S3.")
    except Exception as e:
        print(f"Error downloading video file {video_file_key}: {e}")


    try:
        corruption_check = check_video_corruption(local_video_path)
        has_audio, has_video = check_blank_content(local_video_path)
        size_check = check_file_size(local_video_path)

        print(f"Corruption Check: {corruption_check}, Audio: {has_audio}, Video: {has_video}, Size Check: {size_check}")

        quality_rating = calculate_quality_rating(corruption_check, has_audio, has_video)
        save_results_to_db(video_file_key, s3_arrival_time, corruption_check, has_audio, has_video, quality_rating)

        if quality_rating >= 2 and size_check:
            forward_to_next_queue(video_file_key, s3_arrival_time)
        else:
            reason = "corruption" if not corruption_check else "size_check" if not size_check else "audio/video issues"
            print(f"Video {video_file_key} failed quality check with rating {quality_rating}. Reason: {reason}")
            move_to_failed_folder(video_file_key)

    finally:
        if os.path.exists(local_video_path):
            os.remove(local_video_path)
            print(f"Temporary file removed: {local_video_path}")



def check_video_corruption(video_path):
    """
    Checks for video corruption using FFmpeg.
    Returns True if the video is valid, False otherwise.
    """
    try:
        subprocess.run(
                    ['/opt/ffmpeglib/ffmpeg', '-hide_banner', '-loglevel', 'error', '-i', video_path, '-f', 'null', '-'],
                    check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        print("No corruption found in the video.")
        return True
    except subprocess.CalledProcessError:
        print("Video corruption detected.")
        return False


def check_blank_content(video_path):
    """
    Checks for blank content (missing audio or video streams) using FFmpeg.
    Returns a tuple (has_audio, has_video) indicating the presence of streams.
    """
    result = subprocess.run(
        ['/opt/ffprobelib/ffprobe', '-i', video_path, '-show_streams', '-select_streams', 'v', '-loglevel', 'error'],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    has_video = b"codec_type=video" in result.stdout

    result = subprocess.run(
        ['/opt/ffprobelib/ffprobe', '-i', video_path, '-show_streams', '-select_streams', 'a', '-loglevel', 'error'],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    has_audio = b"codec_type=audio" in result.stdout

    print(f"Video stream: {'Found' if has_video else 'Not Found'}; Audio stream: {'Found' if has_audio else 'Not Found'}")
    return has_audio, has_video


def check_file_size(video_path):
    """
    Checks if the file size is above a defined threshold.
    Returns True if the size is valid, False otherwise.
    """
    file_size_mb = os.path.getsize(video_path) / (1024 * 1024)
    if file_size_mb >= FILE_SIZE_THRESHOLD_MB:
        print(f"File size is valid: {file_size_mb:.2f} MB.")
        return True
    else:
        print(f"File size anomaly detected: {file_size_mb:.2f} MB.")
        return False


def calculate_quality_rating(corruption_check, has_audio, has_video):
    """
    Calculates the quality rating based on the checks and quality of audio/video.
    """
    if not corruption_check:
        return 0  # Corrupted video

    if not has_audio and not has_video:
        return 0  # Blank video

    if not has_video and has_audio:
        return 1  # No video but audio present

    if not has_audio and has_video:
        return 2  # No audio but video present

    # Both streams present; evaluate quality
    if has_audio and has_video:
        return 5  # Best quality

    return 3  # Mixed quality


def save_results_to_db(video_key, s3_arrival_time, corruption_check, has_audio, has_video, quality_rating):
    """
    Saves the quality check results into the PostgreSQL database.
    """
    connection = connect_to_postgres()
    try:
        normalized_video_key = os.path.splitext(os.path.basename(video_key))[0]

        with connection.cursor() as cursor:
            # Use both video_key and s3_arrival_time to handle the composite key conflict
            sql = """
            INSERT INTO public.video_processing_status (
                video_key, s3_arrival_time, corruption_check, has_audio, has_video, quality_rating
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (video_key, s3_arrival_time)
            DO UPDATE SET
                corruption_check = EXCLUDED.corruption_check,
                has_audio = EXCLUDED.has_audio,
                has_video = EXCLUDED.has_video,
                quality_rating = EXCLUDED.quality_rating,
                last_updated = CURRENT_TIMESTAMP;  -- Always update the last_updated field
            """
            cursor.execute(sql, (normalized_video_key, s3_arrival_time, corruption_check, has_audio, has_video, quality_rating))
            connection.commit()
            print(f"Results saved to database for {normalized_video_key}, Arrival Time: {s3_arrival_time}.")
    except Exception as e:
        print(f"Error saving results to database for {video_key}: {e}")
        raise
    finally:
        connection.close()


def forward_to_next_queue(video_file_key, s3_arrival_time):
    """
    Forwards the video file to the next FIFO SQS queue for further processing.
    """

    QUALITY_CHECK_QUEUE_URL = os.getenv("QUALITY_CHECK_QUEUE_URL")

    try:
        sqs_client.send_message(
            QueueUrl=QUALITY_CHECK_QUEUE_URL,
            MessageBody=json.dumps({"video_key": video_file_key, "s3_arrival_time": s3_arrival_time})
        )
        print(f"Message forwarded to the next queue for {video_file_key}.")
    except Exception as e:
        print(f"Error forwarding message to the next queue for {video_file_key}: {e}")

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
