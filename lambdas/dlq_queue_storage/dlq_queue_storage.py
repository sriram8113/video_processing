import boto3
import os
import json
from db_utils import connect_to_postgres  # Ensure this utility is correctly set up for DB connection

def process_dlq_messages(dlq_url):
    sqs = boto3.client('sqs')
    conn = connect_to_postgres()
    cur = conn.cursor()

    response = sqs.receive_message(
        QueueUrl=dlq_url,
        MaxNumberOfMessages=10,
        WaitTimeSeconds=20
    )

    messages = response.get('Messages', [])
    for message in messages:
        try:
            message_body = json.loads(message['Body'])
            cur.execute("INSERT INTO dlq_messages (message_body) VALUES (%s)", (json.dumps(message_body),))
            conn.commit()
            sqs.delete_message(
                QueueUrl=dlq_url,
                ReceiptHandle=message['ReceiptHandle']
            )
            print("Message processed and deleted from DLQ")
        except Exception as e:
            print(f"Error processing message from DLQ at {dlq_url}: {e}")
            conn.rollback()

    cur.close()
    conn.close()
    return len(messages)

def lambda_handler(event, context):
    metadata_count = process_dlq_messages(os.getenv('METADATA_QUEUE_DLQ_URL'))
    quality_check_count = process_dlq_messages(os.getenv('QUALITY_CHECK_QUEUE_DLQ_URL'))
    processing_count = process_dlq_messages(os.getenv('PROCESSING_QUEUE_DLQ_URL'))

    total_processed = metadata_count + quality_check_count + processing_count
    return {
        'statusCode': 200,
        'body': f"Processed {total_processed} messages from all DLQs"
    }
