import json
import datetime
from db_utils import connect_to_postgres

class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder for datetime objects."""
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)

def lambda_handler(event, context):
    # Establish a connection to the database
    conn = connect_to_postgres()
    cur = conn.cursor()

    # Extract time frame from the event
    begin_time = event['queryStringParameters']['begin_time']
    end_time = event['queryStringParameters']['end_time']

    # Execute the stored procedure for fetching video details
    cur.execute("SELECT * FROM get_video_details(%s, %s);", (begin_time, end_time))

    # Fetch and format the results
    columns = [desc[0] for desc in cur.description]
    records = [dict(zip(columns, row)) for row in cur.fetchall()]

    cur.close()
    conn.close()

    # Use the custom encoder to serialize the response
    response_body = json.dumps(records, cls=DateTimeEncoder)

    return {
        'statusCode': 200,
        'body': response_body,
        'headers': {'Content-Type': 'application/json'}
    }
