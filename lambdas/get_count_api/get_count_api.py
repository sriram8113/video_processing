import json
from db_utils import connect_to_postgres

def lambda_handler(event, context):
    # Establish a connection to the database
    conn = connect_to_postgres()
    cur = conn.cursor()

    # Extract time frame from the event
    begin_time = event['queryStringParameters']['begin_time']
    end_time = event['queryStringParameters']['end_time']

    # Execute the stored procedure for fetching counts
    cur.execute("SELECT * FROM get_counts(%s, %s);", (begin_time, end_time))

    # Fetch and format the results
    columns = [desc[0] for desc in cur.description]
    records = [dict(zip(columns, row)) for row in cur.fetchall()]

    cur.close()
    conn.close()

    return {
        'statusCode': 200,
        'body': json.dumps(records),
        'headers': {'Content-Type': 'application/json'}
    }
