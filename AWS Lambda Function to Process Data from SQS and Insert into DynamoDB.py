import boto3
from datetime import datetime
import time
# Configuration
secret_key = "0nTuDPM9IG9aosPFreWFFBUgXUxuoAwsGfT5ypmB"
access_key = "AKIAU6GDX6EDJ5QHKNVN"
sqs_queue_url = "https://sqs.ap-southeast-2.amazonaws.com/339712930054/testqueue"

# Initialize SQS & DynamoDB client
sqs = boto3.client('sqs', region_name="ap-southeast-2", aws_access_key_id=access_key, aws_secret_access_key=secret_key)
dynamodb = boto3.resource('dynamodb', region_name='your-region')
dynamodb_table = dynamodb.Table('stock')

def process_and_store_data():
    # Continuously process and store data
    while True:
        # Receive up to 10 messages from the SQS queue
        response = sqs.receive_message(
            QueueUrl=sqs_queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=10
        )

        # Get the messages from the response
        messages = response.get('Messages', [])

        # Process each message
        for message in messages:
            # Get the body of the message
            body = message['Body']
            # Parse the stock data from the message body
            stock_data = dict(item.split(":") for item in body.split(","))

            # Prepare the item for DynamoDB
            timestamp = datetime.now().isoformat()
            dynamodb.put_item(
                TableName=dynamodb_table,
                Item={
                    'Symbol': {'S': stock_data['symbol']},
                    'Identifier': {'S': stock_data['identifier']},
                    'Open': {'N': stock_data['open']},
                    'DayHigh': {'N': stock_data['dayHigh']},
                    'DayLow': {'N': stock_data['dayLow']},
                    'LastPrice': {'N': stock_data['lastPrice']},
                    'PreviousClose': {'N': stock_data['previousClose']},
                    'Change': {'N': stock_data['change']},
                    'YearHigh': {'N': stock_data['yearHigh']},
                    'YearLow': {'N': stock_data['yearLow']},
                    'Timestamp': {'S': timestamp}
                }
            )

            # Delete the processed message from the queue
            sqs.delete_message(
                QueueUrl=sqs_queue_url,
                ReceiptHandle=message['ReceiptHandle']
            )

        # Wait for 1 minute before checking the queue again
        time.sleep(60)
