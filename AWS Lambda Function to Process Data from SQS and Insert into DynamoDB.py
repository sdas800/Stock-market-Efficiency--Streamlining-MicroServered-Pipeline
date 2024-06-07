import json
import boto3
from datetime import datetime

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb', region_name='your-region')
table = dynamodb.Table('your-dynamodb-table')


def lambda_handler(event, context):
    for record in event['Records']:
        payload = json.loads(record['body'])
        process_and_store_data(payload)


def process_and_store_data(data):
    stock_price = data['price']
    stock_high = data['high']
    stock_low = data['low']
    timestamp = datetime.now().strftime('%d-%Y-%M %H:%M:%S')

    item = {
        'StockPrice': stock_price,
        'High': stock_high,
        'Low': stock_low,
        'Timestamp': timestamp
    }

    table.put_item(Item=item)

    return item
