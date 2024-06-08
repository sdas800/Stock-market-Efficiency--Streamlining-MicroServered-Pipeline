import requests
import boto3
import time
import json

# Configuration
secret_key = "0nTuDPM9IG9aosPFreWFFBUgXUxuoAwsGfT5ypmB"
access_key = "AKIAU6GDX6EDJ5QHKNVN"
sqs_queue_url = "https://sqs.ap-southeast-2.amazonaws.com/339712930054/testqueue"
rapid_api_url = "https://latest-stock-price.p.rapidapi.com/any"
querystring = {"Indices": "NIFTY"}

# Initialize SQS client
sqs = boto3.client('sqs', region_name="ap-southeast-2", aws_access_key_id=access_key, aws_secret_access_key=secret_key)

def fetch_stock_data():
    try:
        headers = {
            'x-rapidapi-key': "0c0e291746msh83d095396f0e1e3p1a5710jsn8715e16feff9",
            'x-rapidapi-host': "latest-stock-price.p.rapidapi.com"
        }
        response = requests.get(rapid_api_url, headers=headers, params=querystring)
        if response.status_code == 200:
            data = response.json()  # Assuming the API returns JSON data
            return {
                'statusCode': 200,
                'body': data
            }
        else:
            return {
                'statusCode': response.status_code,
                'body': f"Error: {response.reason}"
            }
    except Exception as e: 
        return {
            'statusCode': 500,
            'body': f"Exception occurred: {str(e)}"
        }

def process_stock_data(data):
    processed_data = []
    for element in data.get("body", []):
        stock_info = {
            'symbol': element['symbol'],
            'identifier': element['identifier'],
            'open': element['open'],
            'dayHigh': element['dayHigh'],
            'dayLow': element['dayLow'],
            'lastPrice': element['lastPrice'],
            'previousClose': element['previousClose'],
            'change': element['change'],
            'yearHigh': element['yearHigh'],
            'yearLow': element['yearLow']
        }
        processed_data.append(stock_info)
    return processed_data

def send_to_sqs(data):
    for stock in data:
        message_body = (f"symbol:{stock['symbol']},"
                        f"identifier:{stock['identifier']},"
                        f"open:{stock['open']},"
                        f"dayHigh:{stock['dayHigh']},"
                        f"dayLow:{stock['dayLow']},"
                        f"lastPrice:{stock['lastPrice']},"
                        f"previousClose:{stock['previousClose']},"
                        f"change:{stock['change']},"
                        f"yearHigh:{stock['yearHigh']},"
                        f"yearLow:{stock['yearLow']}")
        
        response = sqs.send_message(
            QueueUrl=sqs_queue_url,
            MessageBody=message_body



        )
    return response

def main():
    while True:
        stock_data = fetch_stock_data()
        if stock_data['statusCode'] == 200:
            processed_data = process_stock_data(stock_data)
            send_to_sqs(processed_data)
        else:
            print(f"Failed to fetch data: {stock_data['body']}")
        time.sleep(60)  # Wait for 1 minute

if __name__ == "__main__":
    main()
