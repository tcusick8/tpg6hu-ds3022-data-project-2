# prefect flow goes here

import boto3
import requests
import time
from prefect import task, flow

url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/tpg6hu"

sqs = boto3.client('sqs')

@task
def delete_messages(url, receipt_handle):
    try:
        if not receipt_handle:
            print("No messages to delete")
            return
        
        enteries = [
            {"Id": str(i), "ReceiptHandle": rh}
            for i, rh in enumerate(receipt_handle)
        ]

        response = sqs.delete_message(
            QueueUrl=url,
            ReceiptHandle=receipt_handle
        )

        successful = response.get("Successful", [])
        failed = response.get("Failed", [])


        print(f"Response: {response}")
    except Exception as e:
        print(f"Error deleting message: {e}")
        raise e

batch_size=10

@task
def get_messages(url, batch_size):
    # try to get any messages with message-attributes from SQS queue:
    try:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MessageSystemAttributeNames=['All'],
            MaxNumberOfMessages=batch_size,
            VisibilityTimeout=60,
            MessageAttributeNames=['All'],
            WaitTimeSeconds=10
        )

        messages = response.get("messages: ", [])

        if not messages:
            print("no messages left")
            return None

        receipt_handle = response['Messages'][0]['ReceiptHandle']
        print(f"Reciept handle: {receipt_handle}")
        delete_messages(queue_url, receipt_handle)

        print(f"{response}")

        # print the MessageAttributes:
#         print(f"MessageAttributes: {response['Messages'][0]['MessageAttributes']}")

        # print the order_no:
        print(f"Order No: {response['Messages'][0]['MessageAttributes']['order_no']['StringValue']}")
        print(f"Word: {response['Messages'][0]['MessageAttributes']['word']['StringValue']}")
        print(f"Receipt Handle: {receipt_handle}")
        return response['Messages'][0]

    except Exception as e:
        print(f"Error getting message: {e}")
        raise e


payload = requests.post(url).json()

queue_url = payload["sqs_url"]
print(f"SQS URL: {queue_url}")

@task
def get_queue_attributes(queue_url):
    try:
        response = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=['All']
        )
        attrs = response["Attributes"]
        available = int(attrs.get('ApproximateNumberOfMessages', 0))
        delayed = int(attrs.get('ApproximateNumberOfMessagesDelayed', 0))
        in_flight = int(attrs.get('ApproximateNumberOfMessagesNotVisible', 0))
        total = available + delayed + in_flight
        
        print(f"Queue Status - Available: {available}, Delayed: {delayed}, In-flight: {in_flight}, Total: {total}")
        return attrs    
        
    except Exception as e:
        print(f"Error getting queue attributes: {e}")
        raise e


@task
def monitor_queue(queue_url, interval=15):
    """Continuously monitor the queue and print status"""
    try:
        response = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=['All']
        )
        attrs = response["Attributes"]
        available = int(attrs.get('ApproximateNumberOfMessages', 0))
        delayed = int(attrs.get('ApproximateNumberOfMessagesDelayed', 0))
        in_flight = int(attrs.get('ApproximateNumberOfMessagesNotVisible', 0))
        total = available + delayed + in_flight
        
        print(f"Queue Status - Available: {available}, Delayed: {delayed}, In-flight: {in_flight}, Total: {total}")
        return {
            'available': available,
            'delayed': delayed,
            'in_flight': in_flight,
            'total': total
        }
    except Exception as e:
        print(f"Error monitoring queue: {e}")
        raise e

@flow
def pipeline():
    print("Starting pipeline...")
    
    # Initial queue status
    attrs = get_queue_attributes(queue_url)
    
    # Collect all messages
    messages = []
    collected_count = 0
    
    while True:
        # Monitor queue status every 15 seconds
        if collected_count % 1 == 0:  # Print status every time
            status = monitor_queue(queue_url)
        
        # Try to get messages
        try:
            response = sqs.receive_message(
                QueueUrl=queue_url,
                MessageSystemAttributeNames=['All'],
                MaxNumberOfMessages=10,
                VisibilityTimeout=60,
                MessageAttributeNames=['All'],
                WaitTimeSeconds=10
            )
            
            if 'Messages' not in response:
                print("No messages available, waiting...")
                time.sleep(5)
                continue
            
            for msg in response['Messages']:
                receipt_handle = msg['ReceiptHandle']
                
                # Parse message
                attributes = msg['MessageAttributes']
                order_no = int(attributes['order_no']['StringValue'])
                word = attributes['word']['StringValue']
                
                print(f"Received - Order: {order_no}, Word: {word}")
                
                messages.append((order_no, word))
                
                # Delete message
                sqs.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=receipt_handle
                )
                collected_count += 1
                
        except Exception as e:
            print(f"Error processing messages: {e}")
            time.sleep(5)
            continue
        
        # Check if we've collected all 21 messages
        if len(messages) >= 21:
            print(f"Collected all {len(messages)} messages!")
            break
    
    # Sort messages by order number
    sorted_messages = sorted(messages, key=lambda x: x[0])
    words = [word for _, word in sorted_messages]
    phrase = " ".join(words)
    
    # Print the phrase to terminal
    print(f"\n{'='*60}")
    print(f"FINAL REASSEMBLED PHRASE:")
    print(f"'{phrase}'")
    print(f"{'='*60}\n")

    # Submit solution (COMMENTED OUT FOR TESTING - Uncomment to submit)
    submission_url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
    
    try:
        response = sqs.send_message(
            QueueUrl=submission_url,
            MessageBody=f"Solution for tpg6hu",
            MessageAttributes={
                'uvaid': {
                    'DataType': 'String',
                    'StringValue': 'tpg6hu'
                },
                'phrase': {
                    'DataType': 'String',
                    'StringValue': phrase
                },
                'platform': {
                    'DataType': 'String',
                    'StringValue': 'prefect'
                }
            }
        )
        print(f"Solution submitted successfully!")
        print(f"Response: {response}")
    except Exception as e:
        print(f"Error submitting solution: {e}")
        raise e
    
    print("Submission commented out for testing.")

if __name__ == "__main__":
    pipeline()




