# airflow DAG goes here

import boto3
import requests
import time
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk.timezone import utcnow

# Default arguments
default_args = {
    'owner': 'tpg6hu',
    'depends_on_past': False,
    'start_date': utcnow() - timedelta(days=1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize SQS client
sqs = boto3.client('sqs')

# Create DAG
dag = DAG(
    'ds3022_DP2_airflow_dag',
    default_args=default_args,
    description='DS3022 DP2 Airflow DAG - Reassemble message fragments from SQS',
    schedule=None,  # Run manually
    catchup=False,
    tags=['ds3022', 'puzzle', 'sqs'],
)

def populate_sqs_queue(**context):
    """Call the API to populate the SQS queue with 21 messages."""
    url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/tpg6hu"
    response = requests.post(url)
    response.raise_for_status()
    payload = response.json()
    sqs_url = payload['sqs_url']
    
    print(f"SQS URL: {sqs_url}")
    
    # Store the SQS URL in XCom for other tasks
    context['task_instance'].xcom_push(key='sqs_url', value=sqs_url)
    return sqs_url

def get_queue_attributes(**context):
    """Get queue attributes to monitor message availability."""
    sqs_url = context['task_instance'].xcom_pull(task_ids='populate_queue', key='sqs_url')
    
    response = sqs.get_queue_attributes(
        QueueUrl=sqs_url,
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

def collect_all_messages(**context):
    """Collect all messages from the SQS queue."""
    sqs_url = context['task_instance'].xcom_pull(task_ids='populate_queue', key='sqs_url')
    messages = []
    collected_count = 0
    
    print(f"Starting to collect messages from {sqs_url}")
    
    while len(messages) < 21:
        # Monitor queue status
        attrs = get_queue_attributes(**context)
        
        # Try to get messages
        try:
            response = sqs.receive_message(
                QueueUrl=sqs_url,
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
                    QueueUrl=sqs_url,
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
    
    # Store messages in XCom for next task
    context['task_instance'].xcom_push(key='messages', value=messages)
    return messages

def reassemble_phrase(**context):
    """Reassemble the phrase by sorting messages by order_no."""
    messages = context['task_instance'].xcom_pull(task_ids='collect_messages', key='messages')
    
    # Sort by order_no
    sorted_messages = sorted(messages, key=lambda x: x[0])
    
    # Extract words in order
    words = [word for _, word in sorted_messages]
    phrase = " ".join(words)
    
    print(f"\nReassembled phrase: {phrase}\n")
    
    # Store phrase in XCom for next task
    context['task_instance'].xcom_push(key='phrase', value=phrase)
    
    # Print the phrase to terminal
    print(f"\n{'='*60}")
    print(f"FINAL REASSEMBLED PHRASE:")
    print(f"'{phrase}'")
    print(f"{'='*60}\n")
    
    return phrase

def submit_solution(**context):
    """Submit the solution to the submission queue."""
    phrase = context['task_instance'].xcom_pull(task_ids='reassemble_phrase', key='phrase')
    
    # COMMENTED OUT FOR TESTING - Uncomment to submit
    '''
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
                    'StringValue': 'airflow'
                }
            }
        )
        print(f"Solution submitted successfully!")
        print(f"Response: {response}")
        return response
    except Exception as e:
        print(f"Error submitting solution: {e}")
        raise e
    '''
    
    print("Submission commented out for testing.")
    return phrase

# Define tasks
populate_queue_task = PythonOperator(
    task_id='populate_queue',
    python_callable=populate_sqs_queue,
    dag=dag,
)

get_queue_attrs_task = PythonOperator(
    task_id='get_queue_attributes',
    python_callable=get_queue_attributes,
    dag=dag,
)

collect_messages_task = PythonOperator(
    task_id='collect_messages',
    python_callable=collect_all_messages,
    dag=dag,
)

reassemble_phrase_task = PythonOperator(
    task_id='reassemble_phrase',
    python_callable=reassemble_phrase,
    dag=dag,
)

submit_solution_task = PythonOperator(
    task_id='submit_solution',
    python_callable=submit_solution,
    dag=dag,
)

# Define task dependencies
populate_queue_task >> get_queue_attrs_task >> collect_messages_task >> reassemble_phrase_task # >> submit_solution_task
