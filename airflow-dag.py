# airflow DAG goes here

import boto3
import requests
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.timezone import utcnow

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
sqs = boto3.client('sqs', region_name='us-east-1')

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
    logger = logging.getLogger(__name__)
    logger.info("Starting SQS queue population task")
    
    url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/tpg6hu"
    logger.debug(f"Making POST request to: {url}")
    
    try:
        response = requests.post(url)
        response.raise_for_status()
        payload = response.json()
        sqs_url = payload['sqs_url']
        
        print(f"SQS URL: {sqs_url}")
        logger.info(f"Successfully populated queue. SQS URL: {sqs_url}")
        
        # Store the SQS URL in XCom for other tasks
        context['task_instance'].xcom_push(key='sqs_url', value=sqs_url)
        logger.debug("SQS URL stored in XCom for downstream tasks")
        
        return sqs_url
    except Exception as e:
        logger.error(f"Failed to populate SQS queue: {str(e)}")
        raise

def get_queue_attributes(**context):
    """Get queue attributes to monitor message availability."""
    logger = logging.getLogger(__name__)
    sqs_url = context['task_instance'].xcom_pull(task_ids='populate_queue', key='sqs_url')
    
    logger.debug("Retrieving queue attributes")
    
    try:
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
        logger.info(f"Queue Status - Available: {available}, Delayed: {delayed}, In-flight: {in_flight}, Total: {total}")
        
        return {
            'available': available,
            'delayed': delayed,
            'in_flight': in_flight,
            'total': total
        }
    except Exception as e:
        logger.error(f"Error getting queue attributes: {str(e)}")
        raise

def collect_all_messages(**context):
    """Collect all messages from the SQS queue."""
    logger = logging.getLogger(__name__)
    sqs_url = context['task_instance'].xcom_pull(task_ids='populate_queue', key='sqs_url')
    messages = []
    collected_count = 0
    
    print(f"Starting to collect messages from {sqs_url}")
    logger.info(f"Starting message collection from {sqs_url}")
    logger.info("Target: 21 messages")
    
    while len(messages) < 21:
        # Monitor queue status
        attrs = get_queue_attributes(**context)
        
        # Try to get messages
        try:
            logger.debug("Attempting to receive batch of messages")
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
                logger.debug("No messages available in queue, waiting 5 seconds")
                time.sleep(5)
                continue
            
            logger.info(f"Received batch of {len(response['Messages'])} messages")
            
            for msg in response['Messages']:
                receipt_handle = msg['ReceiptHandle']
                
                # Parse message
                attributes = msg['MessageAttributes']
                order_no = int(attributes['order_no']['StringValue'])
                word = attributes['word']['StringValue']
                
                print(f"Received - Order: {order_no}, Word: {word}")
                logger.info(f"Processing message - Order: {order_no}, Word: '{word}'")
                
                messages.append((order_no, word))
                
                # Delete message
                logger.debug(f"Deleting processed message with receipt handle: {receipt_handle[:20]}...")
                sqs.delete_message(
                    QueueUrl=sqs_url,
                    ReceiptHandle=receipt_handle
                )
                collected_count += 1
                logger.debug(f"Message deleted successfully. Total collected: {collected_count}")
                
        except Exception as e:
            print(f"Error processing messages: {e}")
            logger.error(f"Error processing messages: {e}")
            logger.info("Waiting 5 seconds before retrying")
            time.sleep(5)
            continue
        
        # Check if we've collected all 21 messages
        if len(messages) >= 21:
            print(f"Collected all {len(messages)} messages!")
            logger.info(f"Successfully collected all {len(messages)} messages!")
            break
    
    # Store messages in XCom for next task
    context['task_instance'].xcom_push(key='messages', value=messages)
    logger.debug("Messages stored in XCom for downstream tasks")
    
    return messages

def reassemble_phrase(**context):
    """Reassemble the phrase by sorting messages by order_no."""
    logger = logging.getLogger(__name__)
    messages = context['task_instance'].xcom_pull(task_ids='collect_messages', key='messages')
    
    logger.info(f"Starting phrase reassembly from {len(messages)} collected messages")
    
    # Sort by order_no
    sorted_messages = sorted(messages, key=lambda x: x[0])
    logger.debug("Messages sorted by order number")
    
    # Extract words in order
    words = [word for _, word in sorted_messages]
    phrase = " ".join(words)
    
    print(f"\nReassembled phrase: {phrase}\n")
    logger.info(f"Successfully reassembled phrase: '{phrase}'")
    
    # Store phrase in XCom for next task
    context['task_instance'].xcom_push(key='phrase', value=phrase)
    logger.debug("Phrase stored in XCom for submission task")
    
    # Print the phrase to terminal
    print(f"\n{'='*60}")
    print(f"FINAL REASSEMBLED PHRASE:")
    print(f"'{phrase}'")
    print(f"{'='*60}\n")
    
    return phrase

def submit_solution(**context):
    """Submit the solution to the submission queue."""
    logger = logging.getLogger(__name__)
    phrase = context['task_instance'].xcom_pull(task_ids='reassemble_phrase', key='phrase')
    
    logger.info("Starting solution submission phase")
    logger.debug(f"Retrieved phrase from XCom: '{phrase}'")
    

    submission_url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
    logger.debug(f"Submitting solution to: {submission_url}")
    
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
        logger.info("Solution submitted successfully!")
        logger.debug(f"Submission response: MessageId={response.get('MessageId', 'N/A')}")
        return response
    except Exception as e:
        print(f"Error submitting solution: {e}")
        logger.error(f"Failed to submit solution: {str(e)}")
        raise e

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
populate_queue_task >> get_queue_attrs_task >> collect_messages_task >> reassemble_phrase_task >> submit_solution_task
