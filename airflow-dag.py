import boto3
import requests
import time
import logging
from datetime import timedelta
from airflow.decorators import dag, task
from airflow.utils.timezone import utcnow

# Default arguments
default_args = {
    'owner': 'tpg6hu',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize SQS client
sqs = boto3.client('sqs', region_name='us-east-1')


@dag(
    dag_id='ds3022_DP2_airflow_dag',
    default_args=default_args,
    description='DS3022 DP2 Airflow DAG - Reassemble message fragments from SQS',
    start_date=utcnow() - timedelta(days=1),
    schedule=None,  # Run manually
    catchup=False,
    tags=['ds3022', 'puzzle', 'sqs'],
)
def ds3022_dp2_dag():

    @task
    def populate_sqs_queue():
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

            return sqs_url
        except Exception as e:
            logger.error(f"Failed to populate SQS queue: {str(e)}")
            raise

    @task
    def get_queue_attributes(sqs_url: str) -> dict:
        """Get queue attributes to monitor message availability."""
        logger = logging.getLogger(__name__)
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

    @task
    def collect_all_messages(sqs_url: str) -> list:
        """Collect all messages from the SQS queue."""
        logger = logging.getLogger(__name__)
        messages = []
        collected_count = 0

        print(f"Starting to collect messages from {sqs_url}")
        logger.info(f"Starting message collection from {sqs_url}")
        logger.info("Target: 21 messages")

        while len(messages) < 21:
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

                logger.info(f"Received batch of {len(response['Messages'])} messages")

                for msg in response['Messages']:
                    receipt_handle = msg['ReceiptHandle']
                    attributes = msg['MessageAttributes']
                    order_no = int(attributes['order_no']['StringValue'])
                    word = attributes['word']['StringValue']

                    print(f"Received - Order: {order_no}, Word: {word}")
                    messages.append((order_no, word))

                    sqs.delete_message(
                        QueueUrl=sqs_url,
                        ReceiptHandle=receipt_handle
                    )
                    collected_count += 1

            except Exception as e:
                print(f"Error processing messages: {e}")
                logger.error(f"Error processing messages: {e}")
                time.sleep(5)
                continue

        print(f"Collected all {len(messages)} messages!")
        logger.info(f"Successfully collected all {len(messages)} messages!")
        return messages

    @task
    def reassemble_phrase(messages: list) -> str:
        """Reassemble the phrase by sorting messages by order_no."""
        logger = logging.getLogger(__name__)

        sorted_messages = sorted(messages, key=lambda x: x[0])
        words = [word for _, word in sorted_messages]
        phrase = " ".join(words)

        print(f"\nReassembled phrase: {phrase}\n")
        logger.info(f"Successfully reassembled phrase: '{phrase}'")

        print(f"\n{'='*60}")
        print(f"FINAL REASSEMBLED PHRASE:")
        print(f"'{phrase}'")
        print(f"{'='*60}\n")

        return phrase

    @task
    def submit_solution(phrase: str):
        """Submit the solution to the submission queue."""
        logger = logging.getLogger(__name__)
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
            logger.info("Solution submitted successfully!")
            return response
        except Exception as e:
            print(f"Error submitting solution: {e}")
            logger.error(f"Failed to submit solution: {str(e)}")
            raise e

    # DAG flow definition
    sqs_url = populate_sqs_queue()
    get_queue_attributes(sqs_url)
    messages = collect_all_messages(sqs_url)
    phrase = reassemble_phrase(messages)
    submit_solution(phrase)


# Expose the DAG object
dag = ds3022_dp2_dag()
