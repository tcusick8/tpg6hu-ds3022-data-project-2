# DS3022 Data Project 2 - Puzzle Solver

This data project is a puzzle. Your goal is to put the pieces back together in the right order.

## Solution Overview

This repository contains a complete Prefect data pipeline that:

1. **Populates SQS Queue**: Calls the API to populate the SQS queue with 21 messages containing word fragments
2. **Monitors Queue**: Waits for all messages to become available (handling random delays of 30-900 seconds)
3. **Collects Messages**: Retrieves all messages from SQS, parsing their order numbers and word fragments
4. **Reassembles Phrase**: Sorts the fragments by order number to reconstruct the complete phrase
5. **Submits Solution**: Sends the completed phrase to the submission queue

## Files

- `prefect.py` - Main Prefect pipeline implementation
- `test_pipeline.py` - Test script to verify individual components
- `Pipfile` - Python dependencies
- `airflow.py` - Airflow DAG implementation (optional)

## Setup and Installation

1. **Install dependencies**:
   ```bash
   pipenv install
   ```

2. **Configure AWS credentials** (if not already configured):
   ```bash
   aws configure
   ```

3. **Install Prefect** (if not using pipenv):
   ```bash
   pip install prefect boto3 requests
   ```

## Running the Pipeline

### Option 1: Run with Prefect CLI
```bash
# Activate the virtual environment
pipenv shell

# Run the pipeline
python prefect.py
```

### Option 2: Run as Prefect Flow
```bash
# Start Prefect server (optional)
prefect server start

# Run the flow
prefect flow run ds3022-puzzle-solver
```

### Option 3: Test Individual Components
```bash
# Run the test script to verify components
python test_pipeline.py
```

## Pipeline Architecture

The pipeline consists of the following Prefect tasks:

1. **`populate_sqs_queue()`** - Calls the API to populate the SQS queue
2. **`get_queue_attributes()`** - Monitors queue status
3. **`wait_for_messages()`** - Waits for all messages to become available
4. **`receive_and_parse_message()`** - Receives and parses individual messages
5. **`delete_message()`** - Deletes processed messages
6. **`collect_all_messages()`** - Orchestrates message collection
7. **`reassemble_phrase()`** - Sorts and reassembles the phrase
8. **`submit_solution()`** - Submits the solution to the submission queue

## Key Features

- **Robust Error Handling**: Comprehensive error handling and logging throughout
- **Delay Management**: Handles random message delays (30-900 seconds)
- **Message Cleanup**: Ensures all messages are properly deleted after processing
- **Monitoring**: Real-time queue status monitoring
- **Logging**: Detailed logging for debugging and monitoring

## Expected Behavior

1. The pipeline calls the API to populate the queue with 21 messages
2. It waits for all messages to become available (handling delays)
3. It collects all 21 messages, parsing their order numbers and words
4. It sorts the fragments by order number and reassembles the phrase
5. It submits the complete phrase to the submission queue

## Troubleshooting

- **AWS Credentials**: Ensure your AWS credentials are properly configured
- **Network Issues**: The pipeline includes retry logic for network issues
- **Message Delays**: The pipeline waits up to 30 minutes for all messages to become available
- **Queue Monitoring**: Check the logs for detailed queue status information

## Original Assignment Details

## Task 1 - Populate your SQS Queue

To populate your SQS queue with messages, you must make a request of an API. Using your UVA computing ID, append it to this API endpoint:

```
https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/<UVA_ID>
```

Your pipeline must call this URL by way of an HTTP `POST` request. This is possible using the `requests` or `httpx` libraries in python:

```
import requests

url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/mst3k"

payload = requests.post(url).json
```

```
import httpx

url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/mst3k"

payload = httpx.post(url).json
```

In either case the `payload` object returns your SQS URL (as a reminder if you need it):

```
>>> payload
{'hello': 'mst3k', 'sqs_url': 'https://sqs.us-east-1.amazonaws.com/440848399208/mst3k'}
```

Your request to this API will send exactly **21** messages to your SQS queue. These have been sent with a variety of random `DelaySeconds` values ranging from 30 to 900 seconds.

**Keep these delays in mind as your pipeline proceeds to the next task.**

**NOTE** This step (sending a `POST` request to the API) should not be repeated if your pipeline needs to run more than once, i.e. on a cron timer, as it gathers all messages. The API request clears your queue of all previous messages and repopulates all 21 messages each time.

## Task 2 - Monitor Your Queue then Collect Messages

Next, devise a way for your pipeline to track how many messages are available for pickup using the `get_queue_attributes()` method. As it gets attributes about your queue, notice that there are three values that count messages:

- `ApproximateNumberOfMessages`
- `ApproximateNumberOfMessagesNotVisible`
- `ApproximateNumberOfMessagesDelayed`

Together, these make up the total count of messages in your queue.

You should determine a strategy for how/when to pick up messages, and code according to that strategy.

When receiving messages, each has a `MessageBody` that contains the same word (i.e. meaningless content). The meaningful content of each message is contained within the `['MessageAttributes']` segment of each message, and that you must parse into each attribute and get out its `['StringValue']`.

To get these values:

```
# order number:
['Messages'][0]['MessageAttributes']['order_no']['StringValue']

# word:
['Messages'][0]['MessageAttributes']['word']['StringValue']
```

Store the values you fetch for each message and keep them paired together. There are a variety of ways to store them.

Some notes:

- Recall that even though the `order_no` value will appear as if it is an integer, Python will see it as a string by default.
- You must also fetch the `ReceiptHandle` for each message and delete it after storing its data.
- Your pipeline must receive, parse, and delete ALL messages completely when run. Do not leave "dangling" messages that have been read but not deleted.
- Take care that when you request a message using the boto3 `receive_message` method that you handle errors gracefully without throwing an error and breaking your pipeline. If there are messages but they are invisible or delayed, and you try to poll for it, the returned response will not match the format of a successful message request.

## Task 3 - Reassemble the Messages and Submit Your Answer via SQS

Using the two fields collected from each message, now reassemble the words into a single phrase by ordering them using the `order_no` value.

Take this example:

```
order_no         word
---------------------------------------
3                brown
2                quick
4                fox.
1                The
```

would result in "This quick brown fox."

There are a variety of ways to sort lists or key-value pairs in both Python and SQL.

You may run and observe your pipeline multiple times if you care to, but in the end your code must fetch and reassemble all messages in order without human intervention. You may not receive messages or sort the fragments manually.

Your completed phrase should now be submitted as a message attribute for a new message that you send to a separate SQS queue, along with your computing ID and the platform (either "prefect" or "airflow") used for your pipeline.

Submit your answer to this queue:
```
https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit
```

To send a message with attributes, use this syntax:

```
def send_solution(uvaid, phrase, platform)
    try:
        response = sqs.send_message(
            QueueUrl=url,
            MessageBody=message,
            MessageAttributes={
                'uvaid': {
                    'DataType': 'String',
                    'StringValue': uvaid
                },
                'phrase': {
                    'DataType': 'String',
                    'StringValue': phrase
                },
                'platform': {
                    'DataType': 'String',
                    'StringValue': platform
                }
            }
        )
        print(f"Response: {response}")

    . . .
```
Be sure that your response returns a `200` HTTP response message, indicating that it has been received.

## Task 4 (Optional) - Rewrite this Pipeline as an Airflow DAG

For additional points, write a second data pipeline compatible with Apache Airflow. This step is not *in place of writing a Prefect flow* but in addition to it.

Be sure that your DAG runs successfully within Airflow when you executed in your AWS EC2 instance. It should produce identical results to your Prefect flow, but the final "platform" message attribute you submit should be set to "airflow".

## Notes / Submission

1. Be sure to fork this repository and commit/push your code back to it for grading.
2. Your Prefect flow should be saved to a file named `prefect.py`.
3. When running your Prefect flow you may use the remote host profile we set up in class `[profiles.uvasds]`, or  `[profiles.local]`. Either is fine.
4. If you attempt to write an Airflow DAG that should be saved to a file named `airflow.py`.
5. Secondary Prefect flows or Airflow DAGs are permissible. That is, one flow may also trigger another flow; one DAG may call another DAG, etc.
6. Your code should log using the built-in logging methods for either Prefect or Airflow. You do not need to use a separate logging package. Do not save or commit log files to your repo.
7. Do not save or commit any data or database files.

## AWS Issues

If you experience permissions errors with AWS and your SQS queue, you have two options:

1. Generate a new Access Key and Secret Access Key in your AWS account and then run `aws configure` in your local terminal. Fresh credentials may be needed to authenticate to your own account.
2. Alternatively, use the credentials I distribute with this assignment in Canvas. These keys have very limited access to only the SQS service, so they are only good for this project. Be sure to save your own credentials to restore after this assignment.

Of course if you experience some unusual error please be in touch with the instructor.

## Reference

- [Working with SQS - Practical Examples](https://github.com/nmagee/learn-sqs)
- [`boto3` - SQS Client Documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html)
- [Prefect Reference](https://docs.prefect.io/v3/get-started)
- [Airflow DAG Reference](https://s3.amazonaws.com/uvasds-systems/pdfs/Ultimate-Guide-to-Apache-Airflow-DAGs.pdf)