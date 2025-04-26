import boto3
import time

ATHENA_DB = 'access_logs_db'
ATHENA_OUTPUT = 's3://zero-trust-access-logs/athena'  # Store results in a subfolder
SNS_TOPIC_ARN = 'arn:aws:sns:us-east-1:123456789012:ZeroTrustAlerts'  # Replace with your SNS topic ARN

def lambda_handler(event, context):
	client = boto3.client('athena')
	sns_client = boto3.client('sns')

	queries = {
    	"Top Suspicious IPs (Login Failed Attempts)": """
        	SELECT col3 AS ip_address, COUNT(*) AS attempts
        	FROM access_logs
        	WHERE col4 = 'login_failed'
        	GROUP BY col3
        	ORDER BY attempts DESC
        	LIMIT 5
    	"""
	}

	for label, query in queries.items():
    	print(f"Running query for {label}...")

    	# Start Athena query
    	response = client.start_query_execution(
        	QueryString=query,
        	QueryExecutionContext={'Database': ATHENA_DB},
        	ResultConfiguration={'OutputLocation': ATHENA_OUTPUT}
    	)

    	execution_id = response['QueryExecutionId']

    	# Wait for query to finish
    	while True:
        	result = client.get_query_execution(QueryExecutionId=execution_id)
        	state = result['QueryExecution']['Status']['State']
        	if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            	break
        	time.sleep(2)

    	# Send SNS notification based on the query result status
    	if state == 'SUCCEEDED':
        	message = f"The query for {label} has completed successfully. You can view the results here: https://console.aws.amazon.com/athena/home?#query/history/{execution_id}"
        	print(f"{label} query succeeded.")
    	else:
        	message = f"The query for {label} failed. Check the Athena console for more details. Execution ID: {execution_id}"
        	print(f"{label} query failed.")

    	# Publish the message to SNS
    	sns_response = sns_client.publish(
        	TopicArn=SNS_TOPIC_ARN,
        	Subject=f"Zero Trust: Athena Query Result - {label}",
        	Message=message
    	)
    	print(f"Published SNS notification: {sns_response}")

	return {"status": "completed"}

