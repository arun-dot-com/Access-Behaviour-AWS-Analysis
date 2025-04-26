import csv
import random
from datetime import datetime, timedelta
import boto3
from botocore.exceptions import ClientError

# S3 Setup
bucket_name = 'zero-trust-access-logs' 
object_key = 'logs/access_logs.csv'

# Initialize S3 client
s3 = boto3.client('s3', region_name='us-east-1')  # Ensure region is set here

def create_bucket(bucket_name):
	try:
    	# Try creating the bucket (in the default region or provided region)
    	s3.create_bucket(Bucket=bucket_name)
    	print(f"Bucket {bucket_name} created successfully.")
	except ClientError as e:
    	print(f"Error creating bucket: {e}")
    	raise

def generate_logs(file_name="access_logs.csv", entries=100):
	users = ["alice", "bob", "charlie", "dave"]
	trusted_ips = ["192.168.1.1", "10.0.0.5"]
	untrusted_ips = ["185.45.12.7", "103.10.15.8", "8.8.8.8"]
	resources = ["db", "admin", "auth", "storage"]
	statuses = ["allowed", "denied"]

	with open(file_name, "w", newline="") as f:
    	writer = csv.writer(f)
    	writer.writerow(["username", "timestamp", "ip", "resource", "status"])
    	for _ in range(entries):
        	user = random.choice(users)
        	ip = random.choice(trusted_ips + untrusted_ips)
        	resource = random.choice(resources)
        	status = random.choices(statuses, weights=[0.9, 0.1])[0]
        	minutes_ago = random.randint(0, 2880)
        	timestamp = (datetime.now() - timedelta(minutes=minutes_ago)).strftime("%Y-%m-%d %H:%M:%S")
        	writer.writerow([user, timestamp, ip, resource, status])

def upload_to_s3(file_name, bucket, object_key):
	s3.upload_file(file_name, bucket, object_key)
	print(f"{file_name} uploaded to s3://{bucket}/{object_key}")

# Create the bucket (this will create the bucket unconditionally)
create_bucket(bucket_name)

# Generate logs and upload to S3
generate_logs()
upload_to_s3("access_logs.csv", bucket_name, object_key)

