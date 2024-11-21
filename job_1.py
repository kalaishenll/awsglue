import boto3
import os
import sys
import zipfile
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession

job_name = "spark_glue"
zip_s3_path = "s3://gluee-bucket/spark_demo1.zip"

# Initialize the Glue and Spark contexts
glueContext = GlueContext(SparkSession.builder.getOrCreate())
spark = glueContext.spark_session
job = Job(glueContext)
job.init(job_name, {})
print("STARTING>>>>")

# Download the .zip file from S3
s3_client = boto3.client('s3')
bucket, key = zip_s3_path.replace("s3://", "").split("/", 1)
local_zip_path = "/tmp/spark_demo1.zip"
s3_client.download_file(bucket, key, local_zip_path)
print(f"Downloaded zip file to {local_zip_path}")

# Extract the .zip file to /tmp directory
with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
    zip_ref.extractall("/tmp/spark_demo1")
    print("Extracted files:")
    print(os.listdir("/tmp/spark_demo1/spark_demo1"))  # Print extracted files

# Add the extracted modules to the Python path
sys.path.append("/tmp/spark_demo1/spark_demo1")

# Define the path to the main.py file within the extracted directory
main_script_path = "/tmp/spark_demo1/spark_demo1/main.py"

# List of argument sets for different iterations
main_script_args_list = [
    [
        'module', "customer_id",
        'property_id', '30', 
        '30', 'hh',
        'refresh_token', 'primary_goal', 'secondary_goal'
    ],
    [
        'module111', "customer_id",
        'property_id', '30', 
        '30', 'hh111',
        'refresh_token', 'primary_goal', 'secondary_goal'
    ],
]

# Function to run the Spark job
def run_spark(main_script_args):
    if os.path.exists(main_script_path):
        print(f"Executing main.py with arguments: {main_script_args}")
        sys.argv = ['main.py'] + main_script_args
        try:
            from main import main
            main(spark)  # Execute the main function with the Spark session
            print("main.py executed successfully.")
        except Exception as e:
            print(f"Error executing main.py: {e}")
    else:
        print(f"Error: {main_script_path} not found.")

# Iterate over the argument sets and execute the script
for i, main_script_args in enumerate(main_script_args_list, start=1):
    print(f"Iteration {i} started.")
    run_spark(main_script_args)
    print(f"Iteration {i} completed.")

# Commit the Glue job
job.commit()
