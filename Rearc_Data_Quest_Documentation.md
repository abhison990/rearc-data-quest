**Introduction**
This document outlines the steps taken to complete the Rearc Data Quest. It details the infrastructure setup, data extraction, transformation, and analysis process using AWS and PySpark.

****Part 1:** AWS S3 & Sourcing Datasets**
Task
Republish an open dataset in an Amazon S3 bucket.
Provide a public link for data access.
Implementation Steps
Created a publicly accessible S3 bucket: s3://abh-de-rearc
Uploaded files from the open dataset to the S3 bucket.
Modified bucket permissions to allow public read-only access:
Disabled "Block Public Access."
Applied the following bucket policy:

{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "PublicReadGetObject",
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": "arn:aws:s3:::abh-de-rearc/*"
        }
    ]
}
Automated dataset synchronization using a Python script:

Script syncs data from the source to the S3 bucket.
Checks for new, updated, or deleted files and updates S3 accordingly.
S3 Bucket Details:

S3 URI: s3://abh-de-rearc/
Public Access: Read-only
Object URL Example: https://abh-de-rearc.s3.ap-south-1.amazonaws.com/bls-data/pr.data.0.Current

****Part 2:** API Data Extraction**
Task
Fetch data from an API and store it in S3.
Implementation Steps
Created a Python script (fetch_and_upload_api_data.py) to:

Fetch data from the API.
Save the response as a JSON file.
Upload it to S3.
API Data Storage:

S3 Path: s3://abh-de-rearc/api-data/us_population.json

**Part 3: Data Analytics**
Task
Load data from Part 1 and Part 2 into Spark DataFrames.
Perform statistical analysis:
Compute mean and standard deviation of the population (2013–2018).
Find the best year for each series_id based on total value.
Generate a final report for series_id = PRS30006032 and period = Q01.
Implementation Steps
Loaded CSV (Part 1) and JSON (Part 2) into Spark DataFrames.

Data Cleaning:

Trimmed white spaces in column values.
Converted year and population to integer data types.
Analysis Performed:

Mean & Standard Deviation of US Population (2013–2018):
Computed using mean() and stddev().
Best Year for Each Series ID:
Grouped data by series_id and year, computed the sum of values.
Selected the year with the highest total value per series_id.
Final Report for PRS30006032 & Q01:
Filtered data for PRS30006032 and Q01.
Merged with population dataset.

Results saved in S3: s3://abh-de-rearc/analytics-results/

Notebook File: part_3/data_analysis.ipynb
Script File: part_3/part3_data_analysis.py

**Part 4: Infrastructure as Code (AWS CDK)**
Objective
Automate data pipeline deployment using AWS CDK.

Steps Taken
Created an AWS CDK project and defined the following infrastructure:

S3 Bucket (abh-de-rearc) for dataset and API response storage.
SQS Queue (DataProcessingQueue) triggered on new S3 uploads.
EventBridge Scheduled Event to invoke Lambda functions daily.
AWS Lambda Functions for data ingestion and processing.
AWS CDK Implementation Details:

Defined an S3 bucket (abh-de-rearc) to store datasets and API responses.
Configured S3 Event Notifications:
When a JSON file is uploaded, an event is sent to SQS Queue.
    Created an SQS Queue (DataProcessingQueue):
    Triggers a Lambda function (report_lambda.py) to process incoming data.
    Created Two Lambda Functions:
        data_fetch_lambda.py
            Fetches data from the source and API.
            Saves the data to S3.
        report_lambda.py
            Reads data from S3.
            Processes it using PySpark.
            Logs and outputs results.
            Scheduled the Data Fetch Lambda:
            Configured AWS EventBridge to invoke data_fetch_lambda.py daily.
Deployment Instructions
Follow these steps to deploy the AWS CDK stack:

Step 1: Install AWS CDK & Dependencies
Ensure that AWS CDK and dependencies are installed on your system.

# Install AWS CDK globally
npm install -g aws-cdk

# Install required Python dependencies
pip install -r requirements.txt
Step 2: Bootstrap the AWS Environment
(Only required for the first-time setup.)


cdk bootstrap

This sets up resources required by CDK in your AWS account.

Step 3: Synthesize the CloudFormation Template
Run the following command to generate the CloudFormation template from your CDK code.

cdk synth
Step 4: Deploy the AWS CDK Stack
Run the following command to deploy the infrastructure:

cdk deploy
Step 5: Verify the Deployment
Once the stack is deployed, verify the created resources in the AWS Console:

S3 Bucket: Check if the abh-de-rearc bucket exists.
SQS Queue: Confirm that DataProcessingQueue is receiving messages.
EventBridge Rule: Ensure the scheduled event is configured correctly.
Lambda Functions: Test data_fetch_lambda.py and report_lambda.py.

**Conclusion**
This project successfully automates the ingestion, storage, and processing of open datasets using AWS services. AWS CDK provides a structured, scalable, and maintainable way to manage infrastructure as code.
