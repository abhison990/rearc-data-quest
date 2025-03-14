Rearc Data Quest Documentation
This document describes the approach, implementation, and automation of the Rearc Data Quest project using AWS CDK, Python, and PySpark.


Part 1: AWS S3 & Sourcing Datasets
Objective: Republish the open dataset in an Amazon S3 bucket and share the link.
Steps Taken
Created a public S3 bucket: `abh-de-rearc`
Uploaded datasets from the open data source to S3
Configured S3 permissions to allow read-only access
Wrote a Python script (`fetch_and_sync_s3.py`) to sync S3 data with the source website



Part 2: API Data Fetching
Objective: Fetch data from an open API and store it as a JSON file in S3.
Fetched data from the API: `https://datausa.io/api/data?drilldowns=Nation&measures=Population`
Stored API response as JSON in S3
Automated data fetching daily using AWS Lambda & EventBridge




Part 3: Data Analytics
Objective: Perform data analysis using PySpark.
Steps Taken
Loaded CSV data from S3 (`pr.data.0.Current`)
Loaded JSON data from S3 (`us_population.json`)
Performed statistical analysis using PySpark




Part 4: Infrastructure as Code (AWS CDK)
Objective: Automate data pipeline deployment using AWS CDK.
Steps Taken
Created an AWS CDK project and defined the following infrastructure:
- S3 Bucket for data storage
- SQS Queue triggered on new S3 uploads
- EventBridge scheduled event for daily Lambda execution
- AWS Lambda functions for data fetching and reporting
AWS CDK Implementation Details
1. Defined an S3 bucket (`abh-de-rearc`) to store datasets and API responses.
2. Configured S3 Event Notifications to send messages to an SQS queue when a new JSON file is uploaded.
3. Created an SQS queue (`DataProcessingQueue`) that triggers a Lambda function upon receiving messages.
4. Created two Lambda functions:
   - `data_fetch_lambda.py`: Fetches data from the source and API, then uploads it to S3.
   - `report_lambda.py`: Reads data from S3, processes it using PySpark, and logs the results.
5. Scheduled the data fetch Lambda to run daily using AWS EventBridge.
Deployment Instructions
1. Install AWS CDK and required dependencies:
   ```sh
   npm install -g aws-cdk
   pip install -r requirements.txt
   ```
2. Bootstrap the AWS environment (only required for the first-time setup):
   ```sh
   cdk bootstrap
   ```
3. Synthesize the CloudFormation template:
   ```sh
   cdk synth
   ```
4. Deploy the AWS CDK stack:
   ```sh
   cdk deploy
   ```
Conclusion
This project successfully automated the ingestion, storage, and processing of open datasets using AWS services. AWS CDK provided a structured and reusable way to manage infrastructure as code, ensuring scalability and maintainability.
