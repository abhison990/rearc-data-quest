import json
import boto3
import requests
from bs4 import BeautifulSoup

# AWS Configuration
S3_BUCKET_NAME = "abh-de-rearc"
DATA_SOURCE = "https://download.bls.gov/pub/time.series/pr/"
HEADERS = {"User-Agent": "Abhijeet (abhison990@gmail.com)"}  # Prevents 403 errors

API_URL = "https://datausa.io/api/data?drilldowns=Nation&measures=Population"
S3_FILE_KEY = "api-data/us_population.json"

# Initialize AWS Clients
s3_client = boto3.client("s3")
s3_resource = boto3.resource("s3")
bucket = s3_resource.Bucket(S3_BUCKET_NAME)


def get_s3_files():
    """Retrieve list of existing files in S3."""
    return {obj.key.replace("bls-data/", "") for obj in bucket.objects.all()}


def get_website_files():
    """Scrape BLS website for available files."""
    response = requests.get(DATA_SOURCE, headers=HEADERS)
    soup = BeautifulSoup(response.text, "html.parser")
    return {link.get_text().strip() for link in soup.find_all("a") if "." in link.get_text().strip()}


def upload_file_to_s3(file_name):
    """Download and upload a file to S3."""
    file_url = DATA_SOURCE + file_name
    file_content = requests.get(file_url, headers=HEADERS).content
    s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=f"bls-data/{file_name}", Body=file_content)
    print(f"Uploaded {file_name} to S3")


def sync_s3_with_website():
    """Sync S3 bucket with BLS website data."""
    s3_files = get_s3_files()
    website_files = get_website_files()

    # Upload new or modified files
    for file in website_files - s3_files:
        upload_file_to_s3(file)

    # Delete obsolete files
    for file in s3_files - website_files:
        print(f"Deleting obsolete file: {file}")
        bucket.Object(f"bls-data/{file}").delete()

    print("S3 bucket is fully synchronized with BLS website.")


def fetch_and_upload_api_data():
    """Fetch API data and upload JSON to S3."""
    response = requests.get(API_URL)
    if response.status_code == 200:
        print("Fetched API data successfully.")
        json_data = response.json()
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=S3_FILE_KEY,
            Body=json.dumps(json_data, indent=4),
            ContentType="application/json"
        )
        print(f"Uploaded API data to S3: s3://{S3_BUCKET_NAME}/{S3_FILE_KEY}")
    else:
        print(f"Failed to fetch API data. HTTP {response.status_code}")


def lambda_handler(event, context):
    """AWS Lambda Entry Point."""
    sync_s3_with_website()
    fetch_and_upload_api_data()
    return {"statusCode": 200, "body": json.dumps("Data sync completed successfully")}
