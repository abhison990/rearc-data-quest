import json
import boto3
import requests
from datetime import datetime

# AWS S3 Configuration
S3_BUCKET_NAME = "abh-de-rearc"  # Personal S3 bucket
S3_FILE_KEY = "api-data/us_population.json"  # Path in S3

# API Configuration
API_URL = "https://datausa.io/api/data?drilldowns=Nation&measures=Population"

# Initialize S3 session using personal AWS profile
session = boto3.Session(profile_name="abhi_data_engg")
s3 = session.client("s3")


def fetch_api_data():
    """
    Fetches population data from the Data USA API.

    Returns:
        dict: The JSON response from the API, or None if the request fails.
    """
    print("Fetching U.S. population data from API...")
    response = requests.get(API_URL)

    if response.status_code == 200:
        print("API data fetched successfully.")
        return response.json()  # Convert response to JSON
    else:
        print(f"Failed to fetch API data. HTTP Status: {response.status_code}")
        return None


def upload_json_to_s3(data):
    """
    Uploads the JSON response directly to S3 without saving locally.

    Args:
        data (dict): The JSON data from the API.
    """
    json_string = json.dumps(data, indent=4)  # Convert to JSON string

    print(f"Uploading API data directly to S3: s3://{S3_BUCKET_NAME}/{S3_FILE_KEY}")

    # Upload JSON data directly to S3
    s3.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=S3_FILE_KEY,
        Body=json_string,
        ContentType="application/json"
    )

    print("Upload successful.")


def main():
    """
    Main function to fetch API data and upload it directly to S3.
    """
    data = fetch_api_data()

    if data:
        upload_json_to_s3(data)


if __name__ == "__main__":
    main()
