import boto3
import requests
from bs4 import BeautifulSoup

# AWS S3 Configuration
S3_BUCKET_NAME = "abh-de-rearc"  # Personal AWS S3 bucket
DATA_SOURCE = "https://download.bls.gov/pub/time.series/pr/"  # Base URL for BLS data
HEADERS = {"User-Agent": "Abhijeet (abhison990@gmail.com)"}  # Required to avoid 403 errors

# Initialize S3 session using personal AWS profile
session = boto3.Session(profile_name="abhi_data_engg")
s3 = session.resource("s3")
bucket = s3.Bucket(S3_BUCKET_NAME)


def get_s3_files():
    """
    Retrieve a list of all files currently in the S3 bucket.

    Returns:
        set: A set of file names (without S3 folder path).
    """
    return {obj.key.replace("bls-data/", "") for obj in bucket.objects.all()}


def get_website_files():
    """
    Scrape the BLS website to get a list of available files.

    Returns:
        set: A set of valid file names from the BLS website.
    """
    print("Fetching file list from BLS website...")
    response = requests.get(DATA_SOURCE, headers=HEADERS)
    soup = BeautifulSoup(response.text, "html.parser")

    # Extract valid filenames from the directory listing
    files = set()
    for link in soup.find_all("a"):
        file_name = link.get_text().strip()
        if not file_name.startswith("[") and "." in file_name:  # Exclude non-file entries
            files.add(file_name)

    return files


def sync_files():
    """
    Synchronizes the S3 bucket with the BLS dataset directory.

    Steps:
    - Fetch all available files from the BLS website.
    - Upload files that are new.
    - Update files that have changed.
    - Delete files from S3 that no longer exist on the website.
    """
    s3_files = get_s3_files()
    website_files = get_website_files()

    # Identify files that need to be deleted from S3
    files_to_delete = s3_files - website_files

    for file_name in website_files:
        file_url = DATA_SOURCE + file_name
        file_dl = requests.get(file_url, headers=HEADERS)

        if file_name not in s3_files:
            # Upload new file to S3
            print(f"Uploading new file: {file_name}")
            bucket.put_object(Key=f"bls-data/{file_name}", Body=file_dl.content)
        else:
            # Check if the file content has changed before updating
            s3_object = bucket.Object(f"bls-data/{file_name}")
            s3_file_content = s3_object.get()["Body"].read()

            if file_dl.content != s3_file_content:
                print(f"Updating modified file: {file_name}")
                bucket.put_object(Key=f"bls-data/{file_name}", Body=file_dl.content)

    # Delete files that are no longer available on the BLS website
    for file_name in files_to_delete:
        print(f"Deleting obsolete file: {file_name}")
        bucket.Object(f"bls-data/{file_name}").delete()

    print("S3 bucket is now fully synchronized with BLS website.")


if __name__ == "__main__":
    sync_files()
