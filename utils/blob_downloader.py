from google.oauth2 import service_account
from google.cloud import storage


def get_bucket(service_cred_path, bucket_name):
    credentials = service_account.Credentials.from_service_account_file(service_cred_path)

    storage_client = storage.Client(credentials=credentials)
    bucket = storage_client.get_bucket(bucket_name)

    return bucket


def get_text(bucket, text_file):
    blob = bucket.blob(text_file)
    text = blob.download_as_text().replace('\r', '')

    return text


def download_text_file(bucket, text_file, text_download_path):
    blob = bucket.blob(text_file)
    blob.download_to_filename(text_download_path)
