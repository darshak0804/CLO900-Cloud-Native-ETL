import io
from google.oauth2 import service_account
from googleapiclient.discovery import build
import boto3
import mimetypes

# Google Drive credentials
credentials = service_account.Credentials.from_service_account_file(
    'F:\clo900 CP ETL\credentials.json',
    scopes=['https://www.googleapis.com/auth/drive']
)

# AWS S3 credentials
s3 = boto3.client('s3')

def download_file_from_google_drive(service, file_id, file_name):
    try:
        # Check file type and handle download accordingly
        file_metadata = service.files().get(fileId=file_id).execute()
        mime_type = file_metadata.get('mimeType')

        if mime_type == 'application/vnd.google-apps.document':
            # Export Google Docs as PDF
            request = service.files().export_media(fileId=file_id, mimeType='application/pdf')
        elif mime_type == 'application/vnd.google-apps.spreadsheet':
            # Export Google Sheets as Excel
            request = service.files().export_media(fileId=file_id, mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        elif mime_type == 'application/vnd.google-apps.presentation':
            # Export Google Slides as PDF
            request = service.files().export_media(fileId=file_id, mimeType='application/pdf')
        else:
            # Download other binary files directly
            request = service.files().get_media(fileId=file_id)

        # Download the file content
        fh = io.BytesIO()
        downloader = request.execute()
        fh.write(downloader)
        fh.seek(0)  # Reset file position

        return fh, mime_type

    except Exception as e:
        print(f"An error occurred while downloading '{file_name}': {e}")
        return None, None

def upload_file_to_s3(file_content, file_name, bucket_name, folder_name, mime_type=None):
    try:
        # Determine content type based on file extension if not provided
        if not mime_type:
            mime_type, _ = mimetypes.guess_type(file_name)

        # Upload the file to AWS S3 with specified ContentType and folder structure
        s3_key = f"{folder_name}/{file_name}"
        s3.upload_fileobj(file_content, bucket_name, s3_key, ExtraArgs={'ContentType': mime_type})
        print(f"File '{file_name}' uploaded to S3 bucket '{bucket_name}' under folder '{folder_name}' with ContentType '{mime_type}'.")

    except Exception as e:
        print(f"An error occurred while uploading '{file_name}' to S3: {e}")

def download_and_upload_folder(service, folder_id, folder_name, bucket_name, s3_folder_name):
    try:
        # List all files in the folder
        results = service.files().list(q=f"'{folder_id}' in parents",
                                       fields="files(id, name, mimeType)").execute()
        items = results.get('files', [])

        if not items:
            print(f"No files found in Google Drive folder '{folder_name}'.")
        else:
            # Iterate through each file and download/upload to S3
            for item in items:
                file_id = item['id']
                file_name = item['name']
                file_mime_type = item.get('mimeType', '')

                # Skip Google Drive system files
                if file_mime_type == 'application/vnd.google-apps.folder':
                    print(f"Skipping folder '{file_name}'.")
                    continue

                # Download the file content and MIME type
                file_content, mime_type = download_file_from_google_drive(service, file_id, file_name)

                if file_content:
                    # Upload the file to AWS S3 with specified ContentType and folder structure
                    upload_file_to_s3(file_content, file_name, bucket_name, s3_folder_name, mime_type)

    except Exception as e:
        print(f"An error occurred while processing folder '{folder_name}': {e}")

# Example usage:
if __name__ == "__main__":
    folder_id = '1v0Y8xiCaKmAMQnVhb0xfgnipxVUrvw0F'
    folder_name = 'googledrivedatafiles'
    bucket_name = 'etl-clo900-s3bucket'
    s3_folder_name = 'GoogleDrivedata'
    
    # Build the Google Drive service
    service = build('drive', 'v3', credentials=credentials)
    
    download_and_upload_folder(service, folder_id, folder_name, bucket_name, s3_folder_name)
