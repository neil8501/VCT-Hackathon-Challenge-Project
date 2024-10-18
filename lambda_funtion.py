import boto3
import gzip
from io import BytesIO

s3_client = boto3.client('s3')

DESTINATION_BUCKET = 'unzipped-vct-data'  # Replace with your source bucket name
SOURCE_BUCKET = 'vct-game-data-np1'  # Replace with your destination bucket name
FOLDER = 'vct-international/games/2024/val:00676ae0-0a4d-4577-be1e-ab4d3a9889aa.json.gz'  # Replace with your folder path

def lambda_handler(event, context):
    # List all .gz files in the folder
    response = s3_client.list_objects_v2(Bucket=SOURCE_BUCKET, Prefix=FOLDER)

    # Check if the response contains any files
    if 'Contents' in response:
        for obj in response['Contents']:
            file_key = obj['Key']

            # Process only .gz files
            if file_key.endswith('.gz'):
                try:
                    # Download the .gz file from S3
                    gz_file = s3_client.get_object(Bucket=SOURCE_BUCKET, Key=file_key)
                    gz_data = gz_file['Body'].read()

                    # Unzip the .gz file in chunks
                    unzipped_data = b''
                    chunk_size = 1024 * 1024  # 1 MB chunk size

                    with gzip.GzipFile(fileobj=BytesIO(gz_data), mode='rb') as gzipped_file:
                        while True:
                            chunk = gzipped_file.read(chunk_size)
                            if not chunk:
                                break
                            unzipped_data += chunk

                    # Create the new file key for the unzipped content
                    unzipped_file_key = file_key.replace('.gz', '')

                    # Upload the unzipped file to the destination S3 bucket
                    s3_client.put_object(Bucket=DESTINATION_BUCKET, Key=unzipped_file_key, Body=unzipped_data)

                    print(f"Unzipped and uploaded: {file_key} to {unzipped_file_key}")

                except Exception as e:
                    print(f"Error processing {file_key}: {str(e)}")
                    continue  # Skip to the next file

    return {
        'statusCode': 200,
        'body': 'Unzipping complete'
    }
