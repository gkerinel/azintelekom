import boto3

import os

from datetime import datetime
 


source_bucket = 'asdatalocalizationcopy'

destination_bucket = 'azintelecomcopybucket'

log_bucket = 'asdatadailylog'  
 


scality_access_key = os.getenv('SCALITY_ACCESS_KEY')

scality_secret_key = os.getenv('SCALITY_SECRET_KEY')

scality_region = os.getenv('SCALITY_REGION')

scality_endpoint = os.getenv('SCALITY_ENDPOINT')
 


scality_s3 = boto3.client(

    's3',

    aws_access_key_id=scality_access_key,

    aws_secret_access_key=scality_secret_key,

    region_name=scality_region,

    endpoint_url=scality_endpoint

)
 
# def log_latest_activity(last_uploaded_file, last_deleted_file):

#     """Son yüklenen ve silinen dosyayı loglamak için fonksiyon."""

#     timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

#     log_content = f"Timestamp: {timestamp}\n\nLast Uploaded File in AWS:\n" + \
#     (last_uploaded_file if last_uploaded_file else "None") + \
# "\n\nLast Deleted File from AWS:\n" + (last_deleted_file if last_deleted_file else "None")

#     log_key = f"logs/{timestamp}.txt"

#     s3 = boto3.client('s3')

#     s3.put_object(Body=log_content, Bucket=log_bucket, Key=log_key)

def log_latest_activity(last_uploaded_file, last_deleted_file):

    """Son yüklenen ve silinen dosyayı azdailylogtest bucket'ındaki daily-log dosyasına loglamak için fonksiyon."""

    s3 = boto3.client('s3')

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    log_content = f"Timestamp: {timestamp}\n\nLast Uploaded File in AWS:\n" + \
        (last_uploaded_file if last_uploaded_file else "None") + \
        "\n\nLast Deleted File from AWS:\n" + (last_deleted_file if last_deleted_file else "None") + "\n\n"
 
    # Daily log dosyasının key'i

    log_key = "daily-log.txt"
 
    try:

        # Var olan log dosyasını oku

        response = s3.get_object(Bucket=log_bucket, Key=log_key)

        existing_content = response['Body'].read().decode('utf-8')

    except s3.exceptions.NoSuchKey:

        existing_content = ""  # Dosya yoksa içerik boş olacak
 
    # Diğer .txt dosyaların içeriklerini birleştirme

    objects = s3.list_objects_v2(Bucket=log_bucket)

    if 'Contents' in objects:

        for obj in objects['Contents']:

            key = obj['Key']

            if key.endswith(".txt") and key != log_key:

                try:

                    # Dosyanın içeriğini oku ve birleştir

                    file_response = s3.get_object(Bucket=log_bucket, Key=key)

                    file_content = file_response['Body'].read().decode('utf-8')

                    existing_content += f"\n\n \n\n" + file_content

                    # Dosyayı sil

                    s3.delete_object(Bucket=log_bucket, Key=key)

                except Exception as e:

                    print(f"Error processing file {key}: {str(e)}")
 
    # Yeni log içeriğini ekleme

    updated_content = existing_content + "\n\n \n\n" + log_content
 
    # Güncellenmiş içeriği daily-log.txt'ye yazma

    s3.put_object(Body=updated_content, Bucket=log_bucket, Key=log_key)

    print("Log updated successfully.")

 

 
def retain_last_three_files_aws(s3_client, bucket_name):

    """AWS bucket'ta yalnızca son yüklenen 3 dosyayı tutar ve eski dosyaları siler."""

    objects = s3_client.list_objects_v2(Bucket=bucket_name)

    deleted_files = []
 
    if 'Contents' in objects:

        

        sorted_objects = sorted(objects['Contents'], key=lambda x: x['LastModified'], reverse=True)

        for obj in sorted_objects[3:]:  

            s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])

            deleted_files.append(obj['Key'])
 
    return deleted_files[-1] if deleted_files else None  

def retain_last_three_files_scality(scality_s3_client, bucket_name):
    """Scality bucket'ta yalnızca son yüklenen 3 dosyayı tutar ve eski dosyaları siler."""
    objects = scality_s3_client.list_objects_v2(Bucket=bucket_name)
    deleted_files = []
    if 'Contents' in objects:
        sorted_objects = sorted(objects['Contents'], key=lambda x: x['LastModified'], reverse=True)
        for obj in sorted_objects[3:]:  # İlk 3 dosyayı tut, kalanları sil
            scality_s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])
            deleted_files.append(obj['Key'])
    return deleted_files[-1] if deleted_files else None       
     
def lambda_handler(event, context):

    s3 = boto3.client('s3')
    objects = s3.list_objects_v2(Bucket=source_bucket)
 
    last_uploaded_file = None
    
    if'Contents'in objects:

        latest_object = max(objects['Contents'], key=lambda x: x['LastModified'])
        key = latest_object['Key']
 
        
        response = s3.get_object(Bucket=source_bucket, Key=key)
        body = response['Body'].read()
        scality_s3.put_object(Body=body, Bucket=destination_bucket, Key=key)
 
        last_uploaded_file = key  

      
 
    

    last_deleted_file_aws = retain_last_three_files_aws(s3, source_bucket)

    
    last_deleted_file_scality = retain_last_three_files_scality(scality_s3, destination_bucket)
 
    

    log_latest_activity(last_uploaded_file, last_deleted_file_aws)
 
    return {

        'status': 'success',

        'last_uploaded_file': last_uploaded_file,

        'last_deleted_file_aws': last_deleted_file_aws

    }

 
