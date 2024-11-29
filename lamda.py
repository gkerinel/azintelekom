import boto3

import os

from datetime import datetime
 
# AWS kaynaklarını tanımlama

source_bucket = 'asdatalocalizationcopy'

destination_bucket = 'azintelecomcopybucket'

log_bucket = 'asdatadailylog'  # Logları tutmak için yeni bir bucket
 
# Scality erişim bilgileri

scality_access_key = os.getenv('SCALITY_ACCESS_KEY')

scality_secret_key = os.getenv('SCALITY_SECRET_KEY')

scality_region = os.getenv('SCALITY_REGION')

scality_endpoint = os.getenv('SCALITY_ENDPOINT')
 
# Scality S3 istemcisini oluşturma

scality_s3 = boto3.client(

    's3',

    aws_access_key_id=scality_access_key,

    aws_secret_access_key=scality_secret_key,

    region_name=scality_region,

    endpoint_url=scality_endpoint

)
 
def log_latest_activity(last_uploaded_file, last_deleted_file):

    """Son yüklenen ve silinen dosyayı loglamak için fonksiyon."""

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    log_content = f"Timestamp: {timestamp}\n\nLast Uploaded File in AWS:\n" + \
    (last_uploaded_file if last_uploaded_file else "None") + \
"\n\nLast Deleted File from AWS:\n" + (last_deleted_file if last_deleted_file else "None")

    log_key = f"logs/{timestamp}.txt"

    s3 = boto3.client('s3')

    s3.put_object(Body=log_content, Bucket=log_bucket, Key=log_key)
 
def retain_last_three_files_aws(s3_client, bucket_name):

    """AWS bucket'ta yalnızca son yüklenen 3 dosyayı tutar ve eski dosyaları siler."""

    objects = s3_client.list_objects_v2(Bucket=bucket_name)

    deleted_files = []
 
    if 'Contents' in objects:

        # Dosyaları yüklenme tarihine göre sıralama

        sorted_objects = sorted(objects['Contents'], key=lambda x: x['LastModified'], reverse=True)

        for obj in sorted_objects[3:]:  # İlk 3 dosyayı tut, kalanları sil

            s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])

            deleted_files.append(obj['Key'])
 
    return deleted_files[-1] if deleted_files else None  # Son silinen dosya varsa döndür

def retain_last_three_files_scality(scality_s3_client, bucket_name):
    """Scality bucket'ta yalnızca son yüklenen 3 dosyayı tutar ve eski dosyaları siler."""
    objects = scality_s3_client.list_objects_v2(Bucket=bucket_name)
    deleted_files = []
    if 'Contents' in objects:
        sorted_objects = sorted(objects['Contents'], key=lambda x: x['LastModified'], reverse=True)
        for obj in sorted_objects[3:]:  # İlk 3 dosyayı tut, kalanları sil
            scality_s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])
            deleted_files.append(obj['Key'])
    return deleted_files[-1] if deleted_files else None  # Son silinen dosya varsa döndür      
     
def lambda_handler(event, context):
# AWS S3'tan en son yüklenen dosyayı alıp Scality bucket'a kopyalama
    s3 = boto3.client('s3')
    objects = s3.list_objects_v2(Bucket=source_bucket)
 
    last_uploaded_file = None
    
    if'Contents'in objects:
# Dosyaları yüklenme tarihine göre sıralama ve en son dosyayı seçme
        latest_object = max(objects['Contents'], key=lambda x: x['LastModified'])
        key = latest_object['Key']
 
        # Dosyayı AWS'den al ve Scality'ye kopyala
        response = s3.get_object(Bucket=source_bucket, Key=key)
        body = response['Body'].read()
        scality_s3.put_object(Body=body, Bucket=destination_bucket, Key=key)
 
        last_uploaded_file = key  # En son yüklenen dosyayı sakla

      
 
    # AWS bucket'ında yalnızca son 3 dosyayı tutma işlemi

    last_deleted_file_aws = retain_last_three_files_aws(s3, source_bucket)

    # Scality bucket'ında yalnızca son 3 dosyayı tutma işlemi
    last_deleted_file_scality = retain_last_three_files_scality(scality_s3, destination_bucket)
 
    # Son yüklenen ve silinen dosyaları log bucket'ına ekleme

    log_latest_activity(last_uploaded_file, last_deleted_file_aws)
 
    return {

        'status': 'success',

        'last_uploaded_file': last_uploaded_file,

        'last_deleted_file_aws': last_deleted_file_aws

    }

 
