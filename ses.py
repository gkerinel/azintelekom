import os
import boto3
from datetime import datetime, timedelta
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
 
def lambda_handler(event, context):
    # Environment variables for Scality credentials and settings
    scality_access_key = os.getenv("SCALITY_ACCESS_KEY")
    scality_secret_key = os.getenv("SCALITY_SECRET_KEY")
    scality_endpoint_url = os.getenv("SCALITY_ENDPOINT_URL")
    scality_region = os.getenv("SCALITY_REGION")
    # SES settings
    ses_client = boto3.client('ses')
    sender_email = "info@abm.aws.pmicloud.biz"
    recipient_emails = ["goker.inel@contracted.pmi.com","rephael.hayun@pmi.com","diana.gloss@pmi.com","ecaterina.serebreanschii@contracted.pmi.com","lia.rostashvili@pmi.com"]
    subject = "Data Transfer Status"
    # Connect to Scality S3
    s3_client = boto3.client(
        's3',
        aws_access_key_id=scality_access_key,
        aws_secret_access_key=scality_secret_key,
        endpoint_url=scality_endpoint_url,
        region_name=scality_region  # Added region information here
    )
    # Define bucket name
    bucket_name = "azintelecomcopybucket"
    # Check files in the bucket
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        files = response.get("Contents", [])
        file_count = len(files)
        # Get today's date and date three days ago
        today = datetime.now().strftime("%d/%m/%Y")
        three_days_ago = (datetime.now() - timedelta(days=3)).strftime("%d/%m/%Y")
        # Determine message body based on conditions
        if file_count == 3:
            subject = "Data Transfer Status - Success"
            message_body = (f"I am pleased to inform you that the file from {today} has been successfully "
                            f"uploaded, and the file from {three_days_ago} has been successfully removed.\n\n"
                            f"Diana Gloss\n"
                            f"Information Technology\n"
                            f"Manager IT Consumer\n\n"
                            f"M: +972 (050) 7733310")
        elif file_count > 3:
            subject = "Data Transfer Status - Failed"
            message_body = (f"Error: Delete Fail. File from {three_days_ago} has not been successfully removed.\n\n"
                            f"Diana Gloss\n"
                            f"Information Technology\n"
                            f"Manager IT Consumer\n\n"
                            f"M: +972 (050) 7733310")
        else:
            # Check if there is a file with today's date
            uploaded_today = any(f['LastModified'].strftime("%Y-%m-%d") == today for f in files)
            if not uploaded_today:
                subject = "Data Transfer Status - Failed"
                message_body = (f"Error: Upload Fail. There is no new file {today}.\n\n"
                                f"Diana Gloss\n"
                                f"Information Technology\n"
                                f"Manager IT Consumer\n\n"
                                f"M: +972 (050) 7733310")
            else:
                return {"status": "No action needed"}  # Optional: Exit if no action is needed
 
        # Send email using AWS SES
        response = ses_client.send_email(
            Source=sender_email,
            Destination={'ToAddresses': recipient_emails},
            Message={
                'Subject': {'Data': subject},
                'Body': {'Text': {'Data': message_body}}
            }
        )
        return {"status": "Email sent", "response": response}
 
    except (NoCredentialsError, PartialCredentialsError) as e:
        return {"status": "Error", "message": str(e)}
    except Exception as e:
        return {"status": "Error", "message": str(e)}
