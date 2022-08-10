''' UPLOADS3:
    Faz upload de qualquer tipo de arquivo para o bucket informado
'''
import boto3

#BUCKET = "flaskdrive"
#AWS_ACCESS_KEY="aws_access_key"
#AWS_SECERT_KEY="aws_secret_key"

def upload_fileS3(file_name, bucket, AWS_ACCESS_KEY, AWS_SECERT_KEY):
    # faz upload do arquivo para o bucket S3

    object_name = file_name
    s3_client = boto3.client('s3',
                             aws_access_key_id=AWS_ACCESS_KEY,
                             aws_secret_access_key=AWS_SECERT_KEY)
    response = s3_client.upload_file(filename=file_name, bucket=bucket, key=object_name)

    return response