
import boto3

def UploadFile_arquivosPMCprocessedS3(NomeBucketS3, nomeArquivo, pathArquivo):
    client = boto3.client(
        service_name='s3',
        aws_access_key_id='AKIAYIG3IZTMCIAHG5GV',
        aws_secret_access_key='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',
        region_name='eu-west-1' # voce pode usar qualquer regiao
        ) 

    client.upload_file(pathArquivo, NomeBucketS3, nomeArquivo)        