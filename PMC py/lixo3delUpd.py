import os
#import urllib3.request
import urllib.request
import requests
import datetime
import eventlet
import pandas as pd
import boto3

def UploadCSVfile_arquivosPMCrawS3(NomeBucketS3, nomeArquivo, pathArquivo):
    client = boto3.client(
        service_name='s3',
        aws_access_key_id='AKIATG2MKBIBCUNRCCOX',
        aws_secret_access_key='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',
        region_name='eu-west-1' # voce pode usar qualquer regiao
        )    

    client.upload_file(pathArquivo, NomeBucketS3, nomeArquivo)

def ApagaUltimoArquivoBaixado_arquivosPMCrawS3(NomeBucketS3, pathArquivo):
    client = boto3.client(
        service_name='s3',
        aws_access_key_id='AKIATG2MKBIBCUNRCCOX',
        aws_secret_access_key='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',
        region_name='eu-west-1' # voce pode usar qualquer regiao
        )    

    client.delete_object(Bucket=NomeBucketS3, Key=pathArquivo)     

NomeBucketS3 = 'arquivos-pmc-raws3'
arquivoUltimoBaixado = 'ultimoBaixado.txt'
patharquivoUltimoBaixado = 'arquivosPMCraw/ultimoBaixado.txt' #'/tmp/ultimoBaixado.txt'
tmpAux = 'arquivosPMCraw/' #'/tmp/

#ApagaUltimoArquivoBaixado_arquivosPMCrawS3(NomeBucketS3, arquivoUltimoBaixado) 

UploadCSVfile_arquivosPMCrawS3(NomeBucketS3, arquivoUltimoBaixado, patharquivoUltimoBaixado) 