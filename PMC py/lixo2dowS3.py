import os
import urllib3.request
#import requests
import datetime
#import eventlet
import pandas as pd
import boto3

def LeUltimoBaixado_arquivosPMCrawS3(NomeBucketS3, arquivoUltimoBaixado, patharquivoUltimoBaixado):
    client = boto3.client(
        service_name='s3',
        aws_access_key_id='AKIATG2MKBIBCUNRCCOX',
        aws_secret_access_key='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',
        region_name='eu-west-1' # voce pode usar qualquer regiao
        )    
    
    client.download_file(NomeBucketS3, arquivoUltimoBaixado, patharquivoUltimoBaixado)
    d = open("c:/lixo/ultimoBaixado.txt")
    print(d.readlines())
    # ['\n', 'Olá, S3!\n']    

NomeBucketS3 = 'arquivos-pmc-raws3'
arquivoUltimoBaixado = 'ultimoBaixado.txt'
patharquivoUltimoBaixado = 'c:/lixo/ultimoBaixado.txt'
LeUltimoBaixado_arquivosPMCrawS3(NomeBucketS3, arquivoUltimoBaixado, patharquivoUltimoBaixado)