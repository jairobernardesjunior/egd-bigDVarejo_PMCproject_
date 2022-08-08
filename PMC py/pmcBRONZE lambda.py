''' CAMADA BRONZE:
    BAIXA OS ARQUIVOS .xls DISPONÍVEIS NO SITE DO IBGE NO ITEM DE COMÉRCIO E SERVIÇOS,
    CONVERTE EM ARQUIVOS .csv E ARMAZENA NO BUCKET arquivosPMCrawS3 COMO DADOS BRUTOS:

    https://ftp.ibge.gov.br/Comercio_e_Servicos/Pesquisa_Mensal_de_Comercio/Tabelas
'''

import os
#import urllib3.request
#import urllib.request
import requests
import datetime
import eventlet
import pandas as pd
import boto3

def UploadCSVfile_arquivosPMCrawS3(NomeBucketS3, nomeArquivo, pathArquivo):
    client = boto3.client(
        service_name='s3',
        aws_access_key_id='xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
        aws_secret_access_key='xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
        region_name='eu-west-1' # voce pode usar qualquer regiao
        )    

    client.upload_file(pathArquivo, NomeBucketS3, nomeArquivo)

def LeUltimoBaixado_arquivosPMCrawS3(NomeBucketS3, arquivoUltimoBaixado, patharquivoUltimoBaixado):
    client = boto3.client(
        service_name='s3',
        aws_access_key_id='xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
        aws_secret_access_key='xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
        region_name='eu-west-1' # voce pode usar qualquer regiao
        )    
    
    client.download_file(NomeBucketS3, arquivoUltimoBaixado, patharquivoUltimoBaixado)

def ApagaUltimoArquivoBaixado_arquivosPMCrawS3(NomeBucketS3, pathArquivo):
    client = boto3.client(
        service_name='s3',
        aws_access_key_id='xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
        aws_secret_access_key='xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
        region_name='eu-west-1' # voce pode usar qualquer regiao
        )    

    client.delete_object(Bucket=NomeBucketS3, Key=pathArquivo)   

def ConvertArquivoParaCSV(url, tmpAux):
    arquivoxls = pd.read_excel(tmpAux + url[85:103])
    arquivoxls.to_csv(tmpAux + url[85:99] + 'csv', index=False)

def BaixaArquivo(url):
    print('..... baixa arquivo - ' + str(datetime.datetime.now()))
    #with eventlet.Timeout(None):
        #urllib3.request.urlretrieve(url, tmpAux + url[85:103])

    with eventlet.Timeout(None):
        arqXLS = requests.get(url, verify = False) 
        with open(tmpAux + url[85:103], 'wb') as pmcArq:
            pmcArq.write(arqXLS.content)

    print('..... arquivo baixado - ' + str(datetime.datetime.now()))

def SalvaUltimaURL(patharquivoUltimoBaixado, ultimaURL):
    arquivo = open(patharquivoUltimoBaixado,'w')
    arquivo.write(ultimaURL)
    arquivo.close  

def lambda_handler(event, context):
    # ******************** INÍCIO
    # Baixa arquivo ultimoBaixado.txt do Buckets3
    NomeBucketS3 = 'arquivos-pmc-raws3'
    arquivoUltimoBaixado = 'ultimoBaixado.txt'
    patharquivoUltimoBaixado = 'arquivosPMCraw/ultimoBaixado.txt' #'arquivosPMCraw/ultimoBaixado.txt' '/tmp/ultimoBaixado.txt'
    tmpAux = 'arquivosPMCraw/' #arquivosPMCraw/' '/tmp/'
    patharquivoUltimoBaixado = '/tmp/ultimoBaixado.txt' #'arquivosPMCraw/ultimoBaixado.txt' '/tmp/ultimoBaixado.txt'
    tmpAux = '/tmp/' #arquivosPMCraw/' '/tmp/'    
    LeUltimoBaixado_arquivosPMCrawS3(NomeBucketS3, arquivoUltimoBaixado, patharquivoUltimoBaixado)

    # verifica se o arquivo ultimoBaixado.txt existe e, se não, cria o mesmo
    if os.path.exists(patharquivoUltimoBaixado) == False:
        arquivo = open(patharquivoUltimoBaixado,'w')
        arquivo.write("vazio")
        arquivo.close

    # lê o arquivo ultimoBaixado.txt para pegar o último arquivo baixado
    arquivo = open(patharquivoUltimoBaixado,'r')
    for linha in arquivo:
        linha = linha.rstrip()
    arquivo.close()

    # verifica se o arquivo ultimoBaixado.txt tem a informação do último arquivo.xls baixado
    if linha == 'vazio':
        url = 'https://ftp.ibge.gov.br/Comercio_e_Servicos/Pesquisa_Mensal_de_Comercio/Tabelas/2018/pmc_201801_00.xls'   
    else:
        url = linha

    ultimaURL = ''
    seq = url[96:98]

    ano=url[80:84]
    anomes= url[89:95]

    while True: # loop infinito que procura até 12 sequências de arquivos dentro de cada mês
        seq= int(seq) + 1
        seq = '%02d' % seq

        url = url[:80] + str(ano) + url[84:89] + str(anomes) + '_' + str(seq) + url[98:102]

        print('+++++ verifica url - ' + str(datetime.datetime.now()))
        print(url)
        response = requests.get(url, verify=False, timeout=None)

        if int(seq) < 13: # procura até 12 sequências do arquivo dentro do mês

            if response.status_code == 200: # vê se url existe
                #print('Web site exists')
                BaixaArquivo(url) 
                ConvertArquivoParaCSV(url, tmpAux)
                UploadCSVfile_arquivosPMCrawS3(NomeBucketS3, url[85:99] + 'csv', tmpAux + url[85:99] + 'csv')
                ultimaURL = url
            else:
                response = requests.get(url + 'x', verify=False, timeout=None)
                if response.status_code == 200: # vê se url existe
                    response = requests.get(url + 'x', verify=False, timeout=None)
                    if response.status_code == 200: # vê se url existe
                        BaixaArquivo(url + 'x') 
                        ConvertArquivoParaCSV(url + 'x', tmpAux)           
                        UploadCSVfile_arquivosPMCrawS3(NomeBucketS3, url[85:99] + 'csv', tmpAux + url[85:99] + 'csv')
                        ultimaURL = url                

        else: 
            # passa para o próximo mês e verifica se quebrou o ano
            #print('Web site does not exist')

            seq = 0
            anomes = int(anomes) + 1
            anocorrente = datetime.date.today().year
            anomesStr = str(anomes)

            if int(anomesStr[4:6]) > 12:
                if int(anocorrente) > int(ano):
                    ano = int(ano) + 1
                    anomes = str(ano) + '01'
                else:
                    break

    if len(ultimaURL) > 0: # grava a posição do último arquivo pmc.xls baixado
        SalvaUltimaURL(patharquivoUltimoBaixado, ultimaURL)
        #ApagaUltimoArquivoBaixado_arquivosPMCrawS3(NomeBucketS3, patharquivoUltimoBaixado) 
        UploadCSVfile_arquivosPMCrawS3(NomeBucketS3, arquivoUltimoBaixado, patharquivoUltimoBaixado) 
