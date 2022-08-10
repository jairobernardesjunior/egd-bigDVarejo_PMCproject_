''' CAMADA SILVER:
    LE OS ARQUIVOS .csv baixados DO IBGE, COM ETL SEPARA AS TABELAS AGRUPADAS POR UF DAS
    TABELAS AGRUPADAS POR CATEGORIAS DE COMÉRCIO, SELECIONA AS COLUNAS NECESSÁRIAS
    TRANSFORMANDO EM ARQUIVOS JSON, ARMAZENANDO NO BUCKET arquivosPMCprocessedS3 E 
    DISPONIBILIZANDO PARA A CAMADA GOLD (obs os arquivo gerados, txt e parquet são somente
    para verificar a viabilidade da gravação, os arquivos parquet definitivos serão gravados
    na camada 3)
'''

#from ast import Try
#from socket import timeout
#from numpy import setxor1d

#import urllib.request
#import eventlet
#import requests

#import xlrd
#import strip
#import uploadS3 as upds3

from ast import Return
import os
import datetime
from numpy import meshgrid

import pandas as pd

import boto3
import botocore

import pmcSILVER_Monta_CatComercio_Json as MCatC
import pmcSILVER_Monta_UF_Json as MUF


''' MontaUFJson:
    recebe a tabela de percentual de crescimento de vendas de comércio por Unidade da Federação
    e grava em arquivo json, txt e parquet
'''
def Monta_UF_Json(tabela, ano, mes, PathArquivoJson):
    qtd_linhas = 32 #tabela.shape[0] - 1
    linhaXLS= 5
    i=0
    
    registro= []
    descricao= []

    m1anterior= []
    m2anterior= []
    m3anterior= []

    m1anteriorD= []
    m2anteriorD= []
    m3anteriorD= []

    m1mensal= []
    m2mensal= []
    m3mensal= []

    m1mensalD= []
    m2mensalD= []
    m3mensalD= []    

    m1acumulado= []
    m2acumulado= []
    m3acumulado= []

    m1acumuladoD= []
    m2acumuladoD= []
    m3acumuladoD= []

    m1u12m= []
    m2u12m= []
    m3u12m= []   

    m1u12mD= []
    m2u12mD= []
    m3u12mD= []    

    m1anomes= []
    m2anomes= []
    m3anomes= []               

    while linhaXLS <= qtd_linhas:
        valor= tabela.iloc[linhaXLS, 1]

        if str(valor) == '-':
            valor = 0

        try:
            float(valor)
        except ValueError:
            break

        try:
            testa= tabela.iloc[5, 7]
        except IndexError:
            break

        i= i+1
        registro.append(i)
        descricao.append(tabela.iloc[linhaXLS, 0])

        m1anterior.append(str(tabela.iloc[linhaXLS, 1]).replace('- ','0'))
        m2anterior.append(str(tabela.iloc[linhaXLS, 2]).replace('- ','0'))
        m3anterior.append(str(tabela.iloc[linhaXLS, 3]).replace('- ','0'))
        m1anteriorD.append(str(tabela.iloc[4, 1]) + ' - Mês anterior')
        m2anteriorD.append(str(tabela.iloc[4, 2]) + ' - Mês anterior')
        m3anteriorD.append(str(tabela.iloc[4, 3]) + ' - Mês anterior')

        m1mensal.append(str(tabela.iloc[linhaXLS, 4]).replace('- ','0'))
        m2mensal.append(str(tabela.iloc[linhaXLS, 5]).replace('- ','0'))
        m3mensal.append(str(tabela.iloc[linhaXLS, 6]).replace('- ','0'))
        m1mensalD.append(str(tabela.iloc[4, 4]) + ' - Mensal')
        m2mensalD.append(str(tabela.iloc[4, 5]) + ' - Mensal')
        m3mensalD.append(str(tabela.iloc[4, 6]) + ' - Mensal')

        m1acumulado.append(str(tabela.iloc[linhaXLS, 7]).replace('- ','0'))
        m2acumulado.append(str(tabela.iloc[linhaXLS, 8]).replace('- ','0'))
        m3acumulado.append(str(tabela.iloc[linhaXLS, 9]).replace('- ','0'))
        m1acumuladoD.append(str(tabela.iloc[4, 7]) + ' - Acumulado no ano')
        m2acumuladoD.append(str(tabela.iloc[4, 8]) + ' - Acumulado no ano')
        m3acumuladoD.append(str(tabela.iloc[4, 9]) + ' - Acumulado no ano')

        m1u12m.append(str(tabela.iloc[linhaXLS, 10]).replace('- ','0'))
        m2u12m.append(str(tabela.iloc[linhaXLS, 11]).replace('- ','0'))
        m3u12m.append(str(tabela.iloc[linhaXLS, 12]).replace('- ','0'))
        m1u12mD.append(str(tabela.iloc[4, 10]) + ' - Últimos 12 meses')
        m2u12mD.append(str(tabela.iloc[4, 11]) + ' - Últimos 12 meses')
        m3u12mD.append(str(tabela.iloc[4, 12]) + ' - Últimos 12 meses')

        mes_2= int(mes) -2
        mes_1= int(mes) -1
        mes_2 = '%02d' % mes_2
        mes_1 = '%02d' % mes_1
        m1anomes.append(str(ano) + str(mes_2))
        m2anomes.append(str(ano) + str(mes_1))
        m3anomes.append(str(ano) + str(mes))          

        linhaXLS=linhaXLS+1

    if i>0:
        df=pd.DataFrame({
                "registro":registro,
                "UF":descricao,

                "m1anterior":m1anterior,
                "m2anterior":m2anterior,
                "m3anterior":m3anterior,
                "m1anteriorD":m1anteriorD,
                "m2anteriorD":m2anteriorD,
                "m3anteriorD":m3anteriorD,

                "m1mensal":m1mensal,
                "m2mensal":m2mensal,
                "m3mensal":m3mensal,
                "m1mensalD":m1mensalD,
                "m2mensalD":m2mensalD,
                "m3mensalD":m3mensalD,

                "m1acumulado":m1acumulado,
                "m2acumulado":m2acumulado,
                "m3acumulado":m3acumulado,
                "m1acumuladoD":m1acumuladoD,
                "m2acumuladoD":m2acumuladoD,
                "m3acumuladoD":m3acumuladoD,

                "m1u12m":m1u12m,
                "m2u12m":m2u12m, 
                "m3u12m":m3u12m,
                "m1u12mD":m1u12mD,
                "m2u12mD":m2u12mD, 
                "m3u12mD":m3u12mD,

                "m1anomes":m1anomes,
                "m2anomes":m2anomes,
                "m3anomes":m3anomes,
                })

        df.to_parquet(PathArquivoJson + '.pq')
        df.to_string(PathArquivoJson + '.txt')
        df.to_json(PathArquivoJson + '.json')
        return True 
    else:
        return False

def UploadJSONfile_arquivosPMCprocessedS3(NomeBucketS3, nomeArquivo, pathArquivo):
    client = boto3.client(
        service_name='s3',
        aws_access_key_id='xxxxxxxxxxxxxxxxxxxxxxxx',
        aws_secret_access_key='xxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
        region_name='eu-west-1' # voce pode usar qualquer regiao
        )    

    client.upload_file(pathArquivo, NomeBucketS3, nomeArquivo)

def Le_arquivosBucketS3(NomeBucketS3, arquivoUltimoProcessado, patharquivoUltimoProcessado):
    client = boto3.client(
        service_name='s3',
        aws_access_key_id='xxxxxxxxxxxxxxxxxxxxxxxxxxxx',
        aws_secret_access_key='xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
        region_name='eu-west-1' # voce pode usar qualquer regiao
        )    

    retorno = False

    try:
        client.download_file(NomeBucketS3, arquivoUltimoProcessado, patharquivoUltimoProcessado)
        retorno = True
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            retorno = False

    return retorno

def SalvaUltimaURL(patharquivoUltimoProcessado, ultimaURL):
    arquivo = open(patharquivoUltimoProcessado,'w')
    arquivo.write(ultimaURL)
    arquivo.close     

def lambda_handler(event, context):
    # ******************** INÍCIO

    # Baixa arquivo ultimoProcessado.txt do Buckets3
    NomeBucketS3processed = 'arquivos-pmc-processeds3'
    NomeBucketS3raw = 'arquivos-pmc-raws3'
    arquivoUltimoProcessado = 'ultimoProcessado.txt'
    patharquivoUltimoProcessado = 'arquivosPMCprocessed/ultimoProcessado.txt' #'arquivosPMCprocessed/ultimoProcessado.txt' '/tmp/ultimoProcessado.txt'
    tmpAux = 'arquivosPMCprocessed/' #arquivosPMCprocessed/' '/tmp/'
    tmpAuxRaw = 'arquivosPMCraw/' #arquivosPMCraw/' '/tmp/'

    patharquivoUltimoProcessado = '/tmp/ultimoProcessado.txt' #'arquivosPMCprocessed/ultimoProcessado.txt' '/tmp/ultimoProcessado.txt'
    tmpAux = '/tmp/' #arquivosPMCprocessed/' '/tmp/'  
    tmpAuxRaw = '/tmp/' #arquivosPMCraw/' '/tmp/' 
 
    retornoLe = Le_arquivosBucketS3(NomeBucketS3processed, arquivoUltimoProcessado, patharquivoUltimoProcessado)

    if retornoLe == False:
        print('arquivo não encontrado: ' + arquivoUltimoProcessado + 
              ' no bucket ' + NomeBucketS3processed)
        exit()

    # verifica se o arquivo ultimoProcessado.txt existe e, se não, cria o mesmo
    if os.path.exists(patharquivoUltimoProcessado) == False:
        arquivo = open(patharquivoUltimoProcessado,'w')
        arquivo.write("vazio")
        arquivo.close

    # lê o arquivo ultimoProcessado.txt para pegar o último arquivo .csv transformado em Json
    linha='vazio'
    arquivo = open(patharquivoUltimoProcessado,'r')
    for linha in arquivo:
        linha = linha.rstrip()
    arquivo.close()

    # verifica se o arquivo ultimoProcessado.txt tem a informação do último arquivo.csv transformado em Json
    if linha == 'vazio':
        url = 'pmc_201800_00.csv'   
    else:
        url = linha

    ultimoArquivoProcessado = ''
    seq = url[11:13]
    seq = int(seq) + 1
    seq = '%02d' % seq

    ano= url[4:8]
    anomes= url[4:10]
    mes= anomes[5:7]

    anomes = int(anomes) + 1
    mes= int(mes) + 1
    mes = '%02d' % mes

    nomeArq= 'pmc_{}_{}.csv'
    nomeArqJson= 'pmc_{}_{}.json'
    anocorrente = datetime.date.today().year
    mescorrente = datetime.date.today().month
    mescorrente = '%02d' % mescorrente
    anoMesCorrente = str(anocorrente) + str(mescorrente)

    while int(anomes) <= int(anoMesCorrente):
    #while int(ano) <= int(anocorrente):

        print(str(anomes) + ' - ' + str(anoMesCorrente))

        while int(mes) <= 1: #12:

            print('mes ' + str(meshgrid))

            while int(seq) <= 3: #13:

                print('seq ' + str(seq))

                #print(nomeArq.format(anomes, seq))

                # verifica se o arquivo arquivoPMCprocessed .csv existe e processa o mesmo
                retornoLe = Le_arquivosBucketS3(NomeBucketS3raw, 
                                       nomeArq.format(anomes, seq), 
                                       tmpAuxRaw + nomeArq.format(anomes, seq))  

                arquivo= tmpAuxRaw + nomeArq.format(anomes, seq)

                if retornoLe == True:
                    #if os.path.exists(arquivo) == True:
                    tabela= pd.read_csv(arquivo)

                    #print(tabela)
                    #print(tabela.iloc[0])
                    #print(tabela.iloc[5, 0])

                    #exit()

                    if (tabela.iloc[5, 0] == 'Brasil ') or (tabela.iloc[5, 0] == 'Brasil'):
                        nomeArquivoJson= 'PercUF_' + nomeArqJson.format(anomes, seq)[:13]
                        PathArquivoJson= ('arquivosPMCprocessed/PercUF_' + 
                                            nomeArq.format(anomes, seq)[:13])
                        retorno= MUF.Monta_UF_Json(tabela, ano, mes, PathArquivoJson)
                    else:
                        nomeArquivoJson= 'PercCAT_COMERCIO_' + nomeArqJson.format(anomes, seq)[:13]
                        PathArquivoJson= ('arquivosPMCprocessed/PercCAT_COMERCIO_' + 
                                            nomeArq.format(anomes, seq)[:13])                    
                        retorno= MCatC.Monta_CatComercio_Json(tabela, ano, mes, PathArquivoJson)

                    if retorno == True:
                        UploadJSONfile_arquivosPMCprocessedS3(NomeBucketS3processed, 
                                        nomeArquivoJson + '.json', 
                                        PathArquivoJson + '.json')
                        ultimoArquivoProcessado = nomeArq.format(anomes, seq)

                    #print(nomeArq.format(anomes, seq))
            
                seq = int(seq) + 1
                seq = '%02d' % seq

            seq= '01'
            mes= int(mes) + 1
            mes = '%02d' % mes
            anomes = int(anomes) + 1

        mes= '01'
        ano= int(ano) + 1
        anomes= str(ano) + str(mes)
        
    if len(ultimoArquivoProcessado) > 0: # grava a posição do último arquivo pmc.csv Processado
        SalvaUltimaURL(patharquivoUltimoProcessado, ultimoArquivoProcessado)
        UploadJSONfile_arquivosPMCprocessedS3(NomeBucketS3processed, arquivoUltimoProcessado, patharquivoUltimoProcessado)    