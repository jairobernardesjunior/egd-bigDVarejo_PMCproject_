''' CAMADA SILVER:
    LE OS ARQUIVOS .xls BAIXADOS DO IBGE, COM ETL SEPARA AS TABELAS AGRUPADAS POR UF DAS
    TABELAS AGRUPADAS POR CATEGORIAS DE COMÉRCIO, SELECIONA AS COLUNAS NECESSÁRIAS
    TRANSFORMANDO EM ARQUIVOS PARQUET, ARMAZENANDO NO BUCKET arquivosPMCprocessedS3 E 
    DISPONIBILIZANDO PARA A CAMADA GOLD
'''

#from ast import Try
#from socket import timeout
#from numpy import setxor1d

#import urllib.request
#import eventlet
#import requests

#import pandas as pd
#import strip

import os
import datetime
import xlrd

import uploadS3 as upds3
import montaCatComercioParquet as catp
import montaUFparquet as ufp

def le_xls(arquivo):
    tabela = xlrd.open_workbook(arquivo).sheet_by_index(0)
    return tabela 

# verifica se a pasta arquivosPMCprocessed existe e, se não, cria a mesma
if os.path.exists('arquivosPMCprocessed') == False:
    os.mkdir('arquivosPMCprocessed')

# verifica se a pasta ultimoProcessado existe e, se não, cria a mesma
if os.path.exists('ultimoProcessado') == False:
    os.mkdir('ultimoProcessado')    

# verifica se o arquivo ultimoProcessado.txt existe e, se não, cria o mesmo
if os.path.exists('ultimoProcessado/ultimoProcessado.txt') == False:
    arquivo = open('ultimoProcessado/ultimoProcessado.txt','w')
    arquivo.write("vazio")
    arquivo.close

# lê o arquivo ultimoProcessado.txt para pegar o último arquivo .xls transformado em parquet
linha='vazio'
arquivo = open('ultimoProcessado/ultimoProcessado.txt','r')
for linha in arquivo:
    linha = linha.rstrip()
arquivo.close()

# verifica se o arquivo ultimoProcessado.txt tem a informação do último arquivo.xls transformado em parquet
if linha == 'vazio':
    url = 'pmc_201800_00.xls'   
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

nomeArq= 'pmc_{}_{}.xls'
anocorrente = datetime.date.today().year

while int(ano) <= int(anocorrente):

    while int(mes) <= 12:

        while int(seq) <= 13:

            #print(nomeArq.format(anomes, seq))

            # verifica se o arquivo arquivoPMCprocessed .xls existe e processa o mesmo
            arquivo= 'arquivosPMCraw/' + nomeArq.format(anomes, seq)

            if os.path.exists(arquivo) == True:
                tabela= le_xls(arquivo)

                if tabela.row(6)[0].value[:6] == 'Brasil':
                    PathArquivoParquet= ('arquivosPMCprocessed/PercUF_' + 
                                         nomeArq.format(anomes, seq)[:13])
                    retorno= ufp.Monta_UF_parquet(tabela, ano, mes, PathArquivoParquet)
                else:
                    PathArquivoParquet= ('arquivosPMCprocessed/PercCAT_COMERCIO_' + 
                                         nomeArq.format(anomes, seq)[:13])                    
                    retorno= catp.Monta_CatComercio_parquet(tabela, ano, mes, PathArquivoParquet)

                if retorno == True:
                    #upds3.upload_fileS3(PathArquivoParquet, 'arquivosPMCstagedS3', 'aws_access_key', 'aws_secret_key')
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
    
if len(ultimoArquivoProcessado) > 0: # grava a posição do último arquivo pmc.xls baixado
    arquivo = open('ultimoProcessado/ultimoProcessado.txt','w')
    arquivo.write(ultimoArquivoProcessado)
    arquivo.close 