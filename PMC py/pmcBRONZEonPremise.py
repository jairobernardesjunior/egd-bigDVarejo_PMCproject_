''' CAMADA BRONZE:
    BAIXA OS ARQUIVOS .xls DISPONÍVEIS NO SITE DO IBGE NO ITEM DE COMÉRCIO E SERVIÇOS E
    ARMAZENA NO BUCKET arquivosPMCrawS3 COMO DADOS BRUTOS:

    https://ftp.ibge.gov.br/Comercio_e_Servicos/Pesquisa_Mensal_de_Comercio/Tabelas
'''

import os
from socket import timeout
import urllib.request
import requests
import datetime
import eventlet
import pandas as pd
import uploadS3 as upds3

def ConvertArquivoParaCSV(url):
    arquivoxls = pd.read_excel('arquivosPMCraw/' + url[85:103])
    arquivoxls.to_csv('arquivosPMCraw/' + url[85:99] + 'csv', index=False)

def BaixaArquivo(url):
    print('..... baixa arquivo - ' + str(datetime.datetime.now()))
    with eventlet.Timeout(None):
        urllib.request.urlretrieve(url, 'arquivosPMCraw/' + url[85:103])

    print('..... arquivo baixado - ' + str(datetime.datetime.now()))

# verifica se a pasta arquivosPMC existe e, se não, cria a mesma
if os.path.exists('arquivosPMCraw') == False:
    os.mkdir('arquivosPMCraw')

# verifica se a pasta ultimoBaixado existe e, se não, cria a mesma
if os.path.exists('ultimoBaixado') == False:
    os.mkdir('ultimoBaixado')    

# verifica se o arquivo ultimoBaixado.txt existe e, se não, cria o mesmo
if os.path.exists('ultimoBaixado/ultimoBaixado.txt') == False:
    arquivo = open('ultimoBaixado/ultimoBaixado.txt','w')
    arquivo.write("vazio")
    arquivo.close

# lê o arquivo ultimoBaixado.txt para pegar o último arquivo baixado
arquivo = open('ultimoBaixado/ultimoBaixado.txt','r')
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
            ConvertArquivoParaCSV(url)
                     
            #upds3.upload_fileS3('arquivosPMCraw/' + url[85:102], 'arquivosPMCrawS3', 'aws_access_key', 'aws_secret_key')
            ultimaURL = url
        else:
            response = requests.get(url + 'x', verify=False, timeout=None)
            if response.status_code == 200: # vê se url existe
                response = requests.get(url + 'x', verify=False, timeout=None)
                if response.status_code == 200: # vê se url existe
                    BaixaArquivo(url + 'x') 
                    ConvertArquivoParaCSV(url + 'x')           
                    #upds3.upload_fileS3('arquivosPMCraw/' + url[85:99] + 'csv', 'arquivosPMCrawS3', 'aws_access_key', 'aws_secret_key')
                    ultimaURL = url                

    else: # passa para o próximo mês e verifica se quebrou o ano
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
    arquivo = open('ultimoBaixado/ultimoBaixado.txt','w')
    arquivo.write(ultimaURL)
    arquivo.close    