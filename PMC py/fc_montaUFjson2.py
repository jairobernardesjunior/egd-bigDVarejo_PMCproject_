''' MontaUFJson:
    recebe a tabela de percentual de crescimento de vendas de comércio por Unidade da Federação
    e grava em arquivo json, txt e parquet
'''

import pandas as pd

def Monta_UF_Json(tabela, ano, mes, PathArquivoJson):
    qtd_linhas = 32 #tabela.shape[0] - 1
    linhaXLS= 5
    i=0
    
    registro= []
    descricao= []
    anomes= []
    ano_x= []
    mes_x= []    

    m3mensal= []              

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
        anomes.append(str(ano) + str(mes))
        ano_x.append(str(ano))
        mes_x.append(str(mes))

        m3mensal.append(str(tabela.iloc[linhaXLS, 6]).replace('- ','0'))      

        linhaXLS=linhaXLS+1

    if i>0:
        df=pd.DataFrame({
                "registro":registro,
                "UF":descricao,
                "ano_mes":anomes,
                "ano":ano_x,
                "mes":mes_x,                

                "m3mensal":m3mensal,
                })

        df.to_parquet(PathArquivoJson + '.pq')
        df.to_string(PathArquivoJson + '.txt')
        df.to_json(PathArquivoJson + '.json')
        df.to_csv(PathArquivoJson + '.csv')
        return True 
    else:
        return False