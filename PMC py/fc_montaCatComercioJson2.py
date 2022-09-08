''' MontaCatComercioJson:
    recebe a tabela de percentual de crescimento de vendas de comércio por categoria de produto
    e grava em arquivo json, txt e parquet
'''

import pandas as pd

def Monta_CatComercio_Json(tabela, ano, mes, PathArquivoJson):
    qtd_linhas = 19 #tabela.shape[0] - 1
    linhaXLS= 5
    i=0
    
    registro= []
    descricao= []
    anomes= []

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

        m3mensal.append(str(tabela.iloc[linhaXLS, 6]).replace('- ','0'))         

        linhaXLS=linhaXLS+1

    if i>0:
        df=pd.DataFrame({
                "registro":registro,
                "classe_comercio":descricao,
                "ano_mes":anomes,

                "m3mensal":m3mensal,
                })

        df.to_parquet(PathArquivoJson + '.pq')
        df.to_string(PathArquivoJson + '.txt')
        df.to_json(PathArquivoJson + '.json')
        df.to_csv(PathArquivoJson + '.csv')
        return True 
    else:
        return False