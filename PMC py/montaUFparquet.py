''' MontaUFparquet:
    recebe a tabela de percentual de crescimento de vendas de comércio por Unidade da Federação
    e grava em arquivo json, txt e parquet
'''

import pandas as pd

def Monta_UF_parquet(tabela, ano, mes, PathArquivoParquet):
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

        df.to_parquet(PathArquivoParquet + '.pq')
        df.to_string(PathArquivoParquet + '.txt')
        df.to_json(PathArquivoParquet + '.json')
        return True 
    else:
        return False