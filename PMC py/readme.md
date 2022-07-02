Arquivos de código responsáveis pela ingestão de dados da Pesquisa Mensal de Comércio(PMC) no Data Lake de Varejo salvando no AWS S3:
(esses arquivos .xls contêm os índices de crescimento de vendas do comércio varejista por categoria de produto e por UF, e são disponibilizados mensalmente)
pmcBRONZE.py: responsável pela camada raw de carga dos arquivos .xls (dados bruto/cru) no bucket arquivosPMCrawS3 da AWS.
pmcSILVER.py: responsável pela camada staged de carga, por transformar os arquivos .xls baixados, separar as tabelas de informações em percentual de crescimento das vendas por categoria de produtos e percentual de crescimento das vendas por Unidade da Federação, gravando os dados em arquivo parquet, armazenando os mesmos no bucket arquivosPMCstagedS3 da AWS.
