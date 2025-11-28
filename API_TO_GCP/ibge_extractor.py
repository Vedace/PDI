import requests
import pandas as pd 


ibge = 'https://servicodados.ibge.gov.br/api/v1/localidades/municipios?view=nivelado'

dados_ibge = requests.get (ibge)

ibge_json = dados_ibge.json()

df = pd.json_normalize(ibge_json, sep = '_')

df_ibge = pd.DataFrame(df)

ibge_columns = ["id_municipio", "nome_municipio","id_microrregiao","nome_microrregiao", "id_mesorregiao", "nome_mesorregiao", "id_regiao_imediata", "nome_regiao_imediata", "id_regiao_intermediaria", "nome_regiao_intermediaria", "id_uf","sigla_uf", "nome_uf", "id_regiao", "sigla_regiao", "nome_regiao"]

df_ibge.columns = ibge_columns

print(df_ibge.head())

df_ibge.to_parquet("ibge.parquet", index=False) # modificar para cloud storage