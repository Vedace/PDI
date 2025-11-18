import requests
import pandas as pd 

ibge = 'https://servicodados.ibge.gov.br/api/v1/localidades/estados/{UF}/municipios'

dados_ibge = requests.get (ibge)

ibge_json = dados_ibge.json()

df_ibge = pd.json_normalize(ibge_json, sep = '_')

df_ibge.to_parquet("ibge.parquet", index=False) # modificar para cloud storage