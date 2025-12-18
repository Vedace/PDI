import requests
import pandas as pd
from datetime import datetime, timedelta

#Creando un timestamp actual y otro d-1 
timestamp_atual = datetime.now().strftime("%Y%m%d_%H%M%S")
timestamp_ontem = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d_%H%M%S")

ibge = 'https://servicodados.ibge.gov.br/api/v1/localidades/municipios?view=nivelado'

dados_ibge = requests.get (ibge)

ibge_json = dados_ibge.json()

df = pd.json_normalize(ibge_json, sep = '_')

df_ibge = pd.DataFrame(df)

ibge_columns = ["id_municipio", "nome_municipio","id_microrregiao","nome_microrregiao", "id_mesorregiao", "nome_mesorregiao", "id_regiao_imediata", "nome_regiao_imediata", "id_regiao_intermediaria", "nome_regiao_intermediaria", "id_uf","sigla_uf", "nome_uf", "id_regiao", "sigla_regiao", "nome_regiao"]

df_ibge.columns = ibge_columns

df_ibge_len = len(df_ibge)
df_ibge_mitade = df_ibge_len // 2 

df_ibge1 = df.iloc[:df_ibge_mitade]
df_ibge2 = df.iloc[df_ibge_mitade:]

print(df_ibge.head())

df_ibge.to_parquet(f"ibge_{timestamp_ontem}.parquet", index=False)
df_ibge1.to_parquet(f"ibge1_{timestamp_atual}.parquet", index=False)
df_ibge2.to_parquet(f"ibge2_{timestamp_atual}.parquet", index=False) # modificar para cloud storage