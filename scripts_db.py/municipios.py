import os

import mysql.connector
import pandas as pd

df = pd.read_csv(
    "scripts_db.py/dct_municipio_uf.csv", delimiter=";", encoding="iso-8859-1"
)

df.rename(
    columns={
        "id_uf_ibge": "uf_id_ibge",
        "sg_uf": "uf",
        "id_municipio_ibge": "id",
        "nm_municipio": "nome",
    },
    inplace=True,
)

print(df)

# host = os.environ["DB_HOST"]
# user = os.environ["DB_USER"]
# password = os.environ["DB_PASS"]
# database = os.environ["DB_NAME"]

# conexao = mysql.connector.connect(
#     host=host,
#     user=user,
#     password=password,
#     database=database
# )
# cursor = conexao.cursor()

# dados_para_inserir = [tuple(x) for x in df.to_numpy()]
# query = "INSERT INTO municipios (uf_id_ibge, uf, id, nome) VALUES (%s, %s, %s, %s)"
# cursor.executemany(query, dados_para_inserir)
# conexao.commit()
# cursor.close()
# conexao.close()
