# pylint: disable=W1514,W1309
import os

import polars as pl
import psycopg2
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from src.env.conf import env
from src.infra.db.repositories.sqls import LISTAGEM_FCI
from src.infra.db.settings.connection_local import \
    DBConnectionHandler as LocalDBConnectionHandler

dsn_string = "dbname={} user={} password='{}' host={} port={}".format(
    env.get("DB_DATABASE", "-"),
    env.get("DB_USER", "-"),
    env.get("DB_PASSWORD", "-"),
    env.get("DB_HOST", "-"),
    env.get("DB_PORT", "-")
)
conn = psycopg2.connect(dsn_string)

cur = conn.cursor()


query = cur.mogrify(LISTAGEM_FCI)
query = query.decode('utf-8')

with open('out.csv', 'w') as f:
    cur.copy_expert(
        "COPY ({}) TO STDOUT WITH DELIMITER ';' CSV HEADER ENCODING 'UTF-8'".format(query), f)
schema_overrides = {
    "id_cidadao_pec": pl.UInt64(),
    "co_cidadao": pl.String(),
    "nu_cns": pl.String(),
    "nu_cpf": pl.String(),
    "no_cidadao": pl.String(),
    "co_dim_tempo_nascimento": pl.Date(),
    "no_social_cidadao": pl.String(),
    "co_dim_unidade_saude": pl.UInt64(),
    "co_dim_sexo": pl.UInt64(),
    "sg_sexo": pl.String(),
    "co_dim_identidade_genero": pl.UInt64(),
    "ds_raca_cor": pl.String(),
    "co_dim_tipo_localizacao": pl.UInt64(),
    "no_mae_cidadao": pl.String(),
    "no_pai_cidadao": pl.String(),
    "no_profissional": pl.String(),
    "dt_obito": pl.Date(),
    "nu_declaracao_obito": pl.String(),
    "idade": pl.UInt64(),
    "total_meses": pl.UInt64(),
    "total_dias": pl.String()
}


df = pl.read_csv('out.csv', separator=';', schema_overrides=schema_overrides)
with LocalDBConnectionHandler() as db_local:
    try:
        sql = f'DROP TABLE demograficos;'

        db_local.get_engine().connect().execute(text(sql))
    except OperationalError:
        print('Tabela demograficos não existe. Criando...')

    for i in schema_overrides.items():
        if isinstance(i[1], pl.Date):
            df = df.with_columns(
                pl.col(f'{i[0]}').dt.strftime('%Y-%m-%d')
            )

    df.write_database(
        table_name='demograficos',
        connection=db_local.get_engine(),
        if_table_exists='append',
        engine='sqlalchemy'
    )

os.remove('out.csv')
