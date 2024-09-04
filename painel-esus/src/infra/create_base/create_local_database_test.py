# pylint: disable=W1514,W1309,W0622,W0611
import polars as pl
import sqlalchemy
from sqlalchemy import text
from src.infra.create_base.create_local_database import CreateLocalDataBase
from src.infra.db.repositories.sqls import LISTAGEM_FCI
from src.infra.db.settings.connection import DBConnectionHandler
from src.infra.db.settings.connection_local import DBConnectionHandler as LocalDBConnectionHandler


def test_create_base():
    pl.Config.set_streaming_chunk_size(1)
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
        "total_dias": pl.UInt64()
    }

    local_db = LocalDBConnectionHandler()

    local_engine = local_db.get_engine()
    with local_db.get_engine().connect() as db_con:
        sql = f'DROP TABLE demograficos;'
        db_con.execute(text(sql))

    with DBConnectionHandler() as db:
        # with LocalDBConnectionHandler() as db_local:
        (pl.read_database(LISTAGEM_FCI,
                          connection=db.get_engine(),
                          schema_overrides=schema_overrides)
            .lazy()
            .collect(streaming=True)
            .with_columns(
                pl.col(f'dt_obito').dt.strftime('%Y-%m-%d'),
                pl.col(f'co_dim_tempo_nascimento').dt.strftime('%Y-%m-%d')
        )
            .write_database(
                if_table_exists='append',
                connection=local_engine,
                table_name='demograficos',
                engine="sqlalchemy"
        )
        )

        print(pl.read_database("select count(*) from demograficos;",
              connection=local_engine, schema_overrides=schema_overrides).item(0, 0))
