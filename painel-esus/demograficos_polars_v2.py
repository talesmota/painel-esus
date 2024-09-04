# pylint: disable=W1514,W1309,C0103,W0611,W0612,R1703,W0622
import random
import sys
from time import sleep

import polars as pl
from sqlalchemy import text
from sqlalchemy.exc import InternalError
from sqlalchemy.exc import OperationalError
from src.infra.create_base.create_local_database import CreateLocalDataBase
from src.infra.db.repositories.sqls import LISTAGEM_FCI
from src.infra.db.settings.connection import DBConnectionHandler
from src.infra.db.settings.connection_local import \
    DBConnectionHandler as LocalDBConnectionHandler

sql_fci = '''
select
distinct on (t1.co_seq_fat_cidadao_pec)
    t1.co_seq_fat_cidadao_pec as id_cidadao_pec,
    t1.co_cidadao,
    t2.nu_cns,
    t2.nu_cpf,
    t2.no_cidadao,
    t2.dt_nascimento as co_dim_tempo_nascimento,
    upper(t2.no_social) as no_social_cidadao,
    t3.co_dim_unidade_saude as co_dim_unidade_saude,
    t1.co_dim_sexo,
    t15.sg_sexo,
    t1.co_dim_identidade_genero,
    t16.ds_raca_cor,
    coalesce( tfcd.co_dim_tipo_localizacao, '1') co_dim_tipo_localizacao,
    upper(t2.no_mae) as no_mae_cidadao, upper(t2.no_pai) as no_pai_cidadao,
    upper(t13.no_profissional) as no_profissional,
    t2.dt_obito, t2.nu_documento_obito as nu_declaracao_obito,
    extract(year from age(current_date,t2.dt_nascimento)) as idade,
    extract(year from age(current_date, t2.dt_nascimento)) * 12 + extract(month from age(current_date, t2.dt_nascimento)) as total_meses,
    current_date - (t2.dt_nascimento  -  INTERVAL '1 DAY') as total_dias
from
    tb_fat_cidadao_pec t1
    inner join tb_cidadao t2 on t1.co_cidadao = t2.co_seq_cidadao
    left join tb_fat_cad_individual t3 on t2.co_unico_ultima_ficha  = t3.nu_uuid_ficha
    left join tb_dim_unidade_saude t4_fci on t3.co_dim_unidade_saude = t4_fci.co_seq_dim_unidade_saude
    left join tb_dim_equipe t5_fci on t3.co_dim_equipe = t5_fci.co_seq_dim_equipe
    left join tb_dim_unidade_saude t8_cc on t8_cc.co_seq_dim_unidade_saude = t1.co_dim_unidade_saude_vinc
    left join tb_dim_equipe t9_cc on t9_cc.co_seq_dim_equipe = t1.co_dim_equipe_vinc
    left join tb_dim_profissional t11 on t11.co_seq_dim_profissional = t3.co_dim_profissional
    left join tb_prof_historico_cns t12 on t12.nu_cns = t11.nu_cns
    left join tb_prof t13 on t13.co_seq_prof = t12.co_prof
    left join tb_dim_sexo t15 on  t15.co_seq_dim_sexo = t3.co_dim_sexo
    left join tb_dim_raca_cor t16 on t3.co_dim_raca_cor = t16.co_seq_dim_raca_cor
    left join tb_equipe t29 on t29.nu_ine = t9_cc.nu_ine --equipe cc
    left join tb_tipo_equipe t30 on t29.tp_equipe = t30.co_seq_tipo_equipe
    left join tb_fat_familia_territorio tfft on tfft.co_fat_cidadao_pec = t1.co_seq_fat_cidadao_pec
    left join tb_fat_cad_domiciliar tfcd on tfcd.co_seq_fat_cad_domiciliar = tfft.co_fat_cad_domiciliar
where
    (
        (t2.st_ativo = 1 and t3.co_dim_tipo_saida_cadastro = 3 and t5_fci.nu_ine is not null and t3.st_ficha_inativa = 0)
        or
        (t3.co_dim_tipo_saida_cadastro is null and t2.st_ativo = 1 and t2.st_faleceu = 0 and t29.st_ativo = 1 and t30.nu_ms in ('70', '76'))
    ) 
'''


def test_create_base(args):

    if len(args) > 1:
        chunk_size = int(args[1]) if args[1] else 1000
    else:
        chunk_size = 1000
    print('chunk_size', chunk_size)
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

    with local_db.get_engine().connect() as db_local:
        try:
            sql = f'DROP TABLE demograficos;'

            db_local.execute(text(sql))
        except OperationalError:
            print('Tabela demograficos não existe. Criando...')

    next = True
    offset = 0

    while next:
        with DBConnectionHandler() as db:
            try:
                df = pl.read_database(
                    query=f'{sql_fci}  LIMIT {chunk_size} OFFSET {offset};',
                    connection=db.get_engine(),
                    schema_overrides=schema_overrides,
                )
                df = df.with_columns(
                    pl.col(f'dt_obito').dt.strftime('%Y-%m-%d'),
                    pl.col(f'co_dim_tempo_nascimento').dt.strftime('%Y-%m-%d')
                )

                if df.shape[0] is not None and df.shape[0] > 0:
                    next = True
                else:
                    next = False

                offset += chunk_size

                with LocalDBConnectionHandler() as db_local:
                    df.write_database(
                        table_name="demograficos",
                        connection=db_local.get_engine(),
                        if_table_exists="append",
                        engine="sqlalchemy",
                    )
                    print('total Registros: ', pl.read_database("select count(*) from demograficos;",
                                                                connection=db_local.get_engine(), schema_overrides=schema_overrides).item(0, 0))
            except InternalError as exc:
                if 'invalid DSA memory alloc request size' in str(exc):
                    print('Error detectado. Aguardando 2s para tentar novamente.')
                    sleep(2)


test_create_base(sys.argv)
