# pylint: disable=W1514,W1309,W0611,R1703,W0612,W0622,C0103
import duckdb
import polars as pl
from src.infra.db.settings.connection_local import (
    DBConnectionHandler as LocalDBConnectionHandler,
)


print("iniciando")

sql = """
SELECT DISTINCT ON (t1.co_seq_fat_cidadao_pec)
    t1.co_seq_fat_cidadao_pec AS id_cidadao_pec,
    t1.co_cidadao,
    t2.nu_cns,
    t2.nu_cpf,
    t2.no_cidadao,
    t2.dt_nascimento AS co_dim_tempo_nascimento,
    UPPER(t2.no_social) AS no_social_cidadao,
    t3.co_dim_unidade_saude AS co_dim_unidade_saude,
    t1.co_dim_sexo,
    t15.sg_sexo,
    t1.co_dim_identidade_genero,
    t16.ds_raca_cor,
    COALESCE(tfcd.co_dim_tipo_localizacao, '1') AS co_dim_tipo_localizacao,
    UPPER(t2.no_mae) AS no_mae_cidadao,
    UPPER(t2.no_pai) AS no_pai_cidadao,
    UPPER(t13.no_profissional) AS no_profissional,
    t2.dt_obito,
    t2.nu_documento_obito AS nu_declaracao_obito,
    EXTRACT(YEAR FROM AGE(CURRENT_DATE, t2.dt_nascimento)) AS idade,
    EXTRACT(YEAR FROM AGE(CURRENT_DATE, t2.dt_nascimento)) * 12 + EXTRACT(MONTH FROM AGE(CURRENT_DATE, t2.dt_nascimento)) AS total_meses
FROM
    read_csv('exported_data/tb_fat_cidadao_pec.csv', sep =';') as t1
    INNER JOIN read_csv('exported_data/tb_cidadao.csv', sep=';') as t2 ON t1.co_cidadao = t2.co_seq_cidadao
    LEFT JOIN read_csv('exported_data/tb_fat_cad_individual.csv', sep=';') as t3 ON t2.co_unico_ultima_ficha = t3.nu_uuid_ficha
    LEFT JOIN read_csv('exported_data/tb_dim_equipe.csv', sep=';') as t5_fci ON t3.co_dim_equipe = t5_fci.co_seq_dim_equipe
    LEFT JOIN read_csv('exported_data/tb_dim_profissional.csv', sep=';') as t11 ON t11.co_seq_dim_profissional = t3.co_dim_profissional
    LEFT JOIN read_csv('exported_data/tb_prof_historico_cns.csv', sep=';') as t12 ON t12.nu_cns = t11.nu_cns
    LEFT JOIN read_csv('exported_data/tb_prof.csv', sep=';') as t13 ON t13.co_seq_prof = t12.co_prof
    LEFT JOIN read_csv('exported_data/tb_dim_sexo.csv', sep=';') as t15 ON t15.co_seq_dim_sexo = t3.co_dim_sexo
    LEFT JOIN read_csv('exported_data/tb_dim_raca_cor.csv', sep=';') as t16 ON t3.co_dim_raca_cor = t16.co_seq_dim_raca_cor
    LEFT JOIN read_csv('exported_data/tb_dim_equipe.csv', sep=';') as t9_cc ON t9_cc.co_seq_dim_equipe = t1.co_dim_equipe_vinc
    LEFT JOIN read_csv('exported_data/tb_equipe.csv', sep=';') as t29 ON t29.nu_ine = t9_cc.nu_ine
    LEFT JOIN read_csv('exported_data/tb_tipo_equipe.csv', sep=';') as t30 ON t29.tp_equipe = t30.co_seq_tipo_equipe
    LEFT JOIN read_csv('exported_data/tb_fat_familia_territorio.csv', sep=';') as tfft ON tfft.co_fat_cidadao_pec = t1.co_seq_fat_cidadao_pec
    LEFT JOIN read_csv('exported_data/tb_fat_cad_domiciliar.csv', sep=';') as tfcd ON tfcd.co_seq_fat_cad_domiciliar = tfft.co_fat_cad_domiciliar
WHERE
    (
        (t2.st_ativo = 1 AND t3.co_dim_tipo_saida_cadastro = 3 AND t5_fci.nu_ine IS NOT NULL AND t3.st_ficha_inativa = 0)
        OR
        (t3.co_dim_tipo_saida_cadastro IS NULL AND t2.st_ativo = 1 AND t2.st_faleceu = 0 AND t29.st_ativo = 1 AND t30.nu_ms IN ('70', '76'))
    );
"""
schema_overrides = {
    "id_cidadao_pec": pl.UInt64(),
    "co_cidadao": pl.UInt64(),
    "nu_cns": pl.UInt64(),
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
}

demograficos = duckdb.sql(sql)
demograficos_pl = pl.DataFrame(
    duckdb.sql(sql).arrow(),
    schema_overrides=schema_overrides,
)
demograficos_pl = demograficos_pl.with_columns(
    pl.col(f"dt_obito").dt.strftime("%Y-%m-%d"),
    pl.col(f"co_dim_tempo_nascimento").dt.strftime("%Y-%m-%d"),
)
print(demograficos_pl.shape)
# with LocalDBConnectionHandler() as db_local:
#     print("local")

#     demograficos_pl.write_database(
#         table_name="demograficos",
#         connection=db_local.get_engine(),
#         if_table_exists="append",
#         engine="sqlalchemy",
#     )
