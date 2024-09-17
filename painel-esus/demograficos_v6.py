from pprint import pprint

import polars as pl


def print_types(df):
    pprint(list(zip(df.columns, df.dtypes)))


cidadao_pec = pl.read_csv(
    "exported_data/tb_fat_cidadao_pec.csv", separator=";", encoding="utf8"
).with_columns(
    pl.col("co_cidadao").fill_null(value=0).str.to_integer(base=10, strict=False),
)

tb_cidadao = pl.read_csv("exported_data/tb_cidadao.csv", separator=";", encoding="utf8")


cad_individual = pl.read_csv(
    "exported_data/tb_fat_cad_individual.csv",
    separator=";",
    encoding="utf8",
)

dim_equipe = pl.read_csv(
    "exported_data/tb_dim_equipe.csv", separator=";", encoding="utf8"
)

dim_profissional = pl.read_csv(
    "exported_data/tb_dim_profissional.csv", separator=";", encoding="utf8"
).with_columns(
    pl.col("nu_cns").cast(pl.String()),
)


prof_historico = pl.read_csv(
    "exported_data/tb_prof_historico_cns.csv", separator=";", encoding="utf8"
).with_columns(
    pl.col("nu_cns").cast(pl.String()),
)


prof = pl.read_csv("exported_data/tb_prof.csv", separator=";", encoding="utf8")
dim_sexo = pl.read_csv("exported_data/tb_dim_sexo.csv", separator=";", encoding="utf8")
raca_cor = pl.read_csv(
    "exported_data/tb_dim_raca_cor.csv", separator=";", encoding="utf8"
)
tb_equipe = pl.read_csv("exported_data/tb_equipe.csv", separator=";", encoding="utf8")
tipo_equipe = pl.read_csv(
    "exported_data/tb_tipo_equipe.csv", separator=";", encoding="utf8"
)
familia_territorio = pl.read_csv(
    "exported_data/tb_fat_familia_territorio.csv", separator=";", encoding="utf8"
)
cad_domiciliar = pl.read_csv(
    "exported_data/tb_fat_cad_domiciliar.csv", separator=";", encoding="utf8"
)

cidadao_pec = cidadao_pec.join(
    tb_cidadao,
    left_on=["co_cidadao"],
    right_on=["co_seq_cidadao"],
    suffix="_tb_cidadao",
    how="inner",
)
cidadao_pec = cidadao_pec.join(
    cad_individual,
    left_on=["co_unico_ultima_ficha"],
    right_on=["nu_uuid_ficha"],
    suffix="_cad_individual",
    how="left",
)
cidadao_pec = cidadao_pec.join(
    dim_equipe,
    left_on=["co_dim_equipe"],
    right_on=["co_seq_dim_equipe"],
    suffix="_cad_individual",
    how="left",
)
cidadao_pec = cidadao_pec.join(
    dim_profissional,
    left_on=["co_dim_profissional"],
    right_on=["co_seq_dim_profissional"],
    suffix="_dim_profissional",
    how="left",
)
cidadao_pec = cidadao_pec.join(
    prof_historico,
    left_on=["nu_cns"],
    right_on=["nu_cns"],
    suffix="_prof_historico",
    how="left",
)
cidadao_pec = cidadao_pec.join(
    prof,
    left_on=["co_pro"],
    right_on=["co_seq_pro"],
    suffix="_pro",
    how="left",
).with_columns(
    pl.col("co_dim_sexo")
    .cast(pl.String())
    .fill_null(value=0)
    .str.to_integer(base=10, strict=False),
)
cidadao_pec = cidadao_pec.join(
    dim_sexo,
    left_on=["co_dim_sexo"],
    right_on=["co_seq_dim_sexo"],
    suffix="_dim_sexo",
    how="left",
)
cidadao_pec = cidadao_pec.join(
    raca_cor,
    left_on=["co_dim_raca_cor"],
    right_on=["co_seq_dim_raca_cor"],
    suffix="_raca_cor",
    how="left",
).with_columns(
    pl.col("co_dim_equipe_vinc")
    .cast(pl.String())
    .fill_null(value=0)
    .str.to_integer(base=10, strict=False),
)
cidadao_pec = cidadao_pec.join(
    dim_equipe,
    left_on=["co_dim_equipe_vinc"],
    right_on=["co_seq_dim_equipe"],
    suffix="_dim_equipe",
    how="left",
).with_columns(
    pl.col("nu_ine")
    .cast(pl.String())
    .fill_null(value=0)
    .str.to_integer(base=10, strict=False),
)
cidadao_pec = cidadao_pec.join(
    tb_equipe,
    left_on=["nu_ine"],
    right_on=["nu_ine"],
    suffix="_tb_equipe",
    how="left",
)
cidadao_pec = cidadao_pec.join(
    tipo_equipe,
    left_on=["tp_equipe"],
    right_on=["co_seq_tipo_equipe"],
    suffix="_tipo_equipe",
    how="left",
)
cidadao_pec = cidadao_pec.join(
    familia_territorio,
    left_on=["co_seq_fat_cidadao_pec"],
    right_on=["co_fat_cidadao_pec"],
    suffix="_familia_territorio",
    how="left",
)
cidadao_pec = cidadao_pec.join(
    cad_domiciliar,
    left_on=["co_fat_cad_domiciliar"],
    right_on=["co_seq_fat_cad_domiciliar"],
    suffix="_cad_domiciliar",
    how="left",
).with_columns(
    pl.col("st_faleceu").fill_null(value=0).str.to_integer(base=10, strict=False),
)
cidadao_pec = cidadao_pec.filter(
    (
        (pl.col("st_ativo") == 1)
        & (pl.col("co_dim_tipo_saida_cadastro") == 3)
        & (~pl.col("nu_ine_dim_equipe").is_null())
        & (pl.col("st_ficha_inativa") == 0)
    )
    | (
        (pl.col("co_dim_tipo_saida_cadastro").is_null())
        & (pl.col("st_ativo") == 1)
        & (pl.col("st_faleceu") == 0)
        & (pl.col("st_ativo_tb_equipe") == 1)
        & (pl.col("nu_ms").is_in(["70", "76"]))
    )
)
print(cidadao_pec.shape)
