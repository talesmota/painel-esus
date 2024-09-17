from src.infra.create_base.export_tables import ExportTables


def test_export_tables():
    export_tables = ExportTables()
    tables = [
        # ("tb_fat_cidadao_pec", "exported_data/tb_fat_cidadao_pec.csv"),
        # ("tb_cidadao", "exported_data/tb_cidadao.csv"),
        # ("tb_fat_cad_individual", "exported_data/tb_fat_cad_individual.csv"),
        # ("tb_dim_equipe", "exported_data/tb_dim_equipe.csv"),
        # ("tb_dim_profissional", "exported_data/tb_dim_profissional.csv"),
        # ("tb_prof_historico_cns", "exported_data/tb_prof_historico_cns.csv"),
        # ("tb_prof", "exported_data/tb_prof.csv"),
        # ("tb_dim_sexo", "exported_data/tb_dim_sexo.csv"),
        # ("tb_dim_raca_cor", "exported_data/tb_dim_raca_cor.csv"),
        # ("tb_dim_equipe", "exported_data/tb_dim_equipe.csv"),
        # ("tb_equipe", "exported_data/tb_equipe.csv"),
        # ("tb_tipo_equipe", "exported_data/tb_tipo_equipe.csv"),
        # ("tb_fat_familia_territorio", "exported_data/tb_fat_familia_territorio.csv"),
        # ("tb_fat_cad_domiciliar", "exported_data/tb_fat_cad_domiciliar.csv"),
        ("tb_fat_atendimento_odonto", "exported_data/tb_fat_atendimento_odonto.csv"),
    ]
    for table in tables:
        export_tables.export_table(
            f"select * from {table[0]}",
            table[1],
        )
