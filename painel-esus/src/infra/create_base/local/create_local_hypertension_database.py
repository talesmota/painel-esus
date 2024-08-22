import polars as pl
from sqlalchemy.exc import OperationalError
from sqlalchemy.exc import ResourceClosedError
from src.data.interfaces.create_bases.create_bases_repository import \
    CreateBasesRepositoryInterface
from src.errors import InvalidArgument
from src.errors import NoSuchTableError
from src.infra.create_base.create_local_database import CreateLocalDataBase
from src.infra.db.repositories.sqls.listagem_fci import LISTAGEM_FCI


class CreateLocalDemographicDatabase(CreateBasesRepositoryInterface, CreateLocalDataBase,):
    table_name = 'demograficos'

    def create_base(self):
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
        sql = LISTAGEM_FCI
        self.create_table(self.table_name, sql, schema_overrides)

    def destroy_base(self):
        if self.table_name is None:
            raise InvalidArgument('Creation base not passed.')
        try:
            self.drop_table(self.table_name)
        except (OperationalError, ResourceClosedError) as exc:
            raise NoSuchTableError(
                f'No {self.table_name} table found') from exc
