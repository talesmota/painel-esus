import polars as pl
from sqlalchemy.exc import OperationalError
from sqlalchemy.exc import ResourceClosedError
from src.data.interfaces.create_bases.create_bases_repository import \
    CreateBasesRepositoryInterface
from src.errors import InvalidArgument
from src.errors import NoSuchTableError
from src.errors.logging import logging
from src.infra.create_base.create_local_database import CreateLocalDataBase
from src.infra.db.repositories.sqls import ATENDIMENTO_INDIVIDUAL_CID_CIAPS


class CreateLocalAtendimentoIndividualFiltroDatabase(CreateLocalDataBase, CreateBasesRepositoryInterface):
    table_name = 'atendimento_individual_cid_ciaps'

    def get_base(self):
        return self.table_name

    def create_base(self):
        schema_overrides = {
            "co_seq_fat_atd_ind": pl.UInt64(),
            "co_dim_tempo": pl.String(),
            "nu_cns": pl.String(),
            "nu_peso": pl.UInt64(),
            "nu_altura": pl.UInt64(),
            "co_dim_unidade_saude": pl.UInt64(),
            "co_dim_faixa_etaria": pl.UInt64(),
            "co_dim_sexo": pl.UInt64(),
            "co_dim_local_atendimento": pl.UInt64(),
            "co_fat_cidadao_pec": pl.UInt64(),
            "nu_cpf_cidadao": pl.String(),
            "codigo": pl.String(),
            "tipo": pl.String(),
            "co_dim_cbo_1": pl.UInt64()
        }
        sql = ATENDIMENTO_INDIVIDUAL_CID_CIAPS
        self.create_table(self.table_name, sql, schema_overrides)
        logging.info(
            f"Creating Base {self.table_name}!")

    def destroy_base(self):
        if self.table_name is None:
            raise InvalidArgument('Creation base not passed.')
        try:
            self.drop_table(self.table_name)
        except (OperationalError, ResourceClosedError) as exc:
            raise NoSuchTableError(
                f'No {self.table_name} table found') from exc
