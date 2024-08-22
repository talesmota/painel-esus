# pylint: disable=invalid-name
# pylint: disable=unused-import
# pylint: disable=redefined-outer-name
# pylint: disable=E0401
import math
import os
from pathlib import Path
from typing import Dict

import pandas as pd
import polars as pl
import pytest
from pandas import DataFrame
from sqlalchemy import create_engine
from src.data.use_cases.tests.data_frame_mocks.atendimento_individual_df import atendimento_individual_df
from src.data.use_cases.tests.data_frame_mocks.atendimento_individual_df import atendimento_individual_df_empty
from src.data.use_cases.tests.data_frame_mocks.atendimento_individual_df import atendimento_individual_df_no_rurals
from src.data.use_cases.tests.data_frame_mocks.atendimento_individual_df import atendimento_individual_df_only_rurals
from src.data.use_cases.tests.data_frame_mocks.calc_age_data_frame import \
    calc_age_df
from src.data.use_cases.tests.data_frame_mocks.demographics_info_df import demographics_info_df
from src.data.use_cases.tests.data_frame_mocks.demographics_info_df import demographics_info_df_empty
from src.domain.entities.diabetes import Diabetes
from src.domain.entities.hypertension import Hypertension
from src.domain.entities.pregnancy import Pregnants
from src.env.conf import env
from src.errors import InvalidArgument
from src.infra.create_base.create_local_database import CreateLocalDataBase
from src.infra.db.repositories.sqls.listagem_fci import LISTAGEM_FCI
from src.infra.db.repositories.sqls.listagem_fci import LISTAGEM_FCI_COUNT
from src.infra.db.settings.connection import DBConnectionHandler
from src.infra.db.settings.connection_local import \
    DBConnectionHandler as LocalDBConnectionHandler

from .demographics_info import DemographicsInfoRepository


@pytest.mark.usefixtures("calc_age_df")
def test_calculate_age(calc_age_df):
    df = calc_age_df
    demographics_info = DemographicsInfoRepository()
    response = demographics_info.parse_date(df)
    assert response['idade'].iloc[0] == 1
    assert response['idade'].iloc[1] == 35


@pytest.mark.usefixtures("demographics_info_df")
def test_retrieve_demographics_info(demographics_info_df):

    demographics_info = DemographicsInfoRepository()
    response = demographics_info.retrieve_demographics_info(
        demographics_info_df)

    assert response['total'] == 10
    assert response['locationArea']['rural'] == 4
    assert response['locationArea']['urbano'] == 6
    assert response['gender']['feminino'] == 5
    assert response['gender']['masculino'] == 5

    assert response['ageGroups']['Masculino']['0 a 5 anos']['Rural'] == 1
    assert response['ageGroups']['Masculino']['13 a 17 anos']['Rural'] == 1

    assert response['ageGroups']['Masculino']['30 a 44 anos']['Urbano'] == 1
    assert response['ageGroups']['Masculino']['45 a 59 anos']['Urbano'] == 1
    assert response['ageGroups']['Masculino']['60 + anos']['Urbano'] == 1

    assert response['ageGroups']['Feminino']['0 a 5 anos']['Rural'] == 1
    assert response['ageGroups']['Feminino']['6 a 12 anos']['Rural'] == 1

    assert response['ageGroups']['Feminino']['30 a 44 anos']['Urbano'] == 1
    assert response['ageGroups']['Feminino']['45 a 59 anos']['Urbano'] == 1
    assert response['ageGroups']['Feminino']['60 + anos']['Urbano'] == 1


@pytest.mark.usefixtures("demographics_info_df_empty")
def test_retrieve_demographics_info_when_empty_data(demographics_info_df_empty):
    demographics_info = DemographicsInfoRepository()
    response = demographics_info.retrieve_demographics_info(
        demographics_info_df_empty)
    assert response['total'] == 0
    assert response['locationArea']['rural'] == 0
    assert response['locationArea']['urbano'] == 0
    assert response['gender']['feminino'] == 0
    assert response['gender']['masculino'] == 0

    assert response['ageGroups']['Masculino']['0 a 5 anos']['Rural'] == 0
    assert response['ageGroups']['Masculino']['13 a 17 anos']['Rural'] == 0

    assert response['ageGroups']['Masculino']['30 a 44 anos']['Urbano'] == 0
    assert response['ageGroups']['Masculino']['45 a 59 anos']['Urbano'] == 0
    assert response['ageGroups']['Masculino']['60 + anos']['Urbano'] == 0

    assert response['ageGroups']['Feminino']['0 a 5 anos']['Rural'] == 0
    assert response['ageGroups']['Feminino']['6 a 12 anos']['Rural'] == 0

    assert response['ageGroups']['Feminino']['30 a 44 anos']['Urbano'] == 0
    assert response['ageGroups']['Feminino']['45 a 59 anos']['Urbano'] == 0
    assert response['ageGroups']['Feminino']['60 + anos']['Urbano'] == 0


def test_retrieve_demographics_info_when_invalid_argument():
    demographics_info = DemographicsInfoRepository()
    with pytest.raises(InvalidArgument) as exc:
        demographics_info.retrieve_demographics_info([])
        assert exc.message == 'df must be a DataFrame instance'


@pytest.mark.usefixtures('atendimento_individual_df')
def test_parse_indicators(atendimento_individual_df):
    hypertension = Hypertension()
    hypertension_df = hypertension.filter_registers(atendimento_individual_df)

    diabetes = Diabetes()
    diabetes_df = diabetes.filter_registers(atendimento_individual_df)

    gestantes = Pregnants()
    gestantes_df = gestantes.filter_registers(atendimento_individual_df)

    demographics = DemographicsInfoRepository()
    demographics.parse_indicators(
        diabetes=diabetes_df,
        hipertensao=hypertension_df,
        gestantes=gestantes_df
    )
    response = demographics.indicators
    assert response['diabetes']['rural'] == 1
    assert response['diabetes']['urbano'] == 1

    assert response['gestantes']['rural'] == 1
    assert response['gestantes']['urbano'] == 1

    assert response['hipertensao']['rural'] == 1
    assert response['hipertensao']['urbano'] == 1


@pytest.mark.usefixtures('atendimento_individual_df_no_rurals')
def test_parse_indicators_no_rurals(atendimento_individual_df_no_rurals):
    hypertension = Hypertension()
    hypertension_df = hypertension.filter_registers(
        atendimento_individual_df_no_rurals)

    diabetes = Diabetes()
    diabetes_df = diabetes.filter_registers(
        atendimento_individual_df_no_rurals)

    gestantes = Pregnants()
    gestantes_df = gestantes.filter_registers(
        atendimento_individual_df_no_rurals)

    demographics = DemographicsInfoRepository()
    demographics.parse_indicators(
        diabetes=diabetes_df,
        hipertensao=hypertension_df,
        gestantes=gestantes_df
    )
    response = demographics.indicators
    assert response['diabetes']['rural'] == 0
    assert response['diabetes']['urbano'] == 2

    assert response['gestantes']['rural'] == 0
    assert response['gestantes']['urbano'] == 2

    assert response['hipertensao']['rural'] == 0
    assert response['hipertensao']['urbano'] == 2


@pytest.mark.usefixtures('atendimento_individual_df_only_rurals')
def test_parse_indicators_only_rurals(atendimento_individual_df_only_rurals):
    hypertension = Hypertension()
    hypertension_df = hypertension.filter_registers(
        atendimento_individual_df_only_rurals)

    diabetes = Diabetes()
    diabetes_df = diabetes.filter_registers(
        atendimento_individual_df_only_rurals)

    gestantes = Pregnants()
    gestantes_df = gestantes.filter_registers(
        atendimento_individual_df_only_rurals)

    demographics = DemographicsInfoRepository()
    demographics.parse_indicators(
        diabetes=diabetes_df,
        hipertensao=hypertension_df,
        gestantes=gestantes_df
    )
    response = demographics.indicators
    assert response['diabetes']['rural'] == 2
    assert response['diabetes']['urbano'] == 0

    assert response['gestantes']['rural'] == 2
    assert response['gestantes']['urbano'] == 0

    assert response['hipertensao']['rural'] == 2
    assert response['hipertensao']['urbano'] == 0


@pytest.mark.usefixtures('atendimento_individual_df_only_rurals')
def test_parse_indicators_empty(atendimento_individual_df_empty):
    hypertension = Hypertension()
    hypertension_df = hypertension.filter_registers(
        atendimento_individual_df_empty)

    diabetes = Diabetes()
    diabetes_df = diabetes.filter_registers(atendimento_individual_df_empty)

    gestantes = Pregnants()
    gestantes_df = gestantes.filter_registers(atendimento_individual_df_empty)

    demographics = DemographicsInfoRepository()
    demographics.parse_indicators(
        diabetes=diabetes_df,
        hipertensao=hypertension_df,
        gestantes=gestantes_df
    )
    response = demographics.indicators
    assert response['diabetes']['rural'] == 0
    assert response['diabetes']['urbano'] == 0

    assert response['gestantes']['rural'] == 0
    assert response['gestantes']['urbano'] == 0

    assert response['hipertensao']['rural'] == 0
    assert response['hipertensao']['urbano'] == 0


@pytest.mark.usefixtures('atendimento_individual_df_only_rurals')
def test_parse_indicators_with_invalid_arguments(atendimento_individual_df_only_rurals):
    demographics = DemographicsInfoRepository()
    with pytest.raises(InvalidArgument) as exc:
        demographics.parse_indicators(
            diabetes=[],
            hipertensao=[],
            gestantes=[]
        )
        assert exc.message == 'diabetes must be a DataFrame instance'

    diabetes = Diabetes()
    diabetes_df = diabetes.filter_registers(
        atendimento_individual_df_only_rurals)
    with pytest.raises(InvalidArgument) as exc:
        demographics.parse_indicators(
            diabetes=diabetes_df,
            hipertensao=[],
            gestantes=[]
        )
        assert exc.message == 'hipertensao must be a DataFrame instance'

        hypertension = Hypertension()
        hypertension_df = hypertension.filter_registers(
            atendimento_individual_df_only_rurals)
        with pytest.raises(InvalidArgument) as exc:
            demographics.parse_indicators(
                diabetes=diabetes_df,
                hipertensao=hypertension_df,
                gestantes=[]
            )
            assert exc.message == 'gestantes must be a DataFrame instance'


@pytest.mark.skip(reason="Avoid hit on BD")
def test_get_demographics_info():
    demographics = DemographicsInfoRepository()
    response = demographics.get_demographics_info()
    assert isinstance(response, Dict)

    response = demographics.get_demographics_info(1)
    assert isinstance(response, Dict)


@pytest.mark.skip(reason="Avoid hit on BD")
def test_get_demographics_info_invalid_argument():
    demographics = DemographicsInfoRepository()
    with pytest.raises(InvalidArgument) as exc:
        demographics.get_demographics_info('1')
        assert exc.message == 'CNES must be int'


def test_ibge_number():
    df = pd.read_csv('ibge.csv', sep=";")
    ibge = int(env.get("CIDADE_IBGE", 0))
    if ibge == '-':
        ibge_population = 0
    else:
        df_ibge = df[df['IBGE'] == ibge]
        ibge_population = df_ibge['POPULACAO'].iloc[0]
        ibge_population = f'{ibge_population:_.0f}'.replace('_', '.')

    assert ibge_population == '6.877'


def test_path_csv():

    path = os.getcwd()
    path = Path(path)
    path = os.path.join(path, 'ibge.csv')
    print(path)


def test_load_db():
    with DBConnectionHandler() as db:

        sum_ = 0
        # total = pl.read_database(
        #     query=f'{LISTAGEM_FCI_COUNT};',
        #     connection=db.get_engine(),
        # )
        # total = total.with_columns(pl.col('qtd')).item(0, 0)
        # # assert total == 6803
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
        for df in pl.read_database(
            query=f'{LISTAGEM_FCI};',
            connection=db.get_engine(),
            schema_overrides=schema_overrides,
            iter_batches=True,
            batch_size=1000,
        ):
            sum_ += df.shape[0]
            with LocalDBConnectionHandler() as db_local:
                for i in schema_overrides.items():
                    if isinstance(i[1], pl.Date):
                        df = df.with_columns(
                            pl.col(f'{i[0]}').dt.strftime('%Y-%m-%d')
                        )

                df.write_database(
                    table_name="demografico",
                    connection=db_local.get_engine(),
                    if_table_exists='append',
                    engine='sqlalchemy'
                )
        # assert total == sum_


def test_load_db_class():
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

    create_local_database = CreateLocalDataBase(
        DBConnectionHandler(),
        LocalDBConnectionHandler()
    )
    create_local_database.create_table(
        name="demograficos", sql=sql, schema_overrides=schema_overrides)


def test_drop_table():
    create_local_database = CreateLocalDataBase(
        DBConnectionHandler(),
        LocalDBConnectionHandler()
    )
    create_local_database.drop_table('demografico')
