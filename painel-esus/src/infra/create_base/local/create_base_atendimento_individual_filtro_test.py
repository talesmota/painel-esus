from src.infra.create_base.local.create_base_factory import CreateLocalDatabaseFactory


def test_destroy_base():
    atendimento = CreateLocalDatabaseFactory.atendimento_individual_filtro_factory()
    atendimento.destroy_base()


def test_create_base():
    atendimento = CreateLocalDatabaseFactory.atendimento_individual_filtro_factory()
    atendimento.create_base()
