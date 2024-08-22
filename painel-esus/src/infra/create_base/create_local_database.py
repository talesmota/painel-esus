import polars as pl
from sqlalchemy import text
from src.infra.db.settings.db_connection_handler_interface import \
    DBConnectionHandlerInterface


class CreateLocalDataBase:

    def __init__(self, db_connection_handler: DBConnectionHandlerInterface, local_db_connection_handler: DBConnectionHandlerInterface,):
        self.db_connection_handler = db_connection_handler
        self.local_db_connection_handler = local_db_connection_handler

    def create_table(self, name, sql, schema_overrides):

        with self.db_connection_handler as db:
            for df in pl.read_database(
                query=sql,
                connection=db.get_engine(),
                schema_overrides=schema_overrides,
                iter_batches=True,
                batch_size=1000,
            ):
                with self.local_db_connection_handler as db_local:
                    for i in schema_overrides.items():
                        if isinstance(i[1], pl.Date):
                            df = df.with_columns(
                                pl.col(f'{i[0]}').dt.strftime('%Y-%m-%d')
                            )

                    df.write_database(
                        table_name=name,
                        connection=db_local.get_engine(),
                        if_table_exists='append',
                        engine='sqlalchemy'
                    )

    def drop_table(self, name):
        with self.local_db_connection_handler.get_engine().connect() as db_con:
            sql = f'DROP TABLE {name};'
            db_con.execute(text(sql))
