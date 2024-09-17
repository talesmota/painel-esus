# pylint: disable=C0116,W1514
import psycopg2
from src.infra.db.settings.connection import DBConnectionHandler


class ExportTables:
    def __init__(self): ...

    def export_table(
        self,
        query: str,
        table_file_name: str,
    ):
        with DBConnectionHandler() as con:
            conn = psycopg2.connect(con.get_dns_string())
            cur = conn.cursor()
            query = cur.mogrify(query)
            query = query.decode("utf-8")

        with open(f"{table_file_name}", "w") as f:
            cur.copy_expert(
                "COPY ({}) TO STDOUT WITH DELIMITER ';' CSV HEADER ENCODING 'UTF-8'".format(
                    query
                ),
                f,
            )
