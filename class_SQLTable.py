from __future__ import annotations  # fällt in Python 3.10 aus
from pathlib import Path
from typing import Union, Tuple, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from . import ExasolConnection

from config import _config as config


class SQLTable:
    table: str
    schema: str
    server: ExasolConnection

    # TODO: options.case = None OR autolower OR autoupper
    # TODO: EXA_ Tabellen autodetect ohne Schema https://docs.exasol.com/sql_references/metadata/metadata_system_tables.htm
    # TODO: Schema zu Tabellen ohne Schema erganzen
    #   Exception falls es mehrere Tabellen in unterschiedlichen Schemata gibt

    def __init__(self,
                 schema_or_schematable: Union[str, Tuple[str, str], SQLTable],
                 table: Optional[str] = None,
                 *,
                 server: Optional[ExasolConnection] = None) -> None:

        if not table:  # nur ein Parameter gesetzt
            if isinstance(schema_or_schematable, tuple):
                schematable_split = schema_or_schematable
            else:
                schematable_split = str(schema_or_schematable).split('.')

            if len(schematable_split) != 2:
                raise ValueError(f"Tabellenname {schema_or_schematable!r} mit Schema muss genau mit einem Punkt sein.")

            schema, table = schematable_split
            if len(schema) == 0: raise ValueError("Schema darf nicht leer sein")
            if len(table) == 0: raise ValueError("Tabellenname darf nicht leer sein")

        else:  # zwei Parameter gesetzt
            schema = schema_or_schematable
            if '.' in schema: raise ValueError(f"Schema {schema!r} darf keine Punkte beinhalten")
            if '.' in table: raise ValueError(
                f"Tabelle {table!r} darf beim gesetzten Schema {schema!r} keine Punkte beinhalten")
        # /if

        self.schema, self.table, self.server = schema, table, server

    # /def __init__

    def asstring(self) -> str:
        return self.schema + '.' + self.table

    def __str__(self) -> str:
        return self.asstring()

    def __repr__(self) -> str:
        return repr(str(self))

    def astuple(self) -> tuple:
        return self.schema, self.table

    def asdict(self, keys: [str, str] = None) -> dict:  # für unpacking **SQLTable
        if not keys:
            keys = ['schema', 'table']
        return {keys[0]: self.schema, keys[1]: self.table}

    def exists(self, server: Optional[ExasolConnection] = None) -> bool:
        server = server or self.server
        if not hasattr(server, 'import_from_pandas'):
            raise Exception("Ein Exasol Server muss definiert werden")

        return bool(server.list_tables(table=self.table, schema=self.schema).fetchone())

    # /def exists

    def truncate(self, server: Optional[ExasolConnection] = None):
        server = server or self.server
        if not hasattr(server, 'import_from_pandas'):
            raise Exception("Ein Exasol Server muss definiert werden")

        if self.exists(server):
            server.execute(f'truncate table {self};', echo=True)

    # /def truncate

    def drop(self, server: Optional[ExasolConnection] = None, if_exists=True):
        server = server or self.server
        if not hasattr(server, 'import_from_pandas'):
            raise Exception("Ein Exasol Server muss definiert werden")

        # if exists
        if if_exists and self.exists() or not if_exists:
            server.execute(f'drop table {self};', echo=True)

    def insert_from(self,
                    server_from: ExasolConnection,
                    table_from: Optional[str, SQLTable] = None,
                    *,
                    server: Optional[ExasolConnection] = None,
                    truncate=False,
                    where=''
                    ):

        server = server or self.server
        if not hasattr(server, 'import_from_pandas'):
            raise Exception("Ein Exasol Server muss definiert werden")

        if not hasattr(server_from, 'export_to_pandas'):
            raise Exception("server_from muss von Typ ExasolConnection sein")

        table_from = SQLTable(table_from or self, server=server_from)

        if str(self) == str(table_from):
            from_print = server_from.server.upper()
        else:
            from_print = f"{table_from} ({server_from.server.upper()})"

        print(f"Laden von {self} ({server.server.upper()}) aus {from_print}")

        if not self.exists():
            raise Exception(f"Die Zieltabelle {self} ist nicht da")

        if table_from.exists():
            df = server_from.export_to_pandas(f'select * from {table_from} {where}')
            print(f"{len(df.index)} Zeilen aus PROD importiert")
            if truncate:
                self.truncate(server)
            rowcount = server.import_from_pandas(df, self, rowcount=True)
            print(f"{rowcount} Zeilen nach DEV exportiert")
            print()
            return rowcount
        else:
            print(f'Tabelle {table_from!r} ist nicht da, INSERT wird nicht ausgeführt')

        print()
    # /def insert_from


# /class SQLTable


if __name__ == '__main__':
    from fiv4e import FIV4E

    fiv4e = FIV4E()

    fiv4e.exasol_dev.table('DWH.DIM_ZIELE').drop()
    fiv4e.exasol_dev.table('DWH_TECH.DWT_VTNR_ZIELE').drop()
    fiv4e.exasol_dev.table('DWH_TECH.DWT_BNR_ZIELE').drop()
    fiv4e.exasol_dev.table('DWH_TECH.DWT_VGB_ZIELE').drop()

    fiv4e.exasol_dev.table('DWH.DIM_ZIELE').insert_from(fiv4e.exasol_prod, truncate=True, where='limit 1')
    fiv4e.exasol_dev.table('DWH_TECH.DWT_VTNR_ZIELE').insert_from(fiv4e.exasol_prod, truncate=True, where='limit 1')
    fiv4e.exasol_dev.table('DWH_TECH.DWT_BNR_ZIELE').insert_from(fiv4e.exasol_prod, truncate=True, where='limit 1')
    fiv4e.exasol_dev.table('DWH_TECH.DWT_VGB_ZIELE').insert_from(fiv4e.exasol_prod, truncate=True, where='limit 1')
