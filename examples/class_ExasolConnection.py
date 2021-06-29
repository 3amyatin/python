from typing import Union, Optional

import pandas as pd
import sqlalchemy
import pyodbc

from .class_SQLTable import SQLTable


class ExasolConnection:  # Exasol Connection Interface
    """
    # Implementierte Schnittstelle:
    * pyodbc mit sqlalchemy-exasol https://github.com/mkleehammer/pyodbc voll mit DB-API 2.0 kompatibel
      (-) langsam beim IMPORT, Zeilen-Vorgang zur Spaltenorientierte Datenbank
      * sqlalchemy_exasol https://github.com/blue-yonder/sqlalchemy_exasol
        * Schnittstelle für import_from_pandas() sonst funktioniert df.to_sql() nicht
        (!) Always use all lower-case identifiers for schema, table and column names. SQLAlchemy treats all lower-case identifiers as case-insensitive, the dialect takes care of transforming the identifier into a case-insensitive representation of the specific database (in case of EXASol this is upper-case as for Oracle)

      * Exasol ODBC Treiber für Windows installieren: (aus https://github.com/exasol/python-exasol#prerequisites-and-installation-on-windows)
        * https://www.exasol.com/portal/display/DOWNLOAD/7.0#id-7.0-Latest7.0Downloads
        * https://www.exasol.com/support/secure/attachment/119097/EXASOL_ODBC-7.0.4-x86_64.msi
        * https://www.exasol.com/support/secure/attachment/135969/EXASOL_ODBC-7.0.9-x86_64.msi

    
    FYI: Python Database API Specification v2.0 https://www.python.org/dev/peps/pep-0249/

    # Mögliche Alternativen:
    * turbodbc https://github.com/blue-yonder/turbodbc empfohlen von pyexasol, voll mit Python DB-API 2.0 kompatibel
      * läuft auf py 3.8+ aktuell nicht
    * pypyodbc https://github.com/pypyodbc/pypyodbc
    * pyexasol.db2 https://github.com/badoo/pyexasol/blob/master/docs/DBAPI_COMPAT.md pyexasol Wrapper voll mit DB-API 2.0 kompatibel
    * pyexasol von Badoo https://github.com/badoo/pyexasol beschränkt mit DB-API 2.0 kompatibel
    * python-exasol von Exasol https://github.com/exasol/python-exasol NICHT MEHR AKTUALISIERT
"""
    __server: str
    __driver: str
    __servers = {'dev', 'prod'}
    echo: bool
    engine: sqlalchemy.engine
    connection: sqlalchemy.engine.Connection.connection  # eine direkte Verbindung zu der über pyexasol / pyodbc / sqlalchemy initialisierte Connection
    cursor: pyodbc.Cursor  # Ergebniss letzes .execute()

    @property
    def server(self) -> str:
        return self.__server

    @server.setter
    def server(self, value: str):
        if value in self.__servers:
            self.__server = value
        else:
            raise Exception(f'Unbekannter Exasol-Server {value!r}')

    @property
    def driver(self) -> str:
        return self.__driver

    @driver.setter
    def driver(self, value: str):
        if value in self.__drivers:
            self.__driver = value
        else:
            raise Exception(f'Unbekannter Exasol-Treiber {value!r}')

    def __init__(self, *args, _driver='sqlalchemy', _server='dev', _echo=False, **kwargs):
        self.driver = _driver
        self.server = _server
        self.echo = _echo

        if self.driver == 'sqlalchemy':

            self.sqlalchemy: sqlalchemy.engine = sqlalchemy.create_engine(sqlalchemy.engine.url.URL(*args, **kwargs), echo=self.echo)
            # self.sqlalchemy = sqlalchemy.create_engine(f"exa+pyodbc://{config.connect['dev-pyodbc']['user']}:{config.connect['dev-pyodbc']['password']}@{config.connect['dev-pyodbc']['exahost']}/?driver=ExaSolution+Driver")
            # self.sqlalchemy = sqlalchemy.create_engine(f"exa+pyodbc://exasol_dev")
            self.engine = self.sqlalchemy
            self.connection: sqlalchemy.engine.Connection.connection = self.engine.connect().connection  # Python DBAPI Proxy, pyodbc kompatibel
            self.cursor: pyodbc.Cursor = self.connection.cursor()
            self.SQLError = pyodbc.ProgrammingError

        elif self.driver == 'pyodbc':
            self.pyodbc = pyodbc.connect(*args, **kwargs)
            self.connection = self.pyodbc
            self.cursor = self.connection.cursor()
            self.SQLError = pyodbc.ProgrammingError

        elif self.driver == 'pyexasol':
            self.pyexasol = pyexasol.connect(**kwargs)
            self.connection = self.pyexasol
            self.SQLError = pyexasol.ExaError
            # self.cursor = self.connection   # es gibt kein pur Cursor in pyexasol

    #/def __init__

    def execute(self, stmt: str, *args, echo: bool = False, spaces: str = '', output: str = 'tuple', split=False, **kwargs) -> pyodbc.Cursor:

        if self.driver in ('sqlalchemy', 'pyodbc'):
            if split:
                for stmt_part in stmt.split(';'):
                    if echo:
                        print(spaces + stmt_part)
                    self.cursor = self.cursor.execute(stmt_part, *args, **kwargs)
            else:
                if echo:
                    print(spaces + stmt)
                self.cursor = self.cursor.execute(stmt, *args, **kwargs)

        elif self.driver == 'pyexasol':
            if echo:
                print(spaces + stmt)
            self.cursor = self.connection.execute(stmt, *args, **kwargs)

        if output == 'tuple':
            return self.cursor
        elif output == 'dict':
            return self.cursor_by_name
        else:
            raise Exception(f'Unbekanter {output=}')

    #/def execute

    def export_to_pandas(self, stmt: str, **kwargs):

        if self.driver == 'sqlalchemy':
            df = pd.read_sql(sql=stmt, con=self.connection, **kwargs)
            self.cursor = self.connection.cursor()
            return df

        elif self.driver == 'pyodbc':
            df = pd.read_sql(sql=stmt, con=self.connection, **kwargs)
            self.cursor = self.connection.cursor()
            return df

        elif self.driver == 'pyexasol':
            self.cursor = None  # cursor() ist nicht direkt in pyexasol unterstützt
            return self.connection.export_to_pandas(stmt, **kwargs)
    # /def export_to_pandas

    def import_from_pandas(self, df: pd.DataFrame, schematable: Union[str, SQLTable], truncate=False, rowcount=False, **kwargs) -> Optional[int]:
        schematable = self.table(schematable)

        if truncate:
            schematable.truncate()

        if self.driver == 'sqlalchemy':
            df.to_sql(name=schematable.table.lower(), con=self.sqlalchemy.connect(), index=False, if_exists="append", schema=schematable.schema.lower(), **kwargs)

        elif self.driver == 'pyodbc':
            # TODO: pyodbc.import_from_pandas
            return None

        elif self.driver == 'pyexasol':
            self.connection.import_from_pandas(df, schematable.astuple(), **kwargs)

        if rowcount:
            return self.execute(f'select count(1) from {schematable}').fetchval()
    # /def import_from_pandas

    def row_count(self):

        if self.driver == 'sqlalchemy':
            return self.cursor.rowcount  # funktioniert nach import_from_pandas() und execute('INSERT') nicht!

        elif self.driver == 'pyodbc':
            return self.cursor.rowcount

        elif self.driver == 'pyexasol':
            return self.connection.last_stmt.rowcount()

    # /def row_count

    def list_tables(self, table:str = None, schema:str = None):
        """
        # https://github.com/mkleehammer/pyodbc/wiki/Cursor#tablestablenone-catalognone-schemanone-tabletypenone
        # https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqltables-function
        # tables(table=None, catalog=None, schema=None, tableType=None)
          # [0] table_cat
          # [1] table_schem
          # [2] table_name
          # [3] table_type in ('TABLE', 'VIEW', 'SYSTEM TABLE')
          # [4] remarks: description
        """
        table = table.upper()
        schema = schema.upper()

        if self.driver in ('sqlalchemy', 'pyodbc'):
            return self.connection.cursor().tables(table = table, schema = schema)

        elif self.driver == 'pyexasol':
            #TODO: nicht rückwärts getestet
            #TODO: soll Parameter table unterstützen
            return self.connection.meta.list_tables(table_schema_pattern = schema)

    #/def list_tables

    def table(self, schema_or_schematable: Union[str, SQLTable], table: Optional[str] = None) -> SQLTable:
        return SQLTable(schema_or_schematable, table, server=self)

    from .pandas_to_sql_file import pandas_to_sql_file
    from .export_to_sql_file import export_to_sql_file
#/class ExasolConnection
