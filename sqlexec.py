""" SQL execution

By: Kevin Lundeen
For: CPSC 4300, S17
"""
import os
import re
import sys
from bsddb3 import db as bdb
from sqlparse import RESERVED_WORDS
import heap_storage
import schema_tables

DB_ENV = '/Users/klundeen/cpsc4300env/data'  # this can get changed by calling initialize_db_env

def initialize_db_env(db_env = None):
    """ Initialize the database environment. Currently, nothing is required here. """
    global DB_ENV
    if db_env is not None:
        DB_ENV = db_env
    heap_storage.initialize(DB_ENV)
    schema_tables.initialize()

def dispatch(parse):
    """ Factory that creates the right type of SQLExec object for a given parse tree. """
    # Following magic, for example, turns a table_definition parse tree into the string, "SQLExecTableDefinition"
    basename = 'SQLExec'
    clsname = basename + "".join([word.capitalize() for word in parse.getName().split('_')])

    sqlexec = sys.modules[__name__]
    try:
        cls = getattr(sqlexec, clsname)
    except AttributeError:
        cls = getattr(sqlexec, basename)
    return cls(parse)

class SQLExec(object):
    """ Base class for all SQLExec* classes. """
    def __init__(self, parse):
        self.parse = parse
        self.statement_type = parse.getName()
        self.db = bdb.DB()  # handle to Berkeley DB

    def execute(self):
        """ Usually overridden by the subclass. If not, it's when we see a parse tree we don't understand (yet). """
        return None, None, None, self.statement_type + ' is not implemented'

class SQLExecShowTablesStatement(SQLExec):
    """ SHOW TABLES """
    def execute(self):
        """ Executes: SELECT * FROM _tables """
        cn, ca, rows, message = SQLExecQuery({'table_names': ['_tables'], 'columns': '*'}).execute()
        rows = [row for row in rows if row['table_name'] not in schema_tables.SCHEMA_TABLES]
        message = 'successfully returned ' + str(len(rows)) + ' rows'
        return cn, ca, rows, message

class SQLExecShowColumnsStatement(SQLExec):
    """ SHOW COLUMNS """
    def __init__(self, parse):
        super().__init__(parse)
        self.table_name = parse['table_name']

    def execute(self):
        _tables = schema_tables.Tables()
        table = _tables.get_table(self.table_name)
        column_names = ['column_name']
        column_attributes = {'column_name': {'data_type': 'TEXT'}}
        rows = [{'column_name': result} for result in table.column_names]
        return column_names, column_attributes, rows, 'successfully returned ' + str(len(rows)) + ' rows'

class SQLExecDropTableStatement(SQLExec):
    """ DROP TABLE ... """
    def __init__(self, parse):
        super().__init__(parse)
        self.table_name = parse['table_name']
        if self.table_name in schema_tables.SCHEMA_TABLES:
            raise ValueError('Cannot drop a schema table!')

    def execute(self):
        """ Drop the table. """
        where = {'table_name': self.table_name}

        # get the table
        _tables = schema_tables.Tables()
        table = _tables.get_table(self.table_name)

        # remove from _tables schema
        _tables.delete(next(_tables.select(where)))

        # remove from _columns schema
        _columns = schema_tables.Columns()
        for handle in _columns.select(where):
            _columns.delete(handle)

        # remove table
        table.drop()
        return None, None, None, 'dropped ' + self.table_name

class SQLExecTableDefinition(SQLExec):
    """ CREATE TABLE ... """
    def __init__(self, parse):
        """ Create a table with given table_name (string) and table_element_list (from parse tree). """
        super().__init__(parse)
        self.table_name = parse['table_name']
        self.columns = parse['table_element_list']

    def execute(self):
        """ Execute the statement. """
        # update _tables schema
        _tables = schema_tables.Tables()
        _tables.insert({'table_name': self.table_name})
        try:
            # update _columns schema
            column_order = [c['column_name'] for c in self.columns]
            column_attributes = {c['column_name']: {'data_type': c['data_type']} for c in self.columns}
            _columns = schema_tables.Columns()
            try:
                for column_name in column_order:
                    _columns.insert({'table_name': self.table_name,
                                     'column_name': column_name,
                                     'data_type': column_attributes[column_name]['data_type']})

                # create table
                table = heap_storage.HeapTable(self.table_name, column_order, column_attributes)
                table.create()
            except:
                # attempt to undo the insertions into _columns
                try:
                    for column_name in column_order:
                        for row in _columns.select({'table_name': self.table_name}):
                            _columns.delete(row)
                except:
                    pass
                raise
        except:
            # attempt to undo the insertion into _tables
            try:
                print(next(_tables.select({'table_name': self.table_name})))
                _tables.delete(next(_tables.select({'table_name': self.table_name})))
            except:
                pass
            raise

        return None, None, None, 'created ' + self.table_name


class SQLExecQuery(SQLExec):
    """ SELECT ... """
    # FIXME: totally bare bones for now

    def __init__(self, parse):
        self.table_names = parse['table_names']
        self.query_columns = parse['columns'] if parse['columns'] != '*' else None

    def execute(self):
        table_name = self.table_names[0]
        table = schema_tables.Tables().get_table(table_name)
        rows = [table.project(handle, self.query_columns) for handle in table.select()]
        return table.column_names, table.columns, rows, 'successfully returned ' + str(len(rows)) + ' rows'