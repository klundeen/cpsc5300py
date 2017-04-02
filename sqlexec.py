""" SQL execution

By: Kevin Lundeen
For: CPSC 4300, S17
"""
import os
import sys
import heap_storage
from schema_tables import Schema

DB_ENV = '~/cpsc4300env/data'  # this can get changed by calling initialize_db_env

def initialize_db_env(db_env = None):
    """ Initialize the database environment. Currently, nothing is required here. """
    global DB_ENV
    if db_env is not None:
        DB_ENV = db_env
    DB_ENV = os.path.expanduser(DB_ENV)
    heap_storage.initialize(DB_ENV)
    Schema.initialize()

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

    def execute(self):
        """ Usually overridden by the subclass. If not, it's when we see a parse tree we don't understand (yet). """
        return None, None, None, self.statement_type + ' is not implemented'

class SQLExecShowTablesStatement(SQLExec):
    """ SHOW TABLES """
    def execute(self):
        """ Executes: SELECT * FROM _tables """
        cn, ca, rows, message = SQLExecQuery({'table_names': ['_tables'], 'columns': '*'}).execute()
        rows = [row for row in rows if row['table_name'] not in Schema.SCHEMA_TABLES]
        message = 'successfully returned ' + str(len(rows)) + ' rows'
        return cn, ca, rows, message

class SQLExecShowColumnsStatement(SQLExec):
    """ SHOW COLUMNS FROM <table> """
    def __init__(self, parse):
        super().__init__(parse)
        self.table_name = parse['table_name']

    def execute(self):
        table = Schema.tables.get_table(self.table_name)
        column_names = ['column_name']
        column_attributes = {'column_name': {'data_type': 'TEXT'}}
        rows = [{'column_name': result} for result in table.column_names]
        return column_names, column_attributes, rows, 'successfully returned ' + str(len(rows)) + ' rows'

class SQLExecShowIndexStatement(SQLExec):
    """ SHOW INDEX FROM <table> """
    def __init__(self, parse):
        super().__init__(parse)
        self.table_name = parse['table_name']

    def execute(self):
        return SQLExecQuery({'table_names': ['_indices'], 'columns': '*'}).execute()

class SQLExecDropTableStatement(SQLExec):
    """ DROP TABLE ... """
    def __init__(self, parse):
        super().__init__(parse)
        self.table_name = parse['table_name']
        if self.table_name in Schema.SCHEMA_TABLES:
            raise ValueError('Cannot drop a schema table!')

    def execute(self):
        """ Drop the table. """
        # get the table
        table = Schema.tables.get_table(self.table_name)

        where = {'table_name': self.table_name}
        table_handle = next(Schema.tables.select(where))

        # remove indices
        to_drop = {}
        for handle in Schema.indices.select(where):
            index_attributes = Schema.indices.project(handle)
            to_drop.add(index_attributes['index_name'])
        for index_name in to_drop:
            index = Schema.indices.get_index(self.table_name, index_name)
            index.drop()
            Schema.indices.remove_from_cache(self.table_name, index_name)
        for handle in Schema.indices.select(where):
            Schema.indices.delete(handle)

        # remove from _tables schema
        Schema.tables.delete(table_handle)

        # remove from _columns schema
        for handle in Schema.columns.select(where):
            Schema.columns.delete(handle)

        # remove table
        table.drop()
        Schema.tables.remove_from_cache(self.table_name)
        return None, None, None, 'dropped ' + self.table_name

class SQLExecDropIndexStatement(SQLExec):
    """ DROP INDEX ... """
    def __init__(self, parse):
        super().__init__(parse)
        self.table_name = parse['table_name']
        self.index_name = parse['index_name']

    def execute(self):
        """ Drop the index. """
        index = Schema.indices.get_index(self.table_name, self.index_name)
        where = {'table_name': self.table_name, 'index_name': self.index_name}
        for handle in Schema.indices.select(where):
            Schema.indices.delete(handle)
        index.drop()
        Schema.indices.remove_from_cache(self.table_name, self.index_name)
        return None, None, None, 'dropped index ' + self.index_name

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
        Schema.tables.insert({'table_name': self.table_name})
        try:
            # update _columns schema
            column_order = [c['column_name'] for c in self.columns]
            column_attributes = {c['column_name']: {'data_type': c['data_type']} for c in self.columns}
            try:
                for column_name in column_order:
                    Schema.columns.insert({'table_name': self.table_name,
                                     'column_name': column_name,
                                     'data_type': column_attributes[column_name]['data_type']})

                # create table
                table = heap_storage.HeapTable(self.table_name, column_order, column_attributes)
                table.create()
                Schema.tables.add_to_cache(self.table_name, table)
            except:
                # attempt to undo the insertions into _columns
                try:
                    for column_name in column_order:
                        for row in Schema.columns.select({'table_name': self.table_name}):
                            Schema.columns.delete(row)
                except:
                    pass
                raise
        except:
            # attempt to undo the insertion into _tables
            try:
                Schema.tables.delete(next(Schema.tables.select({'table_name': self.table_name})))
            except:
                pass
            raise

        return None, None, None, 'created ' + self.table_name

class SQLExecIndexDefinition(SQLExec):
    """" CREATE INDEX ... """
    def __init__(self, parse):
        """ Create an index with given table_name (string) and columns (from parse tree). """
        super().__init__(parse)
        self.table_name = parse['table_name']
        self.columns = parse['columns']
        self.index_name = parse['index_name']
        try:
            self.index_type = parse['index_type']
        except KeyError:
            self.index_type = 'BTREE'
        try:
            self.is_unique = bool(parse['unique'])
        except KeyError:
            self.is_unique = False

    def execute(self):
        """ Execute the statement. """
        table = Schema.tables.get_table(self.table_name)
        row = {'table_name': self.table_name,
               'index_name': self.index_name,
               'seq_in_index': 0,
               'index_type': self.index_type,
               'is_unique': self.is_unique
               }
        for column_name in self.columns:
            row['seq_in_index'] += 1
            row['column_name'] = column_name
            Schema.indices.insert(row)

        index = Schema.indices.get_index(self.table_name, self.index_name)
        index.create()
        return None, None, None, 'created index ' + self.index_name


class SQLExecQuery(SQLExec):
    """ SELECT ... """
    # FIXME: totally bare bones for now

    def __init__(self, parse):
        self.table_names = parse['table_names']
        self.query_columns = parse['columns'] if parse['columns'] != '*' else None

    def execute(self):
        table_name = self.table_names[0]
        table = Schema.tables.get_table(table_name)
        rows = [table.project(handle, self.query_columns) for handle in table.select()]
        return table.column_names, table.columns, rows, 'successfully returned ' + str(len(rows)) + ' rows'