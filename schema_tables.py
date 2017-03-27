""" Schema tables:
        _tables table
        _columns table
"""

import re
from heap_storage import HeapTable
from sqlparse import RESERVED_WORDS

SCHEMA_TABLES = ['_tables', '_columns']

def initialize():
    """ Initialize the schema tables. """
    Tables().create_if_not_exists()
    Columns().create_if_not_exists()

def acceptable_name(name):
    """ Check that the name is all Latin letters, digits, underscores, and dollar signs, but not all
        digits, not an SQL keyword, and 32 characters or less.
    """
    if name.upper() in RESERVED_WORDS:
        return False
    if not re.fullmatch(r"[a-zA-Z0-9_$]+", name):
        return False
    if re.fullmatch(r"[0-9]*", name):
        return False
    return True

def acceptable_data_type(data_type):
    return data_type == 'INT' or data_type == 'TEXT'


class Tables(HeapTable):
    """ The table that stores the metadata for all other tables.
        For now, we are not indexing anything, so a query requires sequential scan of table.
    """
    TABLE_NAME = '_tables'
    COLUMN_ORDER = ('table_name',)
    COLUMNS = {'table_name': {'data_type': 'TEXT', 'not_null': True, 'validate': acceptable_name}}

    def __init__(self):
        super().__init__(self.TABLE_NAME, self.COLUMN_ORDER, self.COLUMNS)

    def create(self):
        """ Create the file and also, manually add schema tables. """
        super().create()
        self.insert({'table_name': '_tables'})
        self.insert({'table_name': '_columns'})

    def insert(self, row):
        """ Manually check that table_name is unique. """
        if 'table_name' in row:
            duplicates = [self.project(id) for id in self.select(where={'table_name': row['table_name']})]
            if duplicates:
                raise ValueError('Table ' + row['table_name'] + ' already exists.')
        return super().insert(row)

    def get_columns(self, table_name):
        """ Return a list of column names and column attributes for given table. """
        _columns = Columns()
        column_rows = [_columns.project(handle) for handle in _columns.select({'table_name': table_name})]
        column_names = [row['column_name'] for row in column_rows]
        column_attributes = {row['column_name']: {'data_type': row['data_type']} for row in column_rows}
        return column_names, column_attributes

    def get_table(self, table_name):
        """ Return a table for given table_name. """
        column_names, column_attributes = self.get_columns(table_name)
        return HeapTable(table_name, column_names, column_attributes)

class Columns(HeapTable):
    """ The table that stores the column metadata for all other tables.
        For now, we are not indexing anything, so a query requires sequential scan of table.
    """
    TABLE_NAME = '_columns'
    COLUMN_ORDER = ('table_name', 'column_name', 'data_type')
    COLUMNS = {'table_name': {'data_type': 'TEXT', 'not_null': True},
               'column_name': {'data_type': 'TEXT', 'not_null': True, 'validate': acceptable_name},
               'data_type': {'data_type': 'TEXT', 'not_null': True, 'validate': acceptable_data_type}}

    def __init__(self):
        super().__init__(self.TABLE_NAME, self.COLUMN_ORDER, self.COLUMNS)

    def create(self):
        """ Create the file and also, manually add schema tables. """
        super().create()
        self.insert({'table_name': '_tables', 'column_name': 'table_name', 'data_type': 'TEXT'})
        self.insert({'table_name': '_columns', 'column_name': 'table_name', 'data_type': 'TEXT'})
        self.insert({'table_name': '_columns', 'column_name': 'column_name', 'data_type': 'TEXT'})
        self.insert({'table_name': '_columns', 'column_name': 'data_type', 'data_type': 'TEXT'})

    def insert(self, row):
        """ Manually check that (table_name, column_name) is unique. """
        if 'table_name' in row and 'column_name' in row:
            duplicates = [self.project(id) for id in self.select(where={'table_name': row['table_name'],
                                                                        'column_name': row['column_name']})]
            if duplicates:
                raise ValueError('Column ' + row['column_name'] + ' for ' + row['table_name'] + ' already exists.')
        return super().insert(row)
