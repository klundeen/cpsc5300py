""" Schema tables:
        _tables table
        _columns table
"""

import re
from storage_engine import DbIndex
from heap_storage import HeapTable
from sqlparse import RESERVED_WORDS
from btree_index import BTreeIndex, BTreeTable


class Schema(object):
    SCHEMA_TABLES = ['_tables', '_columns', '_indices']
    tables = None
    columns = None
    indices = None

    @classmethod
    def initialize(cls):
        """ Initialize the schema tables. """
        cls.tables = _Tables()
        cls.columns = _Columns()
        cls.indices = _Indices()
        _Tables.table_cache['_tables'] = cls.tables
        _Tables.table_cache['_columns'] = cls.columns
        _Tables.table_cache['_indices'] = cls.indices
        cls.tables.create_if_not_exists()
        cls.columns.create_if_not_exists()
        cls.indices.create_if_not_exists()


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
    return data_type in ('INT', 'TEXT', 'BOOLEAN')


class _Tables(HeapTable):
    """ The table that stores the metadata for all other tables.
        For now, we are not indexing anything, so a query requires sequential scan of table.
    """
    TABLE_NAME = '_tables'
    COLUMN_ORDER = ('table_name', 'storage_engine')
    COLUMNS = {'table_name': {'data_type': 'TEXT', 'not_null': True, 'validate': acceptable_name},
               'storage_engine': {'data_type': 'TEXT', 'not_null': True}}
    table_cache = {}  # We use this to avoid having to do concurrency control between different instances in this app

    # In general, we only want to open each file once.

    def __init__(self):
        super().__init__(self.TABLE_NAME, self.COLUMN_ORDER, self.COLUMNS)

    def create(self):
        """ Create the file and also, manually add schema tables. """
        super().create()
        self.insert({'table_name': '_tables', 'storage_engine': 'HEAP'})
        self.insert({'table_name': '_columns', 'storage_engine': 'HEAP'})
        self.insert({'table_name': '_indices', 'storage_engine': 'HEAP'})

    def insert(self, row):
        """ Manually check that table_name is unique. """
        if 'table_name' in row:
            duplicates = [self.project(handle) for handle in self.select(where={'table_name': row['table_name']})]
            if duplicates:
                raise ValueError('Table ' + row['table_name'] + ' already exists.')
        return super().insert(row)

    @staticmethod
    def get_columns(table_name, include_primary_key=False):
        """ Return a list of column names and column attributes for given table. """
        _columns = Schema.columns
        column_rows = [_columns.project(handle) for handle in _columns.select({'table_name': table_name})]
        column_names = [row['column_name'] for row in column_rows]
        column_attributes = {row['column_name']: {'data_type': row['data_type']} for row in column_rows}
        if not include_primary_key:
            return column_names, column_attributes
        pk = {row['primary_key_seq']: row['column_name'] for row in column_rows}
        pk_count = max(pk)
        primary_key = [pk[i] for i in range(1, max(pk)+1)] if pk_count > 0 else None
        return column_names, column_attributes, primary_key

    def get_table(self, table_name):
        """ Return a table for given table_name. """
        if table_name in _Tables.table_cache:
            return _Tables.table_cache[table_name]
        column_names, column_attributes, primary_key = self.get_columns(table_name, include_primary_key=True)
        storage_engine = [self.project(h) for h in self.select({'table_name': table_name})][0]['storage_engine']
        if storage_engine == 'BTREE':
            table = BTreeTable(table_name, column_names, column_attributes, primary_key=primary_key)
        else:
            table = HeapTable(table_name, column_names, column_attributes)
        _Tables.table_cache[table_name] = table
        return table

    @staticmethod
    def add_to_cache(table_name, table):
        _Tables.table_cache[table_name] = table

    @staticmethod
    def remove_from_cache(table_name):
        try:
            del _Tables.table_cache[table_name]
        except KeyError:
            pass


class _Columns(HeapTable):
    """ The table that stores the column metadata for all other tables.
        For now, we are not indexing anything, so a query requires sequential scan of table.
    """
    TABLE_NAME = '_columns'
    COLUMN_ORDER = ('table_name', 'column_name', 'data_type')
    COLUMNS = {'table_name': {'data_type': 'TEXT', 'not_null': True},
               'column_name': {'data_type': 'TEXT', 'not_null': True, 'validate': acceptable_name},
               'data_type': {'data_type': 'TEXT', 'not_null': True, 'validate': acceptable_data_type},
               'primary_key_seq': {'data_type': 'INT', 'not_null': True}}

    def __init__(self):
        super().__init__(self.TABLE_NAME, self.COLUMN_ORDER, self.COLUMNS)

    def create(self):
        """ Create the file and also, manually add schema tables. """
        super().create()
        bootstrap = {'_tables': ['table_name', 'storage_engine'],
                     '_columns': ['table_name', 'column_name', 'data_type', 'primary_key_seq'],
                     '_indices': ['table_name', 'index_name', 'seq_in_index', 'column_name', 'index_type', 'is_unique']}
        for table_name in bootstrap:
            for column_name in bootstrap[table_name]:
                self.insert({'table_name': table_name, 'column_name': column_name, 'data_type': 'TEXT',
                             'primary_key_seq': 0})

    def insert(self, row):
        """ Manually check that (table_name, column_name) is unique. """
        if 'table_name' in row and 'column_name' in row:
            duplicates = [self.project(handle) for handle in
                          self.select(where={'table_name': row['table_name'], 'column_name': row['column_name']})]
            if duplicates:
                raise ValueError('Column ' + row['column_name'] + ' for ' + row['table_name'] + ' already exists.')
        return super().insert(row)


class DummyIndex(DbIndex):
    """ Temporary stub. """

    def create(self): pass

    def drop(self): pass

    def open(self): pass

    def close(self): pass

    def lookup(self, key): super().lookup(key)

    def insert(self, handle): pass

    def delete(self, handle): pass


class _Indices(HeapTable):
    """ The table that stores the index metadata for all indices. """
    TABLE_NAME = '_indices'
    COLUMN_ORDER = ('table_name', 'index_name', 'seq_in_index', 'column_name', 'index_type', 'is_unique')
    COLUMNS = {'table_name': {'data_type': 'TEXT', 'not_null': True},
               'index_name': {'data_type': 'TEXT', 'not_null': True, 'validate': acceptable_name},
               'seq_in_index': {'data_type': 'INT', 'not_null': True},
               'column_name': {'data_type': 'TEXT', 'not_null': True},
               'index_type': {'data_type': 'TEXT', 'not_null': True},
               'is_unique': {'data_type': 'BOOLEAN', 'not_null': True, 'default': 0}}
    index_cache = {}

    def __init__(self):
        super().__init__(self.TABLE_NAME, self.COLUMN_ORDER, self.COLUMNS)

    def insert(self, row):
        """ Manually check that (table_name, column_name, index_name, seq_in_index) is unique. """
        # FIXME - uh, do the uniqueness validation
        return super().insert(row)

    def get_columns(self, table_name, index_name):
        """ Return a list of column names and column attributes for given table. """
        column_names = {}
        values = {}
        for handle in self.select({'table_name': table_name, 'index_name': index_name}):
            values = self.project(handle)
            column_names[values['seq_in_index']] = values['column_name']
        index_attributes = values  # the attributes we want on every row
        column_names = [column_names[i] for i in range(1, len(column_names) + 1)]
        return column_names, index_attributes

    def get_index(self, table_name, index_name):
        """ Return an index for given table_name, index_name. """
        if (table_name, index_name) in _Tables.table_cache:
            return _Tables.table_cache[(table_name, index_name)]
        column_names, attributes = self.get_columns(table_name, index_name)
        table = Schema.tables.get_table(table_name)
        if attributes['index_type'] == 'BTREE':
            index = BTreeIndex(table, attributes['index_name'], column_names, attributes['is_unique'])
        else:  # HASH
            index = DummyIndex(table, attributes['index_name'], column_names, attributes['is_unique'])  # FIXME
        self.add_to_cache(table_name, index_name, index)
        return index

    @staticmethod
    def add_to_cache(table_name, index_name, index):
        _Tables.table_cache[(table_name, index_name)] = index

    @staticmethod
    def remove_from_cache(table_name, index_name):
        try:
            del _Tables.table_cache[(table_name, index_name)]
        except KeyError:
            pass

    def get_index_names(self, table_name):
        """ Fetch all index names for given table. """
        return [self.project(h)['index_name'] for h in self.select({'table_name': table_name, 'seq_in_index': 1})]
