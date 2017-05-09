""" SQL execution

By: Kevin Lundeen
For: CPSC 4300, S17
"""
import os
import sys
import heap_storage
from btree_index import BTreeTable
from schema_tables import Schema
from eval_plan import EvalPlanTableScan, EvalPlanSelect, EvalPlanProject
from sqlparse import SQLstatement

DB_ENV = '~/cpsc4300env/pydata'  # this can get changed by calling initialize_db_env


def initialize_db_env(db_env=None):
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
        cn, ca, rows, message = SQLExecQuery(SQLstatement.parseString('SELECT * FROM _tables')).execute()
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

        return SQLExecQuery(SQLstatement.parseString('SELECT * FROM _indices')).execute()


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
        to_drop = set()
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
        columns = parse['table_element_list']
        self.column_order = [c['def_column_name'] for c in columns if "def_column_name" in c]
        self.column_attributes = {c['def_column_name']: {'data_type': c['data_type']}
                                 for c in columns if "def_column_name" in c}
        if 'primary_key' in columns[-1]:
            self.primary_key = [c for c in columns[-1]['primary_key']['key_columns']]
        else:
            self.primary_key = None

    def execute(self):
        """ Execute the statement. """
        # update _tables schema
        storage_engine = 'HEAP' if self.primary_key is None else 'BTREE'
        Schema.tables.insert({'table_name': self.table_name, 'storage_engine': storage_engine })
        try:
            # update _columns schema
            column_order = self.column_order
            column_attributes = self.column_attributes
            pk = {c: i+1 for (i, c) in enumerate(self.primary_key)}
            try:
                for column_name in column_order:
                    Schema.columns.insert({'table_name': self.table_name,
                                           'column_name': column_name,
                                           'data_type': column_attributes[column_name]['data_type'],
                                           'primary_key_seq': pk[column_name] if column_name in pk else 0})

                # create table
                if storage_engine == 'BTREE':
                    table = BTreeTable(self.table_name, column_order, column_attributes, primary_key=self.primary_key)
                else:
                    table = heap_storage.HeapTable(self.table_name, column_order, column_attributes)
                table.create()
                Schema.tables.add_to_cache(self.table_name, table)
            except:
                # attempt to undo the insertions into _columns
                try:
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


def _get_value_from_parse(value, ca, column, error):
    """ Translate the parse tree value into the right data type. """
    if ca['data_type'] == "INT":
        value = int(value)
    elif ca['data_type'] == "TEXT":
        split = value.split('"')
        if len(split) != 3:
            raise ValueError("value for column '" + column + "' expects a literal string")
        value = split[1]
    else:
        raise ValueError("don't know how to handle " + ca['data_type'] + " data type in " + error)
    return value


def _get_where_conjunction(parse_where, columns):
    """ Pull out conjunctions of equality predicates from parse tree. """
    where_list = parse_where.asList()[0]
    if where_list == "":
        return None
    conjunctions = where_list[2::2]
    if len(conjunctions) != 0 and set(conjunctions) != {'AND'}:
        raise ValueError("only support AND conjunctions, not " + str(conjunctions))
    predicates = where_list[1::2]
    if {pred[1] for pred in predicates} != {'='}:
        raise ValueError("only equality predicates currently supported")
    column_refs = [pred[0] for pred in predicates]
    rvals = [pred[2] for pred in predicates]
    where = {}
    for which, col_name in enumerate(column_refs):
        if col_name not in columns:
            raise ValueError("unknown column '" + col_name + "'")
        ca = columns[col_name]
        where[col_name] = _get_value_from_parse(rvals[which], ca, col_name, 'WHERE')
    return where


class SQLExecQuery(SQLExec):
    """ SELECT ... """

    def __init__(self, parse):
        super().__init__(parse)
        self.table_names = parse['table_names']
        self.query_columns = parse['columns'] if parse['columns'] != '*' else None
        self.where = parse['where']

    def execute(self):
        table_name = self.table_names[0]
        table = Schema.tables.get_table(table_name)
        where = _get_where_conjunction(self.where, table.columns)

        # make the evaluation plan
        plan = EvalPlanTableScan(table)
        if where is not None:
            plan = EvalPlanSelect(where, plan)
        if self.query_columns is not None:
            plan = EvalPlanProject(self.query_columns, plan)
        plan = plan.optimize()

        # and execute it
        rows = [row for row in plan.evaluate()]
        return (plan.get_column_names(), plan.get_column_attributes(), rows,
                'successfully returned ' + str(len(rows)) + ' rows')


class SQLExecInsertStatement(SQLExec):
    """ INSERT INTO ... """

    def __init__(self, parse):
        super().__init__(parse)
        self.table_name = parse['table_name']
        try:
            self.columns = parse['columns']
        except KeyError:
            self.columns = None
        self.values = parse['values']

    def execute(self):
        table = Schema.tables.get_table(self.table_name)
        if self.columns is None:
            self.columns = table.column_names

        # do the insert
        row = {}
        for i, value in enumerate(self.values):
            column = self.columns[i]
            ca = table.columns[column]
            row[column] = _get_value_from_parse(value, ca, column, 'INSERT')
        t_insert = table.insert(row)

        # add to indices
        index_names = Schema.indices.get_index_names(self.table_name)
        for index_name in index_names:
            index = Schema.indices.get_index(self.table_name, index_name)
            index.insert(t_insert)
        suffix = ' and ' + str(len(index_names)) + ' indices' if index_names else ""

        return None, None, None, 'successfully inserted 1 row into ' + self.table_name + suffix


class SQLExecDeleteStatement(SQLExec):
    """ DELETE FROM ... """

    def __init__(self, parse):
        super().__init__(parse)
        self.table_name = parse['table_name']
        self.where = parse['where']

    def execute(self):
        table = Schema.tables.get_table(self.table_name)
        where = _get_where_conjunction(self.where, table.columns)

        # make the evaluation plan
        plan = EvalPlanTableScan(table)
        if where is not None:
            plan = EvalPlanSelect(where, plan)
        plan = plan.optimize()

        # and execute it to get a list of handles
        t, handles = plan.pipeline()
        all_handles = [handle for handle in handles]

        # remove from indices
        index_names = Schema.indices.get_index_names(self.table_name)
        for index_name in index_names:
            index = Schema.indices.get_index(self.table_name, index_name)
            for handle in all_handles:
                index.delete(handle)
        suffix = ' and from ' + str(len(index_names)) + ' indices' if index_names else ""

        # remove from table
        for handle in all_handles:
            t.delete(handle)

        return (None, None, None,
                'successfully deleted ' + str(len(all_handles)) + ' rows' + suffix)
