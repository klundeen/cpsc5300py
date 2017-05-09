""" Shell to execute SQL commands

By: Kevin Lundeen
For: CPSC 4300, S17
"""
import os
import unittest
import sqlexec
from sqlparse import SQLstatement


class Shell(object):
    """ Get SQL statements from user and execute them. """
    QUIT = 'quit'
    initialized = False

    @classmethod
    def run(cls, statements=None, dbenv=None):
        """ If statements is None, then get SQL statemnts from user, parse, and execute repeatedly.
            If statements is provided return the results in a list.
        """
        interactive = statements is None

        if not Shell.initialized:
            sqlexec.initialize_db_env(dbenv)
            Shell.initialized = True

        if interactive:
            collect = None
        else:
            collect = []
        for sql in cls.get_statements(statements):
            try:
                parse = SQLstatement.parseString(sql)
                results = sqlexec.dispatch(parse).execute()
                if interactive:
                    print(cls.unparse(parse))
                    cls.print_results(results)
                else:
                    collect.append(results)
            except Exception as x:
                if not interactive:
                    raise
                print(type(x).__name__, ': ', x.args[0], sep='')
                # raise  # FIXME - take this out except for debugging
        return collect

    @classmethod
    def get_statements(cls, statements=None):
        if statements is None:
            print(cls.QUIT, "to end")
            while True:
                sql = input('SQL> ')
                if sql == cls.QUIT:
                    return
                yield sql
        else:
            for sql in statements.split(';'):
                yield sql

    @staticmethod
    def unparse(parse):
        def to_str(a):
            if isinstance(a, list):
                words = [to_str(e) for e in a]
                return ' '.join(words)
            return str(a)
        return to_str(parse.asList())

    @classmethod
    def print_results(cls, results):
        """ Print out the rows which is a list of lists. The columns is dictionary with column info. """
        columns, attributes, rows, message = results
        if not rows:
            print(message)
            return
        print(columns)
        print('-' * 12 * len(columns))
        for row in rows:
            print([row[k] for k in columns])


class TestShell(unittest.TestCase):
    def setUp(self):
        dbenv = os.path.expanduser('~/.dbtests')
        if not os.path.exists(dbenv):
            os.makedirs(dbenv)
        for file in os.listdir(dbenv):
            os.remove(os.path.join(dbenv, file))
        Shell.run(statements="SHOW TABLES", dbenv=dbenv)

    def test_statements(self):
        columns, attributes, rows, message = Shell.run("SHOW COLUMNS FROM _tables")[0]
        self.assertEqual(columns, ['column_name'])
        self.assertEqual(rows, [{'column_name': 'table_name'}, {'column_name': 'storage_engine'}])

        columns, attributes, rows, message = Shell.run("CREATE TABLE hsy67 (a int, b text, c boolean)")[0]
        self.assertEqual(message, 'created hsy67')

        columns, attributes, rows, message = Shell.run("SHOW COLUMNS FROM hsy67")[0]
        self.assertEqual(columns, ['column_name'])
        self.assertEqual(set([row['column_name'] for row in rows]), {'a', 'b', 'c'})

        columns, attributes, rows, message = Shell.run("SHOW TABLES")[0]
        self.assertEqual(columns, ('table_name', 'storage_engine'))
        self.assertEqual(rows, [{'table_name': 'hsy67', 'storage_engine': 'HEAP'}])

        columns, attributes, rows, message = Shell.run("CREATE TABLE abcdefg (abb int, b_$cx text, ara999 boolean)")[0]
        self.assertEqual(message, 'created abcdefg')

        columns, attributes, rows, message = Shell.run("DROP TABLE hsy67")[0]
        self.assertEqual(message, 'dropped hsy67')

        columns, attributes, rows, message = Shell.run("SHOW TABLES")[0]
        self.assertEqual(columns, ('table_name', 'storage_engine'))
        self.assertEqual(rows, [{'table_name': 'abcdefg', 'storage_engine': 'HEAP'}])

        columns, attributes, rows, message = Shell.run("SELECT * from _columns")[0]
        self.assertEqual(columns, ('table_name', 'column_name', 'data_type'))
        result = {}
        for row in rows:
            result[(row['table_name'], row['column_name'])] = row['data_type']
        self.assertEqual(result[('_tables', 'table_name')], 'TEXT')
        self.assertEqual(result[('_columns', 'column_name')], 'TEXT')
        self.assertEqual(result[('abcdefg', 'b_$cx')], 'TEXT')
        self.assertEqual(result[('abcdefg', 'ara999')], 'BOOLEAN')

        columns, attributes, rows, message = Shell.run("CREATE UNIQUE INDEx bmy ON abcdefg (abb, b_$cx) USING HASH")[0]
        self.assertEqual(message, 'created index bmy')

        columns, attributes, rows, message = Shell.run("CREATE unique INDEx xxy ON abcdefg (b_$cx)")[0]
        self.assertEqual(message, 'created index xxy')

        columns, attributes, rows, message = Shell.run("SHOW INDEX FROM abcdefg")[0]
        self.assertEqual(columns, ('table_name', 'index_name', 'seq_in_index', 'column_name', 'index_type',
                                   'is_unique'))
        self.assertEqual(rows, [{'column_name': 'abb', 'index_name': 'bmy', 'index_type': 'HASH', 'is_unique': True,
                                 'seq_in_index': 1, 'table_name': 'abcdefg'},
                                {'column_name': 'b_$cx', 'index_name': 'bmy', 'index_type': 'HASH', 'is_unique': True,
                                 'seq_in_index': 2, 'table_name': 'abcdefg'},
                                {'column_name': 'b_$cx', 'index_name': 'xxy', 'index_type': 'BTREE', 'is_unique': True,
                                 'seq_in_index': 1, 'table_name': 'abcdefg'}])

        columns, attributes, rows, message = Shell.run("DROP INDEX bmy ON abcdefg")[0]
        self.assertEqual(message, 'dropped index bmy')

        Shell.run('CREATE TABLE foo (id INT, data TEXT)')
        columns, attributes, rows, message = Shell.run('INSERT INTO foo VALUES (1,"one")')[0]
        self.assertEqual(message, 'successfully inserted 1 row into foo')
        columns, attributes, rows, message = Shell.run('INSERT INTO foo (data,id) VALUES ("Two",2)')[0]
        self.assertEqual(message, 'successfully inserted 1 row into foo')
        columns, attributes, rows, message = Shell.run('INSERT INTO foo VALUES (3,"three")')[0]
        self.assertEqual(message, 'successfully inserted 1 row into foo')
        columns, attributes, rows, message = Shell.run('SELECT * FROM foo')[0]
        self.assertEqual(rows, [{'data': 'one', 'id': 1}, {'data': 'Two', 'id': 2}, {'data': 'three', 'id': 3}])
        columns, attributes, rows, message = Shell.run('SELECT * FROM foo WHERE data="one"')[0]
        self.assertEqual(rows, [{'data': 'one', 'id': 1}])
        columns, attributes, rows, message = Shell.run('SELECT data FROM foo WHERE id=2')[0]
        self.assertEqual(rows, [{'data': 'Two'}])

        columns, attributes, rows, message = Shell.run("CREATE UNIQUE INDEX fx ON foo (id)")[0]
        self.assertEqual(message, 'created index fx')
        columns, attributes, rows, message = Shell.run('SELECT * FROM foo WHERE data="one"')[0]
        self.assertEqual(rows, [{'data': 'one', 'id': 1}])
        columns, attributes, rows, message = Shell.run('SELECT * FROM foo WHERE id=2')[0]
        self.assertEqual(rows, [{'data': 'Two', 'id': 2}])

        columns, attributes, rows, message = Shell.run("DELETE FROM foo WHERE id=3")[0]
        self.assertEqual(message, 'successfully deleted 1 rows and from 1 indices')
        columns, attributes, rows, message = Shell.run('INSERT INTO foo VALUES (4,"four")')[0]
        self.assertEqual(message, 'successfully inserted 1 row into foo and 1 indices')

        columns, attributes, rows, message = Shell.run("CREATE TABLE bt (id INT, data TEXT, PRIMARY KEY(id))")[0]
        self.assertEqual(message, 'created bt')
        columns, attributes, rows, message = Shell.run('INSERT INTO bt VALUES (1,"one")')[0]
        self.assertEqual(message, 'successfully inserted 1 row into bt')
        columns, attributes, rows, message = Shell.run('INSERT INTO bt (data,id) VALUES ("Two",2)')[0]
        self.assertEqual(message, 'successfully inserted 1 row into bt')
        columns, attributes, rows, message = Shell.run('INSERT INTO bt VALUES (3,"three")')[0]
        self.assertEqual(message, 'successfully inserted 1 row into bt')
        columns, attributes, rows, message = Shell.run('SELECT * FROM bt')[0]
        self.assertEqual(rows, [{'data': 'one', 'id': 1}, {'data': 'Two', 'id': 2}, {'data': 'three', 'id': 3}])
        columns, attributes, rows, message = Shell.run('SELECT * FROM bt WHERE data="one"')[0]
        self.assertEqual(rows, [{'data': 'one', 'id': 1}])
        columns, attributes, rows, message = Shell.run('SELECT data FROM bt WHERE id=2')[0]
        self.assertEqual(rows, [{'data': 'Two'}])
        columns, attributes, rows, message = Shell.run("DELETE FROM bt WHERE id=2")[0]
        self.assertEqual(message, 'successfully deleted 1 rows')
        columns, attributes, rows, message = Shell.run('SELECT * FROM bt')[0]
        self.assertEqual(rows, [{'data': 'one', 'id': 1}, {'data': 'three', 'id': 3}])



if __name__ == "__main__":
    Shell.run()
