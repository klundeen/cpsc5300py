""" Shell to execute SQL commands

By: Kevin Lundeen
For: CPSC 4300, S17
"""
import sqlexec
from sqlparse import SQLstatement
from pyparsing import ParseException

class Shell(object):
    """ Get SQL statements from user and execute them. """
    QUIT = 'quit'
    initialized = False

    @classmethod
    def run(cls):
        """ Get SQL statemnts from user, parse, and execute. """
        if not Shell.initialized:
            sqlexec.initialize_db_env()
            Shell.initialized = True

        print(cls.QUIT, "to end")
        while True:
            sql = input('SQL> ')
            if sql == cls.QUIT:
                return
            try:
                parse = SQLstatement.parseString(sql)
                cls._execute(parse)
            except Exception as x:
                print(type(x).__name__, ': ', x.args[0], sep='')
                # raise  # FIXME - take this out except for debugging

    @classmethod
    def _execute(cls, parse):
        """ Execute the SQL statement from the given parse tree. """
        print(cls.unparse(parse))
        columns, attributes, rows, message = sqlexec.dispatch(parse).execute()
        cls.print_results(columns, attributes, rows, message)

    @staticmethod
    def unparse(parse):
        def to_str(a):
            if isinstance(a, list):
                words = [to_str(e) for e in a]
                return ' '.join(words)
            return str(a)
        return to_str(parse.asList())

    @classmethod
    def print_results(cls, columns, attributes, rows, message):
        """ Print out the rows which is a list of lists. The columns is dictionary with column info. """
        if not rows:
            print(message)
            return
        print(columns)
        print('-' * 12 * len(columns))
        for row in rows:
            print([row[k] for k in columns])

if __name__ == "__main__":
    Shell.run()