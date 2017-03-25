""" SQL execution

By: Kevin Lundeen
For: CPSC 4300, S17
"""
import os
import re
import sys
from bsddb3 import db as bdb
from sqlparse import RESERVED_WORDS

DB_ENV = '/Users/klundeen/cpsc4300env/data'  # this can get changed by calling initialize_db_env

def initialize_db_env(db_env = None):
    """ Initialize the database environment. Currently, nothing is required here. """
    if db_env is None:
        return  # don't change the default
    global DB_ENV
    DB_ENV = db_env

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
        return None, None, self.statement_type + ' is not implemented'

class SQLExecTableDefinition(SQLExec):
    pass  # FIXME

class SQLExecQuery(SQLExec):
    pass  # FIXME