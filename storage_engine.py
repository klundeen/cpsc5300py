""" Database Storage Engines

"""
from abc import ABC, abstractmethod

class DbBlock(ABC):
    """ Abstraction of a storing records in a database file block. """
    def __init__(self, block=None, block_size=None, block_id=None):
        """
        Initialize a DbBlock:
        :param block: page from the database that is using SlottedPage
        :param block_size: initialize a new empty page for the database that is to use SlottedPage
        """
        self.id = block_id
        if block is None:
            self.block = bytearray(b'\0' * block_size)
        else:
            self.block = bytearray(block)

    @abstractmethod
    def add(self, data):
        """ Add a new record to the block. Return its id. """
        raise TypeError("Not implemented")

    @abstractmethod
    def get(self, id):
        """ Get a record from the block. """
        raise TypeError("Not implemented")

    @abstractmethod
    def delete(self, id):
        """ Delete record. """
        raise TypeError("Not implemented")

    @abstractmethod
    def put(self, id, data):
        """ Put record with given id. Overwrite previous data for this id. """
        raise TypeError("Not implemented")

    @abstractmethod
    def ids(self):
        """ Sequence of ids extant in this block (not including deleted ones). """
        raise TypeError("Not implemented")

class DbFile(ABC):
    """ Abstraction of of database file -- a collection of blocks. """

    def __init__(self, name):
        self.name = name

    @abstractmethod
    def create(self):
        """ Create a new database file with given name.
            Raises an exception if the file already exists.
        """
        raise TypeError('not implemented')

    @abstractmethod
    def open(self):
        """ Open an existing database file with given name.
            Raises an exception if the file does not exist or is improperly configured.
        """
        raise TypeError('not implemented')

    def close(self):
        """ Close the file. """
        pass

    @abstractmethod
    def delete(self):
        """ Delete the file."""
        raise TypeError('not implemented')

    @abstractmethod
    def get(self, block_id):
        """ Get a block from the database file.
            Returns the DbBlock that is managing the records in the given block.
        """
        raise TypeError('not implemented')

    @abstractmethod
    def get_new(self):
        """ Allocate a new block for the database file.
            Returns the new empty DbBlock that is managing the records in this block.
        """
        raise TypeError('not implemented')

    @abstractmethod
    def put(self, block):
        """ Signals the intent that the given block should be written back to the database file. """
        raise TypeError('not implemented')

    @abstractmethod
    def block_ids(self):
        """ Sequence of block ids for this file. """
        raise TypeError('not implemented')


class DbRelation(ABC):
    """ Abstraction of a database relation as expressed through a storage engine. """

    def __init__(self, table_name, column_names, column_attributes):
        self.table_name = table_name
        self.column_names = column_names
        self.columns = column_attributes

    @abstractmethod
    def create(self):
        """ Execute: CREATE TABLE <table_name> ( <columns> )
            Is not responsible for metadata storage or validation.
        """
        raise TypeError('not implemented')

    @abstractmethod
    def open(self):
        """ Open existing table. """
        raise TypeError('not implemented')

    @abstractmethod
    def drop(self):
        """ Execute: DROP TABLE <table_name> """
        raise TypeError('not implemented')

    @abstractmethod
    def create_if_not_exists(self):
        """ Execute: CREATE TABLE IF NOT EXISTS <table_name> ( <columns> )
            Is not responsible for metadata storage or validation.
        """
        raise TypeError('not implemented')

    @abstractmethod
    def insert(self, row):
        """ Expect row to be a dictionary with column name keys.
            Execute: INSERT INTO <table_name> (<row_keys>) VALUES (<row_values>)
            Return the handle of the inserted row.
        """
        raise TypeError('not implemented')

    @abstractmethod
    def update(self, handle, new_values):
        """ Expect new_values to be a dictionary with column name keys.
            Conceptually, execute: UPDATE INTO <table_name> SET <new_values> WHERE <handle>
            where handle is sufficient to identify one specific record (e.g., returned from an insert
            or select).
        """
        raise TypeError('not implemented')

    @abstractmethod
    def delete(self, handle):
        """ Conceptually, execute: DELETE FROM <table_name> WHERE <handle>
            where handle is sufficient to identify one specific record (e.g., returned from an insert
            or select).
        """
        raise TypeError('not implemented')

    @abstractmethod
    def select(self, where=None, limit=None, order=None, group=None):
        """ Conceptually, execute: SELECT <handle> FROM <table_name> WHERE <where>
            Returns a list of handles for qualifying rows.
        """
        raise TypeError('not implemented')

    @abstractmethod
    def project(self, handle, column_names=None):
        """ Return a sequence of values for handle given by column_names. """
        raise TypeError('not implemented')
