""" Heap Storage Engine components

"""
import os
import re
import sys
import unittest
from berkeleydb import db as bdb
from storage_engine import DbBlock, DbFile, DbRelation

DB_BLOCK_SIZE = 4096
_DB_ENV = '/home/fac/lundeenk/cpsc5300/data'  # this can get changed by calling initialize_db_env


class SlottedPage(DbBlock):
    """ Manage a database block that contains several records.
        Modeled after slotted-page from Database Systems Concepts, 6ed, Figure 10-9.

        Record id are handed out sequentially starting with 1 as records are added with add().
        Each record has a header which is a fixed offset from the beginning of the block:
            Bytes 0x00 - Ox01: number of records
            Bytes 0x02 - 0x03: offset to end of free space
            Bytes 0x04 - 0x05: size of record 1
            Bytes 0x06 - 0x07: offset to record 1
            etc.

        Public API: SlottedPage(block=block), SlottedPage(block_size=block_size),
                    add(data), get(id), put(id, new_data), delete(id), ids()

    """
    BYTE_ORDER = 'big'

    def __init__(self, block=None, block_size=None, block_id=None):
        """
        :param block: page from the database that is using SlottedPage
        :param block_size: initialize a new empty page for the database that is to use SlottedPage
        """
        super().__init__(block=block, block_size=block_size, block_id=block_id)
        if block is None:
            self.num_records = 0
            self.end_free = block_size - 1
            self._put_header()
        else:
            self.num_records, self.end_free = self._get_header()

    def add(self, data):
        """ Add a new record to the block. Return its id. """
        if not self._has_room(len(data) + 4):
            raise ValueError('Not enough room in block')
        self.num_records += 1
        id = self.num_records
        size = len(data)
        self.end_free -= size
        loc = self.end_free + 1
        self._put_header()
        self._put_header(id, size, loc)
        self.block[loc:loc + size] = data
        return id

    def get(self, id):
        """ Get a record from the block. Return None if it has been deleted. """
        size, loc = self._get_header(id)
        if loc == 0:
            return None  # this is just a tombstone, record has been deleted
        return self.block[loc:loc + size]

    def delete(self, id):
        """ Mark the given id as deleted by changing its size to zero and its location to 0.
            Compact the rest of the data in the block. But keep the record ids the same for everyone.
        """
        size, loc = self._get_header(id)
        self._put_header(id, 0, 0)
        self._slide(loc, loc + size)

    def put(self, id, data):
        """ Replace the record with the given data. Raises ValueError if it won't fit. """
        size, loc = self._get_header(id)
        new_size = len(data)
        if new_size > size:
            extra = new_size - size
            if not self._has_room(extra):
                raise ValueError('Not enough room in block')
            self._slide(loc + new_size, loc + size)
            self.block[loc - extra:loc + new_size] = data
        else:
            self.block[loc:loc + new_size] = data
            self._slide(loc + new_size, loc + size)
        size, loc = self._get_header(id)
        self._put_header(id, new_size, loc)

    def ids(self):
        """ Sequence of all non-deleted record ids. """
        return (i for i in range(1, self.num_records + 1) if self._get_header(i)[1] != 0)

    def _get_header(self, id=0):
        """ Get the size and offset for given id. For id of zero, it is the block header. """
        return self._get_n(4 * id), self._get_n(4 * id + 2)

    def _put_header(self, id=0, size=None, loc=None):
        """ Put the size and offset for given id. For id of zero, store the block header. """
        if size is None:
            size, loc = self.num_records, self.end_free
        self._put_n(4 * id, size)
        self._put_n(4 * id + 2, loc)

    def _has_room(self, size):
        """ Calculate if we have room to store a record with given size. The size should include the 4 bytes
            for the header, too, if this is an add.
        """
        available = self.end_free - (self.num_records + 1) * 4
        return size <= available

    def _slide(self, start, end):
        """ If start < end, then remove data from offset start up to but not including offset end by sliding data
            that is to the left of start to the right. If start > end, then make room for extra data from end to start
            by sliding data that is to the left of start to the left.
            Also fix up any record headers whose data has slid. Assumes there is enough room if it is a left
            shift (end < start).
        """
        shift = end - start
        if shift == 0:
            return

        # slide data
        self.block[self.end_free + 1 + shift: end] = self.block[self.end_free + 1: start]

        # fixup headers
        for id in self.ids():
            size, loc = self._get_header(id)
            if loc <= start:
                loc += shift
                self._put_header(id, size, loc)
        self.end_free += shift
        self._put_header()

    def _get_n(self, offset):
        """ Get 2-byte integer at given offset in block. """
        return int.from_bytes(self.block[offset:offset + 2], byteorder=self.BYTE_ORDER)

    def _put_n(self, offset, n):
        """ Put a 2-byte integer at given offset in block. """
        self.block[offset:offset + 2] = int.to_bytes(n, length=2, byteorder=self.BYTE_ORDER)


class TestSlottedPage(unittest.TestCase):
    def test_basics(self):
        p = SlottedPage(block_size=30);

        # additions
        id = p.add(b'Hello');
        id2 = p.add(b'Wow!');
        self.assertEqual(p.get(id), b'Hello')
        self.assertEqual(p.get(id2), b'Wow!')

        # replacement
        p.put(id, b'Goodbye')
        self.assertEqual(p.get(id2), b'Wow!')
        self.assertEqual(p.get(id), b'Goodbye')
        p.put(id, b'Tiny')
        self.assertEqual(p.get(id2), b'Wow!')
        self.assertEqual(p.get(id), b'Tiny')

        # iteration
        self.assertEqual([i for i in p.ids()], [1, 2])

        # deletion
        p.delete(id)
        self.assertIsNone(p.get(id))
        self.assertEqual([i for i in p.ids()], [2])
        p.add(b'George')
        self.assertEqual([p.get(i) for i in p.ids()], [b'Wow!', b'George'])

        # the block
        self.assertEqual(p.block,
                         b'\x00\x03\x00\x13\x00\x00\x00\x00\x00\x04\x00\x1a\x00\x06\x00\x14\x00\x00\x00WGeorgeWow!')


class HeapFile(DbFile):
    """ Heap file organization. Built on top of Berkeley DB RecNo file. There is one of our
        database blocks for each Berkeley DB record in the RecNo file. In this way we are using Berkeley DB
        for buffer management and file management.
        Uses SlottedPage for storing records within blocks.
    """
    def __init__(self, name, block_size):
        super().__init__(name)
        self.block_size = block_size
        self.closed = True

    def _db_open(self, openflags=0):
        """ Wrapper for Berkeley DB open, which does both open and creation. """
        if not self.closed:
            return
        self.db = bdb.DB()
        self.db.set_re_len(self.block_size)  # record length - will be ignored if file already exists
        self.dbfilename = os.path.join(_DB_ENV, self.name + '.db')
        dbtype = bdb.DB_RECNO  # we always use record number files
        self.db.open(self.dbfilename, None, dbtype, openflags)
        self.stat = self.db.stat(bdb.DB_FAST_STAT)
        self.last = self.stat['ndata']
        self.closed = False

    def create(self):
        """ Create physical file. """
        self._db_open(bdb.DB_CREATE | bdb.DB_EXCL)
        block = self.get_new()  # first block of the file
        self.put(block)

    def delete(self):
        """ Delete the physical file. """
        self.close()
        os.remove(self.dbfilename)

    def open(self):
        """ Open physical file. """
        self._db_open()
        self.block_size = self.stat['re_len']  # what's in the file overrides __init__ parameter

    def close(self):
        """ Close the physical file. """
        self.db.close()
        self.closed = True

    def get(self, block_id):
        """ Get a block from the database file. """
        return SlottedPage(block=self.db.get(block_id), block_id=block_id)

    def get_new(self):
        """ Allocate a new block for the database file.
            Returns the new empty DbBlock that is managing the records in this block and its block id.
        """
        self.last += 1
        return SlottedPage(block_size=self.block_size, block_id=self.last)

    def put(self, block):
        """ Write a block back to the database file. """
        self.db.put(block.id, bytes(block.block))

    def block_ids(self):
        """ Sequence of all block ids. """
        return (i for i in range(1, self.last + 1))


class HeapTable(DbRelation):
    """ Heap storage engine. """

    def __init__(self, table_name, column_names, column_attributes):
        super().__init__(table_name, column_names, column_attributes)
        self.file = HeapFile(table_name, DB_BLOCK_SIZE)

    def create(self):
        """ Execute: CREATE TABLE <table_name> ( <columns> )
            Is not responsible for metadata storage or validation.
        """
        self.file.create()

    def open(self):
        """ Open existing table. Enables: insert, update, delete, select, project"""
        self.file.open()

    def close(self):
        """ Closes the table. Disables: insert, update, delete, select, project"""
        self.file.close()

    def create_if_not_exists(self):
        """ Execute: CREATE TABLE IF NOT EXISTS <table_name> ( <columns> )
            Is not responsible for metadata storage or validation.
        """
        try:
            self.open()
        except bdb.DBNoSuchFileError:
            self.create()

    def drop(self):
        """ Execute: DROP TABLE <table_name> """
        self.file.delete()

    def insert(self, row):
        """ Expect row to be a dictionary with column name keys.
            Execute: INSERT INTO <table_name> (<row_keys>) VALUES (<row_values>)
            Return the handle of the inserted row.
        """
        self.open()
        return self._append(self._validate(row))

    def update(self, handle, new_values):
        """ Expect new_values to be a dictionary with column name keys.
            Conceptually, execute: UPDATE INTO <table_name> SET <new_values> WHERE <handle>
            where handle is sufficient to identify one specific record (e.g., returned from an insert
            or select).
        """
        raise TypeError('FIXME')

    def delete(self, handle):
        """ Conceptually, execute: DELETE FROM <table_name> WHERE <handle>
            where handle is sufficient to identify one specific record (e.g., returned from an insert
            or select).
        """
        raise TypeError('FIXME')

    def select(self, where=None, limit=None, order=None, group=None):
        """ Conceptually, execute: SELECT <handle> FROM <table_name> WHERE <where>
            Returns a list of handles for qualifying rows.
        """
        # FIXME: ignoring where, limit, order, and group
        for block_id in self.file.block_ids():
            for record_id in self.file.get(block_id).ids():
                yield (block_id, record_id)

    def project(self, handle, column_names=None):
        """ Return a sequence of values for handle given by column_names. """
        block_id, record_id = handle
        block = self.file.get(block_id)
        data = block.get(record_id)
        row = self._unmarshal(data)
        if column_names is None:
            return row
        else:
            return {k: row[k] for k in column_names}

    def _validate(self, row):
        """ Check if the given row is acceptable to insert. Raise ValueError if not.
            Otherwise return the full row dictionary.
        """
        full_row = {}
        for column_name in self.columns:
            column = self.columns[column_name]
            if column_name not in row:
                raise ValueError("don't know how to handle NULLs, defaults, etc. yet")
            else:
                value = row[column_name]
            full_row[column_name] = value
        return full_row

    def _append(self, row):
        """ Assumes row is fully fleshed-out. Appends a record to the file. """
        data = self._marshal(row)
        block = self.file.get(self.file.last)
        try:
            record_id = block.add(data)
        except ValueError:
            # need a new block
            block = self.file.get_new()
            record_id = block.add(data)
        self.file.put(block)
        return self.file.last, record_id

    def _marshal(self, row):
        data = bytes()
        for column_name in self.column_names:
            column = self.columns[column_name]
            if column['data_type'] == 'INT':
                data += int.to_bytes(row[column_name], 4, 'big', signed=True)
            elif column['data_type'] == 'TEXT':
                text = row[column_name].encode('utf-8')
                data += int.to_bytes(len(text), length=2, byteorder='big')
                data += text
            else:
                raise ValueError('Cannot marahal ' + column['data_type'])
        return data

    def _unmarshal(self, data):
        row = {}
        offset = 0
        for column_name in self.column_names:
            column = self.columns[column_name]
            if column['data_type'] == 'INT':
                row[column_name] = int.from_bytes(data[offset:offset + 4], byteorder='big', signed=True)
                offset += 4
            elif column['data_type'] == 'TEXT':
                size = int.from_bytes(data[offset:offset + 2], byteorder='big')
                offset += 2
                row[column_name] = data[offset:offset + size].decode('utf-8')
                offset += size
            else:
                raise ValueError('Cannot unmarahal ' + column['data_type'])
        return row


class TestHeapTable(unittest.TestCase):
    def testCreateDrop(self):
        # get rid of underlying file in case it's around from previous failed test
        try: os.remove(os.path.join(_DB_ENV, '_test_create_drop.db'))
        except FileNotFoundError: pass

        table = HeapTable('_test_create_drop', ['a', 'b'], {'a': {}, 'b': {}})
        table.create()
        self.assertTrue(os.path.isfile(table.file.dbfilename))
        table.drop()
        self.assertFalse(os.path.isfile(table.file.dbfilename))

    def testData(self):
        # get rid of underlying file in case it's around from previous failed test
        try: os.remove(os.path.join(_DB_ENV, '_test_data.db'))
        except FileNotFoundError: pass

        table = HeapTable('_test_data', ['a', 'b'], {'a': {'data_type': 'INT'}, 'b': {'data_type': 'TEXT'}})
        table.create_if_not_exists()
        table.close()
        table.open()
        # add about 10 blocks of data
        rows = [{'a': 12, 'b': 'Hello!'}, {'a': -192, 'b': 'Much longer piece of text here' * 100},
                {'a': 1000, 'b': ''}] * 10
        for row in rows:
            table.insert(row)
        for i, handle in enumerate(table.select()):
            self.assertEqual(table.project(handle), rows[i])
        table.drop()

if __name__ == '__main__':
    unittest.main()
