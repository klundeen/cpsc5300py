""" Heap Storage Engine components

"""
import os
import unittest
from bsddb3 import db as bdb
from storage_engine import DbBlock, DbFile, DbRelation

DB_BLOCK_SIZE = 4096
DB_ENV = ''
BYTE_ORDER = 'big'


def initialize(dbenv):
    """ Initialize the Heap Storage Engine. """
    global DB_ENV
    DB_ENV = dbenv


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

    def __init__(self, block_size, block=None, block_id=None):
        """
        :param block_size:
        :param block: page from the database that is using SlottedPage
        :param block_id: id within DbFile
        """
        super().__init__(block=block, block_size=block_size, block_id=block_id)
        self.block_size = block_size
        if block is None:
            self.num_records = 0
            self.end_free = block_size - 1
            self._put_header()
        else:
            self.num_records, self.end_free = self._get_header()

    def __len__(self):
        return sum(1 for _ in self.ids())

    def add(self, data):
        """ Add a new record to the block. Return its id. """
        if not self._has_room(len(data) + 4):
            raise ValueError('Not enough room in block')
        self.num_records += 1
        record_id = self.num_records
        size = len(data)
        self.end_free -= size
        loc = self.end_free + 1
        self._put_header()
        self._put_header(record_id, size, loc)
        self.block[loc:loc + size] = data
        return record_id

    def get(self, record_id):
        """ Get a record from the block. Return None if it has been deleted. """
        size, loc = self._get_header(record_id)
        if loc == 0:
            return None  # this is just a tombstone, record has been deleted
        return self.block[loc:loc + size]

    def delete(self, record_id):
        """ Mark the given record_id as deleted by changing its size to zero and its location to 0.
            Compact the rest of the data in the block. But keep the record ids the same for everyone.
        """
        size, loc = self._get_header(record_id)
        self._put_header(record_id, 0, 0)
        self._slide(loc, loc + size)

    def put(self, record_id, data):
        """ Replace the record with the given data. Raises ValueError if it won't fit. """
        size, loc = self._get_header(record_id)
        new_size = len(data)
        if new_size > size:
            extra = new_size - size
            if not self._has_room(extra):
                raise ValueError('Not enough room in block')
            self._slide(loc, loc - extra)
            self.block[loc - extra:loc + size] = data
        else:
            self.block[loc:loc + new_size] = data
            self._slide(loc + new_size, loc + size)
        size, loc = self._get_header(record_id)
        self._put_header(record_id, new_size, loc)

    def ids(self):
        """ Sequence of all non-deleted record ids. """
        return (i for i in range(1, self.num_records + 1) if self._get_header(i)[1] != 0)

    def clear(self):
        """ Delete all the records. """
        self.num_records = 0
        self.end_free = self.block_size - 1
        self._put_header()

    def _get_header(self, record_id=0):
        """ Get the size and offset for given record_id. For record_id of zero, it is the block header. """
        return self._get_n(4 * record_id), self._get_n(4 * record_id + 2)

    def _put_header(self, record_id=0, size=None, loc=None):
        """ Put the size and offset for given record_id. For record_id of zero, store the block header. """
        if size is None:
            size, loc = self.num_records, self.end_free
        self._put_n(4 * record_id, size)
        self._put_n(4 * record_id + 2, loc)

    def _has_room(self, size):
        """ Calculate if we have room to store a record with given size. The size should include the 4 bytes
            for the header, too, if this is an add.
        """
        available = self.end_free - (self.num_records + 2) * 4
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
        for record_id in self.ids():
            size, loc = self._get_header(record_id)
            if loc <= start:
                loc += shift
                self._put_header(record_id, size, loc)
        self.end_free += shift
        self._put_header()

    def dump(self):
        """ For debugging. """
        for i in range(1, self.num_records + 1):
            hdr = self._get_header(i)
            print(i, hdr, self.get(i) if i > 0 and hdr[1] != 0 else "")


class TestSlottedPage(unittest.TestCase):
    def test_basics(self):
        p = SlottedPage(block_size=32)

        # additions
        record_id = p.add(b'Hello')
        id2 = p.add(b'Wow!')
        self.assertEqual(p.get(record_id), b'Hello')
        self.assertEqual(p.get(id2), b'Wow!')

        # replacement
        p.put(record_id, b'Goodbye')
        self.assertEqual(p.get(id2), b'Wow!')
        self.assertEqual(p.get(record_id), b'Goodbye')
        p.put(record_id, b'Tiny')
        self.assertEqual(p.get(id2), b'Wow!')
        self.assertEqual(p.get(record_id), b'Tiny')

        # iteration
        self.assertEqual([i for i in p.ids()], [1, 2])

        # deletion
        p.delete(record_id)
        self.assertIsNone(p.get(record_id))
        self.assertEqual([i for i in p.ids()], [2])
        p.add(b'George')
        self.assertEqual({bytes(p.get(i)) for i in p.ids()}, {b'Wow!', b'George'})

        # the block
        self.assertEqual(p.block,
                         b'\x00\x03\x00\x15\x00\x00\x00\x00\x00\x04\x00\x1c\x00\x06\x00\x16\x00\x00\x00\x00\x00WGeorgeWow!')

    def test_more_deletes(self):
        p = SlottedPage(block_size=100)
        p.add(b'as;lkdjfa;sldfjk')
        id3 = p.add(b'stuff after')
        id4 = p.add(b'foo')
        id5 = p.add(b'more stuff around it')
        p.put(id4, b'something bigger')
        self.assertEqual(p.get(id3), b'stuff after')
        self.assertEqual(p.get(id4), b'something bigger')
        self.assertEqual(p.get(id5), b'more stuff around it')


class HeapFile(DbFile):
    """ Heap file organization. Built on top of Berkeley DB RecNo file. There is one of our
        database blocks for each Berkeley DB record in the RecNo file. In this way we are using Berkeley DB
        for buffer management and file management.
        Uses SlottedPage for storing records within blocks.
    """
    def __init__(self, name, block_size=DB_BLOCK_SIZE):
        super().__init__(name)
        self.block_size = block_size
        self.write_queue = {}
        self.write_lock = 0
        self.closed = True

    def _db_open(self, openflags=0):
        """ Wrapper for Berkeley DB open, which does both open and creation. """
        if not self.closed:
            return
        self.db = bdb.DB()
        self.db.set_re_len(self.block_size)  # record length - will be ignored if file already exists
        self.dbfilename = os.path.join(DB_ENV, self.name + '.db')
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
        self.open()
        self.close()
        os.remove(self.dbfilename)

    def open(self):
        """ Open physical file. """
        self._db_open()
        self.block_size = self.stat['re_len']  # what's in the file overrides __init__ parameter

    def close(self):
        """ Close the physical file. """
        # flush out any pending writes
        self.write_lock = 1
        self.end_write()
        if not self.closed:
            self.db.close()
            self.closed = True

    def get(self, block_id):
        """ Get a block from the database file. """
        if block_id in self.write_queue:
            return self.write_queue[block_id]
        return SlottedPage(self.block_size, block=self.db.get(block_id), block_id=block_id)

    def get_new(self):
        """ Allocate a new block for the database file.
            Returns the new empty DbBlock that is managing the records in this block.
        """
        self.last += 1
        return SlottedPage(self.block_size, block_id=self.last)

    def put(self, block):
        """ Write a block back to the database file. """
        self.begin_write()
        self.write_queue[block.id] = block
        self.end_write()

    def block_ids(self):
        """ Sequence of all block ids. """
        return (i for i in range(1, self.last + 1))

    def begin_write(self):
        """ Don't write out changes to file until the matching end_write is called. """
        self.write_lock += 1

    def end_write(self):
        """ See begin_write. """
        self.write_lock -= 1
        if self.write_lock == 0:
            for block in self.write_queue.values():
                self.db.put(block.id, bytes(block.block))
            self.write_queue = {}


class HeapTable(DbRelation):
    """ Heap storage engine. """

    def __init__(self, table_name, column_names, column_attributes, primary_key=None):
        super().__init__(table_name, column_names, column_attributes, primary_key)
        self.file = HeapFile(table_name)

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
        row = self.project(handle)
        for key in new_values:
            row[key] = new_values[key]
        full_row = self._validate(row)
        block_id, record_id = handle
        block = self.file.get(block_id)
        block.put(record_id, self._marshal(full_row))
        self.file.put(block)
        return handle

    def delete(self, handle):
        """ Conceptually, execute: DELETE FROM <table_name> WHERE <handle>
            where handle is sufficient to identify one specific record (e.g., returned from an insert
            or select).
        """
        self.open()
        block_id, record_id = handle
        block = self.file.get(block_id)
        block.delete(record_id)
        self.file.put(block)

    def select(self, where=None, limit=None, order=None, group=None, handles=None):
        """ Conceptually, execute: SELECT <handle> FROM <table_name> WHERE <where>
            If handles is specified, then use those as the base set of records to apply a refined selection to.
            Returns a list of handles for qualifying rows.
        """
        # FIXME: ignoring limit, order, group
        self.open()
        if handles is None:
            for block_id in self.file.block_ids():
                for record_id in self.file.get(block_id).ids():
                    if where is None or self._selected((block_id, record_id), where):
                        yield (block_id, record_id)
        else:
            for handle in handles:
                if where is None or self._selected(handle, where):
                    yield handle

    def project(self, handle, column_names=None):
        """ Return a sequence of values for handle given by column_names. """
        self.open()
        block_id, record_id = handle
        block = self.file.get(block_id)
        data = block.get(record_id)
        row = self._unmarshal(data)
        if column_names is None:
            return row
        else:
            return {k: row[k] for k in column_names}

    def begin_write(self):
        """ Don't write out changes to file until the matching end_write is called. """
        self.file.begin_write()

    def end_write(self):
        """ See begin_write. """
        self.file.end_write()

    def _selected(self, handle, where):
        """ Checks if given record succeeds given where clause. """
        row = self.project(handle, where)
        for column_name in where:
            if row[column_name] != where[column_name]:
                return False
        return True

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
            if 'validate' in column:
                if not column['validate'](value):
                    raise ValueError("value for column " + column_name + ", '" + value + "', is unacceptable")
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
        def to_bytes(n, sz, signed=False):
            return n.to_bytes(sz, BYTE_ORDER, signed=signed)

        data = bytes()
        for column_name in self.column_names:
            column = self.columns[column_name]
            if column['data_type'] == 'INT':
                data += to_bytes(row[column_name], 4, signed=True)
            elif column['data_type'] == 'BOOLEAN':
                data += to_bytes(int(row[column_name]), 1)
            elif column['data_type'] == 'TEXT':
                text = row[column_name].encode()
                data += to_bytes(len(text), 2)
                data += text
            else:
                raise ValueError('Cannot marahal ' + column['data_type'])
        return data

    def _unmarshal(self, data):
        def from_bytes(ofs, sz, signed=False):
            return int.from_bytes(data[ofs:ofs+sz], BYTE_ORDER, signed=signed)

        row = {}
        offset = 0
        for column_name in self.column_names:
            column = self.columns[column_name]
            if column['data_type'] == 'INT':
                row[column_name] = from_bytes(offset, 4, signed=True)
                offset += 4
            elif column['data_type'] == 'BOOLEAN':
                row[column_name] = bool(from_bytes(offset, 1))
                offset += 1
            elif column['data_type'] == 'TEXT':
                size = from_bytes(offset, 2)
                offset += 2
                row[column_name] = data[offset:offset + size].decode()
                offset += size
            else:
                raise ValueError('Cannot unmarahal ' + column['data_type'])
        return row


class TestHeapTable(unittest.TestCase):
    def testCreateDrop(self):
        # get rid of underlying file in case it's around from previous failed test
        try:
            os.remove(os.path.join(DB_ENV, '_test_create_drop.db'))
        except FileNotFoundError:
            pass

        table = HeapTable('_test_create_drop', ['a', 'b'], {'a': {}, 'b': {}})
        table.create()
        self.assertTrue(os.path.isfile(table.file.dbfilename))
        table.drop()
        self.assertFalse(os.path.isfile(table.file.dbfilename))

    def testData(self):
        # get rid of underlying file in case it's around from previous failed test
        try:
            os.remove(os.path.join(DB_ENV, '_test_data.db'))
        except FileNotFoundError:
            pass

        table = HeapTable('_test_data', ['a', 'b'], {'a': {'data_type': 'INT'}, 'b': {'data_type': 'TEXT'}})
        table.create_if_not_exists()
        table.close()
        table.open()
        # add about 10 blocks of data
        rows = [{'a': 12, 'b': 'Hello!'}, {'a': -192, 'b': 'Much longer piece of text here' * 100},
                {'a': 1000, 'b': ''}] * 10
        handles = []
        for row in rows:
            handles.append(table.insert(row))
        for i, handle in enumerate(table.select()):
            self.assertEqual(table.project(handle), rows[i])

        # delete
        self.assertEqual([table.project(x) for x in table.select(where=rows[-1])], [rows[-1]] * 10)
        table.delete(handles[-1])
        self.assertEqual([table.project(x) for x in table.select(where=rows[-1])], [rows[-1]] * 9)
        table.delete(handles[0])
        for i, handle in enumerate(table.select()):
            self.assertEqual(table.project(handle), rows[i+1])

        table.update(handles[1], {'a': 999})
        self.assertEqual([table.project(x) for x in table.select(where={'a': 999})][0]['a'], 999)

        table.drop()

if __name__ == '__main__':
    unittest.main()
