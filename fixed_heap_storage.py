""" Heap storage of fixed-length records. """

import os
import unittest
from storage_engine import DbBlock, DbRelation
from heap_storage import DB_BLOCK_SIZE, DB_ENV, HeapFile, HeapTable

class FixedLengthRecordBlock(DbBlock):
    """ Block that stores a series of fixed length records in a heap.
        Each block has a free list, with head pointer the first 2 bytes of the block.
        Record ids start at zero.
    """
    def __init__(self, data_length, block=None, block_size=None, block_id=None):
        """
        :param data_length: size in bytes of each record
        :param block: page from the database that is using SlottedPage
        :param block_size: initialize a new empty page for the database that is to use SlottedPage
        :param block_id: id within DbFile
        """
        super().__init__(block=block, block_size=block_size, block_id=block_id)
        self.data_length = data_length
        self.max_records = (self.block_size - 2) // self.data_length
        if self.max_records == 0:
            raise ValueError('impossible to have data_length > block_size')
        if block is None:
            # set up the free list
            self._put_n(0, 0)  # head = record 0
            self.free_list = set(range(self.max_records))
            for id in self.free_list:
                self._put_n(self._offset(id), id + 1)

    def add(self, data):
        """ Add a new record to the block. Return its id. """
        # take first entry from free list
        id = self._get_n(0)  # record = head
        if id >= self.max_records:
            raise ValueError('Not enough room in block')
        offset = self._offset(id)
        next = self._get_n(offset)  # next = record->next
        self.block[offset:offset+self.data_length] = data
        self._put_n(0, next)  # head = next
        self.free_list.remove(id)
        return id

    def get(self, id):
        """ Get a record from the block. """
        if id in self.free_list:
            return None
        offset = self._offset(id)
        return self.block[offset:offset+self.data_length]

    def delete(self, id):
        """ Delete record. """
        if id in self.free_list:
            return
        # stick it at front of free list
        next = self._get_n(0)  # next = head
        offset = self._offset(id)
        self._put_n(offset, next)  # new->next = next
        self._put_n(0, id)  # head = new
        self.free_list.add(id)

    def put(self, id, data):
        """ Put record with given id. Overwrite previous data for this id. """
        offset = self._offset(id)
        self.block[offset:offset+self.data_length] = data

    def ids(self):
        """ Sequence of ids extant in this block (not including deleted ones). """
        return (id for id in range(self.max_records) if id not in self.free_list)

    def _offset(self, id):
        return id * self.data_length + 2


class TestFixedLengthRecordBlock(unittest.TestCase):
    def test_basics(self):
        p = FixedLengthRecordBlock(data_length=4, block_size=30);

        # additions
        id = p.add(b'Help');
        id2 = p.add(b'Wow!');
        self.assertEqual(p.get(id), b'Help')
        self.assertEqual(p.get(id2), b'Wow!')

        # replacement
        p.put(id, b'Good')
        self.assertEqual(p.get(id2), b'Wow!')
        self.assertEqual(p.get(id), b'Good')
        p.put(id, b'Tiny')
        self.assertEqual(p.get(id2), b'Wow!')
        self.assertEqual(p.get(id), b'Tiny')

        # iteration
        self.assertEqual([i for i in p.ids()], [0, 1])

        # deletion
        p.delete(id)
        self.assertIsNone(p.get(id))
        self.assertEqual([i for i in p.ids()], [1])
        p.add(b'Gent')
        self.assertEqual({bytes(p.get(i)) for i in p.ids()}, {b'Wow!', b'Gent'})

        # the block
        self.assertEqual(p.block,
                    b'\x00\x02GentWow!\x00\x03\x00\x00\x00\x04\x00\x00\x00\x05\x00\x00\x00\x06\x00\x00\x00\x07\x00\x00')


class FixedHeapFile(HeapFile):
    def __init__(self, name, block_size, record_size):
        super().__init__(block_size, record_size)
        self.record_size = record_size

    def get(self, block_id):
        """ Get a block from the database file. """
        return FixedLengthRecordBlock(data_length=self.record_size, block=self.db.get(block_id), block_id=block_id)

    def get_new(self):
        """ Allocate a new block for the database file.
            Returns the new empty DbBlock that is managing the records in this block.
        """
        self.last += 1
        return FixedLengthRecordBlock(data_length=self.record_size, block_size=self.block_size, block_id=self.last)

class FixedHeapTable(HeapTable):
    """ HeapTable with only fixed length fields.
        Currently, only supports columns with data type INT.
    """
    def __init__(self, table_name, column_names, column_attributes, signed=True):
        super(DbRelation, self).__init__(table_name, column_names, column_attributes)
        self.record_size = 0
        for column in column_attributes:
            if column['data_type'] != 'INT':
                raise ValueError(type(self).__name__ + ' only supports INT columns')
            self.record_size += 4
        self.file = FixedHeapFile(table_name, DB_BLOCK_SIZE, self.record_size)
        self.signed = signed

    def _marshal(self, row):
        data = bytes()
        for column_name in self.column_names:
            data += int.to_bytes(row[column_name], 4, 'big', signed=self.signed)
        return data

    def _unmarshal(self, data):
        row = {}
        offset = 0
        for column_name in self.column_names:
            row[column_name] = int.from_bytes(data[offset:offset + 4], byteorder='big', signed=self.signed)
            offset += 4
        return row


class TestFixedHeapTable(unittest.TestCase):
    def testCreateDrop(self):
        # get rid of underlying file in case it's around from previous failed test
        try: os.remove(os.path.join(DB_ENV, '_test_create_drop.db'))
        except FileNotFoundError: pass

        table = HeapTable('_test_fixed_create_drop', ['a', 'b'], {'a': {}, 'b': {}})
        table.create()
        self.assertTrue(os.path.isfile(table.file.dbfilename))
        table.drop()
        self.assertFalse(os.path.isfile(table.file.dbfilename))

    def testData(self):
        # get rid of underlying file in case it's around from previous failed test
        try: os.remove(os.path.join(DB_ENV, '_test_fixed_data.db'))
        except FileNotFoundError: pass

        table = HeapTable('_test_data', ['a', 'b'], {'a': {'data_type': 'INT'}, 'b': {'data_type': 'INT'}})
        table.create_if_not_exists()
        table.close()
        table.open()
        rows = [{'a': 12, 'b': 99}, {'a': -192, 'b': 100},
                {'a': 1000, 'b': 1}] * 1000
        for row in rows:
            table.insert(row)
        for i, handle in enumerate(table.select()):
            self.assertEqual(table.project(handle), rows[i])
        table.drop()

if __name__ == '__main__':
    unittest.main()
