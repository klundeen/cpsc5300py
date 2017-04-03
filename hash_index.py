""" Hash Indexing """

from math import log2
import os
import unittest
from storage_engine import DbIndex
from heap_storage import initialize, HeapFile, HeapTable, DB_BLOCK_SIZE, BYTE_ORDER
from fixed_heap_storage import FixedHeapTable

MAX_BITS = 16
HASH_BYTES = MAX_BITS//8
MAX_BIT_MASK = 2**MAX_BITS - 1  # i.e., 0xffff when MAX_BITS is 16


# This should be a slotted page with each record being full_hash:handles_with_that_hash
# We also need to store number of bits used and value of those bits, i.e., hash_prefix

class _HashBucket(object):
    """ Bucket of (handle, hash) pairs, with possible overflow. """
    def __init__(self, block, bits_used=None, hash_prefix=None):
        """
        Construct a _HashBucket around a SlottedPage block
        :param block: SlottedPage
        :param bits_used: all records in this bucket has bits_used bits of their hash key in common
        :param hash_prefix: the left-most bits_used bits of the hash key of the records in this bucket (all the same)
        """
        self.block = block
        self.id = self.block.id
        if bits_used is not None:
            self.hash_prefix = hash_prefix
            self.bits_used = bits_used
            self.block.add(self._marshal(self.hash_prefix, [(self.bits_used, 0)]))
        else:
            self.hash_prefix, bits_used_list = self._unmarshal(self.block.get(1))
            self.bits_used = bits_used_list[0][0]

    def __len__(self):
        return len(self.block) - 1

    def lookup(self, h=None):
        """ Find the list of handles that correspond to given hash, h. If h is None, then return the first
            record.
        """
        record_id, data = self._find(h)
        if record_id is None:
            return []
        return self._unmarshal(data, just_handles=True)

    def add(self, h, handle, new_list=False, unique=False):
        """ Add handle to the list of handles for the hash, h. """
        record_id, data = self._find(h)
        if record_id is None:
            handles = [handle] if not new_list else handle
            self.block.add(self._marshal(h, handles))
        else:
            if unique:
                raise ValueError('duplicate entry')
            handles = self._unmarshal(data, just_handles=True)
            handles.append(handle)
            self.block.put(record_id, self._marshal(h, handles))

    def remove(self, h, handle):
        """ Remove given handle for hash, h. """
        record_id, data = self._find(h)
        if record_id is None:
            return
        handles = self._unmarshal(data, just_handles=True)
        handles.remove(handle)
        if len(handles) == 0:
            self.block.delete(record_id)
        else:
            self.block.put(record_id, self._marshal(h, handles))

    def delete(self, h):
        """ Delete all handles for given hash, h. """
        record_id, data = self._find(h)
        if record_id is not None:
            self.block.delete(record_id)

    def is_overflow(self):
        """ Is this an overflow bucket? """
        return self.bits_used > MAX_BITS

    def set_overflow(self):
        self.set_hash_prefix(bits_used=MAX_BITS + 1)

    def set_hash_prefix(self, hash_prefix=None, bits_used=None):
        """ Change hash_prefix and bits_used. """
        if hash_prefix is not None:
            self.hash_prefix = hash_prefix
        if bits_used is not None:
            self.bits_used = bits_used
        self.block.put(1, self._marshal(self.hash_prefix, [(self.bits_used, 0)]))

    def records(self):
        """ Generate all the (hash, handles) pairs in this bucket. """
        for record_id in self.block.ids():
            if record_id > 1:
                yield self._unmarshal(self.block.get(record_id))

    def _find(self, h):
        """ Find the record with hash, h. """
        if h is None:
            return 2, self.block.get(2)
        for record_id in self.block.ids():
            if record_id > 1:
                data = self.block.get(record_id)
                this_h = self._unmarshal(data, just_hash=True)
                if h == this_h:
                    return record_id, data
        return None, None

    @staticmethod
    def _marshal(h, handles):
        """ Turn h and handles list into bits.
            <h> <handle[0][0]> <handle[0][1]> <handle[1][0]> <handle[1][1]> etc.
        """
        def to_bytes(n, sz):
            return n.to_bytes(sz, BYTE_ORDER, signed=False)

        data = to_bytes(h, HASH_BYTES)
        for block_id, record_id in handles:
            data += to_bytes(block_id, 4)
            data += to_bytes(record_id, 2)
        return data

    @staticmethod
    def _unmarshal(data, just_hash=False, just_handles=False):
        """ Invert _marshal(). """
        def from_bytes(ofs, sz):
            return int.from_bytes(data[ofs:ofs+sz], BYTE_ORDER)

        h = None
        if not just_handles:
            h = from_bytes(0, HASH_BYTES)
            if just_hash:
                return h
        offset = HASH_BYTES
        end = len(data)
        handles = []
        while offset < end:
            block_id = from_bytes(offset, 4)
            offset += 4
            record_id = from_bytes(offset, 2)
            offset += 2
            handles.append((block_id, record_id))
        if just_handles:
            return handles
        else:
            return h, handles

    def dump(self):
        """ For debugging. """
        print("hash_prefix:", self.hash_prefix, "bits_used: ", self.bits_used)
        for h, handles in self.records():
            print(h, ":", handles)


class HashIndex(DbIndex):
    """ Hash Index on a relation using extendable hashing. Does not support range().
        Doesn't store key values in index; has to fetch row on a lookup.
        Currently we only shrink when a bucket is empty.
        
        Requires that the entire bucket address table fit into memory.
        We limit the max number of hash bits to MAX_BITS. At 16 bits that means a fully-split bucket address table
        will have 64K entries so should easily fit in memory. This index is not recommended in its current state for 
        very large tables.
        
        Stores the bucket address table (referred to as "entries") in a separate FixedHeapTable, 
        <table>-<index>-entries.db.
        Stores the non-overflowing buckets in a HeapFile, <table>-<index>-buckets.db
        Stores each overflowed bucket in its own FixedHeapTable, <table>-<index>-<buckethash>.db
    """

    def __init__(self, relation, name, key, unique=False):
        super().__init__(relation, name, key, unique)
        self.file_prefix = self.relation.table_name + '-' + self.name + '-'  # forces uniqueness within this relation
        self.buckets = HeapFile(self.file_prefix + 'buckets', DB_BLOCK_SIZE)
        self.entries = FixedHeapTable(self.file_prefix + 'entries',
                                      column_names=['bucket_id'], column_attributes={'bucket_id': {'data_type': 'INT'}},
                                      signed=False)
        self.overflow_column_names = ['block_id', 'record_id']  # handle components
        self.overflow_column_attributes = {'block_id': {'data_type': 'INT'}, 'record_id': {'data_type': 'INT'}}
        self.overflow_cache = {}
        self.bucket_table_bits = None
        self.bucket_address_table = None
        self.closed = True

    def create(self):
        """ Create the index. """
        self.buckets.create()
        bucket = _HashBucket(block=self.buckets.get(self.buckets.last), hash_prefix=0, bits_used=0)
        self.buckets.put(bucket.block)

        self.entries.create()
        self.entries.insert({'bucket_id': bucket.id})

        self.bucket_address_table = [bucket.id]
        self.bucket_table_bits = 0
        self.closed = False

        # now build the index! -- add every row from relation into index
        self.buckets.begin_write()
        for handle in self.relation.select():
            self.insert(handle)
        self.buckets.end_write()

    def drop(self):
        """ Drop the index. """
        self.open()
        self.buckets.delete()
        for bucket_number in self.buckets.block_ids():
            bucket = _HashBucket(block=self.buckets.get(bucket_number))
            if bucket.is_overflow():
                overflow = self._get_overflow(bucket)
                overflow.drop()
        self.entries.drop()

    def open(self):
        """ Open existing index. Enables: lookup, [range if supported], insert, delete, update. """
        if self.closed:
            self.buckets.open()
            self.entries.open()
            self._read_bucket_address_table()
            self.closed = False

    def close(self):
        """ Closes the index. Disables: lookup, [range if supported], insert, delete, update. """
        self.buckets.close()
        self.entries.close()
        self.closed = True

    def lookup(self, key):
        """ Find all the rows whose columns are equal to key. Assumes key is a dictionary whose keys are the column 
            names in the index. Returns a list of row handles.
        """
        h = self._hash(key)
        bucket = self._get_bucket(h)
        return (handle for handle in bucket.lookup(h) if self.relation.project(handle, key) == key)

    def insert(self, handle):
        """ Insert a row with the given handle. Row must exist in relation already. """
        key = self.relation.project(handle, self.key)
        h = self._hash(key)
        bucket = self._get_bucket(h)
        success = False
        while not success:
            if bucket.is_overflow():
                self._add_to_overflow(self._get_overflow(bucket), handle)
                return
            else:
                try:
                    bucket.add(h, handle, unique=self.unique)
                    success = True
                except ValueError:
                    self._split(bucket)
                    bucket = self._get_bucket(h)
        self.buckets.put(bucket.block)

    def delete(self, handle):
        """ Delete a row with the given handle. Row must still exist in relation. """
        key = self.relation.project(handle, self.key)
        h = self._hash(key)
        bucket = self._get_bucket(h)
        bucket.remove(h, handle)
        if len(bucket) == 0:
            self._shrink(bucket)

    def _hash(self, key):
        """ Hash function. """
        # NOTE: python already generates a 64-bit hash for most data types, so we use theirs by constructing a tuple of
        #       all the key values in order (remember that self.key is the column names in the index in order)
        return abs(hash(tuple(key[col] for col in self.key))) & MAX_BIT_MASK  # mask off all but the bottom MAX_BITS

    def _get_bucket(self, h):
        """ Find the bucket for the given hash value. """
        bucket_table_entry = h >> (MAX_BITS - self.bucket_table_bits)  # discard the lower bits with right shift
        bucket_id = self.bucket_address_table[bucket_table_entry]
        return _HashBucket(block=self.buckets.get(bucket_id))

    def _get_overflow(self, bucket):
        """ Get the FixedHeapTable holding the overflow for given bucket. """
        if bucket.hash_prefix in self.overflow_cache:
            return self.overflow_cache[bucket.hash_prefix]
        overflow = FixedHeapTable(self.file_prefix + str(bucket.hash_prefix),
                                  column_names=self.overflow_column_names,
                                  column_attributes=self.overflow_column_attributes)
        overflow.open()
        self.overflow_cache[bucket.hash_prefix] = overflow
        return overflow

    @staticmethod
    def _add_to_overflow(overflow, handle):
        block_id, record_id = handle
        overflow.insert({'block_id': block_id, 'record_id': record_id})

    def _read_bucket_address_table(self):
        """ Read in the bucket address table from self.entiries. """
        bat = []
        for handle in self.entries.select():
            row = self.entries.project(handle)
            bat.append(row['bucket_id'])
        self.bucket_address_table = bat
        self.bucket_table_bits = int(log2(len(bat)))  # we know this from the number of entries

    def _split(self, bucket):
        """ Split the given bucket. If there are two or more entries in the bucket address table, then just fix up
            the pointers there. If there is only one entry in the bat, then we have to double the size of the bat, too.
        """
        if bucket.bits_used == MAX_BITS:
            # fully split -- we need an overflow for this bucket
            h, handles = bucket.lookup()
            overflow = FixedHeapTable(self.file_prefix + str(h),
                                      column_names=self.overflow_column_names,
                                      column_attributes=self.overflow_column_attributes)
            self.overflow_cache[h] = overflow
            for handle in handles:
                self._add_to_overflow(overflow, handle)
            bucket.set_overflow()
            self.buckets.put(bucket.block)
            return

        # split the bucket into bucket0 and bucket1
        h0 = bucket.hash_prefix * 2
        h1 = h0 + 1
        bucket0 = bucket
        bucket0.set_hash_prefix(h0, bucket0.bits_used + 1)
        bucket1 = _HashBucket(block=self.buckets.get_new(), hash_prefix=h1, bits_used=bucket0.bits_used)
        to_move = []
        for h, handles in bucket0.records():
            if (h >> (MAX_BITS - bucket0.bits_used)) == h1:
                to_move.append((h, handles))
        for h, handles in to_move:
            bucket0.delete(h)
            bucket1.add(h, handles, new_list=True)
        self.buckets.put(bucket0.block)
        self.buckets.put(bucket1.block)

        # now fix up bucket address table
        if self.bucket_table_bits >= bucket0.bits_used:
            # have more than one pointer to old bucket, now bucket0, so fix the ones that should now point to bucket1
            h1_extended = h1 << (self.bucket_table_bits - bucket1.bits_used)
            next_hash = (h1 + 1) << (self.bucket_table_bits - bucket1.bits_used)
            # fix our in-memory table
            for bucket_table_entry in range(h1_extended, next_hash):
                self.bucket_address_table[bucket_table_entry] = bucket1.id
            # fix up the on-disk version
            for n, entry_handle in enumerate(self.entries.select()):
                if n == next_hash:
                    break
                if n >= h1_extended:
                    self.entries.update(entry_handle, {'bucket_id': bucket1.id})

        else:
            # double the size of the bucket address table
            self.bucket_table_bits += 1
            bat = []
            for bucket_id in self.bucket_address_table:
                bat.append(bucket_id)  # old hash * 2
                bat.append(bucket_id)  # old hash * 2 + 1
            bat[h0] = bucket0.id
            bat[h1] = bucket1.id
            self.bucket_address_table = bat

            # and rewrite the on-disk version
            # first half are updates, next half are inserts
            self.entries.begin_write()
            for n1, entry_handle in enumerate(self.entries.select()):
                self.entries.update(entry_handle, {'bucket_id': bat[n1]})
            for n2 in range(n1+1, len(bat)):
                self.entries.insert({'bucket_id': bat[n2]})
            self.entries.end_write()

    def _shrink(self, bucket):
        """ Remove empty bucket. """
        # FIXME
        pass

    def dump(self):
        """ Print out the internal datastructures for debugging. """
        print('bucket_table_bits:', self.bucket_table_bits)
        print('bucket_address_table:')
        for i, block_id in enumerate(self.bucket_address_table):
            print("{:04b}: {}".format(i, block_id))
        print()
        for block_id in range(1, self.buckets.last+1):
            print(block_id)
            bucket = _HashBucket(block=self.buckets.get(block_id))
            bucket.dump()


class TestHashIndex(unittest.TestCase):
    def setUp(self):
        dbenv = os.path.expanduser('~/.dbtests')
        if not os.path.exists(dbenv):
            os.makedirs(dbenv)
        for file in os.listdir(dbenv):
            os.remove(os.path.join(dbenv, file))
        initialize(dbenv)

    def testHashIndex(self):
        table = HeapTable('foo', ['a', 'b'], {'a': {'data_type': 'INT'}, 'b': {'data_type': 'INT'}})
        table.create()
        row1 = {'a': 12, 'b': 99}
        row2 = {'a': 88, 'b': 101}
        table.insert(row1)
        table.insert(row2)
        index = HashIndex(table, 'fooindex', ['a'])
        index.create()
        result = [table.project(handle) for handle in index.lookup({'a': 12})]
        self.assertEqual(result, [row1])
        result = [table.project(handle) for handle in index.lookup({'a': 88})]
        self.assertEqual(result, [row2])
        result = [table.project(handle) for handle in index.lookup({'a': 6})]
        self.assertEqual(result, [])

        for i in range(1000):
            row = {'a': i+100, 'b': -i}
            index.insert(table.insert(row))
        for i in range(1000):
            if i in [225, 226, 230, 231, 234]:
                continue
            result = [table.project(handle) for handle in index.lookup({'a': i+100})]
            self.assertEqual(result, [{'a': i+100, 'b': -i}])

        # test overflow
        row = {'a': -123, 'b': 0}
        for i in range(300):
            index.insert(table.insert(row))
        handles = [handle for handle in index.lookup(row)]
        self.assertEqual(table.project(handles[0]), row)
        self.assertEqual(len(handles), 300)

        # FIXME: other things to test: delete, multiple keys, unique
