""" B+ Trees
    (WHile technically they are B+ trees, we use the more typical "BTree" terminology.)
"""

import os
import unittest
from storage_engine import DbIndex
from heap_storage import BYTE_ORDER, HeapFile, HeapTable, initialize


class _BTreeNode(object):
    """ Base class for interior and leaf nodes. """

    def __init__(self, file, block_id, key_profile, create=False):
        if create:
            self.block = file.get_new()
        else:
            self.block = file.get(block_id)
        self.file = file
        self.id = self.block.id
        self.key_profile = key_profile

    def _get_handle(self, record_id):
        """ Get the record and turn it into a (block_id,record_id) handle. """
        data = self.block.get(record_id)
        return int.from_bytes(data[0:4], BYTE_ORDER), int.from_bytes(data[4:6], BYTE_ORDER)

    @staticmethod
    def _marshal_handle(handle):
        """ Convert handle into bytes. """
        return handle[0].to_bytes(4, BYTE_ORDER) + handle[1].to_bytes(2, BYTE_ORDER)

    def _get_block_id(self, record_id):
        """ Get the record and turn it into a block ID. """
        return int.from_bytes(self.block.get(record_id), BYTE_ORDER)

    @staticmethod
    def _marshal_block_id(block_id):
        """ Convert block_id into bytes. """
        return block_id.to_bytes(4, BYTE_ORDER)

    def _get_key(self, record_id):
        data = self.block.get(record_id)
        ofs = 0
        values = []
        for data_type in self.key_profile:
            if data_type == "INT":
                values.append(int.from_bytes(data[ofs:ofs + 4], BYTE_ORDER, signed=True))
                ofs += 4
            else:  # TEXT
                size = int.from_bytes(data[ofs:ofs + 2], BYTE_ORDER)
                ofs += 2
                values.append(data[ofs:ofs + size].decode())
                ofs += size
        return tuple(values)

    def _marshal_key(self, tkey):
        """ Convert key to bytes. """
        data = bytearray()
        for idx, data_type in enumerate(self.key_profile):
            if data_type == 'INT':
                data += tkey[idx].to_bytes(4, BYTE_ORDER, signed=True)
            else:  # TEXT
                text = tkey[idx].encode()
                data += len(text).to_bytes(2, BYTE_ORDER)
                data += text
        return data

    def save(self):
        self.file.put(self.block)


class _BTreeStat(_BTreeNode):
    """ Block that holds global info about this index, in particular the block id of the root. """
    ROOT = 1  # record_id where the root's block id is stored
    HEIGHT = ROOT + 1  # record_id where is_leaf is stored

    def __init__(self, file, block_id, new_root=None, key_profile=None):
        super().__init__(file, block_id, key_profile, create=False)
        if new_root is not None:
            self.root_id = new_root
            self.height = 1
            self.save()
        else:
            self.root_id = self._get_block_id(self.ROOT)
            self.height = self._get_block_id(self.HEIGHT)

    def save(self):
        self.block.put(self.ROOT, self._marshal_block_id(self.root_id))
        self.block.put(self.HEIGHT, self._marshal_block_id(self.height))  # not really a block ID but it fits
        super().save()


class _BTreeInterior(_BTreeNode):
    """ Interior B+ tree node. Pointers are block IDs into index file. """

    def __init__(self, file, block_id, key_profile, create=False):
        super().__init__(file, block_id, key_profile, create)
        if not create:
            ids = [record_id for record_id in self.block.ids()]
            self.first = self._get_block_id(ids[0])
            pointers = ids[2::2]  # ids[2], ids[4], ids[6], ..., ids[n-1]
            keys = ids[1::2]      # ids[1], ids[3], ids[5], ..., ids[n-2]
            self.pointers = [self._get_block_id(pointer) for pointer in pointers]
            self.boundaries = [self._get_key(key_id) for key_id in keys]
        else:
            self.first = None
            self.pointers = []
            self.boundaries = []

    def find(self, key, depth):
        """ Get next block down in tree where key must be. """
        down = self.pointers[-1]  # last pointer is correct if we don't find an earlier boundary
        for i, boundary in enumerate(self.boundaries):
            if boundary > key:
                down = self.pointers[i-1] if i > 0 else self.first
                break
        if depth == 2:
            return _BTreeLeaf(self.file, down, self.key_profile)
        else:
            return _BTreeInterior(self.file, down, self.key_profile)

    def insert(self, boundary, block_id, skip_size_check=False):
        """ Insert boundary, block_id pair into block. """
        # check size
        if not skip_size_check:
            self.block.add(self._marshal_key(boundary))
            self.block.add(self._marshal_block_id(block_id))
        for i, check in enumerate(self.boundaries):  # FIXME use binary search
            if boundary == check:
                raise IndexError('Unexpected boundary for new BTree node')
            if boundary < check:
                self.boundaries.insert(i, boundary)
                self.pointers.insert(i, block_id)
                return
        # must go at the end
        self.boundaries.append(boundary)
        self.pointers.append(block_id)

    def save(self):
        self.block.clear()
        self.block.add(self._marshal_block_id(self.first))
        for i, check in enumerate(self.boundaries):
            self.block.add(self._marshal_key(check))
            self.block.add(self._marshal_block_id(self.pointers[i]))
        super().save()


class _BTreeLeaf(_BTreeNode):
    """ Leaf B+ tree node. Pointers are handles into the relation. """

    def __init__(self, file, block_id, key_profile, create=False):
        super().__init__(file, block_id, key_profile, create)
        if not create:
            ids = [record_id for record_id in self.block.ids()]
            self.next_leaf = self._get_block_id(ids[-1]) if len(ids) > 0 else 0
            pointers = ids[0:-1:2]  # ids[0], ids[2], ids[4], ..., ids[n-3]
            keys = ids[1::2]        # ids[1], ids[3], ids[5], ..., ids[n-2]
            self.keys = {self._get_key(keys[i]): self._get_handle(pointers[i]) for i in range(len(keys))}
        else:
            self.next_leaf = 0
            self.keys = {}

    def find_eq(self, key):
        """ Find the key and return the associated handle. Return None if not found. """
        if key in self.keys:
            return self.keys[key]

    def insert(self, tkey, handle):
        """ Insert key, handle pair into block. """
        # check unique
        if tkey in self.keys:
            raise IndexError('Duplicate keys are not allowed in unique index')
        # check size
        self.block.add(self._marshal_handle(handle))
        self.block.add(self._marshal_key(tkey))
        # if that didn't raise then we're good -- insert it
        self.keys[tkey] = handle

    def save(self):
        self.block.clear()
        key_list = sorted(self.keys)
        for i, key in enumerate(key_list):
            self.block.add(self._marshal_handle(self.keys[key]))
            self.block.add(self._marshal_key(key))
        self.block.add(self._marshal_block_id(self.next_leaf))
        super().save()


class BTreeIndex(DbIndex):
    """ The B+ Tree index.
        Only unique indices are supported. Try adding the primary key value to the index key to make it unique, 
        if necessary.
        Only insertion for the moment.
    """

    STAT = 1  # block_id of statistics data

    def __init__(self, relation, name, key, unique=False):
        if not unique:
            raise ValueError('BTreeIndex must be on a unique search key')
        super().__init__(relation, name, key, unique)
        self.file_prefix = self.relation.table_name + '-' + self.name  # forces uniqueness within this relation
        self.file = HeapFile(self.file_prefix)
        self._build_key_profile()
        self.stat = None
        self.root = None
        self.closed = True

    def create(self):
        """ Create the index. """
        self.file.create()
        self.stat = _BTreeStat(self.file, self.STAT, new_root=self.STAT + 1, key_profile=self.key_profile)
        self.root = _BTreeLeaf(self.file, self.stat.root_id, self.key_profile, create=True)
        self.closed = False

        # now build the index! -- add every row from relation into index
        self.file.begin_write()
        for handle in self.relation.select():
            self.insert(handle)
        self.file.end_write()

    def drop(self):
        """ Drop the index. """
        self.file.delete()

    def open(self):
        """ Open existing index. Enables: lookup, [range if supported], insert, delete, update. """
        if self.closed:
            self.file.open()
            self.stat = _BTreeStat(self.file, self.STAT)
            if self.stat.height == 1:
                self.root = _BTreeLeaf(self.file, self.stat.root_id, self.key_profile)
            else:
                self.root = _BTreeInterior(self.file, self.stat.root_id, self.key_profile)
            self.closed = False

    def close(self):
        """ Closes the index. Disables: lookup, [range if supported], insert, delete, update. """
        self.file.close()
        self.stat = self.root = None
        self.closed = True

    def lookup(self, key):
        """ Find all the rows whose columns are equal to key. Assumes key is a dictionary whose keys are the column 
            names in the index. Returns a list of row handles.
        """
        return self._lookup(self.root, self.stat.height, self._tkey(key))

    def _lookup(self, node, depth, tkey):
        """ Recursive lookup. """
        if isinstance(node, _BTreeLeaf):  # base case: a leaf node
            handle = node.find_eq(tkey)
            return [handle] if handle is not None else []
        else:
            return self._lookup(node.find(tkey, depth), depth - 1, tkey)  # recursive case: go down one level


    def insert(self, handle):
        """ Insert a row with the given handle. Row must exist in relation already. """
        tkey = self._tkey(self.relation.project(handle, self.key))

        split_root = self._insert(self.root, self.stat.height, tkey, handle)

        # if we split the root grow the tree up one level
        if split_root is not None:
            rroot, boundary = split_root
            root = _BTreeInterior(self.file, 0, self.key_profile, create=True)
            root.first = self.root.id
            root.insert(boundary, rroot.id)
            root.save()
            self.stat.root_id = root.id
            self.stat.height += 1
            self.stat.save()
            self.root = root

    def range(self, minkey, maxkey):
        """ Finds all the rows whose columns are such that minkey <= columns <= maxkey.  Assumes key is a dictionary 
            whose keys are the column names in the index. Returns a list of row handles.
            Some index subclasses do not support range().
        """
        raise TypeError('not implemented')  # FIXME

    def delete(self, handle):
        """ Delete a row with the given handle. Row must still exist in relation. """
        raise TypeError('not implemented')  # FIXME

    def _tkey(self, key):
        """ Transform a key dictionary into a tuple in the correct order. """
        return tuple(key[column_name] for column_name in self.key)

    def _insert(self, node, depth, tkey, handle):
        """ Recursive insert. If a split happens at this level, return the (new node, boundary) of the split. """
        if isinstance(node, _BTreeLeaf):  # base case: a leaf node
            try:
                node.insert(tkey, handle)
                node.save()
                return None
            except ValueError:
                return self._split_leaf(node, tkey, handle)
        else:
            new_kid = self._insert(node.find(tkey, depth), depth - 1, tkey, handle)  # recursive case
            if new_kid is not None:
                nnode, boundary = new_kid
                try:
                    node.insert(boundary, nnode.id)
                    node.save()
                    return None
                except ValueError:
                    return self._split_node(node, boundary, nnode.id)

    def _split_leaf(self, leaf, tkey, handle):
        """ Split given leaf. Returns the new sister leaf and its min key. """
        leaf_keys = leaf.keys.copy()
        leaf_keys[tkey] = handle

        # create the sister and put her to the right
        nleaf = _BTreeLeaf(self.file, 0, self.key_profile, create=True)
        nleaf.next_leaf = leaf.next_leaf
        leaf.next_leaf = nleaf.id

        # move half of the entries to the sister
        key_list = sorted(leaf_keys.keys())
        split = len(key_list) // 2
        # last half goes into nleaf
        for i in range(split, len(key_list)):
            ikey = key_list[i]
            nleaf.keys[ikey] = leaf_keys[ikey]
            if ikey in leaf.keys:
                del leaf.keys[ikey]

        # save them
        leaf.save()
        nleaf.save()
        return nleaf, key_list[split]

    def _split_node(self, node, boundary, block_id):
        """ Split given interior node. Returns the new sister node and its min key. """
        node.insert(boundary, block_id, skip_size_check=True)

        # create the sister
        nnode = _BTreeNode(self.file, 0, self.key_profile, create=True)

        # move half of the entries to the sister
        split = len(node.boundaries) // 2

        nnode.first = node.pointers[split]
        nboundary = node.boundaries[split]

        nnode.pointers = node.ponters[split+1:]
        node.pointers = node.pointers[:split]

        nnode.boundaries = node.boundaries[split+1:]
        node.boundaries = node.boundaries[:split]

        # save them
        node.save()
        nnode.save()
        return nnode, nboundary

    def _build_key_profile(self):
        """ Figure out the data types of each key component and encode them in self.key_profile, 
            a list of int/str classes.
        """
        types_by_colname = {}
        for i, column_name in enumerate(self.relation.column_names):
            types_by_colname[column_name] = self.relation.columns[column_name]['data_type']
        self.key_profile = [types_by_colname[column_name] for column_name in self.key]


class TestBTreeIndex(unittest.TestCase):
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
        for i in range(1000):
            row = {'a': i+100, 'b': -i}
            table.insert(row)
        index = BTreeIndex(table, 'fooindex', ['a'], unique=True)
        index.create()
        result = [table.project(handle) for handle in index.lookup({'a': 12})]
        self.assertEqual(result, [row1])
        result = [table.project(handle) for handle in index.lookup({'a': 88})]
        self.assertEqual(result, [row2])
        result = [table.project(handle) for handle in index.lookup({'a': 6})]
        self.assertEqual(result, [])

        for j in range(10):
            for i in range(1000):
                result = [table.project(handle) for handle in index.lookup({'a': i+100})]
                self.assertEqual(result, [{'a': i+100, 'b': -i}])

        # FIXME: other things to test: delete, multiple keys
