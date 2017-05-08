""" B+ Trees
    (WHile technically they are B+ trees, we use the more typical "BTree" terminology.)
"""

from abc import ABC, abstractmethod
import os
import unittest
from storage_engine import DbIndex, DbRelation
from heap_storage import BYTE_ORDER, HeapFile, HeapTable, initialize, bdb


class _BTreeNode(ABC):
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
        if len(self.block) < self.HEIGHT:
            self.block.add(self._marshal_block_id(self.root_id))
            self.block.add(self._marshal_block_id(self.height))  # not really a block ID but it fits
        else:
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

    def find(self, key, depth, make_leaf):
        """ Get next block down in tree where key must be. """
        if key is None:
            down = self.first
        else:
            down = self.pointers[-1]  # last pointer is correct if we don't find an earlier boundary
            for i, boundary in enumerate(self.boundaries):
                if boundary > key:
                    down = self.pointers[i-1] if i > 0 else self.first
                    break
        if depth == 2:
            return make_leaf(down)
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


class _BTreeLeafBase(_BTreeNode):
    """ Leaf B+ tree node. Pointers are handles into the relation. """
    def __init__(self, file, block_id, key_profile, create=False):
        super().__init__(file, block_id, key_profile, create)
        if not create:
            ids = [record_id for record_id in self.block.ids()]
            self.next_leaf = self._get_block_id(ids[-1]) if len(ids) > 0 else 0
            pointers = ids[0:-1:2]  # ids[0], ids[2], ids[4], ..., ids[n-3]
            keys = ids[1::2]        # ids[1], ids[3], ids[5], ..., ids[n-2]
            self.keys = {self._get_key(keys[i]): self._get_value(pointers[i]) for i in range(len(keys))}
        else:
            self.next_leaf = 0
            self.keys = {}

    def find_eq(self, key):
        """ Find the key and return the associated handle. Return None if not found. """
        if key in self.keys:
            return self.keys[key]

    def insert(self, tkey, value):
        """ Insert key, handle pair into block. """
        # check unique
        if tkey in self.keys:
            raise IndexError('Duplicate keys are not allowed in unique index')
        # check size
        self.block.add(self._marshal_value(value))
        self.block.add(self._marshal_key(tkey))
        # if that didn't raise then we're good -- insert it
        self.keys[tkey] = value

    def save(self):
        self.block.clear()
        key_list = sorted(self.keys)
        for i, key in enumerate(key_list):
            self.block.add(self._marshal_value(self.keys[key]))
            self.block.add(self._marshal_key(key))
        self.block.add(self._marshal_block_id(self.next_leaf))
        super().save()

    @abstractmethod
    def _get_value(self, record_id):
        pass

    @abstractmethod
    def _marshal_value(self, value):
        pass


class _BTreeIndexLeaf(_BTreeLeafBase):
    """ Leaf B+ tree node. Pointers are handles into the relation. """
    def __init__(self, file, block_id, key_profile, create=False):
        super().__init__(file, block_id, key_profile, create)

    def _get_value(self, record_id):
        """ For index leaf, the value is the handle into the underlying relation. """
        return self._get_handle(record_id)

    def _marshal_value(self, value):
        """ For index leaf, the value is the handle into the underlying relation. """
        return self._marshal_handle(value)


class _BTreeFileLeaf(_BTreeLeafBase):
    """ Leaf of B+ Tree used to store entire tuple. """
    def __init__(self, file, block_id, key_profile, non_indexed_column_names, non_indexed_column_attributes,
                 create=False):
        super().__init__(file, block_id, key_profile, create)
        self.column_names = non_indexed_column_names
        self.columns = non_indexed_column_attributes

    def _get_value(self, record_id):
        """ Get the record and turn it into a (block_id,record_id) handle. """
        # FIXME: it's about time we moved the marshalling/unmarshalling into a common place, right?
        def from_bytes(ofs, sz, signed=False):
            return int.from_bytes(data[ofs:ofs+sz], BYTE_ORDER, signed=signed)

        data = self.block.get(record_id)
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

    def _marshal_value(self, value):
        """ For index leaf, the value is the handle into the underlying relation. """
        # FIXME: it's about time we moved the marshalling/unmarshalling into a common place, right?
        def to_bytes(n, sz, signed=False):
            return n.to_bytes(sz, BYTE_ORDER, signed=signed)

        data = bytes()
        for column_name in self.column_names:
            column = self.columns[column_name]
            if column['data_type'] == 'INT':
                data += to_bytes(value[column_name], 4, signed=True)
            elif column['data_type'] == 'BOOLEAN':
                data += to_bytes(int(value[column_name]), 1)
            elif column['data_type'] == 'TEXT':
                text = value[column_name].encode()
                data += to_bytes(len(text), 2)
                data += text
            else:
                raise ValueError('Cannot marahal ' + column['data_type'])
        return data


class _BTreeBase(DbIndex):
    """ The B+ Tree.
        Only unique indices are supported. Try adding the primary key value to the index key to make it unique, 
        if necessary.
    """

    STAT = 1  # block_id of statistics data

    def __init__(self, relation, name, key, unique=False, use_prefix=True):
        if not unique:
            raise ValueError('BTreeIndex must be on a unique search key')
        super().__init__(relation, name, key, unique)
        self.file_prefix = self.relation.table_name
        if use_prefix:
            self.file_prefix += '-' + self.name  # forces uniqueness within this relation
        self.file = HeapFile(self.file_prefix)
        self._build_key_profile()
        self.stat = None
        self.root = None
        self.closed = True

    def create(self):
        """ Create the index. """
        self.file.create()
        self.stat = _BTreeStat(self.file, self.STAT, new_root=self.STAT + 1, key_profile=self.key_profile)
        self.root = self._make_leaf(self.stat.root_id, create=True)
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
                self.root = self._make_leaf(self.stat.root_id)
            else:
                self.root = _BTreeInterior(self.file, self.stat.root_id, self.key_profile)
            self.closed = False

    def close(self):
        """ Closes the index. Disables: lookup, [range if supported], insert, delete, update. """
        self.file.close()
        self.stat = self.root = None
        self.closed = True

    def lookup(self, key, return_key=False):
        """ Find all the rows whose columns are equal to key. Assumes key is a dictionary whose keys are the column 
            names in the index. Returns a list of row handles.
        """
        self.open()
        tkey = self.tkey(key)
        leaf = self._lookup(self.root, self.stat.height, tkey)
        handle = leaf.find_eq(tkey)
        if return_key:
            return tkey if handle is not None else None
        else:
            return [handle] if handle is not None else []

    @abstractmethod
    def _make_leaf(self, block_id=None, create=None):
        """ Construct a leaf. If block_id is None, then create=True, otherwise create is assumed False unless 
            specified. 
        """
        pass

    def _lookup(self, node, depth, tkey):
        """ Recursive lookup. """
        if depth == 1:
            return node
        else:
            return self._lookup(node.find(tkey, depth, self._make_leaf), depth - 1, tkey)  # recursive case

    def insert(self, handle, projection=None):
        """ Insert a row with the given handle. Row must exist in relation already.
            Specify one of handle or row.
        """
        self.open()
        if projection is None:
            projection = self.relation.project(handle, self.key)
        tkey = self.tkey(projection)

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

    def range(self, minkey, maxkey, return_keys=False):
        """ Finds all the rows whose columns are such that minkey <= columns <= maxkey.  Assumes key is a dictionary 
            whose keys are the column names in the index. Returns a list of row handles.
            Some index subclasses do not support range().
        """
        tmin = self.tkey(minkey)
        tmax = self.tkey(maxkey)
        start = self._lookup(self.root, self.stat.height, tmin)
        for tkey in sorted(start.keys):
            if tmin is None or tkey >= tmin:
                yield start.keys[tkey] if not return_keys else tkey
        next_leaf_id = start.next_leaf
        while next_leaf_id > 0:
            next_leaf = self._make_leaf(next_leaf_id)
            for tkey in sorted(next_leaf.keys):
                if tmax is not None and tkey > tmax:
                    return
                yield next_leaf.keys[tkey] if not return_keys else tkey
            next_leaf_id = next_leaf.next_leaf

    def delete(self, handle):
        """ Delete a row with the given handle. Row must still exist in relation. """
        tkey = self.tkey(self.relation.project(handle))
        leaf = self._lookup(self.root, self.stat.height, tkey)
        if tkey not in leaf.keys:
            raise ValueError("key to be deleted not found in index")
        del leaf.keys[tkey]
        leaf.save()
        # tree never shrinks -- if all keys get deleted we still have an empty shell of tree

    def tkey(self, key):
        """ Transform a key dictionary into a tuple in the correct order. """
        if key is None:
            return None
        return tuple(key[column_name] for column_name in self.key)

    def _insert(self, node, depth, tkey, handle):
        """ Recursive insert. If a split happens at this level, return the (new node, boundary) of the split. """
        if depth == 1:
            try:
                node.insert(tkey, handle)
                node.save()
                return None
            except ValueError:
                return self._split_leaf(node, tkey, handle)
        else:
            new_kid = self._insert(node.find(tkey, depth, self._make_leaf), depth - 1, tkey, handle)  # recursive case
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
        nleaf = self._make_leaf()
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


class BTreeIndex(_BTreeBase):
    """ BTree index. """

    def _make_leaf(self, block_id=None, create=None):
        """ Construct a BTreeIndexLeaf. If block_id is None, then create=True, otherwise create is assumed False unless 
            specified. 
        """
        if block_id is None:
            create = True
        elif create is None:
            create = False
        return _BTreeIndexLeaf(self.file, block_id, self.key_profile, create)


class _BTreeFile(_BTreeBase):
    """ BTree used for BTree Storage Engine.
    """
    def __init__(self, relation, key, non_key_column_names, columns):
        super().__init__(relation, 'main', key, unique=True, use_prefix=False)
        self.non_key_column_names = non_key_column_names
        self.columns = columns

    def _make_leaf(self, block_id=None, create=None):
        """ Construct a BTreeFileLeaf. If block_id is None, then create=True, otherwise create is assumed False unless 
            specified. 
        """
        if block_id is None:
            create = True
        elif create is None:
            create = False
        return _BTreeFileLeaf(self.file, block_id, self.key_profile, self.non_key_column_names, self.columns, create)


class BTreeTable(DbRelation):
    """ BTree storage engine.
        We require a unique primary key.
        For the underlying Btree index, we're storing the non-key row values as the "handle".
        For our clients, we're using the key value as the handle (confusingly, this is what we're storing as the 
        "key" in the underlying Btree index).
    """
    def __init__(self, table_name, column_names, column_attributes, primary_key=None):
        if primary_key is None:
            raise ValueError("BTree Storage Engine table requires a unique primary key")
        super().__init__(table_name, column_names, column_attributes, primary_key)
        non_key_column_names = [name for name in column_names if name not in primary_key]
        self.index = _BTreeFile(self, primary_key, non_key_column_names, column_attributes)

    def create(self):
        """ Execute: CREATE TABLE <table_name> ( <columns> )
            Is not responsible for metadata storage or validation.
        """
        self.index.create()

    def open(self):
        """ Open existing table. Enables: insert, update, delete, select, project"""
        self.index.open()

    def close(self):
        """ Closes the table. Disables: insert, update, delete, select, project"""
        self.index.close()

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
        self.index.drop()

    def insert(self, row):
        """ Expect row to be a dictionary with column name keys.
            Execute: INSERT INTO <table_name> (<row_keys>) VALUES (<row_values>)
            Return the handle of the inserted row.
        """
        row = self._validate(row)
        tkey = self.index.tkey(row)
        return self.index.insert(tkey, projection=row)

    def update(self, tkey, new_values):
        """ Expect new_values to be a dictionary with column name keys.
            Conceptually, execute: UPDATE INTO <table_name> SET <new_values> WHERE <tkey>
            where tkey is sufficient to identify one specific record (e.g., returned from an insert
            or select).
        """
        row = self.project(tkey)
        new_row = row.copy()
        for key in new_values:
            new_row[key] = new_values[key]
        new_row = self._validate(new_row)
        new_tkey = self.index.tkey(new_row)
        self.index.delete(tkey)
        self.index.insert(new_tkey, new_row)
        return new_tkey

    def delete(self, tkey):
        """ Conceptually, execute: DELETE FROM <table_name> WHERE <tkey>
            where tkey is sufficient to identify one specific record (e.g., returned from an insert
            or select).
        """
        self.index.delete(tkey)

    def select(self, where=None, limit=None, order=None, group=None, handles=None):
        """ Conceptually, execute: SELECT <handle> FROM <table_name> WHERE <where>
            If handles is specified, then use those as the base set of records to apply a refined selection to.
            Returns a list of handles for qualifying rows.
        """
        # FIXME: ignoring limit, order, group
        minkey, maxkey, additional_where = self._make_range(where)
        if handles is None:
            for tkey in self.index.range(minkey, maxkey, return_keys=True):
                    if additional_where is None or self._selected(tkey, additional_where):
                        yield tkey
        else:
            for tkey in handles:
                if additional_where is None or self._selected(tkey, additional_where):
                    yield tkey

    def _make_range(self, where):
        """ Turn the where conjunction into a suitable range on the index. """
        # FIXME -- for now always traverse the entire index
        return None, None, where

    def project(self, tkey, column_names=None):
        """ Return a sequence of values for handle given by column_names. """
        row = self.index.lookup(tkey)
        if column_names is None:
            return row
        else:
            return {k: row[k] for k in column_names}

    def _selected(self, handle, where):
        """ Checks if given record succeeds given where clause. """
        # FIXME - a bit unfortunate that we have to re-lookup the leaf block here
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


class TestBTree(unittest.TestCase):
    def setUp(self):
        dbenv = os.path.expanduser('~/.dbtests')
        if not os.path.exists(dbenv):
            os.makedirs(dbenv)
        for file in os.listdir(dbenv):
            os.remove(os.path.join(dbenv, file))
        initialize(dbenv)

    def testIndex(self):
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

        for i in range(1000):
            result = [table.project(handle) for handle in index.lookup({'a': i+100})]
            self.assertEqual(result, [{'a': i+100, 'b': -i}])

        row = {'a': 44, 'b': 44}
        thandle = table.insert(row)
        index.insert(thandle)
        result = [table.project(handle) for handle in index.lookup({'a': 44})]
        self.assertEqual(result, [row])
        index.delete(thandle)
        table.delete(thandle)
        result = [table.project(handle) for handle in index.lookup({'a': 44})]
        self.assertEqual(result, [])

        result = [table.project(handle) for handle in index.range({'a': 100}, {'a': 310})]
        for i in range(210):
            self.assertEqual(result[i]['a'], 100+i)

        count_i = len([handle for handle in index.range(None, None)])
        count_t = len([handle for handle in table.select()])
        self.assertEqual(count_i, count_t)
        for handle in table.select():
            index.delete(handle)
        self.assertEqual(0, len([handle for handle in index.range(None, None)]))

        # FIXME: other things to test: multiple keys
        index.drop()
