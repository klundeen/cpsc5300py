from abc import ABC, abstractmethod
from schema_tables import Schema

class EvalPlan(ABC):
    """ Evaluation plan for a query. """

    def optimize(self):
        """ Return an optimized equivalent plan. """
        # default optimization is to do it exactly as constructed
        return self

    def evaluate(self):
        """ Return a sequence of rows that are the result of evaluating this query. """
        # default implementation is done on self.pipeline (subclass has to override either evaluate or pipeline)
        table, handles = self.pipeline()
        return (table.project(handle) for handle in handles)

    def pipeline(self):
        """ Return a sequence of (table,handle) pairs for the rows in this query. """
        # default implmenetation is done on self.evaluate (subclass has to override either evaluate or pipeline)
        table = PipelineCursor(self.evaluate())
        return table, table.select()

    @abstractmethod
    def get_column_names(self):
        pass

    @abstractmethod
    def get_column_attributes(self):
        pass

class PipelineTable(object):
    """ Table to use when passing pipelined rows (handle is just the row dict). """
    def project(self, row, column_names=None):
        if column_names is None:
            return row
        else:
            return {k: row[k] for k in column_names}

    def select(self, handles, where):
        for handle in handles:
            if where is None or self._selected(handle, where):
                yield handle

    def _selected(self, row, where):
        """ Checks if given record succeeds given where clause. """
        for column_name in where:
            if row[column_name] != where[column_name]:
                return False
        return True


class EvalPlanLoopJoin(EvalPlan):
    """ Evaluation plan to do a loop join. """
    def __init__(self, outer, inner, using):
        self.outer = outer
        self.inner = inner
        self.using = using

    def pipeline(self):
        def helper():
            ot, ohs = self.outer.pipeline()
            for oh in ohs:
                orec = ot.project(oh)
                ocriteria = [orec[k] for k in self.using]
                it, ihs = self.inner.pipeline()
                for ih in ihs:
                    irec = it.project(ih)
                    icriteria = [irec[k] for k in self.using]
                    if ocriteria == icriteria:
                        yield dict(orec, **irec)  # combine the records
        return PipelineTable(), helper()

    def get_column_names(self):
        """ Take union of set of names from outer and inner tables. """
        return set(self.outer.get_column_names()) | set(self.inner.get_column_names())

    def get_column_attributes(self):
        """ Take union of set of attributes from outer and inner tables. Outer attribute wins if in both. """
        attributes = self.outer.get_column_attributes()
        inner = self.inner.get_column_attributes()
        for k in self.get_column_names():
            if k not in attributes:
                attributes[k] = inner[k]
        return attributes


class EvalPlanTableScan(EvalPlan):
    """ Evaluation plan is to scan every record in a physical table. """
    def __init__(self, table):
        self.table = table

    def pipeline(self):
        return self.table, self.table.select()

    def get_column_names(self):
        return self.table.column_names

    def get_column_attributes(self):
        return self.table.columns


class EvalPlanSelect(EvalPlan):
    """ Evaluation plan is to perform a select using the relation's select method. """
    def __init__(self, where, relation):
        self.where = where
        self.relation = relation

    def optimize(self):
        """ Optimize underlying relation. Also look for index opportunity. """
        if isinstance(self.relation, EvalPlanTableScan):
            index_names = Schema.indices.get_index_names(self.relation.table.table_name)
            for index_name in index_names:
                index = Schema.indices.get_index(self.relation.table.table_name, index_name)
                if index.key[0] in self.where:
                    key = {k: self.where[k] for k in self.where if k in index.key}
                    return EvalPlanIndexLookup(key, index)
            return self
        else:
            return EvalPlanSelect(self.where, self.relation.optimize())

    def pipeline(self):
        # base case is select on a table scan
        if isinstance(self.relation, EvalPlanTableScan):
            return self.relation.table, self.relation.table.select(self.where)

        # otherwise recurse into the plan and apply the select onto the handles from the next level down
        table, handles = self.relation.pipeline()
        return table, table.select(handles, self.where)

    def get_column_names(self):
        return self.relation.get_column_names()

    def get_column_attributes(self):
        return self.relation.get_column_attributes()


class EvalPlanProject(EvalPlan):
    """ Evaluation plan is to perform a project using the relation's project method. """
    def __init__(self, projection, relation):
        self.projection = projection
        self.column_names = [name for name in projection]
        self.relation = relation

    def optimize(self):
        """ Optimize underlying relation. """
        return EvalPlanProject(self.projection, self.relation.optimize())

    def evaluate(self):
        table, handles = self.relation.pipeline()
        return (table.project(handle, self.projection) for handle in handles)

    def get_column_names(self):
        return self.column_names

    def get_column_attributes(self):
        ca = self.relation.get_column_attributes()
        return {k: ca[k] for k in self.column_names}


class EvalPlanIndexLookup(EvalPlan):
    """ Evaluation plan is to lookup an equality predicate using an index. """
    def __init__(self, key, index):
        self.key = key
        self.index = index

    def pipeline(self):
        """ Use the index on relation. """
        return self.index.relation, self.index.lookup(self.key)

    def get_column_names(self):
        return self.index.relation.column_names

    def get_column_attributes(self):
        return self.index.relation.columns


class PipelineCursor(object):
    """ Make an evaluation resul look like a DbRelation so we can use it as a pipeline,
        i.e., we have a generator of row dictionaries but we want a generator of handles.
        
        We support both select and project methods, though the anticipated use is via the select method.
        
        As handles we just use the actual row generated from the underlying evaluation. This should be ok
        unless we tried to save it to disk as a handle.
    """

    def __init__(self, evaluation):
        self.evaluation = evaluation

    def select(self, where=None):
        for row in self.evaluation:
            if self._selected(row, where):
                yield row

    def project(self, row, column_names=None):
        if column_names is None:
            return row
        else:
            return {k: row[k] for k in column_names}

    def _selected(self, row, where):
        """ Checks if given record succeeds given where clause. """
        if where is None:
            return True
        for column_name in where:
            if row[column_name] != where[column_name]:
                return False
        return True
