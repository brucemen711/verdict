"""
Verdict Query grammar

query := {
    "source": relops,
    "agg": {
        "alias": agg_func, ...
    },
    "groupby": [
        attr, ...
    ],
    "orderby": [
        alias, ...
    ],
    "limit": int
}

relops := project | select | join | groupby | agg | orderby | limit

project := {
    "op": "project",
    "arg": {
        alias: attr_op, 
        ...
    },
    "source": relops
}

select := {
    "op": "select",
    "arg": attr_op,
    "source": relops
}

join := {
    "op": "join",
    "source": relops,
    "arg": {
        "join_to": rel,
        "left_on": attr,
        "right_on": attr,
        "join_type": join_type
    }
}

join_type := "inner" | "left" | "right" | "outer" | "cross"

groupby := {
    "op": "groupby",
    "arg": [
        base_attr, ...
    ],
    "source": relops
}

agg := {
    "op": "agg",
    "arg": {
        alias: agg_func, 
        ... 
    },
    "source": relops
}

agg_func := {
    "op": "sum" | "count" | "avg",
    "arg": [ attr, ... ]
}

orderby := {
    "op": "orderby",
    "source": relops,
    "arg": [ (attr, 'asc'), ..., ]
}

attr := attr_op | base_attr | constant

base_attr := 'attr op'

attr_op := {
    "op": "add" | "sub", | "mul" | "div" | "and" | "or" | "ne" | "eq" | "gt" | "geq" | "lt" | 
            "leq" | "floor" | "ceil" | "round" | "substr" | "to_str" | "concat" | "length" |
            "replace" | "upper" | "lower" | "startswith" | "contains" | "endswith" | "year" |
            "month" | "day"
    "arg": [
        attr
    ]
}

alias := str

constant := int | float | 'date 2019-01-01' | 'timestamp 2019-01-01 00:00:00.000' | str
"""
import copy
import json
import numpy as np
import pandas as pd
import pdb
import re
import uuid
from datetime import datetime
from typing import Callable, List

from ..common.logging import log
from ..common.tools import *



class Constant(object):

    def __init__(self, value, type_hint=None):
        assert_type(value, (str, int, float))
        self._value = value
        self.type_hint = type_hint

    def value(self):
        return self._value

    @staticmethod
    def date(date_str):
        assert len(date_str) == 10
        c = Constant(datetime.strptime(date_str, '%Y-%m-%d'))
        c.type_hint = 'date'
        return c

    def sql_expr(self):
        if isinstance(self._value, str):
            return f"'{self._value}'"
        elif isinstance(self._value, int) or isinstance(self._value, float):
            return str(self._value)
        elif self.type_hint == 'date':
            return "date '" + self._value.strftime("%Y-%m-%d") + "'"
        else:
            raise ValueError(f'Unexpected value: {self._value} of type {type(self._value)}')

    def pandas_attr(self, df):
        return self._value
        # attr = pd.DataFrame(self._value, index=df.index, columns=[0])[0]
        # return attr

    def __str__(self):
        return f'Constant({self._value})'

    def __repr__(self):
        return self.__str__()


def find_base_tables(rel_obj, include_samples=False):
    def find(o):
        return find_base_tables(o, include_samples)
    if isinstance(rel_obj, str):
        return []
    elif isinstance(rel_obj, Attr):
        return []
    elif isinstance(rel_obj, List):
        return flatten([find(a) for a in rel_obj])
    elif isinstance(rel_obj, tuple):
        return tuple(flatten([find(a) for a in rel_obj]))
    elif isinstance(rel_obj, BaseTable):
        return [rel_obj]
    elif isinstance(rel_obj, SampleTable):
        if include_samples:
            return [rel_obj]
        else:
            return []
    elif isinstance(rel_obj, DerivedTable):
        return find(rel_obj.source()) + find(rel_obj.relop_args())
    else:
        raise ValueError(type(rel_obj))


class Attr(object):

    def __init__(self):
        pass

    def attr_op(self, name: str, args: List = []) -> 'AttrOp':
        objects = []
        for a in args:
            if isinstance(a, str) or isinstance(a, float) or isinstance(a, int):
                objects.append(Constant(a))
            elif isinstance(a, Attr) or isinstance(a, AttrOp):
                objects.append(a)
            elif isinstance(a, Constant):
                objects.append(a)
            else:
                raise ValueError(f"Unexpected value {a} of type {type(a)}")
        return AttrOp(name, objects)

    def eq(self, arg):
        return self.attr_op('eq', [self, arg])

    # def __req__(self, arg):
        return self.attr_op('eq', [self, arg])

    def __lt__(self, arg):
        return self.attr_op('lt', [self, arg])

    def __rlt__(self, arg):
        return self.attr_op('gt', [self, arg])

    def __le__(self, arg):
        return self.attr_op('leq', [self, arg])

    def __rle__(self, arg):
        return self.attr_op('geq', [self, arg])

    def __gt__(self, arg):
        return self.attr_op('gt', [self, arg])

    def __rgt__(self, arg):
        return self.attr_op('lt', [self, arg])

    def __ge__(self, arg):
        return self.attr_op('geq', [self, arg])

    def __rge__(self, arg):
        return self.attr_op('leq', [self, arg])

    def __add__(self, arg):
        return self.attr_op('add', [self, arg])

    def __radd__(self, arg):
        return self.attr_op('add', [self, arg])

    def __sub__(self, arg):
        return self.attr_op('sub', [self, arg])

    def __rsub__(self, arg):
        return self.attr_op('sub', [arg, self])

    def __mul__(self, arg):
        return self.attr_op('mul', [self, arg])

    def __rmul__(self, arg):
        return self.attr_op('mul', [self, arg])

    def __truediv__(self, arg):
        return self.attr_op('div', [self, arg])

    def __rtruediv__(self, arg):
        return self.attr_op('div', [arg, self])

    def land(self, arg):
        return self.attr_op('and', [self, arg])

    def lor(self, arg):
        return self.attr_op('or', [self, arg])

    def __ne__(self, arg):
        return self.attr_op('ne', [self, arg])

    def substr(self, start, length):
        assert_type(start, int)
        assert_type(length, int)
        assert start > 0
        assert length > 0
        return self.attr_op('substr', [self, start, length])

    def to_str(self):
        return self.attr_op('to_str', [self])

    def concat(self, arg):
        return self.attr_op('concat', [self, arg])

    def length(self):
        return self.attr_op('length', [self])

    def replace(self, old, new):
        return self.attr_op('replace', [self, old, new])

    def upper(self):
        return self.attr_op('upper', [self])

    def lower(self):
        return self.attr_op('lower', [self])

    def startswith(self, pattern):
        if isinstance(pattern, str):
            pattern = Constant(pattern)
        assert_type(pattern, Constant)
        return self.attr_op('startswith', [self, pattern])

    def contains(self, pattern):
        if isinstance(pattern, str):
            pattern = Constant(pattern)
        assert_type(pattern, Constant)
        return self.attr_op('contains', [self, pattern])

    def endswith(self, pattern):
        if isinstance(pattern, str):
            pattern = Constant(pattern)
        assert_type(pattern, Constant)
        return self.attr_op('endswith', [self, pattern])

    def round(self):
        return self.attr_op('round', [self])

    def ceil(self):
        return self.attr_op('ceil', [self])

    def floor(self):
        return self.attr_op('floor', [self])

    def year(self):
        return self.attr_op('year', [self])

    def month(self):
        return self.attr_op('month', [self])

    def day(self):
        return self.attr_op('day', [self])

    def sql_expr(self):
        raise NotImplementedError

    def pandas_attr(self, df: pd.DataFrame):
        raise NotImplementedError

    def __str__(self):
        return "AbstractAttr"

    def __repr__(self):
        return self.__str__()


class AttrOp(Attr):
    """Represents an operation to an attribute, such as arithmetic operations,
    comparison operations, etc.
    """

    op_names = set([
        "eq", "gt", "geq", "lt", "leq", "add", "sub", "mul", "div", "floor", "ceil", "round",
        "and", "or", "ne", "substr", "to_str", "concat", "length", "replace", "upper", "lower",
        "startswith", "contains", "endswith", "year", "month", "day"
    ])

    def __init__(self, op: str, args: List = []):
        """
        @param op This should be one of the following:
        1. Comparisons:
            eq (equal to), gt (greater than), geq (greater than or equal to), lt (less than),
            leq (less than or equal to)

        2. Arithemetic:
            add, sub, mul, div, floor, ceil, round

        3. logical:
            and, or, ne (TODO)

        4. String:
            substr(str, start, length), to_str(), concat(another), length(), replace(old, new), 
            upper(), lower(), startswith(pattern), contains(pattern), endswith(pattern)

        5. Datetime:
            year(), month(), day()

        @param name  Alias name
        """
        super().__init__()
        assert_type(op, str)
        assert_type(args, (List, tuple))
        assert op in AttrOp.op_names
        self._op = op
        self._args = args

    def op(self):
        return self._op

    def args(self):
        return self._args

    def set_args(self, args):
        self._args = args

    @staticmethod
    def casewhen(*args):
        """
        @param args  Must in the form of (predicate1, value1, predicate2, value2, ..., else_value)
        """
        assert_equal(len(args)%2, 1)
        new_args = []
        for a in args:
            if isinstance(a, int) or isinstance(a, str) or isinstance(a, float):
                new_args.append(Constant(a))
            elif isinstance(a, Attr):
                new_args.append(a)
            else:
                raise ValueError(a)
        return AttrOp('casewhen', new_args)

    def __str__(self):
        return f"AttrOp({self._op}, " + ", ".join(map(str, self._args)) + ")"



class BaseAttr(Attr):

    # def __init__(self, table: 'Table', name: str):
    #     self._table = table
    #     self._name = name

    def __init__(self, name: str):
        # self._table = table
        self._name = name

    def name(self):
        return self._name

    # def full_name(self):
    #     return self._table.uid + '.' + self._name

    def sql_expr(self):
        return self.name()

    def __hash__(self):
        return hash(self._name)

    def __eq__(self, other):
        if isinstance(other, BaseAttr):
            return other.name() == self._name
        else:
            return False

    def __str__(self):
        return f"BaseAttr({self._name})"


class AggFunc(Attr):
    """Represents a relational aggregation operation.
    """

    agg_ops = set([
        "sum", "count", "avg"
    ])

    def __init__(self, op: str, args=[]):
        """
        @param name  This indicates the type of aggregation. It should be one of sum, count, avg
        """
        assert_type(op, str)
        assert_type(args, (List, tuple, type(None)))
        assert op in AggFunc.agg_ops
        self._op = op
        self._args = args

    def op(self):
        return self._op

    def args(self):
        return self._args

    def set_args(self, args):
        self._arg = args

    @staticmethod
    def sum(arg):
        return AggFunc('sum', [arg])

    @staticmethod
    def count(arg=None):
        if arg is None:
            return AggFunc('count', [])
        elif isinstance(arg, (List, tuple)):
            return AggFunc('count', arg)
        else:
            return AggFunc('count', [arg])

    @staticmethod
    def avg(arg):
        return AggFunc('avg', [arg])

    def sql_expr(self):
        if len(self._args) == 0:
            return f'{self._name}()'
        else:
            args = [a.sql_expr() for a in self._args]
            args = ", ".join(args)
            return f'{self._name}({args})'

    def __str__(self):
        return f"AggFunc({self._op}, args: {self._args})"


class Table(object):

    def __init__(self):
        self.uid = self._gen_id()
        self._pcol_tab = None
        self._pcol_name = None

    def _gen_id(self):
        return 't' + uuid.uuid4().hex[:8]

    def set_uid(self, uid):
        self.uid = uid

    def attr(self, name):
        """References the attribute of this table"""
        return BaseAttr(name)

    # def __getattr__(self, name):
    #     """Shorthand operation for attr()"""
    #     if name.startswith('_'):
    #         raise AttributeError(name)
    #     elif name in dir(Table):
    #         raise AttributeError(name)
    #     return self.attr(name)

    def select(self, pred: AttrOp) -> 'DerivedTable':
        """
        @param pred
            An operation that returns a boolean value.
        """
        return DerivedTable(self, 'select', [pred])

    def filter(self, pred):
        return self.select(pred)

    def project(self, args) -> 'DerivedTable':
        if isinstance(args, dict):
            col_dict = args
            return self.agg([(attr, alias) for alias, attr in col_dict.items()])
        assert_type(args, (List, tuple))
        for attr_alias in args:
            assert_type(attr_alias[0], (BaseAttr, AttrOp))
            assert_type(attr_alias[1], str)
        return DerivedTable(self, 'project', args)

    def join(self, right_join_table, left_on, right_on, join_type):
        """
        @param left_on  The join key for the left table. Typically, Attr is provided. The default
                        value '_auto_key' is reserved for GroupedTable, for which the key for the
                        left table is automatically resolved by _to_partitioned_table().
        """
        assert left_on == '_auto_key' or isinstance(left_on, Attr)
        assert right_on is None or isinstance(right_on, Attr)
        if right_on is None:
            if join_type is None or join_type == 'cross':
                join_type = 'cross'
            else:
                raise ValueError('The join column must be specified for inner, left, and right join.')
        if join_type is None:
            join_type = 'inner'  # default is inner join
        assert (join_type.lower() == 'inner'
                or join_type.lower() == 'left'
                or join_type.lower() == 'right'
                or join_type.lower() == 'cross')
        return DerivedTable(self, 'join', [right_join_table, left_on, right_on, join_type])

    def agg(self, args) -> 'DerivedTable':
        if isinstance(args, dict):
            col_dict = args
            return self.agg([(attr, alias) for alias, attr in col_dict.items()])
        assert_type(args, (List, tuple))
        for attr_alias in args:
            assert_type(attr_alias[0], AggFunc)
            assert_type(attr_alias[1], str)
        return DerivedTable(self, 'agg', args)

    def count(self, alias='count'):
        """Convenience method for agg( AggFunc.count() ) """
        return self.agg({alias: AggFunc.count()})

    def sum(self, col_name, alias='sum'):
        """Convenience method for agg( AggFunc.sum() ) """
        return self.agg({alias: AggFunc.sum(self.attr(col_name))})

    def avg(self, col_name, alias='avg'):
        """Convenience method for agg( AggFunc.sum() ) """
        return self.agg({alias: AggFunc.avg(self.attr(col_name))})

    def is_agg(self):
        return False

    def groupby(self, *attr_list: List[BaseAttr]) -> 'DerivedTable':
        # for groupby([t.attr])
        if len(attr_list) == 1:
            if isinstance(attr_list[0], list) or isinstance(attr_list[0], tuple):
                attr_list = tuple(attr_list[0])

        assert isinstance(attr_list, tuple)
        base_attr_list = []
        for attr in attr_list:
            if isinstance(attr, BaseAttr):
                base_attr_list.append(attr)
            elif isinstance(attr, str):
                base_attr_list.append(BaseAttr(attr))
            else:
                raise ValueError(attr)
        return DerivedTable(self, 'groupby', base_attr_list)

    def is_groupby(self):
        return False

    def orderby(self, orderby) -> 'DerivedTable':
        """
        @param orderby  A list of (alias_name, sort_order) in str. Both groups and agg work for the 
                        names. sort_order must either be 'asc' or 'desc'.
        """
        assert_type(orderby, (list, tuple))
        for alias_sort in orderby:
            assert_type(alias_sort, (list, tuple))
            assert_equal(len(alias_sort), 2)
            alias, sort = alias_sort
            assert_type(alias, str)
            assert sort == 'asc' or sort == 'desc'
        return DerivedTable(self, 'orderby', orderby)

    def is_orderby(self):
        return False

    def limit(self, limit: int) -> 'DerivedTable':
        """
        @param limit  The number of final rows to output
        """
        assert_type(limit, int)
        return DerivedTable(self, 'limit', [Constant(limit)])

    def is_limit(self):
        return False

    def is_basetable(self):
        return False

    def is_sampletable(self):
        return False    

    def key_source_id(self):
        return self._pcol_tab.uid

    def key_col(self):
        return self._pcol_name

    def execute_pandas(self):
        # log(type(self))
        raise ValueError('This is an abstract base class.')

    def sql_expr(self):
        raise ValueError('This is an abstract base class.')

    def has_col(self, name):
        raise ValueError('This is an abstract base class.')

    def __str__(self):
        return "AbstractTable()"

    def __repr__(self):
        return self.__str__()


class PandasTable(Table):
    """Represents (and holds) pandas' dataframe object.
    """
    def __init__(self, df):
        super().__init__()
        self._original_df = df

    def execute_pandas(self):
        # super().execute_pandas()
        df = copy.deepcopy(self._original_df)
        # df.columns = [self.uid + '.' + n for n in df.columns.to_list()]
        return df


class BaseTable(Table):
    """Represents a base table within a database
    """

    def __init__(self, name: str, col_names = None):
        """Represents a regular base table of a database.
        """
        super().__init__()
        assert_type(name, str)
        assert_type(col_names, (List, tuple, type(None)))
        self._name = name
        self._col_names = col_names

    def name(self):
        return self._name

    def column_names(self):
        return self._col_names

    def set_column_names(self, col_names):
        assert_type(col_names, (List, tuple))
        self._col_names = col_names

    def is_basetable(self):
        return True

    def has_col(self, name):
        if self._col_names is None:
            raise ValueError("Column names for this table have not been set.")
        else:
            return name in self._col_names

    def __str__(self):
        return f"BaseTable({self._name}, columns: {self._col_names})"


class SampleTable(Table):
    """Represents a sample table. Its physical location is specific to database engines.
    """

    def __init__(self, name: str, part_col_values=[], col_names = None):
        super().__init__()
        assert_type(name, str)
        assert_type(col_names, (List, tuple, type(None)))
        self._name = name
        self._part_col_values = part_col_values
        self._col_names = col_names

    def name(self):
        return self._name

    def column_names(self):
        return self._col_names

    def set_column_names(self, col_names):
        assert_type(col_names, (List, tuple))
        self._col_names = col_names

    def part_col_values(self):
        return self._part_col_values

    def is_sampletable(self):
        return True

    def has_col(self, name):
        if self._col_names is None:
            raise ValueError("Column names for this table have not been set.")
        else:
            return name in self._col_names

    def __str__(self):
        return f"SampleTable({self._name}, parts_id: {self._part_col_values})"


class DerivedTable(Table):

    op_names = set([
        "project", "select", "join", "groupby", "agg", "orderby", "limit"
    ])

    join_types = set([
        "inner", "left", "right", "outer", "cross"
    ])

    ordering_types = set([ "asc", "desc" ])

    def __init__(self, source: Table, relop_name: str, relop_args: []):
        """Constructs a chain of operations by adding one operation on top of
        a source table.

        @param relop_name
            This should be one of the following:
                select, project, join, group, agg, and setkey
        @param relop_args
            Arguments of this operation. The types of arguments will be different depending on the
            types of the operation. That is,
                1. select: A single AttrOp in a list.
                2. project: A list of AttrOp or Attr
                3. join: [join_type, BaseTable, key_col]
                4. groupby: A list of Attr
                5. agg: [A dictionary from alias to AggFunc]
                6. setkey: A single Attr
            Regardless of the number of arguments, they must be in a list.
        """
        super().__init__()
        assert_type(source, Table)
        assert relop_name in DerivedTable.op_names

        if relop_name == "project":
            for a in relop_args:
                assert_type(a, (tuple, list))
                assert_equal(len(a), 2)
                assert_type(a[0], Attr)
                assert_type(a[1], str)

        elif relop_name == "select":
            assert_type(relop_args, (tuple, list))
            assert len(relop_args) == 1
            for a in relop_args:
                assert_type(a, AttrOp)

        elif relop_name == "join":
            len(relop_args) == 4
            assert_type(relop_args[0], Table)
            assert_type(relop_args[1], BaseAttr)
            assert_type(relop_args[2], BaseAttr)
            assert relop_args[3] in DerivedTable.join_types
            assert_type(relop_args[3], str)

        elif relop_name == "agg":
            for a in relop_args:
                assert_type(a, (tuple, list))
                assert_equal(len(a), 2)
                assert_type(a[0], AggFunc)
                assert_type(a[1], str)

        elif relop_name == "groupby":
            for a in relop_args:
                assert_type(a, BaseAttr)

        elif relop_name == "orderby":
            for a in relop_args:
                assert_type(a, (tuple, list))
                assert_equal(len(a), 2)
                assert_type(a[0], BaseAttr)
                assert_type(a[1], str)
                assert a[1] in DerivedTable.ordering_types

        elif relop_name == "limit":
            len(relop_args) == 1
            assert_type(relop_args[0], int)

        self._source = source
        self._relop_name = relop_name
        self._relop_args = relop_args

    def source(self):
        return self._source

    def set_source(self, source):
        self._source = source

    def relop_name(self):
        return self._relop_name

    def relop_args(self):
        return self._relop_args

    def set_relop_args(self, relop_args):
        self._relop_args = relop_args

    def is_select(self) -> bool:
        return self._relop_name == 'select'

    def is_project(self) -> bool:
        return self._relop_name == 'project'

    def is_join(self) -> bool:
        return self._relop_name == 'join'

    def right_join_table(self) -> Table:
        assert self.is_join()
        return self._relop_args[0]

    def set_right_join_table(self, right_join_table):
        assert self.is_join()
        self._relop_args[0] = right_join_table

    def left_join_col(self):
        assert self.is_join()
        return self._relop_args[1]

    def set_left_join_col(self, left_on: Attr):
        assert self.is_join()
        self._relop_args[1] = left_on

    def right_join_col(self) -> Attr:
        assert self.is_join()
        return self._relop_args[2]

    def set_right_join_col(self, right_on: Attr):
        assert self.is_join()
        self._relop_args[2] = right_on

    def join_type(self) -> str:
        assert self.is_join()
        return self._relop_args[3]

    def set_join_type(self, join_type: str):
        assert self.is_join()
        self._relop_args[3] = join_type

    def is_groupby(self) -> bool:
        return self._relop_name == 'groupby'

    def is_agg(self) -> bool:
        return self._relop_name == 'agg'

    def is_orderby(self) -> bool:
        return self._relop_name == 'orderby'

    def is_limit(self) -> bool:
        return self._relop_name == 'limit'

    def has_col(self, name):
        if self.is_project():
            aliases = [a[1] for a in self._relop_args]
            return name in aliases
        elif self.is_agg():
            aliases = [a[1] for a in self._relop_args]
            return name in aliases
        elif self.is_join():
            return self._source.has_col(name) or self.right_join_table().has_col(name)
        elif self.is_select():
            return self._source.has_col(name)
        elif self.is_groupby():
            return self._source.has_col(name)
        elif self.is_limit():
            return self._source.has_col(name)
        else:
            return ValueError(self)

    def __str__(self):
        return (
            f"DerivedTable({self._relop_name}, "
            f"args: {self._relop_args}, "
            f"source: {self._source})"
            )

    # def __repr__(self):
    #     return self.__str__()

