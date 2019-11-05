"""
There are two types of languages (i.e., string representation of queries) Verdict uses.
The first is SQL, and the second is the verdict query.


**1. SQL**

SQL is mainly for the users who issue queries to Verdict. We try to follow the standard SQL:99,
but there are some limitations and differences. To internally process SQL queries, Verdict
first converts them to the verdict query, which we describe below.


**2. Verdict Query**

The verdict query is mainly for internal communications among Verdict's components. For example,
when sending a query to the backend engine's driver, the driver (only) expects the verdict query
(not SQL). This is an intentional design choice to make Verdict's core logic consistent across 
different drivers. It is each driver's responsibility to convert the verdict query to a backend
engine-compatible query (which will be mostly SQL with some dialects).

A verdict query is a dict object that internally only consists of dict, List (or tuple), or 
str. No Python objects. For this reason, its conversion to a plan string is easy and efficient.
However, the verdict query is expressive enough to represent all relational operations and the
entities (e.g., tables, attributes) participating in the operations.

Specifically, a verdict query must in the following form:

.. code-block::

    verdict_query := rel_source

    rel_source := base_table | rel_op

    base_table := f"table {table_name}"      (e.g., "table lineitem")

    rel_op := {
        "op": "project" | "agg" | "select" | "join" | "groupby" | "orderby" | "limit",
        "arg": project_arg | agg_arg | select_arg | join_arg | groupby_arg | orderby_arg | limit_arg,
        "source": rel_source
    }

    project_arg := { alias: attr_or_const, ... }

    agg_arg := { alias: agg_func,  ... }

    select_arg := attr      # must be evaluated to true or false

    join_arg := { 
        "join_to": rel_source, 
        "left_on": left_join_key, 
        "right_on": right_join_key,
        "join_type": join_type
    }

    left_join_key := base_attr

    right_join_key := base_attr

    join_type := "inner" | "left" | "right"

    groupby_arg := [ base_attr, ... ]

    orderby_arg := [ base_attr, ... ]

    limit_arg := int

    attr_or_const := attr | constant

    attr := base_attr | attr_op

    base_attr := f"attr {attr_name}"        (e.g., "attr price")

    attr_op := {
        "op": "add" | "sub", | "mul" | "div" | "and" | "or" | "ne" | "eq" | "gt" | "geq" | "lt" | 
              "leq" | "floor" | "ceil" | "round" | "substr" | "to_str" | "concat" | "length" |
              "replace" | "upper" | "lower" | "startswith" | "contains" | "endswith" | "year" |
              "month" | "day",
        "arg": [ attr_or_const, ... ]
    }

    agg_func := {
        "op": "count" | "sum" | "avg",
        "arg": [ attr_or_const ]
    }

    constant := timestamp | date | str | float | int

    timestamp := f"timestamp {timestamp_value}"     (e.g., "timestamp 2020-01-01 12:12:59,999")

    date := f"date {date_value}"            (e.g., "date 2020-01-01")

"""

import copy
import pdb
from moz_sql_parser import parse
from .core.relobj import *
from .common.tools import *


def sql2verdict_query(sql):
    """Converts the standard SQL:99 to the json representation that can be understood by Verdict's
    core logic (:class:`~verdict.core.querying2.Querying`).
    """
    assert_type(sql, str)
    sql = copy.deepcopy(sql)
    sql = preprocess(sql)
    json_dict = parse(sql)
    log(f'An intermediate JSON from SQL: {json_dict}', 'debug')
    c = SQL2Json()

    assert 'select' in json_dict
    assert 'from' in json_dict

    to_convert = { 'from': json_dict['from'] }
    if 'where' in json_dict:
        to_convert['where'] = json_dict['where']

    request = {}
    source = c.convert(to_convert)
    agg = c.convert(json_dict['select'], 'select')
    if isinstance(agg[0], Attr):
        agg = [agg]
    is_agg = c.is_agg_select(agg)
    assert_equal(is_agg, True)

    # remove the non-aggregate (i.e., groupby) items
    groupby_names = set()
    if 'groupby' in json_dict:
        groups = c.convert(json_dict['groupby'], 'groupby')
        if not isinstance(groups, (List, tuple)):
            groups = [groups]
        for g in groups:
            assert_type(g, BaseAttr)
            groupby_names.add(g.name())
        request['groupby'] = to_verdict_query(groups)

    agg_dict = {}
    for a in agg:
        attr = a[0]
        alias = a[1]
        assert_type(attr, Attr)
        assert_type(alias, str)
        if not isinstance(attr, AggFunc):
            if alias in groupby_names:
                pass
            else:
                raise ValueError(f"Non aggregate item found at the top level select: {attr}")
        else:
            agg_dict[alias] = to_verdict_query(attr)

    request.update({
        "source": to_verdict_query(source),
        "agg": agg_dict,
    })

    if 'orderby' in json_dict:
        orders = c.convert(json_dict['orderby'], 'orderby')
        assert_type(orders, (List, tuple))
        if isinstance(orders[0], BaseAttr):
            orders = [orders]
        request['orderby'] = to_verdict_query(orders)
    if 'limit' in json_dict:
        limit = json_dict['limit']
        assert_type(limit, int)
        request['limit'] = limit

    return request


def preprocess(sql):
    sql = re.sub(r'cast\(\s*(\w+)\s*[aA][sS]\s*(\w+)\)', r'cast(\g<1>, \g<2>)', sql)
    sql = re.sub(r"date '(\d{4}-\d{2}-\d{2})'", r"'date \g<1>'", sql)
    sql = re.sub(r"timestamp '(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})'",
                 r"'timestamp \g<1>'", sql)
    sql = re.sub(r"timestamp '(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})'",
                 r"'timestamp \g<1>'", sql)
    return sql


class SQL2Json(object):
    """Converts SQL to the *verdict query*."""

    def __init__(self):
        self._alias_counter = 0

    def next_num(self):
        self._alias_counter += 1
        return self._alias_counter

    def is_agg_select(self, attr_alias_pairs):
        agg_func_found = False
        for attr_alias in attr_alias_pairs:
            if isinstance(attr_alias[0], AggFunc):
                agg_func_found = True

        for attr_alias in attr_alias_pairs:
            assert_type(attr_alias, (List, tuple))
            assert_type(attr_alias[0], Attr)
            assert_type(attr_alias[1], str)
            if agg_func_found:
                if isinstance(attr_alias[0], AggFunc):
                    agg_func = attr_alias[0]
                    args = agg_func.args()
                    for a in args:
                        if isinstance(a, AttrOp):
                            msg = (f"Only a base attribute can appear within an aggregate "
                                   f"function, but we see {a}")
                            raise ValueError(msg)

        return agg_func_found

    def convert(self, element, context=None):
        """Converts a SQL query to a *verdict query*.

        :param element:
            The SQL query or some components of it.

        :param context:
            Used internally to set the context for recursive calls.
        """

        def replace(e, ctx=None):
            return self.convert(e, ctx)

        if isinstance(element, (List, tuple)):
            return [replace(e, context) for e in element]

        elif isinstance(element, (int, float)):
            # constant
            if context == 'attr':
                return Constant(element)
            else:
                return element

        elif isinstance(element, str):
            if element == '*':
                if context == 'attr-count':
                    return []
                else:
                    raise ValueError("'*' is not allowed except for count(*).")
            elif element[0] in ["'", '"']:
                return element.strip('\'').strip('"')
            else:
                # attribute
                if context == 'attr':
                    return BaseAttr(element)
                elif context == 'from' or context == 'table':
                    return BaseTable(element)
                else:
                    raise ValueError(f'Unexpected {element} in the context of {context}')

        elif 'literal' in element:
            value = element['literal']
            m = re.match(r"'date (\d{4}-\d{2}-\d{2})'", value)
            if m is not None:
                return Constant(m[1], 'date')
            else:
                return Constant(value)

        elif 'value' in element:
            value = element['value']

            if context == 'select':
                # AttrOp or BaseAttr or AggFunc
                attr = replace(element['value'], 'attr')
                assert_type(attr, Attr)
                alias = None
                if 'name' in element:
                    alias = element['name']
                else:
                    if isinstance(attr, BaseAttr):
                        alias = attr.name()
                    elif isinstance(attr, (AttrOp, AggFunc)):
                        num = self.next_num()
                        alias = attr.op()[0] + str(num)
                    else:
                        raise ValueError(element)
                return (attr, alias)

            elif context == 'from':
                # table
                table = replace(element['value'], 'table')
                assert_type(table, Table)
                if 'name' in element:
                    table.uid = element['name']
                elif isinstance(table, BaseTable):
                    table.uid = table.name()
                return table

            elif context == 'groupby':
                # BaseAttr
                attr = replace(element['value'], 'attr')
                assert_type(attr, BaseAttr)
                return attr

            elif context == 'orderby':
                attr = replace(element['value'], 'attr')
                assert_type(attr, BaseAttr)
                sort = 'asc' if 'sort' not in element else element['sort'].lower()
                assert sort == 'asc' or sort == 'desc'
                return (attr, sort)

            else:
                raise ValueError(element)

        elif 'count' in element:
            arg = replace(element['count'], 'attr-count')
            return AggFunc.count(arg)

        elif 'sum' in element:
            arg = replace(element['sum'], 'attr')
            return AggFunc.sum(arg)

        elif 'avg' in element:
            arg = replace(element['avg'], 'attr')
            return AggFunc.avg(arg)

        elif 'mul' in element:
            args = replace(element['mul'], 'attr')
            assert len(args) >= 1
            converted = None
            for i, arg in enumerate(args):
                if i == 0:
                    converted = arg
                else:
                    converted = converted * arg
            return converted

        elif 'and' in element:
            args = replace(element['and'], 'attr')
            assert len(args) >= 2
            attrop = args[0]
            for i in range(1, len(args)):
                attrop = AttrOp('and', [attrop, args[i]])
            return attrop

        elif 'or' in element:
            args = replace(element['or'], 'attr')
            assert len(args) >= 2
            attrop = args[0]
            for i in range(1, len(args)):
                attrop = AttrOp('or', [attrop, args[i]])
            return attrop

        elif 'eq' in element:
            args = replace(element['eq'], 'attr')
            assert len(args) == 2
            return AttrOp('eq', args)

        elif 'neq' in element:
            args = replace(element['neq'], 'attr')
            assert len(args) == 2
            return AttrOp('ne', args)

        elif 'gt' in element:
            args = replace(element['gt'], 'attr')
            assert len(args) == 2
            return AttrOp('gt', args)

        elif 'lt' in element:
            args = replace(element['lt'], 'attr')
            assert len(args) == 2
            return AttrOp('lt', args)

        elif 'gte' in element:
            args = replace(element['gte'], 'attr')
            assert len(args) == 2
            return AttrOp('geq', args)

        elif 'lte' in element:
            args = replace(element['lte'], 'attr')
            assert len(args) == 2
            return AttrOp('leq', args)

        elif 'substr' in element:
            args = replace(element['substr'], 'attr')
            assert len(args) == 3
            return AttrOp('substr', args)

        elif 'cast' in element:
            args = element['cast']
            assert len(args) == 2
            args[0] = replace(args[0], 'attr')
            to_type = args[1].lower()
            if to_type == 'varchar' or to_type == 'string':
                return AttrOp('to_str', [args[0]])
            else:
                raise ValueError(f"Unexpected casting to {args[1]} type.")

        elif 'year' in element:
            arg = replace(element['year'], 'attr')
            assert_type(arg, Attr)
            return AttrOp('year', [arg])

        elif 'month' in element:
            arg = replace(element['month'], 'attr')
            assert_type(arg, Attr)
            return AttrOp('month', [arg])

        elif 'day' in element:
            arg = replace(element['day'], 'attr')
            assert_type(arg, Attr)
            return AttrOp('day', [arg])

        elif 'round' in element:
            arg = replace(element['round'], 'attr')
            assert_type(arg, Attr)
            return AttrOp('round', [arg])

        elif 'floor' in element:
            arg = replace(element['floor'], 'attr')
            assert_type(arg, Attr)
            return AttrOp('floor', [arg])

        elif 'ceil' in element:
            arg = replace(element['ceil'], 'attr')
            assert_type(arg, Attr)
            return AttrOp('ceil', [arg])

        elif 'add' in element:
            args = replace(element['add'], 'attr')
            assert len(args) == 2
            return AttrOp('add', args)

        elif 'sub' in element:
            args = replace(element['sub'], 'attr')
            assert len(args) == 2
            return AttrOp('sub', args)

        elif 'mul' in element:
            args = replace(element['mul'], 'attr')
            assert len(args) == 2
            return AttrOp('mul', args)

        elif 'div' in element:
            args = replace(element['div'], 'attr')
            assert len(args) == 2
            return AttrOp('div', args)

        # JOIN-related components
        elif 'on' in element:
            if 'inner join' in element:
                table = replace(element['inner join'], 'from')
                self._join_type = 'inner'
            elif 'left join' in element:
                table = replace(element['left join'], 'from')
                self._join_type = 'left'
            elif 'right join' in element:
                table = replace(element['right join'], 'from')
                self._join_type = 'right'
            elif 'outer join' in element:
                table = replace(element['outer join'], 'from')
                self._join_type = 'outer'
            else:
                raise ValueError(element)

            join_cond = element['on']
            assert 'eq' in join_cond
            assert_equal(len(join_cond['eq']), 2)
            join_cols = replace(join_cond['eq'], 'attr')

            return [table, join_cols]

        else:
            if 'limit' in element:
                limit_value = element['limit']['value']
                assert_type(limit_value, int)
                del element['limit']
                rel = replace(element)
                return rel.limit(limit_value)

            elif 'orderby' in element:
                value = element['limit']['value']
                orders = replace(value, 'orderby')
                if isinstance(orders[0], Attr):
                    orders = [orders]
                for o in orders:
                    assert_type(o, (List, tuple))
                    assert_type(o[0], Attr)
                    assert_type(a[1], str)
                del element['orderby']
                rel = replace(element)
                return rel.orderby(orders)

            elif 'select' in element:
                # project or agg
                # this will return (something, alias) or a list of such
                attr_alias_pairs = replace(element['select'], 'select')
                if isinstance(attr_alias_pairs[0], Attr):
                    attr_alias_pairs = [attr_alias_pairs]

                is_agg = self.is_agg_select(attr_alias_pairs)

                attr_alias_without_groups = []
                if is_agg:
                    for attr_alias in attr_alias_pairs:
                        if isinstance(attr_alias[0], AggFunc):
                            attr_alias_without_groups.append(attr_alias)
                del element['select']

                rel = replace(element)
                if is_agg:
                    return rel.agg(attr_alias_without_groups)
                else:
                    return rel.project(attr_alias_pairs)

            elif 'groupby' in element:
                groups = replace(element['groupby'], 'groupby')
                if isinstance(groups, Attr):
                    groups = [groups]
                for g in groups:
                    assert_type(g, BaseAttr)
                del element['groupby']
                rel = replace(element)
                return rel.groupby(groups)

            elif 'where' in element:
                predicate = replace(element['where'], 'where')
                assert_type(predicate, AttrOp)

                del element['where']
                rel = replace(element)
                return rel.select(predicate)

            elif 'from' in element:
                assert len(element.keys()) == 1
                source = replace(element['from'], 'from')
                if isinstance(source, Table):
                    return source
                else:
                    # A table must be followed by a join table and a join condition
                    assert_equal(len(source)%2, 0)
                    joined_table = source[0]
                    for i in range(1, len(source)):
                        right_table = source[i][0]
                        join_cols = source[i][1]
                        join_type = self._join_type
                        left_on = join_cols[0]
                        right_on = join_cols[1]
                        assert_type(right_table, Table)
                        assert_type(left_on, Attr)
                        assert_type(right_on, Attr)
                        assert_type(join_type, str)
                        joined_table = joined_table.join(right_table, left_on, right_on, join_type)
                    return joined_table

            else:
                raise ValueError(element)


def to_verdict_query(rel_obj, context=None):
    """Converts rel_obj to a verdict query"""
    if isinstance(rel_obj, Constant):
        if rel_obj.type_hint is not None:
            return f'{rel_obj.type_hint} {rel_obj.value()}'
        else:
            return rel_obj.value()

    elif isinstance(rel_obj, BaseAttr):
        return f'attr {rel_obj.name()}'

    elif isinstance(rel_obj, AggFunc):
        return {
            'op': rel_obj.op(),
            'arg': [to_verdict_query(a) for a in rel_obj.args()]
        }

    elif isinstance(rel_obj, AttrOp):
        return {
            'op': rel_obj.op(),
            'arg': [to_verdict_query(a) for a in rel_obj.args()]
        }

    elif isinstance(rel_obj, BaseTable):
        # col_names = rel_obj.column_names()
        # col_names = [] if col_names is None else col_names
        return f'table {rel_obj.name()} {rel_obj.uid}'

    elif isinstance(rel_obj, SampleTable):
        parts = rel_obj.part_col_values()
        if len(parts) == 0:
            parts = ''
        else:
            parts = " ".join([str(p) for p in parts])
        # col_names = rel_obj.column_names()
        # col_names = [] if col_names is None else col_names
        return f'sample {rel_obj.name()} [{parts}]'

    elif isinstance(rel_obj, DerivedTable):
        if rel_obj.is_project():
            return {
                'op': rel_obj.relop_name(),
                'source': to_verdict_query(rel_obj.source()),
                'arg': {a[1]: to_verdict_query(a[0]) for a in rel_obj.relop_args()},
                'name': rel_obj.uid
            }
        elif rel_obj.is_select():
            return {
                'op': rel_obj.relop_name(),
                'source': to_verdict_query(rel_obj.source()),
                'arg': to_verdict_query(rel_obj.relop_args()[0]),
                'name': rel_obj.uid
            }
        elif rel_obj.is_join():
            return {
                'op': rel_obj.relop_name(),
                'source': to_verdict_query(rel_obj.source()),
                'arg': {
                    'join_to': to_verdict_query(rel_obj.relop_args()[0]),
                    'left_on': to_verdict_query(rel_obj.relop_args()[1]),
                    'right_on': to_verdict_query(rel_obj.relop_args()[2]),
                    'join_type': to_verdict_query(rel_obj.relop_args()[3])
                }
            }
        elif rel_obj.is_groupby():
            return {
                'op': rel_obj.relop_name(),
                'source': to_verdict_query(rel_obj.source()),
                'arg': to_verdict_query(rel_obj.relop_args())
            }
        elif rel_obj.is_agg():
            return {
                'op': rel_obj.relop_name(),
                'source': to_verdict_query(rel_obj.source()),
                'arg': {a[1]: to_verdict_query(a[0]) for a in rel_obj.relop_args()},
                'name': rel_obj.uid
            }
        elif rel_obj.is_orderby():
            return {
                'op': rel_obj.relop_name(),
                'source': to_verdict_query(rel_obj.source()),
                'arg': [(to_verdict_query(a[0]), a[1]) for a in rel_obj.relop_args()]
            }
        elif rel_obj.is_limit():
            return {
                'op': rel_obj.relop_name(),
                'source': to_verdict_query(rel_obj.source()),
                'arg': rel_obj.relop_args()[0],
            }
        else:
            raise ValueError(type(rel_obj))

    elif isinstance(rel_obj, (List, tuple)):
        # if context == 'attr-alias':
        #     assert_equal(len(rel_obj), 2)
        #     assert_type(rel_obj[0], Attr)
        #     assert_type(rel_obj[1], str)
        #     return {
        #         rel_obj[1]: to_verdict_query(rel_obj[0])
        #     }
        # elif context == 'attr-alias-list':
        #     return [to_verdict_query(a, 'attr-alias') for a in rel_obj]
        # else:
        return [to_verdict_query(a) for a in rel_obj]

    elif isinstance(rel_obj, str):
        if rel_obj in DerivedTable.join_types:
            return rel_obj
        elif rel_obj in DerivedTable.ordering_types:
            return rel_obj
        else:
            return rel_obj

    else:
        raise ValueError(rel_obj)


def from_verdict_query(json_obj):
    """Converts a verdict query to rel_obj"""

    if isinstance(json_obj, (int, float)):
        # constant
        return Constant(json_obj)

    elif isinstance(json_obj, str):
        # special constant
        m = re.match(r"date (\d{4}-\d{2}-\d{2})$", json_obj)
        if m is not None:
            return Constant(m[1], 'date')

        m = re.match(r"timestamp (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})$", json_obj)
        if m is not None:
            return Constant(m[1], 'timestamp')

        m = re.match(r"timestamp (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})$", json_obj)
        if m is not None:
            return Constant(m[1], 'timestamp')

        # attr
        m = re.match(r"attr ([\w.]+)$", json_obj)
        if m is not None:
            attr_name = m[1]
            if '.' in attr_name:
                raise ValueError(f'The dot is not allowed for attribute name: {attr_name}')
            return BaseAttr(attr_name)

        # base table
        m = re.match(r"table ([\w\.]+)$", json_obj)
        if m is not None:
            return BaseTable(m[1])
        m = re.match(r"table ([\w\.]+) (\w+)$", json_obj)
        if m is not None:
            tab_name = m[1]
            alias = m[2]
            table = BaseTable(m[1])
            table.uid = alias
            return table

        # sample table
        m = re.match(r"sample ([\w\.]+) \[([\d ]*)\]$", json_obj)
        if m is not None:
            parts = m[2]
            if len(parts) == 0:
                part_values = []
            else:
                part_values = [int(p) for p in m[2].split(" ")]
            return SampleTable(m[1], part_values)

        return Constant(json_obj)

    elif isinstance(json_obj, dict):
        if 'op' not in json_obj:
            return [(from_verdict_query(something), alias) for alias, something in json_obj.items()]

        op = json_obj['op']
        if op in AggFunc.agg_ops:
            # agg function
            assert 'arg' in json_obj
            query_args = json_obj['arg']
            assert_type(query_args, (List, tuple))
            args = [from_verdict_query(attr) for attr in query_args]
            return AggFunc(json_obj['op'], args)

        elif op in AttrOp.op_names:
            # attr op function
            assert 'arg' in json_obj
            query_args = json_obj['arg']
            assert_type(query_args, (List, tuple))
            args = [from_verdict_query(attr) for attr in query_args]
            return AttrOp(json_obj['op'], args)

        elif op in DerivedTable.op_names:
            assert 'source' in json_obj
            assert 'arg' in json_obj
            source = from_verdict_query(json_obj['source'])

            if op == "project":
                table = DerivedTable(source, op, from_verdict_query(json_obj['arg']))
                if 'name' in json_obj:
                    assert_type(json_obj['name'], str)
                    table.uid = json_obj['name']
                return table

            elif op == "select":
                args = [from_verdict_query(json_obj['arg'])]
                return DerivedTable(source, op, args)

            elif op == "join":
                arg = json_obj['arg']
                assert 'join_to' in arg
                assert 'left_on' in arg
                assert 'right_on' in arg
                join_type = 'inner'
                if 'join_type' in arg:
                    join_type = arg['join_type']

                args = [from_verdict_query(arg['join_to']), 
                        from_verdict_query(arg['left_on']), 
                        from_verdict_query(arg['right_on']),
                        join_type]
                return DerivedTable(source, op, args)

            elif op == "agg":
                table = DerivedTable(source, op, from_verdict_query(json_obj['arg']))
                if 'name' in json_obj:
                    assert_type(json_obj['name'], str)
                    table.uid = json_obj['name']
                return table

            elif op == "groupby":
                args = [from_verdict_query(attr) for attr in json_obj['arg']]
                return DerivedTable(source, op, args)                

            elif op == "orderby":
                args = json_obj['arg']
                assert_type(args, (tuple, list))
                new_args = []
                for a in args:
                    assert_type(args, (tuple, list))
                    assert_equal(len(a), 2)
                    new_args.append((from_verdict_query(a[0]), a[1]))
                return DerivedTable(source, op, new_args)

            elif op == "limit":
                arg = json_obj['arg']
                assert_type(arg, int)
                return DerivedTable(source, op, [arg])

            else:
                raise ValueError(json_obj)
        else:
            raise ValueError(json_obj)

    elif isinstance(json_obj, list):
        return [from_verdict_query(a) for a in json_obj]

    elif isinstance(json_obj, tuple):
        return tuple([from_verdict_query(a) for a in json_obj])

    else:
        raise ValueError(f"Unexpected argument: {json_obj} of type ({type(json_obj)}).")

