.. _syntax:

Query Syntax
===================

This page describes Verdict's query syntax. In general, Verdict follows the syntax of the standard
SQL. There are some temporary limitations (highlighted using blue boxes below), which will be 
gradually lifted in future versions.


Query
--------

The Verdict query must be an aggregate query with optional groupby, orderby, and limit clauses.

.. code::

    query := SELECT aggregate_function, ...
             FROM relation
             [GROUP BY base_attr, ...]
             [ORDER BY alias, ...]
             [LIMIT int]

    alias := str

    base_attr := str


Example:

.. code:: sql

    SELECT sum(price) as price_sum, count(*) as c
    FROM hive.tpch_sf100.lineitem_premerged
    GROUP BY l_linestatus
    ORDER BY price_sum
    LIMIT 5


.. note:: If the groupby clause is present, the grouping columns will be prepended in the result 
          set. In the future, this behavior will be changed to follow the standard SQL semantics.



Relation
------------

A relation can be a base table, joins of relations, or subqueries.

.. code::

    relation := base_table | 
                relation join_expr |
                (SELECT attr_alias, ...
                 FROM relation
                 [WHERE predicate]
                 [GROUP BY attr, ...]) alias

    join_expr := join_type relation ON base_attr = base_attr

    join_type := INNER JOIN | 
                 LEFT JOIN | 
                 RIGHT JOIN | 
                 OUTER JOIN

    attr_alias := attr [AS] alias


Examples of the relation:

.. code:: sql

    -- example 1
    (
        select
            l_returnflag,
            l_quantity,
            l_extendedprice,
            l_discount,
            l_extendedprice,
            l_extendedprice * (1 - l_discount) disc_price,
            l_extendedprice * (1 - l_discount) * (1 + l_tax) charge,
            l_returnflag,
            l_linestatus
        from
            hive.tpch_tiny.lineitem_premerged
        where
            l_shipdate <= date '1998-12-01'
    ) t1

    -- example 2
    (
        select
            l_orderkey,
            l_extendedprice * (1 - l_discount) revenue,
            o_orderdate,
            o_shippriority
        from
            hive.tpch_tiny.lineitem_premerged l 
                inner join hive.tpch_tiny.orders_premerged o
                on l_orderkey = o_orderkey
        where
            c_mktsegment = 'BUILDING'
            and o_orderdate < date '1995-03-22'
            and l_shipdate > date '1995-03-22'
    ) t1

Note that in the above example, ``t1`` is the alias of the subquery relation. If the ``alias``
is omitted the same name is assigned for base attributes and an arbitrary name is assigned for
derived attributes (e.g., ``l_extendedprice * (1 - l_discount)``).

.. note::
    The join type must be equijoin (whether it be inner, left, or right). The attribute that appears
    on the left-hand side of the equality sign is assumed to the attribute in the left join table.
    The similar rule applies for the right attribute.


Attribute
^^^^^^^^^^^^^^^^^^^^^^^^

An attribute can be a base attribute or some functions of it.

.. code::

    attr := base_attr | 
            constant | 
            scalar_function | 
            aggregate_function

    constant := int | 
                str | 
                date '0000-00-00' | 
                timestamp '0000-00-00 00:00:00'

    predicate := logical_expr | 
                 comparative_expr

    logical_expr := predicate AND predicate | 
                    predicate OR predicate  |
                    NOT predicate

    comparative_expr := attr > attr | 
                        attr < attr | 
                        attr >= attr | 
                        attr <= attr | 
                        attr <> attr |
                        attr in [ constant, ... ]


We describe more details about functions in the subsequent sections.



Scalar Functions
------------------

A scalar function is the function that produces an output value for each input value.


.. code::

    scalar_function := math_function | 
                       string_function


Mathematical Functions
^^^^^^^^^^^^^^^^^^^^^^^^

.. code::

    math_function := attr + attr | 
                     attr - attr | 
                     attr * attr | 
                     attr / attr |
                     floor(attr) | 
                     ceil(attr)  | 
                     round(attr)


String Functions
^^^^^^^^^^^^^^^^^^^^^^^^

.. code::

    string_function := SUBSTR(attr, start, length) | 
                       TO_STRING(attr) |
                       CAST(attr AS VARCHAR) | 
                       CONCAT(attr, attr) | 
                       LENGTH(attr) |
                       REPLACE(old, new) |
                       UPPER(attr) |
                       LOWER(attr) |
                       STARTSWITH(attr, pattern) |
                       CONTAINS(attr, pattern) |
                       ENDSWITH(attr, pattern)


.. note:: We are adding more scalar functions.


Aggregate Functions
---------------------

An aggregate function is a function that produces a single row given multiple rows.

.. code::

    aggregate_function := COUNT(*) | 
                          SUM(base_attr) | 
                          AVG(base_attr)


.. note:: To use a derived attribute within aggregate functions, you can first create new attributes
          using subqueries, then attribute those new attributes.
