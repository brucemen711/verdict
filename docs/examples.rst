.. _examples:

Example Queries
=================

We show several example queries here. To run the following examples, first import verdict and create
its instance as follows:

.. code-block:: python

    import verdict
    v = verdict.presto(presto_host='presto')


Traditional Mode
-------------------------------

Use the ``sql(query_string)`` method to obtain a single accuracy-guaranteed answer. The error level
can be specified at query time (1% relative error, by default) .

.. note:: To see details of the class of SQL queries supported by Verdict, see :ref:`syntax`.


Count with arbitrary filters
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    v.sql("""
    select l_shipmode, count(*)
    from hive.tpch_sf1.lineitem_premerged
    where l_shipdate >= date '1994-01-01' or l_shipdate <= date '1995-01-01'
    group by l_shipmode
    order by l_shipmode
    """)


Joins of two large tables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    v.sql("""
    select o_orderstatus, count(*)
    from hive.tpch_sf100.lineitem_premerged l inner join
         hive.tpch_sf100.orders_premerged o on l_orderkey = o_orderkey
    group by o_orderstatus
    order by o_orderstatus
    """)


Exists predicate expressed using a join (TPC-H Q4)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    v.sql("""
    select
        o_orderpriority,
        count(*) as order_count
    from
        hive.tpch_sf100.orders_premerged o left join
        (
            select l_orderkey, count(*) exist_count
            from hive.tpch_sf100.lineitem_premerged
            where l_commitdate < l_receiptdate
            group by l_orderkey
        ) t on o_orderkey = l_orderkey
    where
        o_orderdate >= date '1996-05-01'
        and o_orderdate < date '1996-08-01'
        and exist_count > 0
    group by
        o_orderpriority
    order by
        o_orderpriority
    """)



Stream Mode
------------

Simply change ``sql`` to ``sql_stream``. Then, verdict returns an iterator from which you can obtain
a series of answers that converge to the exact one. Often, this is called progressive analytics.
For example,


Count with arbitrary filters
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    results_itr = v.sql_stream("""
    select count(*)
    from hive.tpch_sf100.lineitem_premerged
    where l_returnflag = 'R' and 
          (l_shipdate >= date '1994-01-01' or l_shipdate <= date '1995-01-01')
    """)

    for result in results_itr:
        print(result)


These successive results can be used by upfront applications (e.g., visualization libraries)
to deliver the results in a more intuitive way.
