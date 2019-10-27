Quickstart
===========

In this quickstart, we will install Verdict using its public docker image. Using included scripts,
we will connect to 100GB test data stored in our Amazon S3 bucket. Our demo will use  Facebook
Presto as an underlying analytics engine, which we will also install using a docker image.


Install
---------

Requirements
^^^^^^^^^^^^^^

1. A machine with 64GB or more memory
2. ``docker`` (get `here <https://docs.docker.com/install/linux/docker-ce/ubuntu/>`_) and 
   ``docker-compose`` (get `here <https://docs.docker.com/compose/install/>`_).
3. AWS access key and its secret credential, which can be obtained freely from `the official page 
   <https://aws.amazon.com/premiumsupport/knowledge-center/create-access-key/>`_.


.. note:: Most computational resource is for running Presto. Verdict itself is quite light-weight.


Steps
^^^^^^

**First**, set your AWS access key and is credential to environment variables:

.. code-block:: bash

    export AWS_ACCESS_KEY_ID=<your access key>
    export AWS_SECRET_ACCESS_KEY=<your secret credential>


**Second**, pull and run docker containers:

.. code-block:: bash

    curl -s https://raw.githubusercontent.com/verdictproject/verdict/master/docker-compose-64gb.yaml \
        | docker-compose -f - up


Running ``docker ps`` will show two containers named ``docker-verdict`` and ``docker-presto``.



**Third**, create external tables that connect to 100GB dataset:

.. code-block:: bash

    docker exec docker-verdict define_presto_tables_and_samples.sh



Connect
-----------

First, open the python shell in docker:

.. code-block:: bash

    docker exec -it docker-verdict python


Then, make a connection to Presto with verdict.

.. code-block:: python

    import verdict
    v = verdict.presto(presto_host='presto')



Info method
^^^^^^^^^^^^^^

To see the tables indexed by verdict, use the ``info()`` method.

.. code-block:: python

    v.info()
    # {   'Registered Tables': [   'hive.tpch_sf100.orders',
    #                              'hive.tpch_sf1.orders',
    #                              'hive.tpch_sf100.lineitem',
    #                              'hive.tpch_sf1.lineitem']}


You can pass an argument to ``info()`` to see more information about it.

.. code-block:: python

    v.info('hive.tpch_sf100.lineitem')
    # {   'Column Names and Types': {   'l_comment': 'varchar',
    #                                   'l_commitdate': 'date',
    #                                   'l_discount': 'double',
    #                                   ...
    #                                   's_nationkey': 'bigint',
    #                                   's_phone': 'varchar'},
    #     'Samples': [   's9487fcfadd71477ead92b02cf587e525_rowid',
    #                    's63d739590a784d959b3d1e8694ef5e3al_orderkey'],
    #     'Row Count': 600037902}

From the above output, you can see two samples have been created for ``hive.tpch_sf100.lineitem``.
Verdict uses these samples (automatically) to speed up its query processing. You may be curious
how they are created, but before describing how to create them, let's first run some queries.




Run Queries
---------------------

Traditional Mode
^^^^^^^^^^^^^^^^^^

To issue queries in the *traditional* mode, we use the ``sql()`` method. For example,

.. code-block:: python

    v.sql("select count(*) from hive.tpch_sf100.lineitem")
    #           c1
    # 0  597536768

    v.sql("select count(*) from hive.tpch_sf100.lineitem where l_linestatus = 'F'")
    #           c1
    # 0  299372544

The above queries will return answers almost instantly.




Stream Mode
^^^^^^^^^^^^^^^^^^

To run queries in the stream mode, use ``sql_stream()`` method. This method returns an iterator
from which you can retrieve a series of answers. For example,

.. code-block:: python

    itr = v.sql_stream("select count(*) from hive.tpch_sf100.lineitem where l_linestatus = 'F'")
    
    for result in itr:
        print(result)


To see more example queries for both traditional and stream modes, see :ref:`examples`.



Bypass Queries
^^^^^^^^^^^^^^^^^^

Finally, you can issue any queries directly to the backend engine (Presto here) by sending a query
to ``sql()`` method with the prefix ``bypass``. For example,


.. code-block:: python

    # this will return an exact answer, but will take longer
    v.sql("bypass select count(*) from hive.tpch_sf100.lineitem where l_linestatus = 'F'")
    #        _col0
    # 0  299979732

The answers are almost identical, but it takes longer this time since the query is directly
processed by the backend engine.


You can also issue metadata queries or DDL queries by prefixing ``bypass``.

.. code-block:: python

    # regular presto query
    v.sql("bypass show catalogs")

    # The above method will return this:
    #
    #   Catalog
    # 0    hive
    # 1     jmx
    # 2  memory
    # 3  system
    # 4    tpch

    v.sql("bypass show schemas in hive")
    #                Schema
    # 0             default
    # 1  information_schema
    # 2             verdict
    # 3            tpch_sf1
    # 4          tpch_sf100

    v.sql("bypass show tables in hive.tpch_sf100")
    #       Table
    # 0  lineitem
    # 1    orders
    # 2  partsupp





.. Run More Queries on Notebooks
.. -------------------------------

.. You can see pre-populated example queries on shipped Zeppelin and Jupyter.

.. * To open Zeppelin, open `localhost:8180 <localhost:8180>`_.
.. * To open Jupyter, type `localhost:8888 <localhost:8888>`_.

.. not

..     If you are running this quickstart in AWS or other cloud, use its public IP address in place of 
..     ``localhost`` above. Also, make sure those ports (i.e., 8180 and 8888) are open for your 
..     instance.


