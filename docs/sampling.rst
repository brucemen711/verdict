Sample Creation
==================

Verdict uses various types of samples to estimate the final query answers. Verdict automatically
determines what types of samples to use at query time; however, those samples must exist in advance 
for Verdict to perform proper operations.

Each sample is uniquely determined by two properties:

1. **Table name**: This is the name of the base table.
2. **Column name**: This column serves as the *key* for creating a sample. Specifically, a sample
   is created by *sampling from the domain of the key column* (not simply randomly choosing 
   individual records). When the key is set to ``_rowid`` (a special keyword), a sample is created
   by randomly choosing values from the row numbers, which is equivalent to uniform random sampling.

Before describing what types of samples Verdict needs, we describe the method for creating samples.



Sample Creation Method
------------------------

To create a sample, use ``VerdictSession.create_sample(table_name, key_col)``. For example,


.. code:: python

    table_name = "hive.tpch_sf100.lineitem"
    key_col = "l_orderkey"
    v.create_sample(table_name, key_col)



What Samples? Rule of Thumb
-------------------------------

We describe what samples are likely to be needed for processing most queries.

1. **Table name**: We need samples for *large* tables. These tables are typically the tables
   including a large number of historical records.
2. **Column name**: ``_rowid`` and every column whose *support* is large, where *support* means
   the number of unique attribute values in the column.



More About Verdict's Sampling
-------------------------------


What Happens?
^^^^^^^^^^^^^^^

When the above method is called, Verdict performs the following operations:

1. Verdict gathers basic statistics about the table (i.e., number of rows, column names and types).
   This information is used for query processing as well as sampling.
2. Verdict writes a SQL statement for sampling and sends it to the backend engine.
3. Verdict stores the information about the sample (i.e., what is the original table, what is the
   key column, etc.) in its metastore (i.e., Redis).
4. Verdict stores a small fraction of the sample (called *cache*) in its in-memory engine (i.e.,
   Pandas SQL). The cache is used for estimating the sample size needed for satisfying the accuracy.


Limitations
^^^^^^^^^^^^

The current version of Verdict has some known limitations:

1. Verdict does not automatically maintain the consistency between the original table and its
   samples. Thus, if new records are inserted into the original table, the sample becomes stale.
2. Verdict does not automatically determine what samples are needed for your data. The manual
   steps must be performed as described below.

We are working to address these limitations.




