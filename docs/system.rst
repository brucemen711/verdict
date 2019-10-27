.. _system:

System Overview
===================

This page overviews Verdict's architecture. First, we describe how Verdict interacts with users
and other analytics engines. Next, we describe Verdict's internal architecture.


Deployment
-------------

There are three entities involved:

1. **Client (or User)**:
   The client or user is the entity that composes/issues analytical queries and consumes their 
   results. The client can be a data analyst, BI tools, Web dashboards, operational services, etc.
2. **Verdict**:
   Verdict is our library that imposes itself as a middleware between the client and analytics
   engines. Verdict intercepts the queries from the client and send rewritten queries to analytics
   engines.
3. **Backend Engine**:
   This entity performs traditional aggregate-style computations. The backend engines can be modern 
   cloud services (e.g., BigQuery, Redshift) or traditional database engines (e.g., MySQL, Oracle, 
   etc.).


Query workflow
^^^^^^^^^^^^^^^^

We describe Verdict's query processing workflow starting from the query issued by the client. Given
a query from the client, Verdict performs **planning**, which consists of two following operations:

1. Verdict look for the data summaries created for the tables in the query
2. Verdict determines how much portion of summaries to process. That is, the actual amount of data
   to  process should be small enough to reduce query latency as much as possible. At the same time.
   the size should be large enough to ensure enough accuracy.

Once the planning is done, Verdict rewrites the query; the rewritten query uses the summaries in
place of the original tables. Then, Verdict sends the rewritten query to an analytics engine (e.g.,
Presto). The written query (which is sent to the analytics engine) looks like a regular query to the
analytics engine. After Verdict receives the answer for its rewritten query, Verdict performs
post-processing, and returns a final result back to the client.


.. note:: For optimal performance, Verdict also optionally caches summaries and uses its in-memory
          analytics engine.



Verdict Architecture
------------------------

In this section, we describe internal components of Verdict and their functions. These are Verdict's
internal components.

1. **Query Planner**: This component determines what summaries to use. Verdict chooses the optimal 
   sample type and ratio among available summaries.
2. **Sampler**: This component specifies the rules for different types of samples.
3. **SQL2IR**: This component translates SQL to Verdict's internal json representation
4. **Drivers**: This component translates Verdict's internal json representation to the 
   engine-specific language.



Internal workflow
^^^^^^^^^^^^^^^^^^^^

**Offline Sampling**

Given a source table name and a sample type, Sampler specifies how the sample table
should be structured. For example, a sample table can be composed of multiple partitions where
each partition corresponds to a random subset of a different size. Depending on attribute values,
different sampling probabilities may be assigned for faster convergence.

Verdict then passes this structure information to the driver for a target analytics engine. Then, 
this driver composes a query that can actually run on the target engine (e.g., SQL for Presto).


**Query Processing**

Given a query, Verdict's Query Planner determines the right sample among the available ones. This
planning stage considers various criteria as follows:

1. **User-requested accuracy:** You may want different accuracy requirement for faster 
   processing or more accurate answers.
2. **Types of aggregate functions:** Depending on the types of aggregate functions (
   e.g., avg, count), we use different formulations to derive the optimal amount of data to process.
3. **Query operations:** Depending on the join patterns, groupby clauses, etc., we may need to use
   different types of samples in a specific way.

Once the planning is done, Verdict's composes a query in its internal json format, which is sent to 
engine-specific drivers. Then, these drivers translate them and run on analytics engines.


.. note:: Creating right types of samples is critical for Verdict's operations. Although we provide
          general mechanisms and guidelines, this may not always be straightforward to all users
          or simply you may not have time to understand it. To help such cases, we are developing 
          an automatic designer.


