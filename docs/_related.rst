.. _related:

Similar Systems
================================

In this page, we compare Verdict to other related systems. We also explain several well-known
algorithms for faster data analytics, which are already supported by most analytical engines.


Systems
-------------------

We describe several well-known systems. In the literature, they are a large number of research 
prototypes, which we do not discuss here.


1. **VerdictDB**: VerdictDB is a predecessor of Verdict. Verdict delivers the same functionality in a
more optimized way with new formal semantics and aggressive in-memory computation.

2. **BlinkDB**: BlinkDB is a predecessor of VerdictDB. Unlike VerdictDB, however, BlinkDB is
strongly tied to Spark (more accurately, Shark, which is an older prototype before Spark SQL.). Due
to this reason, BlinkDB is not platform-independent.

3. **Azure SQL**: Azure SQL offers query-time sampling operators for reducing computational
resource. Azure's sampling operators cannot be used for other database engines.
Due to its nature, Azure's query-time sampling operators are more flexible in supporting ad-hoc
queries; however, it is shown that query-time sampling can deliver about 2x improvement compared
to regular query processing. In contrast, Verdict can deliver more than 100x improvement.

4. **Other DBMS**: Not surprisingly, sampling operators themselves are supported in most commercial
database engines. However, they don't offer any automatic operations about how to use those sampling
operators to produce accuracy-guaranteed answers, which is the core operations offered by Verdict.


Algorithms
-------------

1. **HyperLogLog**: HyperLogLog is a sketching algorithm developed for memory-efficient computation
of distinct-count queries. This algorithm is available in most analytical database systems including
Verdict.

2. **t-digest**: 
