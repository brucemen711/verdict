Introduction
================================

Verdict is a lightweight system that can accurately answer your analytical queries. Instead of
aggregating every value in your data, Verdict **predicts** the answers using its concise data
summary, or equivalently *samples*. To maximize the efficiency, verdict incorporates advanced
types of samples.

Verdict is designed to operate on top of existing analytics engines (e.g., Facebook Presto,
Google BigQuery, Amazon  Redshift). Thus, no data migration is required.

Verdict has two modes of operations:

1. **stream**: Verdict gives you a series of answers as continuously processing data. Each answer is
   statistically unbiased. Internally, Verdict performs optimization to maximize the accuracy of the
   answers. Typically, even the first answer (available in less than a second) is almost identical to
   the exact one, however large your data is.
2. **traditional**: Verdict gives you a single answer that satisfies a requested accuracy level (1% 
   relative error by default). Again, Verdict's latency doesn't increase however large you data is.

.. For acceleration, Verdict relies on advanced statistical properties (and learning theories), which
.. states that in most cases, we can obtain highly accurate answers by using a small fraction of the
.. entire  data. Since processing a fraction of the data is much cheaper/faster than processing the
.. entire data, Verdict can output answers orders-of-magnitude cheaper/faster than usual approaches.

.. Verdict also has an additional mechanism that provides progressive analytics: you obtain
.. increasingly more accurate answers as the system processes more of your data. Using this mechanism,
.. you can see highly accurate answers just in a few seconds (however large your data is) while these
.. answers quickly converge to the exact ones. Verdict also ensures that these answers are always
.. *statistically unbiased*.


.. note::
    We are refactoring our old code; thus, some of the features mentioned in `our website
    <https://verdictdb.org>`_ may not be available yet.
