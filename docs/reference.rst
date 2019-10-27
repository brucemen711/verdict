.. _reference:

Reference
=================


Entry Point
------------------

.. automodule:: verdict
   :members:



Verdict Session
------------------

.. automodule:: verdict.session


.. autoclass:: VerdictSession
   :members:



Internal Interface
---------------------

.. automodule:: verdict.interface


SQL <-> Verdict Query
^^^^^^^^^^^^^^^^^^^^^^^^

.. autofunction:: verdict.interface.sql2verdict_query

.. autoclass:: verdict.interface.SQL2Json
   :members:



Verdict Query <-> Relational Objects
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autofunction:: verdict.interface.to_verdict_query

.. autofunction:: verdict.interface.from_verdict_query



Query Processing
--------------------

.. automodule:: verdict.core.querying2

.. autoclass:: verdict.core.querying2.AggMerger
   :members:

.. autoclass:: verdict.core.querying2.Querying
   :members:

.. autofunction:: verdict.core.querying2.find_replacables
