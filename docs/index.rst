.. Verdict documentation master file, created by
   sphinx-quickstart on Sun Sep  8 13:17:21 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Verdict Documentation
==============================

This documentation introduces Verdict's core concepts and operations. Verdict is developed to
provide extremely fast and resource-efficient analytics, working **on top of** existing engines
(thus, no modifications to the engines).  Unlike other systems, Verdict **predicts** the answers
(instead of simply aggregating individual values) by exploiting statistical properties of analytical
workloads. These predictions are performed almost instantly and come with tight accuracy guarantees.
In many cases, you get more than 99% accurate answers in less than a second.


.. note::

   Everything in this project is under the Apache License.



.. toctree::
   :maxdepth: 1

   intro
   quickstart
   examples
   syntax
   sampling
   system
   reference


.. Indices and tables
.. ==================

.. * :ref:`genindex`
.. * :ref:`modindex`
.. * :ref:`search`
