.. py:currentmodule:: lsst.pipe.base

.. _lsst.pipe.base:

##############
lsst.pipe.base
##############

The ``lsst.pipe.base`` module provides base classes for the task framework.
Tasks package the algorithmic units of the LSST Science Pipelines.
You can create, configure, and run tasks with their Python APIs.
Some tasks, called pipeline tasks, can be packaged into data processing pipelines that you can run from the command line.

.. _lsst.pipe.base-changes:

Changes
=======

.. toctree::
   :maxdepth: 1

   CHANGES.rst

.. _lsst.pipe.base-using:

Using lsst.pipe.base
====================

.. _lsst-pipe-base-overview:

Overview
--------

.. toctree::
   :maxdepth: 1

   task-framework-overview.rst

.. _lsst-pipe-base-developing-tasks:

Developing tasks and pipeline tasks
-----------------------------------

.. toctree::
   :maxdepth: 1

   creating-a-task.rst
   testing-a-pipeline-task.rst
   creating-a-pipelinetask.rst
   task-retargeting-howto.rst

.. _lsst-pipe-base-developing-pipelines:

Developing Pipelines
--------------------

.. toctree::
   :maxdepth: 1

   creating-a-pipeline.rst
   testing-pipelines-with-mocks.rst
   working-with-pipeline-graphs.rst

.. _lsst.pipe.base-contributing:

Contributing
============

``lsst.pipe.base`` is developed at https://github.com/lsst/pipe_base.
You can find Jira issues for this module under the `pipe_base <https://jira.lsstcorp.org/issues/?jql=project%20%3D%20DM%20AND%20component%20%3D%20pipe_base>`_ component.

.. _lsst.pipe.base-pyapi:

Python API reference
====================

.. automodapi:: lsst.pipe.base
   :no-main-docstr:
   :skip: BuildId
   :skip: DatasetTypeName
   :skip: PipelineGraph

.. automodapi:: lsst.pipe.base.pipeline_graph
   :no-main-docstr:

.. automodapi:: lsst.pipe.base.testUtils
   :no-main-docstr:

.. automodapi:: lsst.pipe.base.connectionTypes

.. automodapi:: lsst.pipe.base.pipelineIR
  :no-main-docstr:

.. automodapi:: lsst.pipe.base.tests.mocks

.. automodapi:: lsst.pipe.base.execution_reports
   :no-main-docstr:

QuantumGraph generation API reference
-------------------------------------

.. automodapi:: lsst.pipe.base.quantum_graph_builder
.. automodapi:: lsst.pipe.base.quantum_graph_skeleton
.. automodapi:: lsst.pipe.base.prerequisite_helpers
.. automodapi:: lsst.pipe.base.all_dimensions_quantum_graph_builder
   :skip: DatasetQueryConstraintVariant
.. automodapi:: lsst.pipe.base._datasetQueryConstraints
