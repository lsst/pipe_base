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

.. automodapi:: lsst.pipe.base.testUtils
   :no-main-docstr:

.. automodapi:: lsst.pipe.base.connectionTypes
  :no-main-docstr:

.. automodapi:: lsst.pipe.base.pipelineIR
  :no-main-docstr:
