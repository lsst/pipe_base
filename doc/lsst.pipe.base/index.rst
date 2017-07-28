.. _lsst.pipe.base:

##############
lsst.pipe.base
##############

The ``lsst.pipe.base`` module provides base classes for the task framework.
Tasks package the algorithmic units of the LSST Science Pipelines.
You can create, configure, and run tasks with their Python APIs.
Some tasks, called command-line tasks, are also packaged into data processing pipelines that you can run from the command line.

.. _lsst-pipe-base-overview:

Overview
========

.. toctree::
   :maxdepth: 1

   task-framework-overview.rst

.. _using-command-line-tasks:

Using command-line tasks
========================

.. toctree::
   :maxdepth: 1

   command-line-task-data-repo-howto.rst
   command-line-task-dataid-howto.rst
   command-line-task-config-howto.rst
   command-line-task-prov-howto.rst
   command-line-task-logging-howto.rst
   command-line-task-parallel-howto.rst
   command-line-task-argument-reference.rst

.. _lsst-pipe-base-developing-tasks:

Developing tasks and command-line tasks
=======================================

.. toctree::
   :maxdepth: 1

   creating-a-task.rst
   creating-a-command-line-task.rst
