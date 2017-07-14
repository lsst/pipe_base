.. TODO DM-11685 Add opinionated suggestions on how to use reruns versus output repositories.

.. _command-line-task-data-repo-howto:

#################################################################
Using Butler data repositories and reruns with command-line tasks
#################################################################

:ref:`Command-line tasks <using-command-line-tasks>` use Butler data repositories for reading and writing data.
This page describes two ways for specifying data repositories on the command line:

1. Specify input and output repository URIs (or *file paths* for POSIX-backed Butler data repositories) with the :option:`REPOPATH` and :option:`--output` command-line arguments.
   Read about this in :ref:`command-line-task-data-repo-using-output`.

2. Use reruns, which are output data repositories relative to a single root repository, with the :option:`REPOPATH` and :option:`--rerun` arguments.
   Read about this pattern in :ref:`command-line-task-data-repo-using-reruns`.

.. _command-line-task-data-repo-using-uris:

About repository paths and URIs
===============================

.. TODO DM-11671 Move this section to the Butler user guide.

Butler data repositories can be hosted on a variety of backends, from a local POSIX directory to a more specialized data store like Swift.
All Butler backends are functionally equivalent from a command line user's perspective.
In each case, you specify a data repository with its URI.

For a POSIX filesystem backend, you can specify a path to the repository directory through:

- A relative path.
- An absolute path.
- A URI prefixed with ``file://``.

Other backends always require a URI.
For example, a URI like ``swift://host/path`` points to a Butler repository backed by the Swift object store.

In the how-to topics below, URIs or POSIX paths can be used as needed for the :file:`inputrepo` (:option:`REPOPATH`) and :file:`outputrepo` (:option:`--output`) command-line arguments.

.. _command-line-task-data-repo-using-output:

Using input and output repositories
===================================

How to create a new output repository
-------------------------------------

To have a command-line task read data from a :file:`inputrepo` repository and write to a :file:`outputrepo` output repository, set the :option:`REPOPATH` and :option:`--output` arguments like this:

.. code-block:: text

   task.py inputrepo --output outputrepo ...

The :file:`outputrepo` directory will be created if it does not already exist.

.. _command-line-task-data-repo-using-output-chaining:

How to chain output repositories
--------------------------------

The output repository for one task can become the input repository for the next command-line task.
For example:

.. code-block:: text

   task2.py outputrepo --output outputrepo2 ...

Because Butler data repositories are *chained*, the output repository (here, :file:`outputrepo2`) provides access to all the datasets from the input repositories (here: :file:`inputrepo`, :file:`outputrepo`, and :file:`outputrepo2` itself).

How to re-use output repositories
---------------------------------

An output repository can be the same as the input repository:

.. code-block:: text

   task3.py outputrepo2 --output outputrepo2 ...

This pattern is useful for reducing the number of repositories.
Packing outputs from multiple tasks into one output repository does reduce your flexibility to run a task several times with different configurations and compare outputs, though.

You can also run the same task multiple times with the same output repository.
Be aware that the Science Pipelines will help you maintain the integrity of the processed data's provenance.
If you change a task's configuration and re-run the task into the same output repository, an error "Config does not match existing task config" will be shown.
See :doc:`command-line-task-prov-howto`.

How to use repository path environment variables
------------------------------------------------

The :envvar:`PIPE_INPUT_ROOT` and :envvar:`PIPE_OUTPUT_ROOT` environment variables can help you specify data repository paths more succinctly.
When set, the :option:`REPOPATH` argument path is treated as relative to :envvar:`PIPE_INPUT_ROOT` and the :option:`--output` path is relative to :envvar:`PIPE_OUTPUT_ROOT`.

These environment variables are optional.
Then they aren't set in your shell, the :option:`REPOPATH` and :option:`--output` arguments alone specify the paths or URIs to Butler data repositories.

See :ref:`command-line-task-envvar` for details.

.. _command-line-task-data-repo-using-reruns:

Using reruns to organize outputs in a single data repository
============================================================

.. TODO DM-11685 Add example strategies for organizing reruns.
.. E.g. https://github.com/lsst/pipe_base/pull/37/files#r135243612

An alternative way to organize output data repositories is with **reruns** (:option:`--rerun` command-line argument)
Reruns are a convention for repositories that are located relative to single root data repository.
If the root repository's URI is :file:`file://REPOPATH`, a rerun called ``my_rerun`` automatically has a full URI of:

.. code-block:: text

   file://REPOPATH/rerun/my_rerun

In practice, you don't need to know the full URIs of individual reruns.
Instead, you just need to know the URI of the root repository and the names of individual reruns.
This makes reruns especially convenient in practice.

How to create a rerun
---------------------

To use input data from a :file:`DATA` Butler repository and write outputs to a rerun called ``A``, set a command-line task's :option:`REPOPATH` and :option:`--rerun` like this:

.. code-block:: bash

   task1.py DATA --rerun A ...

.. tip::

   Once you've created an output rerun with one command-line task you can re-use it as the output repository for subsequent command-line task runs (see :ref:`command-line-task-data-repo-using-rerun-reuse`)

   Alternatively, you can chain new reruns together with each processing step (see the next section).

   For perspective on when to create a new rerun, or reuse an existing one, see :ref:`command-line-task-data-repo-using-rerun-strategy`.

.. _command-line-task-data-repo-using-rerun-chaining:

How to use one rerun as input to another (chaining)
---------------------------------------------------

To use data written to rerun ``A`` as inputs but have results written to a new rerun ``B``, use the :option:`--rerun` argument's ``input:output`` syntax, like this:

.. code-block:: bash

   task2.py DATA --rerun A:B ...

This syntax automatically *chains* rerun ``B`` to rerun ``A``, just like Butler repository chaining in general (see :ref:`command-line-task-data-repo-using-output-chaining`).
For example if rerun ``B`` is later used as an **input** rerun, it will provide access to datasets in rerun ``B``, rerun ``A``, and the root repository :file:`DATA` itself.

.. _command-line-task-data-repo-using-rerun-reuse:

How to write outputs to an existing rerun
-----------------------------------------

Tasks can write to an existing rerun.
For example, if rerun ``B`` was already created you can write additional outputs to it:

.. code-block:: bash

   task3.py DATA --rerun B ...

Because reruns are chained, the Butler will start looking for datasets in this rerun ``B``, then in the chained ``A`` rerun, all the way to the root data repository (:file:`DATA`).
This chaining is transparent to you.
You *don't* need to know which repository in the chain a given input dataset comes from.
All you need to know is the root data repository and the terminal rerun's name.

When reusing a rerun for multiple runs of the *same* command-line task, be aware of configuration consistency checks.
See :doc:`command-line-task-prov-howto` for more information.

.. _command-line-task-data-repo-using-rerun-strategy:

When to create a new rerun
--------------------------

.. TODO DM-11685 Add opinionated suggestions on to organize reruns

When using multiple command-line tasks to process data, you have the option of re-using the same rerun or creating a new chained rerun for each successive task.
How you use reruns is up to you.

Reruns are useful for creating processing checkpoints (hence their name).
You can run the same task with different configurations, writing the output of each to a different rerun.
By analyzing and comparing equivalent datasets in each rerun, you can make informed decisions about task configuration.

Without using separate reruns, tasks will report an error if the same task is processing data with different configurations than before.
These checks are in place to ensure that the provenance of data processing is traceable.
See :doc:`command-line-task-prov-howto` for more information.
