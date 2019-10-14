.. _command-line-task-parallel-howto:

###########################################
Parallel processing with command-line tasks
###########################################

:ref:`Command-line tasks <using-command-line-tasks>` can operate in parallel.
Generally if a command-line task is run against several :ref:`data IDs <command-line-task-dataid-howto>`, each data ID can be processed in parallel.
Command-line tasks use two different categories of parallel processing: single-node parallel processing (:option:`-j`) and distributed processing (with the ``pipe_drivers`` and ``ctrl_pool`` packages).
This page describes how to use these parallel processing features.

.. _command-line-task-parallel-howto-multiprocessing:

How to enable single-node parallel processing
=============================================

Command-line tasks can process multiple data IDs in parallel across multiple CPUs of one machine if you provide a :option:`-j`/:option:`--processes` argument with a value greater than ``1``.
For example, to spread processing across up to eight CPUs:

.. code-block:: bash

   task.py REPOPATH --output output --id -j 8 ...

When operating with :option:`-j`, command-line tasks have a default per-process timeout of 720 hours.
You can change this timeout via the :option:`--timeout` command-line argument.

Log messages from each process are combined into the same console output stream.
To discern logging statements from each process use the :option:`--longlog` argument to enable a verbose logging format that includes the data ID being processed.
See :ref:`command-line-task-logging-howto-longlog` for more information.

.. note::

   Internally, :option:`-j`-type multiprocessing is implemented with the `multiprocessing` module in the Python standard library.

   If a task does not support :option:`-j` parallel processing it will report a non-fatal warning:

   .. code-block:: text

      This task does not support multiprocessing; using one process

   Specifying ``-j 1`` disables multiprocessing altogether (the default).

.. _command-line-task-parallel-howto-distributed:

How to enable distributed processing
====================================

Command-line tasks implemented in the ``pipe_drivers`` package can distribute processing across multiple nodes using MPI or batch submission to clusters (PBS or SLURM).
The ``--batchType`` argument controls which processing system is used.
``--batchType None`` disables multiprocessing with "driver" tasks.
See the ``pipe_drivers`` documentation for more information.

.. _command-line-task-parallel-howto-threading:

How to control threading while parallel processing
==================================================

LSST Science Pipelines uses low-level math libraries that support threading.
OpenBLAS_ and MKL_ default to using the same number of threads as CPUs present on the machine.
When parallel processing at a higher level (using :option:`-j`, for example), threading in these libraries can result in a **net decrease in performance** because of thread contention.

When running with :option:`-j` multiprocessing or via ``ctrl_pool``, command-line tasks check if low-level libraries are using their default threading behavior (same number of threads as CPUs).
If so, the task will automatically disable multi-threading in these libraries.
The command-line task also sends a warning when it does so:

.. code-block:: text

   WARNING: You are using OpenBLAS with multiple threads (16), but have not specified the number of threads using one of the OpenBLAS environment variables: OPENBLAS_NUM_THREADS, GOTO_NUM_THREADS, OMP_NUM_THREADS. This may indicate that you are unintentionally using multiple threads, which may cause problems. WE HAVE THEREFORE DISABLED OpenBLAS THREADING. If you know what you are doing and want threads enabled implicitly, set the environment variable LSST_ALLOW_IMPLICIT_THREADS.

The recommended resolution to this warning is to specifically limit the number of threads used by OpenBLAS_ and MKL_.
For example, in a :command:`bash` or similar shell set the ``OMP_NUM_THREADS`` environment variable:

.. code-block:: bash

   export OMP_NUM_THREADS=1

.. note::

   ``OMP_NUM_THREADS`` is recognized by both OpenBLAS_ and MKL_ so it is likely the only environment variable that needs to be set.

   Alternatively, specific environment variables used by the libraries are:

   - OpenBLAS: ``OPENBLAS_NUM_THREADS``, ``GOTO_NUM_THREADS``, and ``OMP_NUM_THREADS``.
   - MLK: ``MKL_NUM_THREADS``, ``MKL_DOMAIN_NUM_THREADS``, ``OMP_NUM_THREADS``.

If necessary, you may bypass a command-line task's control over threading in low-level math libraries by setting the ``LSST_ALLOW_IMPLICIT_THREADS`` environment variable.
For example, in a :command:`bash` shell:

.. code-block:: bash

   export LSST_ALLOW_IMPLICIT_THREADS=1

.. seealso::

   These functions in `lsst.base` implement threading detection and control:

   - `lsst.base.disableImplicitThreading`
   - `lsst.base.getNumThreads`
   - `lsst.base.setNumThreads`

.. _OpenBLAS: https://www.openblas.net
.. _MKL: https://software.intel.com/en-us/mkl
