.. TODO DM-11694 This topic should be edited into the modernized topic-based documentation style.
.. See comments: https://github.com/lsst/pipe_base/pull/37/files#diff-d12322c94d97592a5afef71fa7dd00c9

.. _task-framework-overview:

##############################
Overview of the task framework
##############################

:doc:`lsst.pipe.base <index>` provides a data processing pipeline infrastructure.
Data processing is performed by **tasks**, which are instances of `lsst.pipe.base.Task` or `lsst.pipe.base.PipelineTask`.
Tasks perform a wide range of data processing operations, from basic operations such as assembling raw images into CCD images (trimming overscan), fitting a WCS or detecting sources on an image, to complex combinations of such operations.

**Tasks** are hierarchical.
Each task may may call other tasks to perform some of its data processing; we say that a **parent task** calls a **subtask**.
The highest-level task is called the **top-level task**.
To call a subtask, the parent task constructs the subtask and then calls methods on it.
Thus data transfer between tasks is simply a matter of passing the data as arguments to function calls.

Each task is configured using the `lsst.pex.config` module, using a task-specific subclass of `lsst.pex.config.Config`.
The task's configuration includes all subtasks that the task may call.
As a result, it is easy to replace (or "retarget") one subtask with another.
A common use for this is to provide a camera-specific variant of a particular task, e.g. use one version for SDSS imager data and another version for Subaru Hyper Suprime-Cam data).

Tasks may process multiple items of data in parallel, using the ``ctrl_mpexec`` package and its ``pipetask`` command.

Most tasks have a ``run`` method that performs the primary data processing.
Each task's `run` method should return a `~lsst.pipe.base.Struct`.
This allows named access to returned data, which provides safer evolution than relying on the order of returned values.
All task methods that return more than one or two items of data should return the data in a `~lsst.pipe.base.Struct`.

Many tasks are found in the ``pipe_tasks`` package, especially tasks that use many different packages and don't seem to belong in any one of them.
Tasks that are associated with a particular package should be in that package; for example the instrument signature removal task ``ip.isr.isrTask.IsrTask`` is in the ``ip_isr`` package.

`pipe_base` is written purely in Python. The most important contents are:

- `~lsst.pipe.base.PipelineTask`: base class for pipeline tasks that can be run from pipelines.
- `~lsst.pipe.base.Task`: base class for subtasks that are not meant to be run from the
  command line.
- `~lsst.pipe.base.Struct`: object returned by the run method of a task.
