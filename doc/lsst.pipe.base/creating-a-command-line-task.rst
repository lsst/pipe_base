.. TODO DM-11674 This topic should be edited into the modernized topic-based documentation style.
.. See comments: https://github.com/lsst/pipe_base/pull/37/files#diff-afb70dbd7b37e15ec4ff6c41991730e8

.. _creating-a-command-line-task:

############################
Creating a command-line task
############################

This document describes how to write a command-line task, which is the LSST version of a complete data processing pipeline.
To create a command-line task you will benefit from some background:

- Read :doc:`task-framework-overview` to get a basic idea of what tasks are and what classes are used to implement them.
- Read :doc:`creating-a-task`.
  A command-line task is an enhanced version of a regular task.
  Thus all the considerations for writing a normal task also apply to writing a command-line task, and these will not be repeated in this manual.
- Acquire a basic understanding of data repositories and how to use the butler to read and write data (to be written; for now read existing tasks to see how it is done).

.. _creating-a-command-line-task-intro:

Introduction
============

A command-line task is an enhanced version of a regular task (see :ref:`creating-a-task`).
Regular tasks are only intended to be used as relatively self-contained stages in data processing pipelines, whereas command-line tasks can also be used as complete pipelines.
As such, command-line tasks include :ref:`run scripts <creating-a-command-line-task-run-script>` that run them as pipelines.

Command-line tasks have the following key attributes, in addition to the attributes for regular tasks:

- They are subclasses of `lsst.pipe.base.CmdLineTask`, whereas regular tasks are subclasses of `lsst.pipe.base.Task`.
- They have an associated :ref:`run script <creating-a-command-line-task-run-script>` to run them from the command-line as pipelines (this is common, but not required, for regular tasks).
- They have a ``run`` method which performs the full pipeline data processing.
- By default the ``run`` method takes exactly one argument: a data reference for the item of data to be processed.
  Variations are possible, but require that you provide a :ref:`custom argument parser <creating-a-command-line-task-custom-argparse>` and often a :ref:`custom task runner <creating-a-command-line-task-custom-task-runner>`.
- When run from the command line, most command-line tasks save the configuration used and the metadata generated.
  See :ref:`creating-a-command-line-task-persisting-config-and-metadata` for more information.
- They have an additional :ref:`class variable <creating-a-task-class-variables>`, ``RunnerClass``, that specifies a "task runner" for the task.
  The task runner takes a parsed command and runs the task.
  The default task runner will work for any script whose `run` method accepts a single data reference, such as ``ExampleCmdLineTask``.
  If your task's `run` method needs something else then you will have to provide a :ref:`custom task runner <creating-a-command-line-task-custom-task-runner>`.
- They have an additional :ref:`class variable <creating-a-task-class-variables>` ``canMultiprocess``, which defaults to `True`.
  If your task runner cannot run your task with multiprocessing then set it `False`.
  Note: multiprocessing only affects how the task runner calls the top-level task; thus it is ignored when a task is used as a subtask.

.. _creating-a-command-line-task-run-script:

Run Script
==========

A command-line task can be run as a pipeline via run script.
This is usually a trivial script which merely calls the task's ``parseAndRun`` method. ``parseAndRun`` does the following:

- Parses the command line, which includes determining the :ref:`configuration <creating-a-task-configuration>` for the task and which data items to process.
- Constructs the task.
- Calls the task's ``run`` method once for each data item to process.

``examples/exampleCmdLineTask.py``, the runner script for ``ExampleCmdLineTask``, is typical:

.. code-block:: python

   #!/usr/bin/env python2
   from lsst.pipe.tasks.exampleCmdLineTask import ExampleCmdLineTask
   ExampleCmdLineTask.parseAndRun()

For most command-line tasks you should put the run script into your package's :file:`bin/` directory, so that it is on your ``$PATH`` when you setup your package with eups.
We did not want the run script for ``ExampleCmdLineTask`` to be quite so accessible, so we placed it in the :file:`examples/` directory instead of :file:`bin/`.

Remember to make your run script executable using :command:`chmod +x`.

.. _creating-a-command-line-task-data-io:

Reading and Writing Data
========================

The :ref:`run method <creating-a-command-line-task-intro>` typically receives a single data reference, as mentioned above.
It read and writes data using this data reference (or the underlying butler, if necessary).

.. _creating-a-command-line-task-dataset-types:

Adding Dataset Types
--------------------

Every time you write a task that writes a new kind of data (a new "dataset type") you must tell the butler about it.
Similarly, if you write a new task for which you want to save configuration and metadata (which is the case for most tasks that process data), you have to tell the butler about it.

To add a dataset, edit the mapper configuration file for each observatory package on whose data the task can be run.
If the task is of general interest (wanted for most or all observatory packages) then this process of updating all the mapper configuration files can be time consuming.

There are plans to change how mappers are configured.
But as of this writing, mapper configuration files are contained in the policy directory of the observatory package.
For instance the configuration for the lsstSim mapper is defined in ``obs_lsstSim/policy/LsstSimMapper.paf``.

.. _creating-a-command-line-task-persisting-config-and-metadata:

Persisting Config and Metadata
==============================

Normally when you run a task you want the configuration for the task and the metadata generated by the task to be saved to the data repository.
By default, this is done automatically, using dataset types:

- ``_DefaultName_config`` for the configuration
- ``_DefaultName_metadata`` for the metadata

where ``_DefaultName`` is replaced with the value of the task's ``_DefaultName``, see :ref:`class variable <creating-a-task-class-variables>`.

Whether you use these default dataset types or :ref:`customize the dataset types <creating-a-command-line-task-custom-config-and-metadata-types>`, you will have to :ref:`add dataset types <creating-a-command-line-task-dataset-types>` for the configuration and metadata.

.. _creating-a-command-line-task-custom-config-and-metadata-types:

Customizing Config and Metadata Dataset Types
---------------------------------------------

Occasionally the default dataset types for configuration and metadata are not sufficient.
For instance in the case of the `pipe.tasks.makeSkyMap.MakeSkyMapTask` and various co-addition tasks, the co-add type must be part of the config and metadata dataset type name.
To customize the dataset type of a task's config or metadata, define task methods ``_getConfigName`` and ``_getMetadataName`` to return the desired names.

.. _creating-a-command-line-task-prevent-saving-config:

Prevent Saving Config and Metadata
----------------------------------

For some tasks you may wish to not save config and metadata at all.
This is appropriate for tasks that simply report information without saving data.
To disable saving configuration and metadata, define task methods ``_getConfigName`` and ``_getMetadataName`` methods to return `None`.

.. _creating-a-command-line-task-custom-argparse:

Custom Argument Parser
======================

The default `lsst.pipe.base.argumentParser.ArgumentParser`-type returned by `CmdLineTask._makeArgumentParser` assumes that your task's :ref:`run method <creating-a-task-class-run-method>` processes raw or calibrated images.
If this is not the case you can easily provide a modified argument parser.

Typically this consists of constructing an instance of `lsst.pipe.base.ArgumentParser` and then adding some ID arguments to it using `~lsst.pipe.base.argumentParser.ArgumentParser.add_id_argument`.
This is shown in several examples below.
Please resist the urge to add other kinds of arguments to the argument parser unless truly needed.
One strength of our tasks is how similar they are to each other.
Learning one set of arguments suffices to use many tasks.

.. warning::

   If your task requires a custom argument parser to do more than just change the type of the single data reference, then it also require a :ref:`custom task runner <creating-a-command-line-task-custom-task-runner>` as well.

Here are some examples:

- A task's ``run`` method requires a data reference of some kind other than a raw or calibrated image.
  This is a common case, and easily solved.
  For example the ``processCoadd.ProcessCoaddTask`` processes co-adds, which are specified by sky map patch.
  Here is ``ProcessCoaddTask._makeArgumentParser``:

  .. code-block:: text

     @classmethod
     def _makeArgumentParser(cls):
         """Create an argument parser
         """
         parser = pipeBase.ArgumentParser(name=cls._DefaultName)
         parser.add_id_argument("--id", "deepCoadd", help="data ID, e.g. --id tract=12345 patch=1,2",
                                ContainerClass=CoaddDataIdContainer)
         parser.add_id_argument("--selectId", "calexp", help="data ID, e.g. --selectId visit=6789 ccd=0..9",
                                ContainerClass=SelectDataIdContainer)
         return parser

  - The first argument to `~lsst.pipe.base.argumentParser.ArgumentParser` is the name of the ID argument.
  - The second argument is a dataset type, which specifies the keys that are used with this ID argument.
    The keys associated with a particular dataset type are specified in the mapper configuration file for the observatory package and camera in question, and thus may vary from camera to camera.
    In practice, the keys for ``raw`` and ``calexp`` dataset types usually do vary from camera to camera, but the keys for coadds do not.
    However, this is not a fixed rule.
    For most observatory packages ``deepCoadd`` is one of two coadd dataset types, and the other, ``goodSeeingCoadd``, would work just as well for this argument.
  - A custom ``ContainerClass`` (for example, `lsst.coadd.utils.coaddDataIdContainer.CoaddDataIdContainer`) is provided to support iterating over missing keys (e.g. if you provide a tract but not a patch then the task will iterate over all available patches for that tract).
    This happens automatically for ``raw`` and ``calexp`` dataset types, but not most other dataset types.
    Examine the code in `~lsst.coadd.utils.coaddDataIdContainer.CoaddDataIdContainer` to see how it works.

- A task's ``run`` method requires more than one kind of data reference.
  An example is co-addition, which requires the user to specify the co-add as a sky map patch, and optionally allows the user to specify a list of exposures to co-add.
  `CoaddBaseTask._makeArgumentParser` is a straightforward example of specifying two data IDs arguments: one for the sky map patch, and an optional ID argument for which exposures to co-add:

  .. code-block:: python

     @classmethod
     def _makeArgumentParser(cls):
         """Create an argument parser
         """
         parser = pipeBase.ArgumentParser(name=cls._DefaultName)
         parser.add_id_argument("--id", "deepCoadd", help="data ID, e.g. --id tract=12345 patch=1,2",
                                ContainerClass=CoaddDataIdContainer)
         parser.add_id_argument("--selectId", "calexp", help="data ID, e.g. --selectId visit=6789 ccd=0..9",
                                ContainerClass=SelectDataIdContainer)
         return parser

  In this case the custom container class `~lsst.pipe.tasks.coaddBase.SelectDataIdContainer` adds additional information for the task, to save processing time.

- A task's `run` method requires no data references at all.
  An example is ``makeSkyMap.MakeSkyMapTask``, which makes a sky map for a set of co-adds.
  ``makeSkyMap.MakeSkyMapTask._makeArgumentParser`` is trivial:

  .. code-block:: python

     @classmethod
     def _makeArgumentParser(cls):
         """Create an argument parser
         No identifiers are added because none are used.
         """
         return pipeBase.ArgumentParser(name=cls._DefaultName)

.. _creating-a-command-line-task-custom-task-runner:

Custom Task Runner
==================

The standard task runner is `lsst.pipe.base.TaskRunner`.
It assumes that your task's ``run`` method wants a single data reference and nothing else.
If that is not the case then you will have to provide a custom task runner for your task.
This involves writing a subclass of `lsst.pipe.base.TaskRunner` and specifying it in your task using the ``RunnerClass`` :ref:`class variable <creating-a-task-class-variables>`.

Here are some situations where a custom task runner is required:

- The task's ``run`` method requires extra arguments.
  An example is co-addition, which optionally accepts a list of images to co-add.
  The custom task runner is ``coaddBase.CoaddTaskRunner`` and is pleasantly simple:

  .. code-block:: python

     class CoaddTaskRunner(pipeBase.TaskRunner):
         @staticmethod
         def getTargetList(parsedCmd, **kwargs):
             return pipeBase.TaskRunner.getTargetList(parsedCmd, selectDataList=parsedCmd.selectId.dataList,
                                                      **kwargs)

- The task requires no data references, just a butler.
  An example is ``makeSkyMap.MakeSkyMapTask``, which makes a ``skymap.SkyMap`` for a set of co-adds.
  It uses the custom task runner ``makeSkyMap.MakeSkyMapRunner``, which is more complicated than the previous example because the entire ``__call__`` method must be overridden:

  .. code-block:: python

     class MakeSkyMapRunner(pipeBase.TaskRunner):
         """Only need a single butler instance to run on."""
         @staticmethod
         def getTargetList(parsedCmd):
             return [parsedCmd.butler]
         def __call__(self, butler):
             task = self.TaskClass(config=self.config, log=self.log)
             if self.doRaise:
                 results = task.run(butler)
             else:
                 try:
                     results = task.run(butler)
                 except Exception as e:
                     task.log.fatal("Failed: %s" % e)
                     if not isinstance(e, pipeBase.TaskError):
                         traceback.print_exc(file=sys.stderr)
             task.writeMetadata(butler)
             if self.doReturnResults:
                 return results
