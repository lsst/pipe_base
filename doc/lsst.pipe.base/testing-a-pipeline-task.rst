.. py:currentmodule:: lsst.pipe.base.testUtils

.. _testing-a-pipeline-task:

#######################
Testing a pipeline task
#######################

This document describes how to write unit tests for a pipeline task (i.e., a subtask of `lsst.pipe.base.PipelineTask`).
It covers testing of functionality specific to `~lsst.pipe.base.PipelineTask` but not ordinary tasks, including:

* `~lsst.pipe.base.PipelineTaskConnections` classes
* logic for optional inputs and outputs
* custom implementations of `~lsst.pipe.base.PipelineTask.runQuantum`

This guide does not cover writing a `~lsst.pipe.base.PipelineTask` from scratch, nor testing of a task's core functionality (e.g., whether it processes images correctly).

The examples in this guide make heavy use of the test Butler framework described in :ref:`using-butler-in-tests`.

.. _testing-a-pipeline-task-overview:

Overview
========

The `lsst.pipe.base.testUtils` module provides tools for testing pipeline tasks.
The tools are provided as stand-alone functions rather than as a special class like `lsst.utils.tests.TestCase` to make them easier for developers to mix and match as needed for their specific needs.
Many of the tools provide no testing functionality directly, instead providing the infrastructure to run `~lsst.pipe.base.PipelineTask`-related code inside test environments.

Most tools require a real data repository to read task inputs from (and possibly write outputs to).
See :ref:`using-butler-in-tests` for one way to create a repository in test cases.

.. _testing-a-pipeline-task-runQuantum:

Testing runQuantum
==================

Many pipeline tasks override `lsst.pipe.base.PipelineTask.runQuantum` to handle unusual inputs or data types.
`~lsst.pipe.base.PipelineTask.runQuantum` may contain complex logic such as data ID manipulation, extra arguments to `~lsst.pipe.base.PipelineTask.run`, or default values.
This logic cannot be tested without calling `~lsst.pipe.base.PipelineTask.runQuantum`, but the input arguments are difficult to set up without knowledge of the ``daf_butler`` package.

The `lsst.pipe.base.testUtils.runTestQuantum` function wraps a call to the `~lsst.pipe.base.PipelineTask.runQuantum` method so that the user need only provide the task object, a `~lsst.daf.butler.Butler`, and a `lsst.daf.butler.Quantum` (which currently must be provided separately).
The `runTestQuantum` call can then be tested for particular behavior (e.g., raising exceptions, writing particular datasets, etc.)

`~lsst.daf.butler.Quantum` takes dataset references to the desired inputs and outputs.
The `~lsst.pipe.base.testUtils.refFromConnection` function provides a way to create these references using the information already provided to a pipeline task's `~lsst.pipe.base.PipelineTaskConnections` object.

.. code-block:: py
   :emphasize-lines: 20-31

   import lsst.daf.butler.tests as butlerTests
   from lsst.pipe.base import testUtils

   # A minimal Butler repo, see daf_butler documentation
   dimensions = {
       "instrument": ["notACam"],
       "visit": [101, 102],
       "detector": [42],
   }
   repo = butlerTests.makeTestRepo(tempDir, dimensions)
   butlerTests.addDatasetType(
       repo, "InputType", {"instrument", "visit", "detector"},
       "ExposureF")
   butlerTests.addDatasetType(
       repo, "OutputType", {"instrument", "visit", "detector"},
       "ExposureF")

   ...

   # Set up what we need
   dataId = {"instrument": "notACam", "visit": 101, "detector": 42}
   butler = butlerTests.makeTestCollection(repo)
   task = AwesomeTask()
   connections = task.config.ConnectionsClass(config=task.config)
   quantum = lsst.daf.butler.Quantum(taskClass=SuperTask)
   quantum.addPredictedInput(
       testUtils.refFromConnection(butler, connections.input, dataId))
   quantum.addOutput(
       testUtils.refFromConnection(butler, connections.output, dataId))
   with self.assertRaises(RuntimeError):
       testUtils.runTestQuantum(task, butler, quantum)
