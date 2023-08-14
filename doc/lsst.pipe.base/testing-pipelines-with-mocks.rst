.. py:currentmodule:: lsst.pipe.base.tests.mocks

.. _testing-pipelines-with-mocks:

############################
Testing pipelines with mocks
############################

The `lsst.pipe.base.tests.mocks` package provides a way to build and execute `.QuantumGraph` objects without actually running any real task code or relying on real data.
This is primarily for testing the middleware responsible for `.QuantumGraph` generation and execution, but it can also be used to check that the connections in a configured pipeline are consistent with each other and with any documented recommendations for how to run those steps (e.g., which dimensions can safely be constrained by user expressions).

The high-level entry point to this system is `mock_pipeline_graph` function, which returns a new pipeline graph in which each original task has been replaced by a configuration of `MockPipelineTask` whose connections are analogous to the original.
Passing the ``--mock`` option to ``pipetask qgraph`` or ``pipetask run`` will run this on the given pipeline when building the graph.
When a pipeline is mocked, all task labels and dataset types are transformed by the `get_mock_name` function (so these can live alongside their real counterparts in a single data repository), and the storage classes of all regular connections are replaced with instances of `MockStorageClass`.
The in-memory Python type for `MockStorageClass` is always `MockDataset`, which is always written to disk in JSON format, but conversions between mock storage classes are always defined analogously to the original storage classes they mock, and the `MockDataset` class records conversions (and component access and parameters) when they occur, allowing test code that runs later to load them and inspect exactly how the object was loaded and provided to the task when it was executed.

The `MockPipelineTask.runQuantum` method reads all input mocked datasets that correspond to a `MockStorageClass` and simulates reading any input datasets there were not mocked (via the `MockPipelineTaskConfig.unmocked_dataset_types` config option, or the `mock_task_defs` argument of the same name) by constructing a new `MockDataset` instance for them.
It then constructs and writes new `MockDataset` instances for each of its predicted outputs, storing copies of the input `MockDataset`\s within them.
`MockPipelineTaskConfig` and `mock_task_defs` also have options for causing quanta that match a data ID expression to raise an exception instead.
Dataset types produced by the execution framework - configs, logs, metadata, and package version information - are not mocked, but they are given names with the prefix added by `get_mock_name` by virtue of being constructed from a task label that has that prefix.

Importing the `lsst.pipe.base.tests.mocks` package causes the `~lsst.daf.butler.StorageClassFactory` and `~lsst.daf.butler.FormatterFactory` classes to be monkey-patched with special code that recognizes mock storage class names without being included in any butler configuration files.
This should not affect how any non-mock storage classes are handled, but it is still best to only import `lsst.pipe.base.tests.mocks` in code that is *definitely* using the mock system, even if that means putting the import at function scope instead of module scope.

The `ci_middleware <https://github.com/lsst/ci_middleware.git>`_ package is the primary place where this mocking library is used, and the home of its unit tests, but it has been designed to be usable in regular "real" data repositories as well.
