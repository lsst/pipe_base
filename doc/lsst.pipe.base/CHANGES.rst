lsst-pipe-base v26.0.0 (2023-09-22)
===================================

New Features
------------

- Added system for obtaining data ID packer objects from the combination of an ``Instrument`` class and configuration. (`DM-31924 <https://jira.lsstcorp.org/browse/DM-31924>`_)
- Added a ``PipelineGraph`` class that represents a Pipeline with all configuration overrides applied as a graph. (`DM-33027 <https://jira.lsstcorp.org/browse/DM-33027>`_)
- Added new command ``butler transfer-from-graph`` to transfer results of execution with Quantum-backed butler. (`DM-33497 <https://jira.lsstcorp.org/browse/DM-33497>`_)
- ``buildExecutionButler`` method now supports input graph with all dataset references resolved. (`DM-37582 <https://jira.lsstcorp.org/browse/DM-37582>`_)
- Added convince methods to the Python api for Pipelines.
  These methods allow merging pipelines, adding labels to / removing labels from subsets, and finding subsets containing a specified label. (`DM-37655 <https://jira.lsstcorp.org/browse/DM-37655>`_)
- An ``Instrument`` can now specify the dataset type definition that it would like to use for raw data.
  This can be done by setting the ``raw_definition`` class property to a tuple of the dataset type name, the dimensions to use for this dataset type, and the storage class name. (`DM-37950 <https://jira.lsstcorp.org/browse/DM-37950>`_)
- Modified ``InMemoryDatasetHandle`` to allow it to be constructed with keyword arguments that will be converted to the relevant DataId. (`DM-38091 <https://jira.lsstcorp.org/browse/DM-38091>`_)
- Modified ``InMemoryDatasetHandle`` to allow it to be configured to always deep copy the Python object on ``get()``. (`DM-38694 <https://jira.lsstcorp.org/browse/DM-38694>`_)
- Revived bit-rotted support for "mocked" ``PipelineTask`` execution and moved it here (from ``ctrl_mpexec``). (`DM-38952 <https://jira.lsstcorp.org/browse/DM-38952>`_)
- Formalized support for modifying connections in ``PipelineTaskConnections.__init__`` implementations.

  Connections can now be added, removed, or replaced with normal attribute syntax.
  Removing entries from e.g. ``self.inputs`` in ``__init__`` still works for backwards compatibility, but deleting attributes is generally preferred.
  The task dimensions can also be replaced or modified in place in ``__init__``. (`DM-38953 <https://jira.lsstcorp.org/browse/DM-38953>`_)
- Added a method on ``PipelineTaskConfig`` objects named ``applyConfigOverrides``.
  This method is called by the system executing ``PipelineTask``\ s within a pipeline, and is passed the instrument and config overrides defined within the pipeline for that task. (`DM-39100 <https://jira.lsstcorp.org/browse/DM-39100>`_)
- Add ``Instrument.make_default_dimension_packer`` to restore simple access to the default data ID packer for an instrument. (`DM-39453 <https://jira.lsstcorp.org/browse/DM-39453>`_)
- The back-end to quantum graph loading has been optimized such that duplicate objects are not created in memory, but create shared references.
  This results in a large decrease in memory usage, and decrease in load times. (`DM-39582 <https://jira.lsstcorp.org/browse/DM-39582>`_)
- * A new class ``ExecutionResources`` has been created to record the number of cores and memory that has been allocated for the execution of a quantum.
  * ``QuantumContext`` (newly renamed from ``ButlerQuantumContext``) now has a ``resources`` property that can be queried by a task in ``runQuantum``.
    This can be used to tell the task that it can use multiple cores or possibly should make a more efficient use of the available memory resources. (`DM-39661 <https://jira.lsstcorp.org/browse/DM-39661>`_)
- Made it possible to deprecate ``PipelineTask`` connections. (`DM-39902 <https://jira.lsstcorp.org/browse/DM-39902>`_)
- Parameters defined in a Pipeline can now be used within a config Python block as well as within config files loaded by a Pipeline. (`DM-40198 <https://jira.lsstcorp.org/browse/DM-40198>`_)
- When looking up prerequisite inputs with skypix data IDs (e.g., reference catalogs) for a quantum whose data ID is not spatial, use the union of the spatial regions of the input and output datasets as a constraint.

  This keeps global sequence-point tasks from being given all such datasets in the input collections. (`DM-40243 <https://jira.lsstcorp.org/browse/DM-40243>`_)
- Added support for init-input/output datasets in PipelineTask mocking. (`DM-40381 <https://jira.lsstcorp.org/browse/DM-40381>`_)


API Changes
-----------

- Several changes to API to add support for ``QuantumBackedButler``:

  * Added a ``globalInitOutputRefs`` method to the ``QuantumGraph`` class which returns global per-graph output dataset references (e.g. for "packages" dataset type).
  * ``ButlerQuantumContext`` can work with either ``Butler`` or ``LimitedButler``.
    Its ``__init__`` method should not be used directly, instead one of the two new class methods should be used - ``from_full`` or ``from_limited``.
  * The ``ButlerQuantumContext.registry`` attribute was removed, and ``ButlerQuantumContext.dimensions`` has been added to hold the ``DimensionUniverse``.
  * The abstract method ``TaskFactory.makeTask`` was updated and simplified to accept ``TaskDef`` and ``LimitedButler``. (`DM-33497 <https://jira.lsstcorp.org/browse/DM-33497>`_)
- * ``ButlerQuantumContext`` was updated to only need a ``LimitedButler``.
  * Factory methods ``from_full`` and ``from_limited`` were dropped, a constructor accepting a ``LimitedButler`` instance is now used to make instances. (`DM-37704 <https://jira.lsstcorp.org/browse/DM-37704>`_)
- - Added method ``QuantumGraph.updateRun``.
    This new method updates run collection name and dataset IDs for all output and intermediate datasets in a graph, allowing the graph to be reused.
  - ``GraphBuilder.makeGraph`` method dropped the ``resolveRefs`` argument, the builder now always makes resolved references.
    The ``run`` argument is now required to be non-empty string. (`DM-38780 <https://jira.lsstcorp.org/browse/DM-38780>`_)


Bug Fixes
---------

- Fixed a bug that led to valid storage class conversions being rejected when using execution butler. (`DM-38614 <https://jira.lsstcorp.org/browse/DM-38614>`_)
- Fixed a bug related to checking component datasets in execution butler creation, introduced in `DM-38614 <https://jira.lsstcorp.org/browse/DM-38614>`_. (`DM-38888 <https://jira.lsstcorp.org/browse/DM-38888>`_)
- Fixed handling of storage classes in ``QuantumGraph`` generation.

  This could lead to a failure downstream in execution butler creation, and would likely have led to problems with Quantum-Backed Butler usage as well. (`DM-39198 <https://jira.lsstcorp.org/browse/DM-39198>`_)
- Fixed a bug in ``QuantumGraph`` generation that could result in datasets from ``skip_existing_in`` collections being used as outputs, and another that prevented ``QuantumGraph`` generation when a ``skip_existing_in`` collection has some outputs from a failed quantum. (`DM-39672 <https://jira.lsstcorp.org/browse/DM-39672>`_)
- Fixed a bug in quantum graph builder which resulted in missing datastore records for calibration datasets.
  This bug was causing failures for ``pipetask`` execution with quantum-backed butler. (`DM-40254 <https://jira.lsstcorp.org/browse/DM-40254>`_)
- Ensured QuantumGraphs are built with datastore records for init-input datasets that might have been produced by another task in the pipeline, but will not be because all quanta for that task were skipped due to existing outputs. (`DM-40381 <https://jira.lsstcorp.org/browse/DM-40381>`_)
- ``QuantumGraph.updateRun()`` method was fixed to update dataset ID in references which have their run collection changed. (`DM-40392 <https://jira.lsstcorp.org/browse/DM-40392>`_)


Other Changes and Additions
---------------------------

- Modified the calling signature for the ``Task`` constructor such that only the ``config`` parameter can be positional.
  All other parameters must now be keyword parameters. (`DM-15325 <https://jira.lsstcorp.org/browse/DM-15325>`_)
- The ``Struct`` class is now a subclass of ``SimpleNamespace``. (`DM-36649 <https://jira.lsstcorp.org/browse/DM-36649>`_)
- The ``DuplicateOutputError`` logger now produces a more helpful error message. (`DM-38234 <https://jira.lsstcorp.org/browse/DM-38234>`_)
- * Execution butler creation has been changed to use the ``DatasetRefs`` from the graph rather than creating new registry entries from the dataIDs.
    This is possible now that the graph is always created with resolved refs and ensures that provenance is consistent between the graph and the outputs.
  * This change to execution butler required that ``ButlerQuantumContext.put()`` no longer unresolves the graph ``DatasetRef`` (otherwise there would be a dataset ID mismatch).
    This results in the dataset always using the output run defined in the graph even if the Butler was created with a different default run. (`DM-38779 <https://jira.lsstcorp.org/browse/DM-38779>`_)
- Stopped sorting Pipeline elements on read.

  Ordering specified in pipeline files is now preserved instead. (`DM-38953 <https://jira.lsstcorp.org/browse/DM-38953>`_)
- Loosened documentation of ``QuantumGraph.inputQuanta`` and ``outputQuanta``.
  They are not guaranteed to be (and currently are not) lists, so the new documentation describes them as iterables.

  Documented ``universe`` constructor parameter to ``QuantumGraph``.

  Brought ``QuantumGraph`` property docs in line with DM standards.


An API Removal or Deprecation
-----------------------------

- * Removed deprecated kwargs parameter from in-memory equivalent dataset handle.
  * Removed deprecated ``pipe_base`` ``timer`` module (it was moved to ``utils``).
  * Removed the warning from deprecated ``PipelineIR._read_imports`` and replaced with a raise.
  * Removed the warning from deprecated ``Pipeline._parse_file_specifier`` and replaced with a raise.
  * Removed deprecated methods from ``TaskMetadata``. (`DM-37534 <https://jira.lsstcorp.org/browse/DM-37534>`_)
- - The ``PipelineTaskConfig.saveMetadata`` field is now deprecated and will be removed after v26.
    Its value is ignored and task metadata is always saved.
  - The ``ResourceConfig`` class has been removed; it was never used. (`DM-39377 <https://jira.lsstcorp.org/browse/DM-39377>`_)
- Deprecated the ``reconstituteDimensions`` argument from ``QuantumNode.from_simple`` (`DM-39582 <https://jira.lsstcorp.org/browse/DM-39582>`_)
- ``ButlerQuantumContext`` has been renamed to ``QuantumContext``.
  This reflects the additional functionality it now has. (`DM-39661 <https://jira.lsstcorp.org/browse/DM-39661>`_)
- Removed support for reading quantum graphs in pickle format. (`DM-40032 <https://jira.lsstcorp.org/browse/DM-40032>`_)


lsst-pipe-base v25.0.0 (2023-02-28)
===================================

This is the first release without any support for the Generation 2 middleware.

New Features
------------

- Added ``PipelineStepTester`` class, to enable testing that multi-step pipelines are able to run without error. (`DM-33779 <https://jira.lsstcorp.org/browse/DM-33779>`_)
- ``QuantumGraph`` now saves the ``DimensionUniverse`` it was created with when it is persisted. This removes the need
  to explicitly pass the ``DimensionUniverse`` when loading a saved graph. (`DM-35082 <https://jira.lsstcorp.org/browse/DM-35082>`_)
- * Added support for transferring files into execution butler. (`DM-35494 <https://jira.lsstcorp.org/browse/DM-35494>`_)
- A new class ``InMemoryDatasetHandle`` is now available.
  This class provides a variant of ``lsst.daf.butler.DeferredDatasetHandle`` that does not require a butler and lets you store your in-memory objects in something that looks like one and so can be passed to ``Task.run()`` methods that expect to be able to do deferred loading. (`DM-35741 <https://jira.lsstcorp.org/browse/DM-35741>`_)
- * Add unit test to cover the new ``getNumberOfQuantaForTask`` method.
  * Add graph interface, ``getNumberOfQuantaForTask``, to determine number of quanta associated with a given ``taskDef``.
  * Modifications to ``getQuantaForTask`` to support showing added additional quanta information in the logger. (`DM-36145 <https://jira.lsstcorp.org/browse/DM-36145>`_)
- Allow ``PipelineTasks`` to provide defaults for the ``--dataset-query-constraints`` option for the ``pipetask`` tool. (`DM-37786 <https://jira.lsstcorp.org/browse/DM-37786>`_)


API Changes
-----------

- ``ButlerQuantumContext.get`` method can accept `None` as a reference and returns `None` as a result object. (`DM-35752 <https://jira.lsstcorp.org/browse/DM-35752>`_)
- ``GraphBuilder.makeGraph`` method adds ``bind`` parameter for bind values to use with the user expression. (`DM-36487 <https://jira.lsstcorp.org/browse/DM-36487>`_)
- ``InMemoryDatasetHandle`` now supports storage class conversion on ``get()``. (`DM-4551 <https://jira.lsstcorp.org/browse/DM-4551>`_)


Bug Fixes
---------

- ``lsst.pipe.base.testUtils.makeQuantum`` no longer crashes if given a connection that is set to a dataset component. (`DM-35721 <https://jira.lsstcorp.org/browse/DM-35721>`_)
- Ensure ``QuantumGraphs`` are given a ``DimensionUniverse`` at construction.

  This fixes a mostly-spurious dimension universe inconsistency warning when reading QuantumGraphs, introduced on `DM-35082 <https://jira.lsstcorp.org/browse/DM-35082>`_. (`DM-35681 <https://jira.lsstcorp.org/browse/DM-35681>`_)
- Fixed an error message that says that repository state has changed during ``QuantumGraph`` generation when init input datasets are just missing. (`DM-37786 <https://jira.lsstcorp.org/browse/DM-37786>`_)


Other Changes and Additions
---------------------------

- Make diagnostic logging for empty ``QuantumGraphs`` harder to ignore.

  Log messages have been upgraded from ``WARNING`` to ``FATAL``, and an exception traceback that tends to hide them has been removed. (`DM-36360 <https://jira.lsstcorp.org/browse/DM-36360>`_)


An API Removal or Deprecation
-----------------------------

- Removed the ``Task.getSchemaCatalogs`` and ``Task.getAllSchemaCatalogs`` APIs.
  These were used by ``CmdLineTask`` but are no longer used in the current middleware. (`DM-2850 <https://jira.lsstcorp.org/browse/DM-2850>`_)
- Relocated ``lsst.pipe.base.cmdLineTask.profile`` to ``lsst.utils.timer.profile``.
  This was relocated as part of the Gen2 removal that includes the removal of ``CmdLineTask``. (`DM-35697 <https://jira.lsstcorp.org/browse/DM-35697>`_)
- * ``ArgumentParser``, ``CmdLineTask``, and ``TaskRunner`` classes have been removed and associated gen2 documentation.
  * The ``PipelineIR.from_file()`` method has been removed.
  * The ``getTaskLogger`` function has been removed. (`DM-35917 <https://jira.lsstcorp.org/browse/DM-35917>`_)
- Replaced ``CmdLineTask`` and ``ArgumentParser`` with non-functioning stubs, disabling all Gen2 functionality.
  A deprecation message is now issued but the classes do nothing. (`DM-35675 <https://jira.lsstcorp.org/browse/DM-35675>`_)


lsst-pipe-base v24.0.0 (2022-08-26)
===================================

New Features
------------

- Add the ability for user control over dataset constraints in `~lsst.pipe.base.QuantumGraph` creation. (`DM-31769 <https://jira.lsstcorp.org/browse/DM-31769>`_)
- Builds using ``setuptools`` now calculate versions from the Git repository, including the use of alpha releases for those associated with weekly tags. (`DM-32408 <https://jira.lsstcorp.org/browse/DM-32408>`_)
- Improve diagnostics for empty `~lsst.pipe.base.QuantumGraph`. (`DM-32459 <https://jira.lsstcorp.org/browse/DM-32459>`_)
- A new class has been written for handling `~lsst.pipe.base.Task` metadata.
  `lsst.pipe.base.TaskMetadata` will in future become the default metadata class for `~lsst.pipe.base.Task`, replacing ``lsst.daf.base.PropertySet``.
  The new metadata class is not yet enabled by default. (`DM-32682 <https://jira.lsstcorp.org/browse/DM-32682>`_)
- * Add ``TaskMetadata.to_dict()`` method (this is now used by the ``lsst.daf.base.PropertySet.from_mapping()`` method and triggered by the Butler if type conversion is needed).
  * Use the existing metadata storage class definition if one already exists in a repository.
  * Switch `~lsst.pipe.base.Task` to use `~lsst.pipe.base.TaskMetadata` for storing task metadata, rather than ``lsst.daf.base.PropertySet``.
    This removes a C++ dependency from the middleware. (`DM-33155 <https://jira.lsstcorp.org/browse/DM-33155>`_)
- * Added `lsst.pipe.base.Instrument` to represent an instrument in Butler registry.
  * Added ``butler register-instrument`` command (relocated from ``obs_base``).
  * Added a formatter for ``pex_config`` `~lsst.pex.config.Config` objects. (`DM-34105 <https://jira.lsstcorp.org/browse/DM-34105>`_)


Bug Fixes
---------

- Fixed a bug where imported pipeline parameters were taking preference over "top-level" preferences (`DM-32080 <https://jira.lsstcorp.org/browse/DM-32080>`_)


Other Changes and Additions
---------------------------

- If a `~lsst.pipe.base.PipelineTask` has connections that have a different storage class for a dataset type than the one defined in registry, this will now be allowed if the  storage classes are compatible.
  The `~lsst.pipe.base.Task` ``run()`` method will be given the Python type it expects and can return the Python type it has declared it returns.
  The Butler will do the type conversion automatically. (`DM-33303 <https://jira.lsstcorp.org/browse/DM-33303>`_)
- Topological sorting of pipelines on write has been disabled; the order in which the pipeline tasks were read/added is preserved instead.
  This makes it unnecessary to import all tasks referenced by the pipeline in order to write it. (`DM-34155 <https://jira.lsstcorp.org/browse/DM-34155>`_)


lsst-pipe-base v23.0.1 (2022-02-02)
===================================

Miscellaneous Changes of Minor Interest
---------------------------------------

- Execution butler creation time has been reduced significantly by avoiding unnecessary checks for existence of files in the datastore. (`DM-33345 <https://jira.lsstcorp.org/browse/DM-33345>`_)


lsst-pipe-base v23.0.0 (2021-12-10)
===================================

New Features
------------

- Added a new facility for creating "lightweight" (execution) butlers that pre-fills a local SQLite registry. This can allow a pipeline to be executed without talking to the main registry. (`DM-28646 <https://jira.lsstcorp.org/browse/DM-28646>`_)
- Allow ``PipelineTasks`` inputs and outputs to be optional under certain conditions, so tasks with no work to do can be skipped without blocking downstream tasks from running. (`DM-30649 <https://jira.lsstcorp.org/browse/DM-30649>`_)
- Log diagnostic information when QuantumGraphs are empty because the initial query yielded no results.

  At present, these diagnostics only cover missing input datasets, which is a common way to get an empty QuantumGraph, but not the only way. (`DM-31583 <https://jira.lsstcorp.org/browse/DM-31583>`_)


API Changes
-----------

- ``GraphBuilder`` constructor boolean argument ``skipExisting`` is replaced with
  ``skipExistingIn`` which accepts collections to check for existing quantum
  outputs. (`DM-27492 <https://jira.lsstcorp.org/browse/DM-27492>`_)


Other Changes and Additions
---------------------------

- The logger associated with ``Task`` is now derived from a Python `logging.Logger` and not `lsst.log.Log`.
  This logger includes a new ``verbose()`` log method as an intermediate between ``INFO`` and ``DEBUG``. (`DM-30301 <https://jira.lsstcorp.org/browse/DM-30301>`_)
- Added metadata to QuantumGraphs. This changed the on disk save format, but is backwards compatible with graphs saved with previous versions of the QuantumGraph code. (`DM-30702 <https://jira.lsstcorp.org/browse/DM-30702>`_)
- All Doxygen documentation has been removed and replaced by Sphinx. (`DM-23330 <https://jira.lsstcorp.org/browse/DM-23330>`_)
- New documentation on writing pipelines has been added. (`DM-27416 <https://jira.lsstcorp.org/browse/DM-27416>`_)


lsst-pipe-base v22.0 (2021-04-01)
=================================

New Features
------------

* Add ways to test a PipelineTask's init inputs/outputs [DM-23156]
* Pipelines can now support URIs [DM-28036]
* Graph files can now be loaded and saved via URIs [DM-27682]
* A new format for saving graphs has been developed (with a ``.qgraph`` extension). This format supports the ability to read a subset of a graph from an object store. [DM-27784]
* Graph building with a pipeline that specifies an instrument no longer needs an explicit instrument to be given. [DM-27985]
* A ``parameters`` section has been added to pipeline definitions. [DM-27633]
