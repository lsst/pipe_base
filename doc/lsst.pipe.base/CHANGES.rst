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

- `GraphBuilder` constructor boolean argument `skipExisting` is replaced with
  `skipExistingIn` which accepts collections to check for existing quantum
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
