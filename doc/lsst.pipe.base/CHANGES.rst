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
