pipe_base v23.0.1 2022-02-02
============================

Miscellaneous Changes of Minor Interest
---------------------------------------

- Execution butler creation time has been reduced significantly by avoiding unnecessary checks for existence of files in the datastore. (`DM-33345 <https://jira.lsstcorp.org/browse/DM-33345>`_)


pipe_base v23.0.0 2021-12-10
============================

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


pipe_base v22.0 (2021-04-01)
============================

New Features
------------

* Add ways to test a PipelineTask's init inputs/outputs [DM-23156]
* Pipelines can now support URIs [DM-28036]
* Graph files can now be loaded and saved via URIs [DM-27682]
* A new format for saving graphs has been developed (with a ``.qgraph`` extension). This format supports the ability to read a subset of a graph from an object store. [DM-27784]
* Graph building with a pipeline that specifies an instrument no longer needs an explicit instrument to be given. [DM-27985]
* A ``parameters`` section has been added to pipeline definitions. [DM-27633]
