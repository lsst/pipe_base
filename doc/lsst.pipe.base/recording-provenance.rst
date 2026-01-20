.. _pipe_base_provenance:

.. py:currentmodule:: lsst.pipe.base.quantum_graph

####################
Recording Provenance
####################

The `PredictedQuantumGraph` that is used to predict and control processing also contains a wealth of provenance information, including task configuration and the complete input-output relationships between all datasets.
Instead of storing these graphs directly in a `~lsst.daf.butler.Butler` repository, however, it is better to first augment them with additional provenance information that is only available after execution has completed, producing a `ProvenanceQuantumGraph` that is ingested instead.
By convention, we store provenance in a ``run_provenance`` dataset type with empty dimensions, which means there is exactly one for each `~lsst.daf.butler.CollectionType.RUN` collection.
In addition to the input-output graph itself and full configuration for all tasks, `ProvenanceQuantumGraph` stores status information for each attempt to run a quantum, including exception information and caveats on any successes.
It can also store the full logs and task metadata for each quantum and back distinct per-quantum datasets for those, allowing repositories to store many fewer small files.

The pipeline system has many different execution contexts, and provenance recording is not supported in all of them at this time.

Batch Execution / Quantum-Backed Butler
=======================================

Provenance recording is fully supported in batch workflows that use the `~lsst.daf.butler.QuantumBackedButler` class (e.g. ``pipetask run-qbb``, as run by the ``bps`` tool) to avoid database writes during execution.
This involves the following steps:

- A `PredictedQuantumGraph` is generated as usual (e.g. via ``pipetask qgraph``, as run by ``bps report``) and saved to a known location.
- All quanta are executed via ``pipetask run-qbb``, writing their outputs to butler-managed storage without updating the butler database.
- When all quanta have been attempted, the ``butler aggregate-graph`` tool is run (e.g. in the BPS ``finalJob``) to ingest output datasets into the butler database, and the ``--output`` option is used to save a `ProvenanceQuantumGraph` to a known location.
  This step and the previous one may be run multiple times (e.g. via ``bps restart``) to retry some failures, and it is only necessary to pass ``--output`` the last time (though usually the user does not know when attempt will be the last one).
- When all processing attempts are complete, the ``butler ingest-graph`` tool is used to ingest the graph into the butler database and rewrite all metadata, log, and config datasets to also be backed by the same graph file (deleting the original files).

All of the above happens in a single `~lsst.daf.butler.CollectionType.RUN` collection.
Reference documentation for ``butler aggregate-graph`` and ``butler ingest-graph`` can be found in the `aggregator` and `ingest_graph` modules implement them (respectively); in both cases there are Python interfaces that closely mirror the command-line ones.

Parallelization
---------------

Aggregating and ingesting a large batch run is expensive, and both tools use parallelism whenever possible to improve performance.

The aggregator in particular is explicitly parallel, with separate workers (usually subprocesses) assigned to scan and read metadata and log files (any number of workers), ingest datasets (a single worker), write the provenance graph file (a single worker), and coordinate all of these operations.
Since all information must be passed from the scanners to the ingestion and writer workers, additional parallelism can help when all operations are running at around the same speed (as reported in the logs), but not when ingestion or writing lags significantly behind.
The writer process has substantial startup overhead and will typically lag the others at the beginning before it catches up later.

The `ingest_graph` tool mostly performs database write operations, which do not benefit from parallelism, but it also deletes the original metadata, log, and config files as the new graph-backed variants of those datasets are ingested.
These deletes are delegated to `lsst.resources.ResourcePath.mremove`, which refers to the ``LSST_RESOURCES_NUM_WORKERS``, ``LSST_RESOURCES_EXECUTOR``, and ``LSST_S3_USE_THREADS`` environment variables to control parallelism.
As with other butler bulk-delete operations, the default parallelism is usually fine.

.. note::

  Earlier versions of the `aggregator` would run catastrophically slowly when ``LSST_RESOURCES_EXECUTOR=process``, as this made each scanner process spawn multiple subprocesses constantly.
  In recent versions all parallelism environment variables are ignored by the
  aggregator so this should not occur.

Ingesting Outputs Early
-----------------------

The `aggregator` may be run with `~aggregate.AggregatorConfig.incomplete` set to `True` (``--incomplete`` on the command line) to allow it to be safely run before the graph has finished executing.
Note that while ingestion always picks up where it left off, scanning always has to start at the beginning, and provenance graph writing is disabled when running in ``incomplete`` mode, so while this allows output datasets be be available via the `~lsst.daf.butler.Butler` sooner, it does not generally make the final complete `aggregator` call substantially faster.

Promising Graph Ingestion
-------------------------

By default, the `aggregator` ingests all metadata, log, and config outputs into the butler database in the usual way, i.e. backed by their original individual files.
The `ingest_graph` tool then has to delete these datasets from the butler database before it can ingest new ones and delete the original files.
When it is known in advance that `ingest_graph` will be run later, the `~aggregate.AggregatorConfig.promise_ingest_graph` (`--promise-ingest-graph`) option can be used to tell the `aggregator` *not* to ingest these, saving time for both commands.
This option must be used with care, however: if `ingest_graph` isn't run later, the original files will be orphaned in a butler-managed location without any record in the database, which generally means they'll quietly take up space.
In addition, because the metadata datasets are used by the middleware system as the indicator of a quantum's success, their absence will make any downstream quantum graph builds using ``--skip-existing-in`` to be incorrect.
And of course any downstream processing that actually uses those datasets as input (only metadata should be) will not see them as available.

Deferring Graph Ingestion
-------------------------

Ingesting the provenance graph is not generally necessary to kick off downstream processing by building new quantum graphs for later pipeline steps, and it is always safe to build downstream quantum graphs if `~aggregate.AggregatorConfig.promise_ingest_graph` is left `False`.
It can also be done safely if `~aggregate.AggregatorConfig.promise_ingest_graph` is `True` and:

 - ``--skip-existing-in`` is not used;
 - the downstream processing does not use metadata, log, or config datasets as an overall input (``pipetask build ... --show inputs`` can be used to check for this).

These conditions also must be met in order for `ingest_graph` to be safely run *while* a downstream quantum graph is being executed.
Both of these conditions are *usually* met, and deferring and promising graph ingest each provide significant wall-clock savings, so we recommend the following approach for very large BPS campaigns:

- Submit ``step(N)`` to BPS with ``--promise-ingest-graph`` in the ``finalJob`` invocation of ``aggregate-graph``.
- When ready to move on to ``step(N+1)``, run ``pipetask build ... --show inputs`` to scan for metadata, log, and config inputs that may be from the previous step.
- If there are no such inputs, immediately submit that step to BPS, and run `ingest_graph` on ``step(N)`` as soon as the quantum graph for ``step(N+1)`` is built (it could be built at the same time, but waiting a bit may help spread out database load).
- If there are metadata, log, or config inputs, run `ingest_graph` on ``step(N)`` and wait for it to finish before submitting ``step(N+1)``.

Note that *independent* quantum graph builds (e.g. same tasks, disjoint data IDs) can always be built before or while `ingest_graph` runs.

Recovering from Interruptions
-----------------------------

If the `aggregator` is interrupted it can simply be started again.
Database ingestion will pick up where it left off, while scanning and provenance-graph writing will start over from the beginning.

If `ingest_graph` is interrupted, it can also be started again, and everything will pick up where it left off.
To guarantee this it always modifies the repository in the following order:

- if the ``run_provenance`` dataset does not exist in the collection, all existing metadata/log/config datasets are assumed to be backed by their original files and are deleted from the butler database (without deleting the files);
- the ``run_provenance`` dataset itself is ingested (this ensures the metadata/log/config *content* is safe inside the butler, even if it's not fully accessible);
- in batches, metadata/log/config datasets are reingested into the butler backed by the graph file, and then the corresponding original files are deleted.

This means we can use the existence of ``run_provenance`` and any particular metadata/log/config dataset in the butler database to infer the status of the original files.

In fact, if `ingest_graph` is interrupted at any point, it *must* be tried again until it succeeds, since not doing so will can leave metadata/log/config files orphaned, just like when `~aggregate.AggregatorConfig.promise_ingest_graph` is `True`.

.. note::
  After the ``run_provenance`` dataset is ingested, it is *not* safe to run the `aggregator`: the `aggregator` reads the original metadata and log files to gather provenance information, and will infer the wrong states for quanta if those are missing because `ingest_graph` has deleted them.

  This is why it is not safe to run ``bps restart`` after `ingest_graph`, and why we do not recommend adding `ingest_graph` to the BPS ``finalJob``, even if the user is willing to forgo using ``bps restart``: by default, the ``finalJob`` will be retried on failure, causing the `aggregator` to run again when it may not be safe to do so.
  And if ``finalJob`` retries are disabled, is too easy for the repository to end up in a state that would require manual `ingest_graph` runs to prevent orphan datasets.
