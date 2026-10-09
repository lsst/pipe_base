.. _lsst-pipe-base-runtime-analyzer:

#######################################
Quantum Runtime Analysis (Experimental)
#######################################

.. warning::

    ``lsst.pipe.base._runtime_analyzer`` is an experimental subpackage.
    Its interface may change without a deprecation notice.

The `lsst.pipe.base.quantum_graph.ProvenanceQuantumGraph` ingested for a completed run records how much wall-clock and CPU time, and how much memory, every quantum used.
``lsst.pipe.base._runtime_analyzer`` summarizes those records as per-task statistics, rankings of individual quanta, distributions grouped by DataID dimension, and a classification of tasks that are limited by I/O or by preparation and initialization overhead.
Analysis is available through the ``runtime-analyzer`` command (equivalently ``python -m lsst.pipe.base._runtime_analyzer``) and through a Python API.

``click``, ``matplotlib``, and ``pyarrow`` are needed for the command-line tool, plotting, and Parquet support (the ``[runtime]`` extra of ``lsst.pipe.base``); ``scipy`` (the ``[kde]`` extra) adds kernel density estimates to histogram plots.

Recorded Resources
==================

For each quantum, the provenance graph records:

.. list-table::
   :header-rows: 1
   :widths: 25 15 60

   * - Resource
     - Unit
     - Description
   * - ``prep_time``
     - seconds
     - Preparation time (data loading, graph traversal)
   * - ``init_time``
     - seconds
     - Initialization time (task setup, input retrieval)
   * - ``run_time``
     - seconds
     - Main task execution wall-clock time
   * - ``run_time_cpu``
     - seconds
     - CPU time consumed during execution
   * - ``memory``
     - MiB
     - Peak memory allocation

From these records the analyzer produces task-level summaries, rankings of the slowest or largest quanta, distributions grouped by DataID dimensions such as ``visit``, ``filter``, or ``tract``, and bottleneck diagnostics that classify each task as CPU-bound, I/O-bound, or overhead-dominated.

All analysis operates on a `QuantumRuntimeTable`, an immutable Arrow-backed table (a single-chunk ``pyarrow.Table``) whose columns are dictionary-encoded task labels, fixed-width binary quantum ids, ``int8`` status codes, ``float32`` times and memory sizes, the ``str(DataCoordinate)`` display string, and a ``map<string, string>`` of per-quantum dimension values.
Column accessors return NumPy views rather than copies.

Specifying Input Data
=====================

Each subcommand reads its data in one of two ways: you can pass positional source files, or the source flags below, but not both in the same invocation.

positional sources
    File names given *after* the subcommand name.
    The extension (case-insensitively) decides the format: ``*.parquet`` and ``*.pq`` are read as cached runtime tables, requiring no graph or Butler access, and anything else (``.qg``, ``.fits``, and so on) is read as a provenance quantum graph file.

source flags
    ``--graph``/``-g``, ``--table``/``-T``, and ``--repo``/``-r`` with ``--collection``/``-c``, given *before* the subcommand name.
    The flags specify the format explicitly: ``--graph`` loads a graph file regardless of its extension, and ``--table`` loads a cached table.

.. code-block:: bash

    # From a graph file (positional)
    runtime-analyzer summary /path/to/quantum_graph.qg

    # From a Butler repository and collection (flags)
    runtime-analyzer --repo /path/to/repo --collection calibration_v12 summary

Sources are loaded in the order given on the command line.
Where two sources provide the same ``(task_label, data_id)`` row, the earlier one wins; rows present only in later sources are still included.
Each ``--collection`` applies to the closest preceding ``--repo``, and a repository stays open across intervening ``--graph`` and ``--table`` entries, so ``--repo R1 --collection A --graph g.qg --repo R2 --collection B`` yields ``R1:A``, ``g.qg``, and ``R2:B`` in that order.
A ``--collection`` with no preceding ``--repo`` is rejected at parse time.

.. code-block:: bash

    # Graph, Butler collection, and cached table, merged in the given order
    runtime-analyzer --graph a.qg --repo /repo --collection coll1 --table t.parquet summary

    # Cached rows take precedence over the same quanta in the live collection
    runtime-analyzer --table rerun.parquet --repo /repo --collection run2024 summary

Positional sources may also mix graph files and caches, merging left to right under the same precedence rule.

Subcommands
===========

``runtime-analyzer --help`` lists every option, and the ``help-plots`` and ``help-tables`` subcommands list the available plot names and table subcommands.
The analysis subcommands all accept the group options ``--repo``/``-r``, ``--collection``/``-c``, ``--graph``/``-g``, ``--table``/``-T``, ``--task``/``-t`` (restrict to one task label), and ``--save-intermediate-table`` (write the loaded table to a Parquet file while running).

summary
    Per-task statistics: the quantum count, the mean, percentile (p05, p25, p50, p75, p95), maximum, minimum, and standard deviation of the run time, the mean, median, and maximum memory in MiB, the mean I/O percentage, and the total run time.

    .. code-block:: bash

        runtime-analyzer summary run.parquet
        runtime-analyzer -r myrepo -c calib_v2 --task calexp summary

top-quantities
    The ``--top``/``-n`` quanta with the largest ``run_time`` or ``memory`` (``--metric``/``-m``).
    ``--status``/``-s`` filters by quantum status; ``failed`` matches both FAILED and ABORTED quanta, while ``aborted`` matches only ABORTED.
    The ``pct_of_task_mean`` column compares each quantum's metric against the mean for its task, so a value of 1000 % marks a quantum ten times the task mean.
    It is the quickest way to spot a single slow quantum inside an otherwise well-behaved task.

    .. code-block:: bash

        runtime-analyzer top-quantities -m memory -n 50 run.parquet
        runtime-analyzer -r myrepo -c calib_v2 top-quantities -s failed -n 15

dimension
    Performance grouped by the values of a DataID dimension (``--by``, required), producing one table per task that sees that dimension; tasks without it are skipped with a warning.
    The values come from the `DataCoordinate` recorded with each quantum.

    .. code-block:: bash

        runtime-analyzer dimension --by visit run.parquet

bottleneck
    Two tables: a per-task classification (``BALANCED``, ``I/O_BOUND``, or ``OVERHEAD``, with the prep, init, and run phase percentages and CPU efficiency) and a table of outlier quanta (``--outlier-method``/``-o``, one of ``iqr`` or ``zscore``; ``--top-n``).
    A task is classified ``I/O_BOUND`` when its mean CPU efficiency is below 0.5 and its run-phase percentage exceeds the median across tasks, and ``OVERHEAD`` when preparation and initialization together exceed 30 % of its total time.

    .. code-block:: bash

        runtime-analyzer -r myrepo -c calib_v2 bottleneck -o zscore --top-n 50

plot
    Renders plots to files.
    ``--plots``/``-p`` takes a comma-separated list of plot names (``box``, ``scatter``, ``bar``, ``stacked``, ``dim``, ``hist``, ``percentile``, ``overview``), ``--layout``/``-l`` selects a pre-made layout (currently ``overview``), ``--output-plot``/``-o`` gives the output directory, ``--format`` selects png, pdf, or svg, and ``--by`` supplies the dimension for the ``dim`` plot.

    .. code-block:: bash

        runtime-analyzer -r myrepo -c calib_v2 plot --layout overview -o ./reports

preprocess
    Writes the extracted runtime table to a Parquet cache (``--output``/``-o`` is required), reporting the row count and the recorded fingerprint.
    See :ref:`Caching <lsst-pipe-base-runtime-analyzer-caching>`.

tracked-run
    Records and compares analysis runs over time.
    See :ref:`Run Tracking <lsst-pipe-base-runtime-analyzer-tracking>`.

.. _lsst-pipe-base-runtime-analyzer-caching:

Caching
=======

Every query that reads a graph pays for loading it: the ``run_provenance`` graph is fetched from the Butler (or a file) and flattened quantum by quantum before any analysis runs, which dominates the cost of repeated queries on large runs.
Preprocess a run once into a Parquet cache, then pass the cache positionally to any subcommand:

.. code-block:: bash

    # Preprocess once (the only step that accesses the Butler repository)
    runtime-analyzer --repo /path/to/repo --collection calibration_v12 preprocess -o run.parquet

    # Re-query from the cache
    runtime-analyzer summary run.parquet
    runtime-analyzer dimension --by visit run.parquet
    runtime-analyzer tracked-run record --label "v16.0-calib" run.parquet

An analysis subcommand can also save the table it loads, via ``--save-intermediate-table``:

.. code-block:: bash

    runtime-analyzer --repo /path/to/repo --collection calibration_v12 --save-intermediate-table run.parquet summary

Several caches merge into one file with ``preprocess -o merged.parquet a.parquet b.parquet``.

.. note::

    Preprocessing is worth the extra step when you expect to ask several questions of the same run.
    For a single ``summary`` invocation, passing the repository and collection directly is simpler.

A cache reflects its source as of the time it was written.
If you re-run the collection or graph file behind a cache, the cache no longer matches it, and you should regenerate it with ``preprocess``.
Each cache records metadata with the schema and analyzer versions, the sources, the row count, the creation time, and a fingerprint.
The metadata can be read without loading the data:

.. code-block:: bash

    python -c "
    import json
    import pyarrow.parquet as pq
    md = pq.read_metadata('run.parquet').metadata or {}
    for key in (b'created_at', b'n_rows', b'sources', b'fingerprint',
                b'schema_version', b'runtime_analyzer_version'):
        value = md.get(key, b'').decode()
        if key == b'sources':
            value = ', '.join(json.loads(value))
        print(f'{key.decode():26s} {value}')
    "

.. warning::

    The fingerprint is a digest of the task labels and the quantum count per task.
    It detects changes to the task set, but not value-only changes: re-running a collection with the same tasks and quantum counts but different timings produces an identical fingerprint.
    Regenerate caches after a collection is re-run instead of relying on the fingerprint to detect the change.

.. _lsst-pipe-base-runtime-analyzer-tracking:

Run Tracking
============

``tracked-run`` keeps analysis results in a local SQLite database so you can compare runs over time, trend individual tasks, and check whether a change is statistically significant.
By default the database is stored at ``~/.local/share/runtime-tracker.db``.
To put it elsewhere, set ``db_path`` in ``~/.config/lsst/pipe-base/_runtime_analyzer/config.yaml``:

.. code-block:: yaml

    db_path: /path/to/your-tracker.db

Relative ``db_path`` values resolve against the current working directory.

.. code-block:: bash

    # Record a run
    runtime-analyzer tracked-run record --label "v16.0-calib" run.parquet
    runtime-analyzer -r /butler/repo -c run_v16 tracked-run record --label "HSC-Dr2"
    runtime-analyzer tracked-run record --label "v16.0-full" --raw my_graph.qg

    # List recorded runs
    runtime-analyzer tracked-run list --limit 50 --task calibrate

    # Compare two recorded runs (--raw is required for the Mann-Whitney U
    # test enabled by --full)
    runtime-analyzer tracked-run compare --from v15.0 --to v16.0 --metric p95

    # Trend one task over recorded runs (requires at least 3)
    runtime-analyzer tracked-run trend --task calibrate --metric p50 --plot

    # Report significant changes (optionally --task, --since, or --metric)
    runtime-analyzer tracked-run alerts --task calibrate

Recording prints a comparison with the most recent comparable recorded run, so you see straight away whether the new run looks different.
The comparison table lists, per task, the old and new values of the metric, the percentage change (``delta_pct``, with an ↑ or ↓ arrow), Cohen's d, and a status: ``significant`` when p < 0.05 and the absolute Cohen's d exceeds 0.3, ``stable`` when the effect size is trivial (below 0.2), and ``uncertain`` otherwise.
Tasks present in only one of the runs are listed separately.
The ``alerts`` report applies an additional gate: a change is reported only if the metric also moved by at least 10 %.
Tracker plots (``trend``, ``delta``, ``heatmap``) are saved under ``~/.local/share/runtime-analyzer-plots/``.

Python API
==========

.. code-block:: python

    from lsst.pipe.base._runtime_analyzer import (
        QuantumRuntimeAnalyzer,
        extract_runtime_table,
    )
    from lsst.pipe.base.quantum_graph import ProvenanceQuantumGraph

    with ProvenanceQuantumGraph.from_args("repo", collection="calibration_v12") as (qg, butler):
        analyzer = QuantumRuntimeAnalyzer(extract_runtime_table(qg))

        summary = analyzer.summary(status="SUCCESSFUL")
        print(summary.pformat())

        top = analyzer.top_quantities(metric="run_time", n=10)
        dims = analyzer.dimension_dist(dimension="visit")
        bottlenecks = analyzer.bottleneck(method="iqr", top_n=20)
        print(bottlenecks["task_table"].pformat())

`QuantumRuntimeAnalyzer` wraps an extracted `QuantumRuntimeTable`.
If you would rather not open the graph yourself, `extract_merged_runtime_table` performs the same loading and merging as the CLI, given ordered ``(path, collection_or_None)`` sources with the same precedence rule (a collection of `None` means the path is a graph file or cache rather than a Butler repository):

.. code-block:: python

    from lsst.pipe.base._runtime_analyzer import (
        QuantumRuntimeAnalyzer,
        QuantumRuntimeTable,
        extract_merged_runtime_table,
    )

    table = extract_merged_runtime_table(
        [("my_repo", "calibration_v12"), ("overlay.qg", None)]
    )
    table.to_parquet("run.parquet")          # cache for reuse
    analyzer = QuantumRuntimeAnalyzer(table)

    # Or load directly from caches:
    analyzer = QuantumRuntimeAnalyzer(
        QuantumRuntimeTable.from_parquets(["run.parquet"])
    )

The analysis methods correspond to the subcommands:

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Method
     - Returns
   * - ``analyzer.summary(status=None, task_label=None)``
     - `astropy.table.Table` of per-task statistics
   * - ``analyzer.top_quantities(metric="run_time", n=20, status=None, task_label=None)``
     - `astropy.table.Table` of ranked quanta
   * - ``analyzer.dimension_dist(dimension, task_label=None)``
     - ``dict`` of task label to `astropy.table.Table`
   * - ``analyzer.bottleneck(method="iqr", top_n=20, status=None, task_label=None)``
     - ``dict`` with ``task_table`` and ``outlier_table``

The table accessors return NumPy views, and `select` filters rows without copying:

.. code-block:: python

    import numpy as np

    qt = analyzer.table
    data_rt = qt.run_time[qt.status_codes == 1]   # SUCCESSFUL quanta only
    print(f"custom p99: {np.percentile(data_rt, 99):.1f} s")

Plot, formatting, and export functions are importable from the same module:

.. code-block:: python

    from lsst.pipe.base._runtime_analyzer import (
        export_csv,
        export_parquet,
        format_table,
        plot_overview,
    )

    print(format_table(analyzer.summary()))
    export_csv(analyzer.top_quantities(n=100), "top_quanta.csv")
    export_parquet(analyzer.bottleneck()["task_table"], "bottlenecks.parquet")
    fig = plot_overview(analyzer)
    fig.savefig("overview.png", dpi=300, bbox_inches="tight")

The run-tracking functions (`list_runs`, `record_run`, `compare_runs`, `get_trend`, `check_alerts`) are available for scripted use.

Troubleshooting
===============

"No data available"
    The graph contains no quanta with resource usage.
    The file may be incomplete, or the pipeline run recorded no resource usage at all.

"Task does not have dimension ..."
    Expected when not all tasks see all DataID keys.
    Those tasks are excluded from the grouping with a logged warning.

More than 1000 distinct dimension values
    Usually the cause is a high-cardinality dimension such as ``tract`` in deep imaging.
    Pass ``--task`` to reduce the number of groups.

Every task classified BALANCED
    All tasks have a similar CPU efficiency near 1.0 and similar run-phase percentages, either because the workload is entirely CPU-bound or because the dataset is too small for the classification to be meaningful.

matplotlib missing
    Plotting requires the ``[runtime]`` extra.
    The analysis API works without it.
