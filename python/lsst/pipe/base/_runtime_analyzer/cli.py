"""Command-line interface for the quantum runtime analyzer.

Provides a click-based CLI with subcommands for summary, top-quantities,
dimension analysis, bottleneck diagnosis, and plot rendering.
"""

from __future__ import annotations

__all__ = [
    "main",
]

from collections.abc import Callable, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

try:
    import click
    from click.core import _OptionParser  # same class click.Group uses internally
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "The runtime-analyzer CLI requires 'click >=8,<9'. Install "
        "pipe_base with the [runtime] extra."
    ) from exc

from .cli_helpers import PLOT_NAMES, TABLE_NAMES
from .console import format_table
from .core import QuantumRuntimeAnalyzer
from .plot import get_available_plots, get_plot_by_name
from .runtime_table import QuantumRuntimeTable, extract_merged_runtime_table

# Tracker imports (database and history; the tracker plot/format modules
# load on demand inside the tracked-run command bodies).
from .tracker.database import get_runs, hash_graph, record_run
from .tracker.history import (
    check_alerts,
    compare_runs,
    find_task_changes,
    get_trend,
)

# Positional entries with these suffixes (case-insensitive) are cached
# per-quantum Parquet tables; every other positional keeps the graph-file
# behavior (graph files are zip containers named .qg, .fits, or anything
# else, so the else-branch must stay the default).
_TABLE_SUFFIXES = ('.parquet', '.pq')


def _is_table_path(path: str) -> bool:
    """Return True if a positional entry names a cached Parquet table."""
    return str(path).lower().endswith(_TABLE_SUFFIXES)


def _split_positionals(
    graph_paths: list[str],
) -> tuple[list[str], list[str]]:
    """Split positional entries into (graph_file_paths, table_paths).

    Extension dispatch: ``*.parquet``/``*.pq`` (case-insensitive) are
    cached-table sources; everything else is a graph-file source.
    """
    table_paths = [p for p in graph_paths if _is_table_path(p)]
    graph_file_paths = [p for p in graph_paths if not _is_table_path(p)]
    return graph_file_paths, table_paths


# A tagged source entry names the loader for one input.  The string tag
# in position 0 is the discriminating element:
#   ('graph', path)           -> graph file (extension irrelevant)
#   ('table', path)           -> cached per-quantum Parquet table
#   ('butler', repo, coll)   -> Butler collection
type GraphSource = tuple[Literal["graph"], str]
type TableSource = tuple[Literal["table"], str]
type ButlerSource = tuple[Literal["butler"], str, str]
type Source = GraphSource | TableSource | ButlerSource


def _positional_entries(graph_paths: Sequence[str]) -> list[Source]:
    """Map positional file arguments to tagged source entries.

    Extension dispatch applies *only* to positionals: ``*.parquet``/
    ``*.pq`` (case-insensitive) map to ``('table', path)``; everything
    else maps to ``('graph', path)``.  Flag-sourced entries carry a
    literal kind and never consult this function, so a ``.parquet``
    passed to ``--graph`` loads via the graph reader.

    Parameters
    ----------
    graph_paths : `~collections.abc.Sequence` of `str`
        Positional file arguments exactly as given on the command line.

    Returns
    -------
    entries : `list` of `Source`
        Tagged ``('graph'|'table', path)`` entries in positional order.
    """
    return [
        ('table', str(p)) if _is_table_path(p) else ('graph', str(p))
        for p in graph_paths
    ]


def _source_label(source: Source) -> str:
    """Render one tagged source entry as a cache-metadata label.

    Label scheme: a graph entry renders as its path, a table entry
    renders as its path, and a Butler entry renders as ``"repo:coll"``.
    The scheme is identical for positional and flag inputs, so cache
    metadata carries consistent labels in every input mode.

    Parameters
    ----------
    source : `Source`
        A tagged ``('graph'|'table', path)`` or ``('butler', repo, coll)``
        entry.

    Returns
    -------
    label : `str`
        The label string recorded in cache metadata.
    """
    if source[0] == 'butler':
        return f"{source[1]}:{source[2]}"
    return str(source[1])


def _source_kind(source: Source) -> str:
    """Classify a tagged entry as ``'table'`` or ``'graph'``.

    Butler sources are graph-flavored for the post-load header (they load
    a quantum graph from a collection), so a Butler-only load echoes
    "graph source(s)".
    """
    return 'table' if source[0] == 'table' else 'graph'


def _load_header(
    analyzer: QuantumRuntimeAnalyzer,
    sources: Sequence[Source] = (),
) -> str:
    """Compose the post-load "Loaded N ... quanta." echo.

    Wording follows the input mix: table-only loads say "table
    source(s)"; graph-file and Butler loads say "graph source(s)"; any
    mix of table and graph/Butler sources uses the neutral "source(s)".

    Parameters
    ----------
    analyzer : `QuantumRuntimeAnalyzer`
        Loaded analyzer supplying the source and quanta counts.
    sources : `~collections.abc.Sequence` of `Source`, optional
        The effective ordered source list as tagged entries.  Empty
        keeps the graph wording.

    Returns
    -------
    header : `str`
        The header line to echo.
    """
    n_tables = sum(1 for s in sources if _source_kind(s) == 'table')
    if sources and n_tables == len(sources):
        kind = "table "
    elif n_tables:
        # Mixed graph/Butler + table sources: neutral wording.
        kind = ""
    else:
        kind = "graph "
    return (
        f"Loaded {analyzer.n_sources} {kind}source(s), "
        f"{analyzer.n_loaded} total quanta."
    )


def _load_ordered(
    sources: Sequence[Source],
) -> QuantumRuntimeAnalyzer:
    """Load tagged source entries in order into one merged analyzer.

    The entries are iterated *in the order given*, dispatching each to
    its kind's table producer: ``graph`` via
    ``extract_merged_runtime_table([(path, None)])`` and ``butler`` via
    ``extract_merged_runtime_table([(repo, coll)])`` (one call per
    entry, reusing the graph loading path directly), and ``table`` via
    ``QuantumRuntimeTable.from_parquet``.  Each producer's table enters
    one ``QuantumRuntimeTable.merge_first_wins(*tables)`` merge on the
    ``(task_label, data_id)`` key, and the result backs a
    ``QuantumRuntimeAnalyzer``, so graph, Butler, and cached-table
    inputs interleave freely with precedence following command-line
    order (earlier sources shadow later ones).

    Single-kind lists take bulk fast-paths: a graph- or Butler-only list
    issues one ``extract_merged_runtime_table`` over all its
    ``(path, collection)`` pairs, and a table-only list one
    ``QuantumRuntimeTable.from_parquets`` over all paths (which merges
    internally with the same first-wins precedence).  No loader functions
    exist: every path ends in a runtime-table producer wrapped directly
    in ``QuantumRuntimeAnalyzer``.

    Parameters
    ----------
    sources : `~collections.abc.Sequence` of `Source`
        Ordered tagged entries (``('graph', path)``, ``('table', path)``, or
        ``('butler', repo, coll)``).

    Returns
    -------
    analyzer : `QuantumRuntimeAnalyzer`
        Analyzer backed by the merged rows of every entry.

    Raises
    ------
    click.UsageError
        If a source fails to load (with a kind-appropriate message).
    """
    # Single-kind bulk fast-paths: exactly one bulk producer call per
    # kind (one extract_merged_runtime_table over graph/Butler pairs, or
    # one QuantumRuntimeTable.from_parquets over cache paths).
    if sources and all(s[0] != 'table' for s in sources):
        pairs: list[tuple[str, str | None]] = [
            (s[1], s[2]) if s[0] == 'butler' else (s[1], None)
            for s in sources
        ]
        try:
            return QuantumRuntimeAnalyzer(
                extract_merged_runtime_table(pairs)
            )
        except click.UsageError:
            raise
        except Exception as err:
            # Butler-only lists use the collection wording; anything
            # graph-flavored uses the graph wording.
            if all(s[0] == 'butler' for s in sources):
                msg = "Failed to load Butler collections"
            else:
                msg = "Failed to load graph sources"
            raise click.UsageError(f"{msg}: {err}") from err

    if sources and all(s[0] == 'table' for s in sources):
        table_paths = [str(s[1]) for s in sources]
        try:
            return QuantumRuntimeAnalyzer(
                QuantumRuntimeTable.from_parquets(table_paths)
            )
        except Exception as err:
            raise click.UsageError(f"Failed to load cached tables: {err}") \
                from err

    # Mixed kinds (or empty): interleave loads left-to-right into one
    # table list so precedence follows command-line order whatever the mix.
    tables: list[QuantumRuntimeTable] = []

    for source in sources:
        if source[0] == 'table':
            try:
                tables.append(QuantumRuntimeTable.from_parquet(str(source[1])))
            except Exception as err:
                raise click.UsageError(
                    f"Failed to load cached tables: {err}"
                ) from err
        elif source[0] == 'butler':
            try:
                tables.append(
                    extract_merged_runtime_table([(source[1], source[2])])
                )
            except Exception as err:
                raise click.UsageError(
                    f"Failed to load Butler collections: {err}"
                ) from err
        else:  # 'graph'
            try:
                tables.append(
                    extract_merged_runtime_table([(source[1], None)])
                )
            except Exception as err:
                raise click.UsageError(
                    f"Failed to load graph sources: {err}"
                ) from err

    # Mixed loads pass sources=() on purpose: per-entry producers would
    # stamp butler entries as bare repo labels (no collection), so the
    # cache metadata labels are supplied explicitly by _save_cache
    # instead of guessing here.
    merged = QuantumRuntimeTable.merge_first_wins(
        *tables, n_sources=len(sources), sources=(),
    )
    return QuantumRuntimeAnalyzer(merged)


def _save_cache(
    analyzer: QuantumRuntimeAnalyzer,
    path: str,
    labels: list[str],
) -> tuple[str, int]:
    """Save the analyzer's flat runtime table as a Parquet cache.

    Serializes ``analyzer.as_table()`` via
    ``QuantumRuntimeTable.to_parquet`` with source labels and a
    ``hash_graph(analyzer)`` fingerprint, then echoes
    the confirmation.  The fingerprint is computed exactly once and
    returned so callers (``preprocess``) can reuse it for their own echo
    with a single pass over the data.

    Parameters
    ----------
    analyzer : `QuantumRuntimeAnalyzer`
        Loaded analyzer whose flat table is persisted.
    path : `str`
        Destination Parquet cache path.
    labels : `list` of `str`
        Source labels recorded in the file metadata.

    Returns
    -------
    fingerprint : `str`
        The recorded run fingerprint (``hash_graph(analyzer)``).
    n_rows : `int`
        Number of rows written.
    """
    fingerprint = hash_graph(analyzer)
    n_rows = analyzer.as_table().to_parquet(
        path,
        sources=labels,
        fingerprint=fingerprint,
    )

    click.echo(f"Saved intermediate table: {path} ({n_rows} rows)")
    return fingerprint, n_rows


def _header_sources(
    ctx: click.Context,
    graph: tuple[str, ...] = (),
) -> list[Source]:
    """Source entries to classify the post-load header wording.

    Uses the resolved ``effective_sources`` populated by
    :func:`_load_analyzer`; falls back to mapping the positional
    ``graph`` entries by extension dispatch (relevant when the loader is
    replaced wholesale, e.g. in tests, where no resolution happened).

    Parameters
    ----------
    ctx : `click.Context`
        Click context.
    graph : `tuple` of `str`, optional
        Positional file arguments exactly as given on the command line.

    Returns
    -------
    entries : `list` of `Source`
        Tagged source entries for :func:`_load_header`.
    """
    resolved = ctx.obj.get('effective_sources')
    if resolved is not None:
        return list(resolved)
    return _positional_entries(list(graph or ctx.obj.get('graph', ()) or ()))


def _flag_sources_of(
    ctx: click.Context,
    sources_key: str,
) -> list[Source]:
    """Flag-built sources plus projection-based reconstruction.

    Reads the ordered list stored on ``ctx.obj`` under ``sources_key``;
    when the key is absent (hand-built contexts without the option
    callbacks), reconstructs butler entries from the group ``repo``/
    ``_collections`` projections.

    Parameters
    ----------
    ctx : `click.Context`
        Click context.
    sources_key : `str`
        Key of the ordered tagged-entry list (``'_sources'`` or
        ``'_record_sources'``).

    Returns
    -------
    entries : `list` of `Source`
        The flag-sourced tagged entries (may be empty).
    """
    if sources_key in ctx.obj:
        return list(ctx.obj.get(sources_key) or [])
    if sources_key == '_sources':
        return [
            ('butler', ctx.obj.get('repo'), coll)
            for coll in (ctx.obj.get('_collections') or ())
        ]
    return []


def _check_source_conflict(
    positional_entries: Sequence[Source],
    ctx: click.Context,
    sources_key: str = '_sources',
    pending_keys: Sequence[str] = ('_pending_repo',),
) -> None:
    """Raise a generalized UsageError when positionals meet source flags.

    Parameters
    ----------
    positional_entries : `~collections.abc.Sequence` of `Source`
        Tagged positional entries (possibly empty).
    ctx : `click.Context`
        Context holding the flag-built ordered list and pending state.
    sources_key : `str`, optional
        Key of the flag-built ordered list to consult.
    pending_keys : `~collections.abc.Sequence` of `str`, optional
        Keys of pending-repo dicts on ``ctx.obj`` whose state also counts
        as "flags given" (so a dangling ``--repo`` cannot smuggle
        positionals through).

    Raises
    ------
    click.UsageError
        If positionals and any source flag are combined.
    """
    flags_given = bool(_flag_sources_of(ctx, sources_key)) or any(
        ctx.obj.get(key) is not None for key in pending_keys
    )
    if positional_entries and flags_given:
        raise click.UsageError(
            "Cannot mix positional files with source flags "
            "(--repo/--collection/--graph/--table). Use either positional "
            "files OR source flags, not both."
        )


def _load_analyzer(ctx: click.Context) -> QuantumRuntimeAnalyzer:
    """Load analyzer from the effective ordered source list.

    The effective list is the callback-built ordered ``ctx.obj['_sources']``
    (group ``--graph/--table/--repo/--collection`` flags) or, when no flags
    were given, the positional entries mapped by the extension rule (
    ``.parquet``/``.pq`` -> cached table, anything else -> graph file).
    Positionals and source flags are mutually exclusive.  Loading merges all
    entries first-wins by ``(task_label, data_id)`` in command-line order via
    :func:`_load_ordered`.  When ``--save-intermediate-table`` is set, the
    loaded table is cached to the given path after a successful load (with
    table-only inputs the flag copies through).

    Parameters
    ----------
    ctx : `click.Context`
        Click context containing the ordered source list (and, for contexts
        built without the option callbacks, the ``graph``/``repo``/
        ``_collections`` projections).

    Returns
    -------
    analyzer : `QuantumRuntimeAnalyzer`
        The initialized analyzer.

    Raises
    ------
    click.UsageError
        If positionals and flags are mixed, no sources are given, or
        loading fails.
    """
    positional_entries = _positional_entries(
        list(ctx.obj.get('graph', []) or [])
    )
    flag_sources = _flag_sources_of(ctx, '_sources')
    _check_source_conflict(positional_entries, ctx)

    if positional_entries:
        sources: list[Source] = positional_entries
    elif flag_sources:
        sources = flag_sources
    else:
        raise click.UsageError(
            "Must provide at least one positional graph file or "
            "--repo with --collection flags."
        )

    analyzer = _load_ordered(sources)

    # Make the resolved source labels available to subcommands that save
    # caches themselves (preprocess): one label per entry in order
    # ("repo:coll" for butler, the path for graph/table files).
    labels = [_source_label(s) for s in sources]
    ctx.obj['source_labels'] = labels
    ctx.obj['effective_sources'] = sources

    # Optional one-shot cache of the loaded table (any input mode; with
    # table-only inputs this copies/merges through).
    save_path = ctx.obj.get('save_intermediate_table')
    if save_path:
        _save_cache(analyzer, str(save_path), labels)

    return analyzer


def _render_plots(
    analyzer: QuantumRuntimeAnalyzer,
    plot_names: list[str],
    tasks: list[str] | None,
    output_plot: str | None,
    fmt: str,
    dimension: str | None = None,
    y_scale: str = 'auto',
) -> None:
    """Render specified plots and save to files.

    Parameters
    ----------
    analyzer : `QuantumRuntimeAnalyzer`
        The analyzer instance.
    plot_names : `list` of `str`
        Names of plots to render.
    tasks : `list` of `str` or `None`, optional
        Task filter.
    output_plot : `str` or `None`, optional
        Output directory.
    fmt : `str`, optional
        Output format: png, pdf, svg.
    dimension : `str` or `None`, optional
        Dimension for dimension distribution plot.
    y_scale : `str`, optional
        Y-scale mode passed to box plots: "auto", "log", "symlog" or
        "linear".
    """
    import matplotlib

    output_dir = Path(output_plot) if output_plot else Path('./plots')
    output_dir.mkdir(parents=True, exist_ok=True)
    # Always state the resolved directory so the default ('./plots') is
    # unambiguous to the user.
    click.echo(f"Writing plots to: {output_dir.resolve()}")

    for name in plot_names:
        fig = None
        fig_kwarg: dict[str, Any] = {}
        plot_fn = get_plot_by_name(name)
        if plot_fn is None:
            click.echo(f"Warning: unknown plot '{name}', skipping.")
            continue

        if name == 'overview':
            fig = plot_fn(analyzer)
        elif name == 'dim':
            if dimension:
                fig = plot_fn(analyzer, dimension=dimension, tasks=tasks)
            else:
                click.echo("Warning: dimension required for 'dim' plot, use --by. Skipping.")
                continue
        else:
            if name in ('box', 'hist', 'scatter', 'percentile'):
                fig_kwarg['tasks'] = tasks
            if name == 'box':
                fig_kwarg['y_scale'] = y_scale
            if name == 'bar':
                fig_kwarg['n'] = 10
            fig = plot_fn(analyzer, **fig_kwarg)

        if fig is not None:
            safe_name = name.replace(' ', '_')
            if name == 'dim' and dimension:
                # Keep one file per dimension: `--by tract --by band` must
                # not silently overwrite a previous dim plot.
                safe_name = f"dim_{dimension}".replace(' ', '_')
            outpath = output_dir / f"{safe_name}.{fmt}"
            fig.savefig(str(outpath), dpi=300, bbox_inches='tight')
            matplotlib.pyplot.close(fig)
            click.echo(f"Saved plot: {outpath}")


# ---- Ordered source list plumbing ----
#
# The four source options (--repo/-r, --collection/-c, --graph/-g,
# --table/-T) on the main group AND on ``tracked-run record`` are
# ``expose_value=False`` and emit tagged entries as they are encountered on
# the command line, in exact typed order:
#
#   ('graph', path)          --graph/-g   literal kind, extension ignored
#   ('table', path)          --table/-T  literal kind
#   ('butler', repo, coll)   --collection closes the nearest preceding --repo
#
# ``--repo`` records a pending repo (last value wins); every following
# ``--collection`` binds to it (pair-close: the repo stays open across
# repeats and intervening --graph/--table entries, so ``-r R -c A -c B``
# yields [butler R:A, butler R:B]).  An orphan ``--collection`` with no
# preceding ``--repo`` fails at parse time.
#
# Group flags build ``ctx.obj['_sources']``; record-level flags build the
# separate ``ctx.obj['_record_sources']`` list (distinct keys on the shared
# context object) that fully overrides the group list when present.
#
# Mechanism: plain click option callbacks fire once per *option* (batched at
# the option's first position), which cannot express repeats like
# ``-r R1 -c A --graph g -r R2 -c B``.  The source options are therefore
# declared as ``_SourceOption`` and parsed by ``_SourceParser`` (created via
# ``make_parser``), whose parser-level ``process`` hook runs once per
# command-line *occurrence* -- the same order click records in its internal
# ``state.order`` -- so flags interleave exactly as typed.


def _emit_source_occurrence(
    ctx: click.Context,
    kind: str,
    value: str | None,
    sources_key: str,
    pending_key: str,
) -> None:
    """Handle one source-flag occurrence, appending its tagged entry.

    Parameters
    ----------
    ctx : `click.Context`
        The command context whose ``obj`` holds the ordered source lists
        (shared between the group and its subcommands).
    kind : `str`
        Emission kind: 'repo', 'collection', 'graph', or 'table'.
    value : `str` or `None`
        The raw value token for this occurrence.
    sources_key : `str`
        Key of the ordered tagged-entry list ('_sources' for the group,
        '_record_sources' for tracked-run record).
    pending_key : `str`
        Key of the pending-repo state ('_pending_repo' or
        '_record_pending_repo').

    Raises
    ------
    click.UsageError
        If a ``--collection`` occurrence has no preceding ``--repo``.
    """
    obj = ctx.ensure_object(dict)
    if value is None:
        return
    if kind == 'repo':
        # Pending repo, last value wins; stays open until a
        # --collection binds it or a newer --repo replaces it.
        obj[pending_key] = str(value)
        return
    if kind == 'collection':
        pending = obj.get(pending_key)
        if pending is None:
            raise click.UsageError(
                "--collection requires a preceding --repo"
            )
        obj.setdefault(sources_key, []).append(
            ('butler', str(pending), str(value))
        )
        # The repo stays open: `-r R -c A -c B` yields both pairs.
        return
    obj.setdefault(sources_key, []).append((kind, str(value)))


class _SourceOption(click.Option):
    """Source flag parsed occurrence-by-occurrence (see the
    source-plumbing note above).
    """


class _SourceParser(_OptionParser):
    """Parser that routes source-option occurrences through an emit hook.

    Parameters
    ----------
    ctx : `click.Context`
        Context passed through to the standard parser.
    emit : `callable` or `None`
        ``emit(ctx, kind, value)`` invoked once per parsed occurrence of
        any registered source option, in command-line order.
    source_map : `dict` or `None`
        Maps every registered spelling (long and short) to its kind.
    """

    def __init__(
        self,
        ctx: click.Context,
        emit: Callable[[click.Context, str, str | None], None] | None,
        source_map: dict[str, str] | None,
    ) -> None:
        super().__init__(ctx)
        self._ra_ctx = ctx
        self._emit = emit
        self._source_map = dict(source_map or {})

    def add_option(
        self,
        obj: Any,
        opts: Sequence[str],
        dest: str | None,
        action: str | None = None,
        nargs: int = 1,
        const: Any = None,
    ) -> None:
        """Register an option, wrapping source options to emit in CLI order."""
        super().add_option(
            obj, opts, dest, action=action, nargs=nargs, const=const
        )
        emit = self._emit
        if emit is None:
            return
        for opt in opts:
            low = self._short_opt.get(opt) or self._long_opt.get(opt)
            if low is None or getattr(low, '_ra_source_wrapped', False):
                continue
            kind = self._source_map.get(opt)
            if kind is None:
                continue
            orig = low.process

            def process(
                value: Any,
                state: Any,
                orig: Callable[..., Any] = orig,
                kind: str = kind,
            ) -> None:
                """Forward to click's processing, then emit the occurrence."""
                orig(value, state)
                emit(self._ra_ctx, kind, value)

            setattr(low, "process", process)
            setattr(low, "_ra_source_wrapped", True)


# Every spelling of the source options, mapped to its emission kind.
_GROUP_SOURCE_MAP = {
    '--repo': 'repo', '-r': 'repo',
    '--collection': 'collection', '-c': 'collection',
    '--graph': 'graph', '-g': 'graph',
    '--table': 'table', '-T': 'table',
}


if TYPE_CHECKING:
    # Mixin base trick: at type-check time the mixin "inherits" from
    # ``click.Command`` so ``self.name`` / ``self.get_params`` resolve;
    # at runtime it is a plain mixin and the real bases come from the
    # classes that mix it in (``_SourceGroup``, ``_SourceCommand``).
    _MixinBase = click.Command
else:
    _MixinBase = object


class _SourceCommandMixin(_MixinBase):
    """Command mixin wiring ``make_parser`` to the ordered-source emitter.

    Group-level commands build ``ctx.obj['_sources']``; the tracked-run
    ``record`` command builds its own ``ctx.obj['_record_sources']`` list.
    """

    _ra_source_map = _GROUP_SOURCE_MAP

    def make_parser(self, ctx: click.Context) -> _SourceParser:
        """Create the source-tracking option parser for this command."""
        record_scoped = self.name == "record"
        sources_key = (
            '_record_sources' if record_scoped else '_sources'
        )
        pending_key = (
            '_record_pending_repo' if record_scoped else '_pending_repo'
        )

        def emit(ctx_: click.Context, kind: str, value: str | None) -> None:
            """Record a parsed source occurrence on the context object."""
            _emit_source_occurrence(
                ctx_, kind, value, sources_key, pending_key
            )

        parser = _SourceParser(
            ctx, emit=emit, source_map=self._ra_source_map,
        )
        for param in self.get_params(ctx):
            param.add_to_parser(parser, ctx)
        return parser


class _SourceGroup(_SourceCommandMixin, click.Group):
    """Main click group with ordered-source flag parsing."""


class _SourceCommand(_SourceCommandMixin, click.Command):
    """Command with ordered-source flag parsing (tracked-run record)."""


@click.group(cls=_SourceGroup)
@click.option('--repo', '-r', 'repo', default=None, expose_value=False,
              help='Butler repository path or alias. Pairs with the next '
                   '--collection (pair-close binding).')
@click.option('--collection', '-c', 'collection', multiple=True,
              expose_value=False,
              help='Butler collection name. May be specified multiple times; '
                   'each binds to the nearest preceding --repo.')
@click.option('--graph', '-g', 'graph_flag', multiple=True,
              expose_value=False,
              help='Provenance quantum graph file (.qg, .fits, ...). May be '
                   'specified multiple times; loads as a graph whatever its '
                   'extension.')
@click.option('--table', '-T', 'table_flag', multiple=True,
              expose_value=False,
              help='Cached per-quantum Parquet table. May be specified '
                   'multiple times; loads via the table reader.')
@click.option('--task', '-t', 'task', default=None, help='Filter to a specific task label.')
@click.option('--save-intermediate-table', 'save_intermediate_table', default=None,
              type=click.Path(dir_okay=False, path_type=str),
              help='After a successful load, save the flat runtime table to '
                   'this Parquet path (same format as the preprocess output). '
                   'Accepted with cached-table inputs too (copies/merges through).')
@click.pass_context
def main(ctx: click.Context, task: str | None,
         save_intermediate_table: str | None) -> None:
    """Quantum Runtime Analyzer - analyze quantum usage from pipeline runs.

    Sources can be given as positional file arguments after the subcommand
    OR as ordered source flags before it: --graph/-g (graph files),
    --table/-T (cached Parquet tables), and --repo/-r with --collection/-c
    (Butler collections).  Positionals and source flags are mutually
    exclusive.

    Source flags load in the exact command-line order, first-wins by
    (task_label, data_id), mixing kinds freely, e.g.
    `--graph base.qg --repo /repo --collection coll --table rerun.parquet`
    loads graph, then the Butler collection, then the cached re-run table.

    A --collection binds to the nearest preceding --repo (pair-close), so
    `--repo R1 -c A --graph g --repo R2 -c B` produces three sources:
    R1:A, g, R2:B.  A --collection with no preceding --repo fails at parse
    time.  Every option occurrence is processed in typed order, including
    repeats (`--graph a --table t --graph b` loads a, t, then b).

    Positional arguments dispatch by extension: files ending in .parquet or
    .pq are loaded as cached runtime tables (no graph/Butler access),
    and anything else loads as a provenance quantum graph file (.qg, .fits,
    ...).  Mixed lists (e.g. primary.qg cached.parquet) merge left-to-right.
    Flag kinds are literal and ignore the extension.

    Use `preprocess -o run.parquet` to build a cache from any input, then
    re-query it by passing the .parquet file positionally.
    --save-intermediate-table caches the loaded table of any load in one
    invocation.
    """
    ctx.ensure_object(dict)
    sources = ctx.obj.setdefault('_sources', [])
    butler = [s for s in sources if s[0] == 'butler']
    # Projections derived from the ordered butler entries: 'repo' is the
    # first butler entry's repo, '_collections' holds all of them.
    ctx.obj.update({
        'repo': butler[0][1] if butler else None,
        'collection': butler[0][2] if butler else None,
        'task': task,
        'graph': (),
        '_collections': tuple(s[2] for s in butler),
        'save_intermediate_table': save_intermediate_table,
    })


@main.command()
@click.argument('graph', required=False, nargs=-1)
@click.pass_context
def summary(ctx: click.Context, graph: tuple[str, ...]) -> None:
    """Display task-level aggregated statistics.

    Prints an astropy Table with per-task statistics including quanta count,
    mean/percentiles/max/min/std for run_time, memory metrics, and more.

    Graphs can be specified as one or more positional file arguments, or via
    --repo and --collection flags. Multiple sources are merged with the first
    source having highest precedence.
    """
    ctx.obj["graph"] = graph

    analyzer = _load_analyzer(ctx)
    result = analyzer.summary(status=None, task_label=ctx.obj.get('task'))

    click.echo(_load_header(analyzer, _header_sources(ctx, graph)))

    if result is None or len(result) == 0:
        click.echo("No data available.")
        return

    click.echo(format_table(result))


@main.command('top-quantities')
@click.argument('graph', required=False, nargs=-1)
@click.option('--top', '-n', 'n', default=20, type=int, help='Number of top quanta to show.')
@click.option('--metric', '-m', 'metric', default='run_time',
              type=click.Choice(['run_time', 'memory']), help='Metric to rank by.')
@click.option('--status', '-s', 'status', default=None, help='Filter by status (success, failed, etc.).')
@click.pass_context
def top_quantities(
    ctx: click.Context, graph: tuple[str, ...], n: int, metric: str,
    status: str | None,
) -> None:
    """Display ranked quanta by metric.

    Shows the top N quanta sorted by the chosen metric (run_time or memory).
    """
    ctx.obj['graph'] = graph

    analyzer = _load_analyzer(ctx)
    result = analyzer.top_quantities(
        metric=metric, n=n, status=status, task_label=ctx.obj.get('task'),
    )

    click.echo(_load_header(analyzer, _header_sources(ctx, graph)))

    if result is None or len(result) == 0:
        click.echo("No data available.")
        return

    click.echo(format_table(result))


@main.command()
@click.argument('graph', required=False, nargs=-1)
@click.option('--by', 'dimension', required=True,
              help='DataID dimension to group by '
                   '(e.g., visit, filter, tract).')
@click.pass_context
def dimension(ctx: click.Context, graph: tuple[str, ...], dimension: str) -> None:
    """Display dimension-binned performance distribution.

    Groups quanta by a dataID dimension (visit, filter, tract, etc.)
    and shows per-dimensional statistics.
    """
    ctx.obj["graph"] = graph

    analyzer = _load_analyzer(ctx)
    click.echo(_load_header(analyzer, _header_sources(ctx, graph)))
    result = analyzer.dimension_dist(
        dimension=dimension, task_label=ctx.obj.get('task'),
    )

    if not result:
        click.echo(f"No tasks found with dimension {dimension!r}.")
        return

    for tl, table in result.items():
        click.echo(f"Task: {tl}")
        click.echo("=" * 60)
        for row in table:
            row_str = " | ".join(str(v) for v in row)
            click.echo(f"  {row_str}")
        click.echo()


@main.command()
@click.argument('graph', required=False, nargs=-1)
@click.option('--outlier-method', '-o', 'method', default='iqr',
              type=click.Choice(['iqr', 'zscore']), help='Outlier detection method.')
@click.option('--top-n', default=20, type=int, help='Number of top outliers to show.')
@click.pass_context
def bottleneck(ctx: click.Context, graph: tuple[str, ...], method: str, top_n: int) -> None:
    """Diagnose performance bottlenecks.

    Prints two tables: a task-level summary with bottleneck classification,
    and a per-quantum outlier table.
    """
    ctx.obj["graph"] = graph

    analyzer = _load_analyzer(ctx)
    click.echo(_load_header(analyzer, _header_sources(ctx, graph)))
    result = analyzer.bottleneck(
        method=method, top_n=top_n, status=None,
        task_label=ctx.obj.get('task'),
    )

    task_table = result.get('task_table')
    outlier_table = result.get('outlier_table')

    if task_table is not None and len(task_table) > 0:
        click.echo("Task Bottleneck Analysis:")
        click.echo("=" * 60)
        click.echo(format_table(task_table))
        click.echo()

    if outlier_table is not None and len(outlier_table) > 0:
        # ``bottleneck()`` already ranks outliers by extremity and
        # truncates to ``top_n``; clamp defensively here as well and
        # render exactly once via ``format_table``.
        limited = outlier_table[:top_n]
        click.echo(f"Top {len(limited)} Outlier Quanta:")
        click.echo("=" * 60)
        click.echo(format_table(limited))
        click.echo()


@main.command()
@click.argument('graph', required=False, nargs=-1)
@click.option('--output', '-o', 'output', required=True,
              type=click.Path(dir_okay=False, path_type=str),
              help='Output Parquet cache path (required).')
@click.pass_context
def preprocess(ctx: click.Context, graph: tuple[str, ...], output: str) -> None:
    """Preprocess quantum data into a cached Parquet table.

    Loads quantum data from any supported input mode (positional graph
    files, positional cached .parquet tables, Butler collections, or any
    ordered mix of the --graph/--repo+--collection/--table source flags),
    flattens it, and writes the runtime table to the Parquet path
    given by --output/-o (including a dims map column and file metadata:
    schema version, runtime_analyzer version, sources, row count, creation
    timestamp, and a run fingerprint).

    The metadata ``sources`` lists the ordered source labels: the file
    path for graph/table entries and ``"repo:collection"`` for Butler
    entries.

    When the inputs are already cached tables this re-writes/merges them
    into a single compact cache (first-wins precedence).  Subsequent
    queries can then skip graph loading entirely by passing the output
    file positionally, e.g. ``runtime-analyzer run.parquet summary``.
    """
    ctx.obj["graph"] = graph

    analyzer = _load_analyzer(ctx)
    click.echo(f"Loaded {analyzer.n_sources} source(s), "
               f"{analyzer.n_loaded} quanta.")

    labels = ctx.obj.get('source_labels', [])
    # _save_cache computes hash_graph(analyzer) once and returns it, so the
    # fingerprint echo below reuses the exact recorded value without taking
    # a second pass over the data.
    fingerprint, _n_rows = _save_cache(analyzer, str(output), labels)
    click.echo(f"Fingerprint: {fingerprint}")


@main.command()
@click.argument('graph', required=False, nargs=-1)
@click.option('--plots', '-p', default=None,
              help='Comma-separated plot names: ' + ', '.join(get_available_plots()))
@click.option('--layout', '-l', 'layout', default=None,
              type=click.Choice(['overview']), help='Pre-made layout to render.')
@click.option('--task', '-t', 'task', default=None, help='Filter to a specific task label.')
@click.option('--format', 'fmt', default='png',
              type=click.Choice(['png', 'pdf', 'svg']), help='Output format.')
@click.option('--output-plot', '-o', 'output_dir', default=None, help='Output directory.')
@click.option('--by', 'dimension', default=None, help='Dimension for dimension distribution plot.')
@click.option('--log/--linear', 'log_scale', default=None,
              help='Force log or linear y scale for box plots '
                   '(default: auto, i.e. log at a dynamic range of '
                   '100x or more).')
@click.pass_context
def plot(ctx: click.Context, graph: tuple[str, ...], plots: str | None, layout: str | None,
         task: str | None, fmt: str, output_dir: str | None, dimension: str | None,
         log_scale: bool | None) -> None:
    """Render plots to files.

    Renders specified visualization plots and saves them to the output
    directory as PNG, PDF, or SVG files.
    """
    ctx.obj["graph"] = graph

    analyzer = _load_analyzer(ctx)
    click.echo(_load_header(analyzer, _header_sources(ctx, graph)))
    tasks = [task] if task else None

    plot_names: list[str] = []
    if layout:
        plot_names = [layout]
    elif plots:
        plot_names = [p.strip() for p in plots.split(',')]
    else:
        click.echo("Nothing to plot. Use --plots or --layout.")
        return

    y_scale = 'auto' if log_scale is None else ('log' if log_scale else 'linear')
    _render_plots(analyzer, plot_names, tasks, output_dir, fmt,
                  dimension=dimension, y_scale=y_scale)


@main.command('help-plots')
@click.pass_context
def help_plots(ctx: click.Context) -> None:
    """List all available plot names with descriptions."""
    click.echo("Available plot names:")
    for name, desc in PLOT_NAMES.items():
        click.echo(f"  {name:15s}  {desc}")


@main.command('help-tables')
@click.pass_context
def help_tables(ctx: click.Context) -> None:
    """List all available table subcommands with descriptions."""
    click.echo("Available table subcommands:")
    for name, desc in TABLE_NAMES.items():
        click.echo(f"  {name:20s}  {desc}")


# ---- Tracked run subcommand group ----

tracked_run_group = click.Group(
    "tracked-run",
    help="Track, compare, and analyze runtime performance across multiple runs.",
)


@tracked_run_group.command("record", cls=_SourceCommand)
@click.option('--label', '-l', 'label', required=True, help='Human-readable label for this run.')
@click.option('--repo', '-r', 'repo', default=None, expose_value=False,
              help='Butler repository path or alias. Pairs with the next '
                   '--collection (pair-close binding).')
@click.option('--collection', '-c', 'collection', multiple=True,
              expose_value=False,
              help='Butler collection name. May be specified multiple times; '
                   'each binds to the nearest preceding --repo.')
@click.option('--graph', '-g', 'graph_flag', multiple=True,
              expose_value=False,
              help='Provenance quantum graph file (.qg, .fits, ...). May be '
                   'specified multiple times; loads as a graph whatever its '
                   'extension.')
@click.option('--table', '-T', 'table_flag', multiple=True,
              expose_value=False,
              help='Cached per-quantum Parquet table. May be specified '
                   'multiple times; loads via the table reader.')
@click.argument('graph', required=False, nargs=-1)
@click.option('--raw', is_flag=True, default=False,
              help='Also persist individual quantum-level data for Mann-Whitney U tests.')
@click.pass_context
def tracked_run_record(ctx: click.Context, label: str, graph: tuple[str, ...],
                       raw: bool) -> None:
    """Record a run to the tracker database.

    Loads quantum data from graph files, Butler collections, or cached
    per-quantum Parquet tables — positionally or via the ordered source
    flags (--graph/-g, --repo/-r with --collection/-c, --table/-T) —
    computes summary statistics, and persists the run for later comparison
    and trend analysis.

    Record-level source flags form their own ordered list that fully
    overrides the group-level flags; with no record-level flags the
    group-level ordered list is used (documented usage:
    ``runtime-analyzer --repo R --collection C tracked-run record -l L``).

    The recorded ``repo``/``collection`` columns hold the FIRST ``butler``
    entry of the effective ordered source list (multiple butler sources
    record the first); runs loaded with no Butler source record ``None``
    for both.
    """
    import time

    ctx.obj["graph"] = graph

    # Record-level flags live on this command's ``ctx.params`` (built by
    # the record-scoped callbacks) and override the group-level list
    # entirely when any record-level source flag is given; otherwise fall
    # back to the group ``ctx.obj['_sources']``.
    record_sources = list(ctx.obj.get('_record_sources', []) or [])
    record_flags = bool(record_sources) or (
        ctx.obj.get('_record_pending_repo') is not None
    )
    flag_sources = record_sources if record_flags else _flag_sources_of(
        ctx, '_sources'
    )

    # Extension dispatch applies only to positionals.
    positional_entries = _positional_entries(list(graph))
    _check_source_conflict(
        positional_entries,
        ctx,
        sources_key='_sources',
        pending_keys=('_pending_repo', '_record_pending_repo'),
    )

    if positional_entries:
        sources: list[Source] = positional_entries
    elif flag_sources:
        sources = flag_sources
    else:
        raise click.UsageError(
            "Must provide at least one positional graph file or "
            "--repo with --collection flags."
        )
    labels = [_source_label(s) for s in sources]

    analyzer = _load_ordered(sources)

    click.echo(_load_header(analyzer, sources))

    # Compute summary
    summary = analyzer.summary(status=None, task_label=ctx.obj.get('task'))

    # Generate run_id and graph hash.  hash_graph understands analyzer-like
    # objects directly (it fingerprints task-label counts from the
    # analyzer's table),
    # so the analyzer itself is the canonical hash input.
    graph_hash = hash_graph(analyzer)

    run_id = f"{label}_{int(time.time() * 1000)}"
    from lsst.pipe.base.version import __version__
    analyzer_version = __version__

    # Get raw quanta data if requested
    raw_data = None
    if raw:
        qt = analyzer.table
        row_labels = qt.labels()
        qids = qt.quantum_id_list()
        statuses = qt.status_codes
        memory_all = qt.memory
        prep_all = qt.prep_time
        init_all = qt.init_time
        run_all = qt.run_time
        rtc_all = qt.run_time_cpu
        data_ids = qt.data_id_list()
        raw_data = [
            {
                "task_label": row_labels[i],
                "quantum_id": qids[i],
                "status": int(statuses[i]),
                "memory": float(memory_all[i]),
                "prep_time": float(prep_all[i]),
                "init_time": float(init_all[i]),
                "run_time": float(run_all[i]),
                "run_time_cpu": float(rtc_all[i]),
                "data_id": data_ids[i],
            }
            for i in range(qt.n_rows)
        ]

    # Record run.  repo/collection come from the FIRST butler entry of the
    # effective ordered source list; no butler entry -> None/None.
    butler = [s for s in sources if s[0] == 'butler']
    repo = butler[0][1] if butler else None
    collection_str = butler[0][2] if butler else None
    result = record_run(
        label=label,
        run_id=run_id,
        repo=repo,
        collection=collection_str,
        graph_hash=graph_hash,
        analyzer_version=analyzer_version,
        summary_table=summary,
        raw_quanta_data=raw_data,
    )

    click.echo(f"Saved run '{label}' ({result['quanta']} quanta, {result['tasks']} tasks)")

    # Honor the group-level --save-intermediate-table flag after a
    # successful record, writing the cache with the same _save_cache helper
    # (and fingerprint) used by _load_analyzer/preprocess.
    save_path = ctx.obj.get('save_intermediate_table')
    if save_path:
        _save_cache(analyzer, str(save_path), labels)

    # Auto-compare
    from .tracker.history import _auto_compare
    _auto_compare(label)


@tracked_run_group.command("list")
@click.option('--limit', '-n', 'limit', default=20, type=int, help='Maximum number of runs to show.')
@click.option('--task', '-t', 'task_filter', default=None, help='Filter by task name pattern.')
@click.pass_context
def tracked_run_list(ctx: click.Context, limit: int, task_filter: str | None) -> None:
    """List recorded runs."""
    from .tracker.format import format_run_list
    runs = get_runs(limit=limit, task_filter=task_filter)
    click.echo(format_run_list(runs))


@tracked_run_group.command("compare")
@click.option('--from', 'from_label', required=True, help='Baseline run label.')
@click.option('--to', 'to_label', required=True, help='New run label.')
@click.option('--task', '-t', 'task_filter', default=None, help='Filter to a specific task.')
@click.option('--metric', '-m', 'metric', default='p50',
              type=click.Choice(['p50', 'p95', 'p25', 'p75', 'mean_rt', 'max_rt', 'std_rt']),
              help='Metric to compare.')
@click.option('--diff', 'diff_mode', default='percent', type=click.Choice(['percent', 'absolute']),
              help='Delta display mode.')
@click.option('--full', is_flag=True, default=False,
              help='Perform Mann-Whitney U test (requires raw data).')
@click.option('--plot', '-p', 'render_plot', is_flag=True, default=False,
              help='Render delta bar chart.')
@click.pass_context
def tracked_run_compare(ctx: click.Context, from_label: str, to_label: str,
                        task_filter: str | None, metric: str, diff_mode: str,
                        full: bool, render_plot: bool) -> None:
    """Compare two recorded runs."""
    from .tracker import database
    from .tracker import plot as tracker_plot
    from .tracker.format import format_comparison_table

    comparisons = compare_runs(from_label, to_label, metric=metric, diff_mode=diff_mode)

    added, removed, shared = find_task_changes(from_label, to_label)

    if task_filter:
        comparisons = [c for c in comparisons if task_filter in c["task_label"]]
        if added:
            added = set(t for t in added if task_filter in t)
        if removed:
            removed = set(t for t in removed if task_filter in t)

    if not comparisons and not added and not removed:
        raise click.ClickException(
            f"No shared tasks between run '{from_label}' and run '{to_label}'"
        )

    from_run = database.get_run_by_label(from_label)
    to_run = database.get_run_by_label(to_label)
    if from_run is None or to_run is None:  # pragma: no cover
        raise click.ClickException(
            f"No run found with label {from_label!r} or {to_label!r}"
        )

    if full:
        # Check for raw data
        for run_id, rlabel in [(to_run["run_id"], to_label), (from_run["run_id"], from_label)]:
            count = database.create_connection().execute(
                "SELECT COUNT(*) FROM quanta_raw WHERE run_id = ?", (run_id,)
            ).fetchone()[0]
            database.create_connection().close()
            if count == 0:
                raise click.ClickException(
                    f"--full requires raw data from both runs. Run '{rlabel}' has no raw quantum data. "
                    f"Re-run with --raw flag."
                )

    click.echo(format_comparison_table(
        from_run, to_run, comparisons, sorted(added), sorted(removed),
    ))

    if render_plot and comparisons:
        task_labels = [c["task_label"] for c in comparisons]
        delta_pcts = [c["delta_pct"] for c in comparisons]
        fig = tracker_plot.plot_delta_bar(task_labels, delta_pcts)
        path = tracker_plot.save_tracker_plot(fig, "delta")
        click.echo(f"Saved plot: {path}")


@tracked_run_group.command("trend")
@click.option('--task', '-t', 'task_label', required=True, help='Task to analyze.')
@click.option('--metric', '-m', 'metric', default='p50',
              type=click.Choice(['p50', 'p95', 'p25', 'p75', 'mean_rt', 'max_rt', 'std_rt']),
              help='Metric to track.')
@click.option('--plot', '-p', 'render_plot', is_flag=True, default=False,
              help='Render trend line plot.')
@click.pass_context
def tracked_run_trend(ctx: click.Context, task_label: str, metric: str,
                      render_plot: bool) -> None:
    """Analyze metric trend across runs for a task."""
    from .tracker import plot as tracker_plot
    from .tracker.format import format_trend_table

    trend = get_trend(task_label, metric=metric)

    click.echo(format_trend_table(task_label, metric, trend))

    if render_plot and trend.get("run_data"):
        run_labels = [rd["label"] for rd in trend["run_data"]]
        values = [rd["value"] for rd in trend["run_data"]]
        timestamps = [rd["timestamp"] for rd in trend["run_data"]]
        fig = tracker_plot.plot_trend(task_label, metric, run_labels, values, timestamps)
        path = tracker_plot.save_tracker_plot(fig, "trend")
        click.echo(f"Saved plot: {path}")


@tracked_run_group.command("alerts")
@click.option('--task', '-t', 'task_filter', default=None, help='Filter by task name.')
@click.option('--since', default=None, help='Only show alerts from this run onward.')
@click.option('--metric', '-m', 'metric', default='p50',
              type=click.Choice(['p50', 'p95', 'p25', 'p75', 'mean_rt', 'max_rt', 'std_rt']),
              help='Metric to evaluate.')
@click.pass_context
def tracked_run_alerts(ctx: click.Context, task_filter: str | None,
                       since: str | None, metric: str) -> None:
    """Detect statistically significant performance changes."""
    from .tracker.format import format_alerts
    alerts = check_alerts(
        from_run_label=since,
        task_filter=task_filter,
        metric=metric,
    )
    if not alerts:
        click.echo("No significant changes detected.")
    else:
        click.echo(format_alerts(alerts))


# Register the tracked-run group
main.add_command(tracked_run_group)
