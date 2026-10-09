"""Pure display/formatting helpers for the runtime-analyzer CLI.

Only content that no ``unittest.mock`` target can reach lives here:
the test suite patches the CLI seam (``cli.extract_merged_runtime_table``,
``cli.QuantumRuntimeTable.from_parquets``/``from_parquet``,
``cli._load_analyzer``,
``cli.record_run``, ``cli.get_plot_by_name``), so every name in those
patched names' call chains stays in ``cli.py`` proper.  The module-level
display maps below are read only by the (never-patched) ``help-plots``
and ``help-tables`` command bodies and are re-imported by ``cli``.
"""

from __future__ import annotations

__all__ = [
    "PLOT_NAMES",
    "TABLE_NAMES",
]

PLOT_NAMES = {
    'box': 'Task box plot showing runtime distribution per task',
    'scatter': 'Memory vs run time scatter plot (log-log)',
    'bar': 'Top-N bar chart of worst quanta',
    'stacked': 'Stacked bar chart of prep/init/run time segments',
    'dim': 'Dimension distribution plot',
    'hist': 'Histogram of run times per task',
    'percentile': 'Percentile curve (p0-p95) per task',
    'overview': '2x2 overview layout with key charts',
    'trend': 'Trend line plot showing metric progression across runs',
    'heatmap': 'Heatmap comparing metrics across tasks and runs',
    'delta': 'Delta bar chart showing percentage change per task',
}

TABLE_NAMES = {
    'summary': 'Task-level aggregate statistics',
    'top-quantities': 'Ranked quanta by metric',
    'dimension': 'Dimension-binned performance distribution',
    'bottleneck': 'Bottleneck diagnosis at task and quantum level',
}
