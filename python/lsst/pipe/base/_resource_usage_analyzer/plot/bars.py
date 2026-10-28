"""Top-N horizontal bar chart of the worst quanta by metric."""

from __future__ import annotations

import numpy as np

from ..core import QuantumRuntimeAnalyzer
from ._style import (
    _columns,
    _ensure_matplotlib,
    _metric_label,
    matplotlib,
    plt,
)


def plot_top_n_bar(
    analyzer: QuantumRuntimeAnalyzer,
    n: int = 10,
    metric: str = "run_time",
    color_by: str | None = None,
    ax: matplotlib.axes.Axes | None = None,
) -> matplotlib.figure.Figure:
    """Produce a horizontal bar chart of the top-N quanta by metric.

    Parameters
    ----------
    analyzer : `QuantumRuntimeAnalyzer`
        The analyzer instance.
    n : `int`, optional
        Number of top quanta to show.
    metric : `str`, optional
        Metric to rank by.
    color_by : `str` or `None`, optional
        If "task", color bars by task label.
    ax : `matplotlib.axes.Axes` or `None`, optional
        Axis to draw into. If None, a new figure is created.

    Returns
    -------
    fig : `matplotlib.figure.Figure`
        Bar chart figure.
    """
    _ensure_matplotlib()

    data = _columns(analyzer.table)
    if len(data['task_label']) == 0:
        own_fig = ax is None
        if own_fig:
            fig, ax = plt.subplots()
        else:
            fig = ax.figure
        ax.text(0.5, 0.5, 'No data available', ha='center', va='center')
        return fig

    sorted_indices = np.argsort(-data[metric])[:n]
    order = sorted_indices[::-1]  # ascending for horizontal bars

    labels = []
    for idx in order:
        raw_id = str(data['data_id'][idx] or '')
        # Ids are padded to binary(16): drop trailing NULs so short ids
        # render as a compact hex tail.
        tail_hex = data['quantum_id'][idx][-4:].tobytes().rstrip(
            b"\x00").hex()
        if raw_id:
            data_id = raw_id[:40] + ('...' if len(raw_id) > 40 else '')
            label = (f"{data['task_label'][idx]}: {data_id}"
                     f" [{tail_hex}]")
        else:
            label = (f"{data['task_label'][idx]}"
                     f" [{tail_hex}]")
        # Cap total label length so the y-text column cannot dominate the
        # figure: trim the middle (keep the task head and the uuid tail).
        if len(label) > 70:
            hex_pos = label.rindex(' [')
            head, hex_part = label[:hex_pos], label[hex_pos:]
            keep = max(20, 67 - len(hex_part))
            label = head[:keep] + '...' + hex_part
        labels.append(label)
    values = data[metric][order]

    own_fig = ax is None
    if own_fig:
        fig, ax = plt.subplots(figsize=(10, min(max(4, n * 0.45), 30.0)))
    else:
        fig = ax.figure

    if color_by == "task":
        task_labels = data['task_label'][order]
        unique_tasks = sorted(set(task_labels))
        task_colors = {t: plt.cm.tab20(i / max(len(unique_tasks) - 1, 1))
                       for i, t in enumerate(unique_tasks)}
        colors = [task_colors[tl] for tl in task_labels]
        ax.barh(labels, values, color=colors)
    else:
        ax.barh(labels, values)

    ax.tick_params(axis='y', labelsize=min(9.0, max(6.0, 300.0 / n)))
    # Value labels at bar ends (with headroom so they never clip).
    vmax = float(np.nanmax(values)) if len(values) else 0.0
    if vmax > 0:
        def fmt(v: float) -> str:
            """Format a bar value for display."""
            if v >= 10:
                return f'{v:,.0f}'
            return f'{v:.1f}'

        for bar in ax.containers[0].patches:
            v = bar.get_width()
            if np.isfinite(v):
                ax.text(v + vmax * 0.01, bar.get_y() + bar.get_height() / 2,
                        fmt(v), va='center', fontsize=7)
        ax.set_xlim(0, vmax * 1.14)
    ax.grid(True, axis='x', alpha=0.3, linewidth=0.5)
    ax.set_axisbelow(True)
    ax.set_xlabel(_metric_label(metric))
    ax.set_title(f'Top {n} Quanta by {_metric_label(metric)}')
    if own_fig:
        fig.tight_layout()
    return fig
