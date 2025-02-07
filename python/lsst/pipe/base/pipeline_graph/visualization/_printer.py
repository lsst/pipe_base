# This file is part of pipe_base.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
from __future__ import annotations

__all__ = ("Printer", "make_colorama_printer", "make_default_printer", "make_simple_printer")

import sys
from collections.abc import Callable, Sequence
from typing import Generic, TextIO

from ._layout import _K, Layout, LayoutRow

_CHAR_DECOMPOSITION = {
    # This mapping provides the "logic" for how to decompose the relevant
    # box-drawing symbols into symbols with just a single line segment,
    # allowing us to do set-operations on just the single-segment characters.
    " ": frozenset(),
    "╴": frozenset({"╴"}),
    "╵": frozenset({"╵"}),
    "╶": frozenset({"╶"}),
    "╷": frozenset({"╷"}),
    "╯": frozenset({"╴", "╵"}),
    "─": frozenset({"╴", "╶"}),
    "╮": frozenset({"╴", "╷"}),
    "╰": frozenset({"╵", "╶"}),
    "│": frozenset({"╵", "╷"}),
    "╭": frozenset({"╶", "╷"}),
    "┴": frozenset({"╴", "╵", "╶"}),
    "┤": frozenset({"╴", "╵", "╷"}),
    "┬": frozenset({"╴", "╶", "╷"}),
    "├": frozenset({"╵", "╶", "╷"}),
    "┼": frozenset({"╴", "╵", "╶", "╷"}),
}

_CHAR_COMPOSITION = {v: k for k, v in _CHAR_DECOMPOSITION.items()}


class PrintRow:
    """Base class and default implementation for drawing a single row of a text
    DAG visualization.

    Parameters
    ----------
    width : `int`
        Number of columns the graph will occupy (not including text
        descriptions on the right).
    pad : `str`
        Character to use for empty cells.
    """

    def __init__(self, width: int, pad: str):
        self._cells = [pad] * width

    def set(self, x: int, char: str, style: tuple[str, str] = ("", "")) -> None:
        """Set the character in a single cell, overriding what is there now.

        Parameters
        ----------
        x : `int`
            Column index.
        char : `str`
            Single character value to set.
        style : `tuple` [ `str`, `str` ], optional
            Styling prefix and suffix strings.  Ignored by the base class.
        """
        self._cells[x] = char

    def vert(self, x: int, style: tuple[str, str] = ("", "")) -> None:
        """Add a vertical line, taking into account the current contents of the
        cell.

        Parameters
        ----------
        x : `int`
            Column index.
        style : `tuple` [ `str`, `str` ], optional
            Styling prefix and suffix strings.  Ignored by the base class.

        Notes
        -----
        This merges the vertical line with any other character other than a
        complete complete horizontal segment, which it replaces to represent a
        non-intersection as a visual "hop", i.e. ``───`` becomes ``─│─``.
        """
        if self._cells[x] in (" ", "─"):
            self.set(x, "│", style)
        else:
            self.update(x, "│", style)

    def update(self, x: int, char: str, style: tuple[str, str] = ("", "")) -> None:
        """Combine a new line-drawing character with the line segments already
        present in a cell.

        Parameters
        ----------
        x : `int`
            Column index.
        char : `str`
            Single character value to add.
        style : `tuple` [ `str`, `str` ], optional
            Styling prefix and suffix strings.  Ignored by the base class.
        """
        self.set(x, _CHAR_COMPOSITION[_CHAR_DECOMPOSITION[char] | _CHAR_DECOMPOSITION[self._cells[x]]], style)

    def bend(
        self,
        start: int,
        stop: int,
        start_style: tuple[str, str] = ("", ""),
        stop_style: tuple[str, str] = ("", ""),
    ) -> None:
        """Draw a sideways-S bend representing a connection in one column from
        above to a different column below.

        Parameters
        ----------
        start : `int`
            Incoming column index (connecting from the previous line, above).
        stop : `int`
            Outgoing column index (connecting to the next line, below).
        start_style : `tuple` [ `str`, `str` ], optional
            Styling prefix and suffix strings.  Ignored by the base class.
        stop_style : `tuple` [ `str`, `str` ], optional
            Styling prefix and suffix strings.  Ignored by the base class.

        Notes
        -----
        If the incoming and outgoing columns are the same, this yields a
        vertical line.
        """
        if start < stop:
            self.update(start, "╰", start_style)
            self.update(stop, "╮", stop_style)
            for x in range(start + 1, stop):
                self.update(x, "─", stop_style)
        elif start > stop:
            self.update(start, "╯", start_style)
            self.update(stop, "╭", stop_style)
            for x in range(stop + 1, start):
                self.update(x, "─", stop_style)
        else:
            self.update(start, "│", start_style)

    def finish(self) -> str:
        """Return the line printer's internal state into a string."""
        return "".join(self._cells)


def _default_get_text(node: _K, x: int, style: tuple[str, str]) -> str:
    """Return the default text to associate with a node.

    This function is the default value for the ``get_text`` argument to
    `Printer`; see that for details.
    """
    return str(node)


def _default_get_symbol(node: _K, x: int) -> str:
    """Return the default symbol for a node.

    This function is the default value for the ``get_symbol`` argument to
    `Printer`; see that for details.
    """
    return "⬤"


def _default_get_style(node: _K, x: int) -> tuple[str, str]:
    """Get the default styling suffix/prefix strings.

    This function is the default value for the ``get_style`` argument to
    `Printer`; see that for details.
    """
    return "", ""


class Printer(Generic[_K]):
    """High-level tool for drawing a text-based DAG visualization.

    Parameters
    ----------
    layout_width : `int`
        Logical width of the layout.  Actual width of the graph is
        ``layout_width * 2 + 1`` to space out the nodes and make line
        intersections and non-intersections comprehensible.
    pad : `str`, optional
        Character to use for unpopulated cells.
    make_blank_row : `~collections.abc.Callable`
        Callback that returns a new `PrintRow` instance with no new cells
        populated.  Callback arguments are ``(self.width, self.pad)``.  This
        can return a specialization of `PrintRow` to add support for styling.
    get_text : `~collections.abc.Callable`
        Callback that returns the text description for a node.  Arguments
        are the node key, the column in which the node appears, and a
        style prefix/suffix 2-tuple.
    get_symbol : `~collections.abc.Callable`
        Callback that returns the symbol for a node.  Arguments are the node
        key, the column in which the node appears.
    get_style : `~collections.abc.Callable`
        Callback that returns a prefix/suffix style 2-tuple for a node.  Prefix
        and suffix values are strings, but are otherwise subclass-dependent;
        styles are ignored by the base class.
    """

    def __init__(
        self,
        layout_width: int,
        *,
        pad: str = " ",
        make_blank_row: Callable[[int, str], PrintRow] = PrintRow,
        get_text: Callable[[_K, int, tuple[str, str]], str] = _default_get_text,
        get_symbol: Callable[[_K, int], str] = _default_get_symbol,
        get_style: Callable[[_K, int], tuple[str, str]] = _default_get_style,
    ):
        self.width = layout_width * 2 + 1
        self.pad = pad
        self.make_blank_row = make_blank_row
        self.get_text = get_text
        self.get_symbol = get_symbol
        self.get_style = get_style

    def print_row(
        self,
        stream: TextIO,
        layout_row: LayoutRow[_K],
    ) -> None:
        """Print a single row of the DAG visualization to a file-like object.

        Parameters
        ----------
        stream : `TextIO`
            Output stream to write to.
        layout_row : `LayoutRow`
            Struct that indicates the columns in which nodes an edges should
            be drawn.
        """
        node_style = self.get_style(layout_row.node, layout_row.x)
        if layout_row.continuing or layout_row.connecting:
            print_row = self.make_blank_row(self.width, self.pad)
            for x, source in layout_row.connecting:
                print_row.bend(
                    2 * x,
                    2 * layout_row.x,
                    start_style=self.get_style(source, x),
                    stop_style=node_style,
                )
            for x, source, _ in layout_row.continuing:
                print_row.vert(2 * x, self.get_style(source, x))
            stream.write(print_row.finish())
        stream.write("\n")
        print_row = self.make_blank_row(self.width, self.pad)
        for x, source, _ in layout_row.continuing:
            print_row.vert(2 * x, self.get_style(source, x))
        print_row.set(2 * layout_row.x, self.get_symbol(layout_row.node, layout_row.x), node_style)
        stream.write(print_row.finish())
        stream.write(self.pad * 2)
        stream.write(self.get_text(layout_row.node, layout_row.x, node_style))
        stream.write("\n")

    def print(self, stream: TextIO, layout: Layout) -> None:
        """Print the DAG visualization to a file-like object.

        Parameters
        ----------
        stream : `TextIO`
            Output stream to write to.
        layout : `Layout`
            Struct that determines the rows and columns in which nodes and
            edges are drawn.
        """
        for layout_row in layout:
            self.print_row(stream, layout_row)


class TerminalPrintRow(PrintRow):
    """Specialization of `PrintRow` for interactive terminals.

    This adds support for styling (mostly colors) using terminal escape
    sequences.

    Parameters
    ----------
    width : `int`
        Number of columns the graph will occupy (not including text
        descriptions on the right).
    pad : `str`
        Character to use for empty cells.
    """

    def __init__(self, width: int, pad: str):
        super().__init__(width, pad)
        self._styles = [("", "")] * width

    def set(self, x: int, char: str, style: tuple[str, str] = ("", "")) -> None:
        super().set(x, char)
        self._styles[x] = style

    def finish(self) -> str:
        return "".join(f"{prefix}{char}{suffix}" for char, (prefix, suffix) in zip(self._cells, self._styles))


def make_colorama_printer(layout_width: int, palette: Sequence[str] = ()) -> Printer | None:
    """Return a `Printer` instance that uses terminal escape codes provided
    by the `colorama` package.

    Parameters
    ----------
    layout_width : `int`
        Logical width of the layout.  Actual width of the graph is
        ``layout_width * 2 + 1`` to space out the nodes and make line
        intersections and non-intersections comprehensible.
    palette : `~collections.abc.Sequence` [ `str` ], optional
        Sequence of colors, in which each is a single character (any of
        ``rgbcym``), a color name (any of ``red``, ``green``, ``blue``,
        ``cyan``, ``yellow``, or ``magenta``), or one of those color names
        preceded by ``light`` (with no space).  Case is ignored.  If empty,
        a predefined sequence using both light and dark colors is used.

    Returns
    -------
    printer : `Printer` or `None`
        A `Printer` instance, or `None` if the `colorama` package could not
        be imported.
    """
    try:
        import colorama
    except ImportError:
        return None
    if not palette:
        palette = [
            colorama.Fore.RED,
            colorama.Fore.LIGHTBLUE_EX,
            colorama.Fore.GREEN,
            colorama.Fore.LIGHTMAGENTA_EX,
            colorama.Fore.YELLOW,
            colorama.Fore.LIGHTCYAN_EX,
            colorama.Fore.LIGHTRED_EX,
            colorama.Fore.BLUE,
            colorama.Fore.LIGHTGREEN_EX,
            colorama.Fore.MAGENTA,
            colorama.Fore.LIGHTYELLOW_EX,
            colorama.Fore.CYAN,
        ]
    else:
        translate_color = {
            "R": colorama.Fore.RED,
            "RED": colorama.Fore.RED,
            "LIGHTRED": colorama.Fore.LIGHTRED_EX,
            "G": colorama.Fore.GREEN,
            "GREEN": colorama.Fore.GREEN,
            "LIGHTGREEN": colorama.Fore.LIGHTGREEN_EX,
            "B": colorama.Fore.BLUE,
            "BLUE": colorama.Fore.BLUE,
            "LIGHTBLUE": colorama.Fore.LIGHTBLUE_EX,
            "C": colorama.Fore.CYAN,
            "CYAN": colorama.Fore.CYAN,
            "LIGHTCYAN": colorama.Fore.LIGHTCYAN_EX,
            "Y": colorama.Fore.YELLOW,
            "YELLOW": colorama.Fore.YELLOW,
            "LIGHTYELLOW": colorama.Fore.LIGHTYELLOW_EX,
            "M": colorama.Fore.MAGENTA,
            "MAGENTA": colorama.Fore.MAGENTA,
            "LIGHTMAGENTA": colorama.Fore.LIGHTMAGENTA_EX,
        }
        palette = [translate_color.get(c.upper(), c) for c in palette]
    return Printer(
        layout_width,
        make_blank_row=lambda width, pad: TerminalPrintRow(width, pad),
        get_style=lambda node, x: (palette[x % len(palette)], colorama.Style.RESET_ALL),
    )


def make_simple_printer(layout_width: int) -> Printer:
    """Return a simple `Printer` instance with no styling.

    Parameters
    ----------
    layout_width : `int`
        Logical width of the layout.  Actual width of the graph is
        ``layout_width * 2 + 1`` to space out the nodes and make line
        intersections and non-intersections comprehensible.
    """
    return Printer(layout_width)


def make_default_printer(
    layout_width: int, color: bool | Sequence[str] | None = None, stream: TextIO = sys.stdout
) -> Printer:
    """Return a `Printer` instance with terminal escape-code coloring if
    possible (and requested), or a simple one otherwise.

    Parameters
    ----------
    layout_width : `int`
        Logical width of the layout.  Actual width of the graph is
        ``layout_width * 2 + 1`` to space out the nodes and make line
        intersections and non-intersections comprehensible.
    color : `bool` or `~collections.abc.Sequence` [ `str` ]
        Whether to use terminal escape codes to add color to the graph. Default
        is to add colors only if the `colorama` package can be imported.
        `False` disables colors unconditionally, while `True` or a sequence of
        colors (see `make_colorama_printer`) will result in `ImportError` being
        propagated up if `colorama` is unavailable.
    stream : `io.TextIO`, optional
        Output stream the printer will write to.
    """
    if color is None:
        if stream.isatty():
            if printer := make_colorama_printer(layout_width):
                return printer
    elif color:
        palette = color if color is not True else ()
        printer = make_colorama_printer(layout_width, palette)
        if printer is None:
            raise ImportError("Cannot use color unless the 'colorama' module is available.")
        return printer
    return make_simple_printer(layout_width)
