# This file is part of pipe_base.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
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

__all__ = ("Printer", "StyledPrinter", "make_default_printer", "make_colorama_printer")

import sys
from collections.abc import Sequence
from typing import Generic, TextIO

from ._layout import _K, Layout, LayoutRow

_CHAR_DECOMPOSITION = {
    # TODO: bitsets for values would be more efficient
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
    def __init__(self, width: int, pad: str):
        self._cells = [pad] * width

    def set(self, x: int, char: str, style: str = "") -> None:
        self._cells[x] = char

    def vert(self, x: int, style: str = "") -> None:
        if self._cells[x] in (" ", "─"):
            self.set(x, "│", style)
        else:
            self.update(x, "│", style)

    def update(self, x: int, char: str, style: str = "") -> None:
        self.set(x, _CHAR_COMPOSITION[_CHAR_DECOMPOSITION[char] | _CHAR_DECOMPOSITION[self._cells[x]]], style)

    def bend(self, start: int, stop: int, start_style: str = "", stop_style: str = "") -> None:
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
        return "".join(self._cells)


class StyledPrintRow(PrintRow):
    def __init__(self, width: int, pad: str, reset: str = ""):
        super().__init__(width, pad)
        self._styles = [""] * width
        self._reset = reset

    def set(self, x: int, char: str, style: str = "") -> None:
        super().set(x, char)
        self._styles[x] = style

    def finish(self) -> str:
        return "".join(f"{style}{char}{self._reset}" for char, style in zip(self._cells, self._styles))


class Printer(Generic[_K]):
    def __init__(self, width: int):
        self.width = width

    def make_blank_row(self) -> PrintRow:
        return PrintRow(self.width * 2 + 1, self.get_pad())

    def get_pad(self) -> str:
        return " "

    def get_node_text(self, node: _K) -> str:
        return str(node)

    def get_node_symbol(self, node: _K, x: int) -> str:
        return "⬤"

    def get_node_style(self, node: _K, x: int) -> str:
        return ""

    def print_row(
        self,
        stream: TextIO,
        layout_row: LayoutRow[_K],
    ) -> None:
        node_style = self.get_node_style(layout_row.node, layout_row.x)
        if layout_row.continuing or layout_row.connecting:
            line_row = self.make_blank_row()
            for x, source in layout_row.connecting:
                line_row.bend(
                    2 * x,
                    2 * layout_row.x,
                    start_style=self.get_node_style(source, x),
                    stop_style=node_style,
                )
            for x, source, _ in layout_row.continuing:
                line_row.vert(2 * x, self.get_node_style(source, x))
            stream.write(line_row.finish())
            stream.write("\n")
        line_row = self.make_blank_row()
        for x, source, _ in layout_row.continuing:
            line_row.vert(2 * x, self.get_node_style(source, x))
        line_row.set(2 * layout_row.x, self.get_node_symbol(layout_row.node, layout_row.x), node_style)
        stream.write(line_row.finish())
        stream.write(self.get_pad() * 2)
        stream.write(self.get_node_text(layout_row.node))
        stream.write("\n")

    def print(self, stream: TextIO, layout: Layout) -> None:
        for layout_row in layout:
            self.print_row(stream, layout_row)


class StyledPrinter(Printer[_K]):
    def __init__(self, width: int, palette: Sequence[str], reset: str):
        super().__init__(width)
        self._palette = palette
        self._reset = reset

    def make_blank_row(self) -> PrintRow:
        return StyledPrintRow(self.width * 2 + 1, self.get_pad(), self._reset)

    def get_node_style(self, node: _K, x: int) -> str:
        return self._palette[x % len(self._palette)]


def make_colorama_printer(width: int) -> Printer | None:
    try:
        import colorama
    except ImportError:
        return None
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
    return StyledPrinter(width, palette, reset=colorama.Fore.RESET)


def make_default_printer(width: int) -> Printer:
    if sys.stdout.isatty():
        if printer := make_colorama_printer(width):
            return printer
    return Printer(width)
