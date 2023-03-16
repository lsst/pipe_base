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

__all__ = ("LinePrinter",)

from typing import TYPE_CHECKING, TextIO, TypeVar

if TYPE_CHECKING:
    from ._layout import Layout, LayoutRow


_K = TypeVar("_K")

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


class LineRow:
    def __init__(self, width: int, pad: str):
        self._cells = [pad] * width

    def set(self, x: int, char: str) -> None:
        self._cells[x] = char

    def vert(self, x: int) -> None:
        if self._cells[x] in (" ", "─"):
            self._cells[x] = "│"
        else:
            self.update(x, "│")

    def update(self, x: int, char: str) -> None:
        self._cells[x] = _CHAR_COMPOSITION[_CHAR_DECOMPOSITION[char] | _CHAR_DECOMPOSITION[self._cells[x]]]

    def bend(self, start: int, stop: int) -> None:
        if start < stop:
            self.update(start, "╰")
            self.update(stop, "╮")
            for x in range(start + 1, stop):
                self.update(x, "─")
        elif start > stop:
            self.update(start, "╯")
            self.update(stop, "╭")
            for x in range(stop + 1, start):
                self.update(x, "─")
        else:
            self.update(start, "│")

    def finish(self) -> str:
        return "".join(self._cells)


class LinePrinter:
    def __init__(self, width: int):
        self.width = width

    def get_pad(self) -> str:
        return " "

    def get_node_text(self, node: _K) -> str:
        return str(node)

    def get_node_symbol(self, node: _K) -> str:
        return "⬤"  # "█"

    def print_row(
        self,
        stream: TextIO,
        layout_row: LayoutRow[_K],
    ) -> None:
        if layout_row.continuing or layout_row.connecting:
            line_row = LineRow(self.width * 2 + 1, self.get_pad())
            for x, _ in layout_row.connecting:
                line_row.bend(2 * x, 2 * layout_row.x)
            for x, _, _ in layout_row.continuing:
                line_row.vert(2 * x)
            stream.write(line_row.finish())
            stream.write("\n")
        line_row = LineRow(self.width * 2 + 1, self.get_pad())
        for x, _, _ in layout_row.continuing:
            line_row.vert(2 * x)
        line_row.set(2 * layout_row.x, self.get_node_symbol(layout_row.node))
        stream.write(line_row.finish())
        stream.write(self.get_pad() * 2)
        stream.write(self.get_node_text(layout_row.node))
        stream.write("\n")

    def print(self, stream: TextIO, layout: Layout) -> None:
        for layout_row in layout:
            self.print_row(stream, layout_row)
