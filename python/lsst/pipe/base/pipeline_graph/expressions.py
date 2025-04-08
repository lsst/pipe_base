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
"""Expressions that resolve to subsets of pipelines.

See :ref:`pipeline-graph-subset-expressions`.
"""

from __future__ import annotations

__all__ = (
    "DirectionNode",
    "IdentifierNode",
    "IntersectionNode",
    "Node",
    "NotNode",
    "UnionNode",
    "parse",
)

import dataclasses
import functools
from typing import TYPE_CHECKING, Any, Literal, TypeAlias

from lsst.daf.butler.registry.queries.expressions.parser.ply import lex, yacc

from ._exceptions import InvalidExpressionError

if TYPE_CHECKING:
    from lsst.daf.butler.registry.queries.expressions.parser.parserLex import LexToken
    from lsst.daf.butler.registry.queries.expressions.parser.parserYacc import YaccProduction


class _ParserLex:
    @classmethod
    def make_lexer(cls) -> Any:  # unspecified PLY type.
        return lex.lex(object=cls())

    tokens = (
        "IDENTIFIER",
        "LPAREN",
        "RPAREN",
        "NOT",
        "UNION",
        "INTERSECTION",
        "LT",
        "LE",
        "GT",
        "GE",
    )

    t_LPAREN = r"\("
    t_RPAREN = r"\)"
    t_NOT = "~"
    t_UNION = r"\|"
    t_INTERSECTION = "&"
    t_LT = "<"
    t_LE = "<="
    t_GT = ">"
    t_GE = ">="

    # Identifiers are alphanumeric, and may have a T:, D:, or S: prefix.
    def t_IDENTIFIER(self, t: LexToken) -> LexToken:
        r"""([TDS]:)?\w+"""
        t.type = "IDENTIFIER"
        return t

    # Ignore spaces and tables.
    t_ignore = " \t"

    def t_error(self, t: LexToken) -> LexToken:
        raise InvalidExpressionError(
            f"invalid token in expression near character {t.lexer.lexpos}: {t.value[0]!r}"
        )


class _ParserYacc:
    def __init__(self) -> None:
        self.parser = self._parser_factory()

    @staticmethod
    @functools.cache
    def _parser_factory() -> Any:  # unspecified PLY type.
        return yacc.yacc(module=_ParserYacc, write_tables=False, debug=False)

    def parse(self, input: str) -> Node:
        """Parse input expression and return the parsed tree object.

        Parameters
        ----------
        input : `str`
            Expression to parse.

        Returns
        -------
        node : `Node`
            Root of the parsed expression tree.
        """
        lexer = _ParserLex.make_lexer()
        tree = self.parser.parse(input=input, lexer=lexer)
        return tree

    tokens = _ParserLex.tokens[:]

    start = "expr"

    precedence = (
        ("left", "UNION"),
        ("left", "INTERSECTION"),
        ("right", "NOT", "LT", "LE", "GT", "GE"),
    )

    # Ruff wants 'noqa' on the doc line, pydocstyle wants it on the function.

    @classmethod
    def p_expr_union(cls, p: YaccProduction) -> None:  # noqa: D403
        """expr : expr UNION expr"""  # noqa: D403
        p[0] = UnionNode(lhs=p[1], rhs=p[3])

    @classmethod
    def p_expr_intersection(cls, p: YaccProduction) -> None:  # noqa: D403
        """expr : expr INTERSECTION expr"""  # noqa: D403
        p[0] = IntersectionNode(lhs=p[1], rhs=p[3])

    @classmethod
    def p_expr_not(cls, p: YaccProduction) -> None:  # noqa: D403
        """expr : NOT expr"""  # noqa: D403
        p[0] = NotNode(operand=p[2])

    @classmethod
    def p_expr_parens(cls, p: YaccProduction) -> None:  # noqa: D403
        """expr : LPAREN expr RPAREN"""  # noqa: D403
        p[0] = p[2]

    @classmethod
    def p_expr_inequality(cls, p: YaccProduction) -> None:  # noqa: D403
        """expr : LT identifier
        | LE identifier
        | GT identifier
        | GE identifier
        """  # noqa: D403
        p[0] = DirectionNode(operator=p[1], start=p[2])

    @classmethod
    def p_expr_identifier(cls, p: YaccProduction) -> None:  # noqa: D403
        """expr : identifier"""  # noqa: D403
        p[0] = p[1]

    @classmethod
    def p_identifier_qualified(cls, p: YaccProduction) -> None:  # noqa: D403, D401
        """identifier : IDENTIFIER"""  # noqa: D403, D401
        match p[1].split(":"):
            case [qualifier, label]:
                p[0] = IdentifierNode(qualifier=qualifier, label=label)
            case [label]:
                p[0] = IdentifierNode(qualifier=None, label=label)
            case _:  # pragma: no cover
                raise AssertionError("Unexpected identifier form.")

    @classmethod
    def p_error(cls, p: YaccProduction | None) -> None:
        if p is None:
            raise InvalidExpressionError("Expression ended unexpectedly.")
        else:
            raise InvalidExpressionError(f"Syntax error near character {p.lexpos}: {p.value!r}")


@dataclasses.dataclass
class IdentifierNode:
    """A node that corresponds to a task label, dataset type name, or labeled
    subset.
    """

    qualifier: Literal["T", "D", "S"] | None
    """Qualiifier that indicates whether this is a task (T), dataset type (T),
    or labeled subset (S).

    Unqualified identifiers (`None`) must resolve unambiguously.
    """

    label: str
    """Task label, dataset type name, or subset label."""


@dataclasses.dataclass
class DirectionNode:
    """A node that represents the ancestors or descendents of a task label or
    dataset type.
    """

    operator: Literal["<", ">", "<=", ">="]
    """Which direction to traverse the graph ('>' for descendents, '<' for
    ancestors), and whether to include the operand ('=') or not.
    """

    start: IdentifierNode
    """Node at which to start the DAG traversal."""


@dataclasses.dataclass
class NotNode:
    """A node that represents set inversion (including all elements not in the
    operand).
    """

    operand: Node
    """Node representing the set to invert."""


@dataclasses.dataclass
class UnionNode:
    """Node representing a set union."""

    lhs: Node
    rhs: Node


@dataclasses.dataclass
class IntersectionNode:
    """Node representing a set intersection."""

    lhs: Node
    rhs: Node


def parse(expression: str) -> Node:
    """Parse an expression into a `Node` tree.

    Parameters
    ----------
    expression : `str`
        String expression to parse.  See
        :ref:`pipeline-graph-subset-expressions`.

    Returns
    -------
    node
        Root node of the parsed expression tree.

    Raises
    ------
    InvalidExpressionError
        Raised if the expression could not be parsed.
    """
    return _ParserYacc().parse(expression)


Node: TypeAlias = IdentifierNode | DirectionNode | NotNode | UnionNode | IntersectionNode
