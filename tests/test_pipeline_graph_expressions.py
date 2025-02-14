# This file is part of pipe_base.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
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
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from __future__ import annotations

import unittest

import lsst.pipe.base.pipeline_graph.expressions as pge
import lsst.utils.tests
from lsst.pipe.base.pipeline_graph import InvalidExpressionError


class PipelineGraphExpressionParserTestCase(unittest.TestCase):
    """Test for parsing the small expression language on pipeline subsets."""

    def test_identifiers(self):
        """Test identifiers of various forms."""
        self.assertEqual(pge.parse("a_2"), pge.IdentifierNode(qualifier=None, label="a_2"))
        self.assertEqual(pge.parse("T:b_3"), pge.IdentifierNode(qualifier="T", label="b_3"))
        self.assertEqual(pge.parse("D:c_4"), pge.IdentifierNode(qualifier="D", label="c_4"))
        self.assertEqual(pge.parse("S:d_5"), pge.IdentifierNode(qualifier="S", label="d_5"))
        with self.assertRaises(InvalidExpressionError):
            pge.parse("a-3")
        with self.assertRaises(InvalidExpressionError):
            pge.parse("G:d1")

    def test_directions(self):
        """Test ancestor/descendent expressions."""
        self.assertEqual(pge.parse("<a"), pge.DirectionNode("<", pge.IdentifierNode(None, "a")))
        self.assertEqual(pge.parse(">a"), pge.DirectionNode(">", pge.IdentifierNode(None, "a")))
        self.assertEqual(pge.parse("<= a"), pge.DirectionNode("<=", pge.IdentifierNode(None, "a")))
        self.assertEqual(pge.parse(">= a"), pge.DirectionNode(">=", pge.IdentifierNode(None, "a")))

    def test_binary(self):
        """Test binary operators, including precedence."""
        self.assertEqual(
            pge.parse("a | b"),
            pge.UnionNode(pge.IdentifierNode(None, "a"), pge.IdentifierNode(None, "b")),
        )
        self.assertEqual(
            pge.parse("a & b"),
            pge.IntersectionNode(pge.IdentifierNode(None, "a"), pge.IdentifierNode(None, "b")),
        )
        self.assertEqual(
            pge.parse("a | b & c"),
            pge.UnionNode(
                pge.IdentifierNode(None, "a"),
                pge.IntersectionNode(pge.IdentifierNode(None, "b"), pge.IdentifierNode(None, "c")),
            ),
        )
        self.assertEqual(
            pge.parse("a & b | c"),
            pge.UnionNode(
                pge.IntersectionNode(pge.IdentifierNode(None, "a"), pge.IdentifierNode(None, "b")),
                pge.IdentifierNode(None, "c"),
            ),
        )

    def test_not(self):
        """Test set inversion."""
        self.assertEqual(
            pge.parse("~a"),
            pge.NotNode(pge.IdentifierNode(None, "a")),
        )
        self.assertEqual(
            pge.parse("~<b"),
            pge.NotNode(pge.DirectionNode("<", pge.IdentifierNode(None, "b"))),
        )

    def test_complex(self):
        """Test a large expression with nested parentheses."""
        self.assertEqual(
            pge.parse("(a & ~(b | <c)) | ~>=d"),
            pge.UnionNode(
                pge.IntersectionNode(
                    pge.IdentifierNode(None, "a"),
                    pge.NotNode(
                        pge.UnionNode(
                            pge.IdentifierNode(None, "b"),
                            pge.DirectionNode("<", pge.IdentifierNode(None, "c")),
                        )
                    ),
                ),
                pge.NotNode(pge.DirectionNode(">=", pge.IdentifierNode(None, "d"))),
            ),
        )

    def test_lex_errors(self):
        """Test expressions that should cause a lexer error."""
        with self.assertRaisesRegex(InvalidExpressionError, "near character 9: '!'"):
            pge.parse("frobbler !b")

    def test_parser_errors(self):
        """Test expressions that should cause a parser error."""
        with self.assertRaisesRegex(InvalidExpressionError, "near character 2: '|'"):
            pge.parse("< | c")
        with self.assertRaisesRegex(InvalidExpressionError, "Expression ended"):
            pge.parse("d |")


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
