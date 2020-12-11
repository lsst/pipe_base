# This file is part of pipe_base.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
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
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

"""Tests of things related to the GraphBuilder class."""

import logging
import unittest

from lsst.pipe.base.tests import simpleQGraph
import lsst.utils.tests
from lsst.utils.tests import temporaryDirectory

_LOG = logging.getLogger(__name__)


class GraphBuilderTestCase(unittest.TestCase):

    def testDefault(self):
        """Simple test to verify makeSimpleQGraph can be used to make a Quantum
        Graph."""
        with temporaryDirectory() as root:
            # makeSimpleQGraph calls GraphBuilder.
            butler, qgraph = simpleQGraph.makeSimpleQGraph(root=root)
            # by default makeSimpleQGraph makes a graph with 5 nodes
            self.assertEqual(len(qgraph), 5)

    def testAddInstrumentMismatch(self):
        """Verify that a RuntimeError is raised if the instrument in the user
        query does not match the instrument in the pipeline."""
        with temporaryDirectory() as root:
            pipeline = simpleQGraph.makeSimplePipeline(
                nQuanta=5,
                instrument="lsst.pipe.base.tests.simpleQGraph.SimpleInstrument")
            with self.assertRaises(RuntimeError):
                simpleQGraph.makeSimpleQGraph(root=root,
                                              pipeline=pipeline,
                                              userQuery="instrument = 'foo'")


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
