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

"""Tests for execution butler."""

import os
import unittest

import lsst.utils.tests
from lsst.daf.butler import Butler
from lsst.daf.butler.direct_butler import DirectButler
from lsst.pipe.base.executionButlerBuilder import buildExecutionButler
from lsst.pipe.base.tests import simpleQGraph
from lsst.utils.tests import temporaryDirectory

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class ExecutionTestCase(unittest.TestCase):
    """Small bunch of tests for instantiating execution butler."""

    def test_simple(self) -> None:
        """Test buildExecutionButler with default butler config."""
        with temporaryDirectory() as root:
            butler, qgraph = simpleQGraph.makeSimpleQGraph(root=root, inMemory=False)
            assert isinstance(butler, DirectButler), "Expect DirectButler in tests"
            buildExecutionButler(butler, qgraph, os.path.join(root, "exec_butler"), butler.run)

    def test_obscore(self) -> None:
        """Test buildExecutionButler with obscore table in the butler."""
        with temporaryDirectory() as root:
            config = os.path.join(TESTDIR, "config/butler-obscore.yaml")
            butler = simpleQGraph.makeSimpleButler(root=root, inMemory=False, config=config)
            butler, qgraph = simpleQGraph.makeSimpleQGraph(butler=butler)

            # Need a fresh butler instance to make sure it reads config from
            # butler.yaml, which does not have full obscore config.
            butler = Butler.from_config(root, run=butler.run)
            assert isinstance(butler, DirectButler), "Expect DirectButler in tests"
            buildExecutionButler(butler, qgraph, os.path.join(root, "exec_butler"), butler.run)


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
