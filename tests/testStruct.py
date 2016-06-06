#!/usr/bin/env python
#
# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
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
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.
#
import unittest

import lsst.utils.tests as utilsTests
import lsst.pipe.base as pipeBase

class StructTestCase(unittest.TestCase):
    """A test case for Struct
    """
    def setUp(self):
        self.valDict = dict(
            foo = 1,
            bar = (1, 2, 3),
            baz = "value for baz",
            alist = [3, 5, 7, 9],
        )

    def tearDown(self):
        self.valDict = None

    def testInit(self):
        """Test Struct.__init__
        """
        s = pipeBase.Struct(**self.valDict)
        self.assertEqual(self.valDict, s.getDict())

        for name, val in self.valDict.iteritems():
            self.assertEqual(getattr(s, name), val)

    def testSet(self):
        """Test adding values via struct.name=val
        """
        s = pipeBase.Struct()
        for name, val in self.valDict.iteritems():
            setattr(s, name, val)

        self.assertEqual(self.valDict, s.getDict())

    def testCopy(self):
        """Test copy, which returns a shallow copy
        """
        s = pipeBase.Struct(**self.valDict)
        sc = s.copy()
        self.assertEqual(s.getDict(), sc.getDict())

        # shallow copy, so changing a list should propagate (not necessarily a feature)
        sc.alist[0] = 97
        self.assertEqual(s, sc)

        sc.foo += 1
        self.assertNotEqual(s, sc)

    def testMergeItems(self):
        """Test mergeItems
        """
        s = pipeBase.Struct(**self.valDict)
        newS = pipeBase.Struct()
        newS.mergeItems(s)
        # with no names listed, should merge nothing
        self.assertEqual(len(newS), 0)
        self.assertNotEqual(s, newS)

        newS.mergeItems(s, "foo", "bar")
        self.assertEqual(len(newS), 2)
        self.assertNotEqual(s, newS)

        newS.mergeItems(s, "baz", "alist")
        self.assertEqual(len(newS), 4)

        for name, val in newS.getDict().iteritems():
            self.assertEqual(val, self.valDict[name])
            self.assertRaises(RuntimeError, newS.mergeItems, s, name)


def suite():
    """Return a suite containing all the test cases in this module.
    """
    utilsTests.init()

    suites = []

    suites += unittest.makeSuite(StructTestCase)
    suites += unittest.makeSuite(utilsTests.MemoryTestCase)

    return unittest.TestSuite(suites)


def run(shouldExit=False):
    """Run the tests"""
    utilsTests.run(suite(), shouldExit)

if __name__ == "__main__":
    run(True)
