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

"""Simple unit test for configOverrides.
"""

import unittest

import lsst.pex.config as pexConfig
import lsst.utils.tests
from lsst.pipe.base.configOverrides import ConfigOverrides

# This is used in testSettingVar unit test
TEST_CHOICE_VALUE = 1  # noqa: F841


class ConfigTest(pexConfig.Config):
    fStr = pexConfig.Field(dtype=str, default="default", doc="")
    fBool = pexConfig.Field(dtype=bool, default=False, doc="")
    fInt = pexConfig.Field(dtype=int, default=-1, doc="")
    fFloat = pexConfig.Field(dtype=float, default=-1.0, doc="")

    fListStr = pexConfig.ListField(dtype=str, default=[], doc="")
    fListBool = pexConfig.ListField(dtype=bool, default=[], doc="")
    fListInt = pexConfig.ListField(dtype=int, default=[], doc="")

    fChoiceStr = pexConfig.ChoiceField(dtype=str, allowed=dict(A="a", B="b", C="c"), doc="")
    fChoiceInt = pexConfig.ChoiceField(dtype=int, allowed={1: "a", 2: "b", 3: "c"}, doc="")

    fDictStrInt = pexConfig.DictField(keytype=str, itemtype=int, doc="")


class ConfigOverridesTestCase(unittest.TestCase):
    """A test case for Task"""

    def checkSingleFieldOverride(self, field, value, result=None):
        """Convenience method for override of single field

        Parameters
        ----------
        field : `str`
            Field name.
        value :
            Field value to set, can be a string or anything else.
        result : optional
            Expected value of the field.
        """
        config = ConfigTest()
        overrides = ConfigOverrides()
        overrides.addValueOverride(field, value)
        overrides.applyTo(config)
        self.assertEqual(getattr(config, field), result)

    def testSimpleValueStr(self):
        """Test for applying value override to a string field"""
        field = "fStr"

        # values of supported type
        self.checkSingleFieldOverride(field, "string", "string")

        # invalid input
        with self.assertRaises(pexConfig.FieldValidationError):
            self.checkSingleFieldOverride(field, 1)

    def testSimpleValueBool(self):
        """Test for applying value override to a boolean field"""
        field = "fBool"

        # values of supported type
        self.checkSingleFieldOverride(field, True, True)
        self.checkSingleFieldOverride(field, False, False)

        # supported string conversions
        self.checkSingleFieldOverride(field, "True", True)
        self.checkSingleFieldOverride(field, "False", False)

        # parseable but invalid input
        with self.assertRaises(pexConfig.FieldValidationError):
            self.checkSingleFieldOverride(field, 1)
        with self.assertRaises(pexConfig.FieldValidationError):
            self.checkSingleFieldOverride(field, [])
        with self.assertRaises(pexConfig.FieldValidationError):
            self.checkSingleFieldOverride(field, "1")

    def testSimpleValueInt(self):
        """Test for applying value override to a int field"""
        field = "fInt"

        # values of supported type
        self.checkSingleFieldOverride(field, 0, 0)
        self.checkSingleFieldOverride(field, 100, 100)

        # supported string conversions
        self.checkSingleFieldOverride(field, "0", 0)
        self.checkSingleFieldOverride(field, "100", 100)
        self.checkSingleFieldOverride(field, "-100", -100)
        self.checkSingleFieldOverride(field, "0x100", 0x100)

        # parseable but invalid input
        with self.assertRaises(pexConfig.FieldValidationError):
            self.checkSingleFieldOverride(field, 1.0)
        with self.assertRaises(pexConfig.FieldValidationError):
            self.checkSingleFieldOverride(field, "1.0")
        with self.assertRaises(pexConfig.FieldValidationError):
            self.checkSingleFieldOverride(field, "[]")

    def testSimpleValueFloat(self):
        """Test for applying value override to a float field"""
        field = "fFloat"

        # values of supported type
        self.checkSingleFieldOverride(field, 0.0, 0.0)
        self.checkSingleFieldOverride(field, 100.0, 100.0)

        # supported string conversions
        self.checkSingleFieldOverride(field, "0.", 0.0)
        self.checkSingleFieldOverride(field, "100.0", 100.0)
        self.checkSingleFieldOverride(field, "-1.2e10", -1.2e10)

        # parseable but invalid input
        with self.assertRaises(pexConfig.FieldValidationError):
            self.checkSingleFieldOverride(field, [])
        with self.assertRaises(pexConfig.FieldValidationError):
            self.checkSingleFieldOverride(field, "(1, 1)")

    def testListValueStr(self):
        """Test for applying value override to a list field"""
        field = "fListStr"

        # values of supported type
        self.checkSingleFieldOverride(field, ["a", "b"], ["a", "b"])
        self.checkSingleFieldOverride(field, ("a", "b"), ["a", "b"])

        # supported string conversions
        self.checkSingleFieldOverride(field, '["a", "b"]', ["a", "b"])
        self.checkSingleFieldOverride(field, '("a", "b")', ["a", "b"])

        # parseable but invalid input
        with self.assertRaises(pexConfig.FieldValidationError):
            self.checkSingleFieldOverride(field, "['a', []]")

    def testListValueBool(self):
        """Test for applying value override to a list field"""
        field = "fListBool"

        # values of supported type
        self.checkSingleFieldOverride(field, [True, False], [True, False])
        self.checkSingleFieldOverride(field, (True, False), [True, False])

        # supported string conversions
        self.checkSingleFieldOverride(field, "[True, False]", [True, False])
        self.checkSingleFieldOverride(field, "(True, False)", [True, False])
        self.checkSingleFieldOverride(field, "['True', 'False']", [True, False])

        with self.assertRaises(pexConfig.FieldValidationError):
            self.checkSingleFieldOverride(field, "[1, 2]")
        with self.assertRaises(pexConfig.FieldValidationError):
            self.checkSingleFieldOverride(field, [0, 1])
        with self.assertRaises(pexConfig.FieldValidationError):
            self.checkSingleFieldOverride(field, "5")

    def testListValueInt(self):
        """Test for applying value override to a list field"""
        field = "fListInt"

        # values of supported type
        self.checkSingleFieldOverride(field, [1, 2], [1, 2])
        self.checkSingleFieldOverride(field, (1, 2), [1, 2])

        # supported string conversions
        self.checkSingleFieldOverride(field, "[1, 2]", [1, 2])
        self.checkSingleFieldOverride(field, "(1, 2)", [1, 2])
        self.checkSingleFieldOverride(field, "['1', '2']", [1, 2])

        # parseable but invalid input
        with self.assertRaises(pexConfig.FieldValidationError):
            self.checkSingleFieldOverride(field, "[1.0, []]")
        with self.assertRaises(pexConfig.FieldValidationError):
            self.checkSingleFieldOverride(field, [[], []])
        with self.assertRaises(pexConfig.FieldValidationError):
            self.checkSingleFieldOverride(field, "5")

    def testChoiceValueStr(self):
        """Test for applying value override to a choice field"""
        field = "fChoiceStr"

        # values of supported type
        self.checkSingleFieldOverride(field, "A", "A")
        self.checkSingleFieldOverride(field, "B", "B")

        # non-allowed value
        with self.assertRaises(pexConfig.FieldValidationError):
            self.checkSingleFieldOverride(field, "X")

    def testChoiceValueInt(self):
        """Test for applying value override to a choice field"""
        field = "fChoiceInt"

        # values of supported type
        self.checkSingleFieldOverride(field, 1, 1)
        self.checkSingleFieldOverride(field, 3, 3)

        # supported string conversions
        self.checkSingleFieldOverride(field, "1", 1)

        # parseable but invalid input
        with self.assertRaises(pexConfig.FieldValidationError):
            self.checkSingleFieldOverride(field, "0")
        with self.assertRaises(pexConfig.FieldValidationError):
            self.checkSingleFieldOverride(field, "[1]")
        with self.assertRaises(pexConfig.FieldValidationError):
            self.checkSingleFieldOverride(field, [0, 1])

    def testSettingVar(self):
        """Test setting a field with a string that represents a variable."""
        field = "fChoiceInt"

        # verify loading variable
        self.checkSingleFieldOverride(field, "TEST_CHOICE_VALUE", 1)

        # Verify That python importing a variable works
        config = ConfigTest()
        overrides = ConfigOverrides()
        overrides.addPythonOverride("from math import pi")
        overrides.addValueOverride("fFloat", "pi")
        overrides.applyTo(config)
        from math import pi

        self.assertEqual(config.fFloat, pi)

    def testDictValueInt(self):
        """Test for applying value override to a dict field"""
        field = "fDictStrInt"

        # values of supported type
        self.checkSingleFieldOverride(field, dict(a=1, b=2), dict(a=1, b=2))

        # supported string conversions
        self.checkSingleFieldOverride(field, "{'a': 1, 'b': 2}", dict(a=1, b=2))

        # parseable but invalid input
        with self.assertRaises(pexConfig.FieldValidationError):
            self.checkSingleFieldOverride(field, "{1: 2}")
        with self.assertRaises(pexConfig.FieldValidationError):
            self.checkSingleFieldOverride(field, "{'a': 'b'}")
        with self.assertRaises(pexConfig.FieldValidationError):
            self.checkSingleFieldOverride(field, {"a": "b"})


class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
