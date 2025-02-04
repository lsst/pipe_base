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

import math
import unittest

try:
    import numpy
except ImportError:
    numpy = None

from lsst.pipe.base import TaskMetadata


class TaskMetadataTestCase(unittest.TestCase):
    """Test task metadata."""

    def testTaskMetadata(self):
        """Full test of TaskMetadata API."""
        meta = TaskMetadata()
        meta["test"] = 42
        self.assertEqual(meta["test"], 42)
        meta.add("test", 55)
        self.assertEqual(meta["test"], 55)
        meta.add("test", [1, 2])
        self.assertEqual(meta.getScalar("test"), 2)
        self.assertEqual(meta.getArray("test"), [42, 55, 1, 2])
        self.assertEqual(meta.get("test"), 2)
        meta["new.int"] = 30
        self.assertEqual(meta["new.int"], 30)
        self.assertEqual(meta.get("new.int", 20), 30)
        self.assertEqual(meta.get("not.present.at.all", 20), 20)
        self.assertEqual(meta["new"]["int"], 30)
        self.assertEqual(meta.get("new").get("int"), 30)
        self.assertEqual(meta.getArray("new.int"), [30])
        self.assertEqual(meta.getScalar("new.int"), 30)
        self.assertIsInstance(meta["new"], TaskMetadata)
        self.assertIsInstance(meta.getScalar("new"), TaskMetadata)
        self.assertIsInstance(meta.getArray("new")[0], TaskMetadata)
        self.assertIsInstance(meta.get("new"), TaskMetadata)
        meta.add("new.int", 24)
        self.assertEqual(meta["new.int"], 24)
        meta["new.str"] = "str"
        self.assertEqual(meta["new.str"], "str")

        meta["test"] = "string"
        self.assertEqual(meta["test"], "string")

        self.assertIn("test", meta)
        self.assertIn("new", meta)
        self.assertIn("new.int", meta)
        self.assertNotIn("new2.int", meta)
        self.assertNotIn("test2", meta)

        self.assertEqual(meta.paramNames(topLevelOnly=False), {"test", "new.int", "new.str"})
        self.assertEqual(meta.paramNames(topLevelOnly=True), {"test"})
        self.assertEqual(meta.names(), {"test", "new", "new.int", "new.str"})
        self.assertEqual(meta.keys(), ("test", "new"))
        self.assertEqual(len(meta), 2)
        self.assertEqual(len(meta["new"]), 2)

        meta["new_array"] = ("a", "b")
        self.assertEqual(meta["new_array"], "b")
        self.assertEqual(meta.getArray("new_array"), ["a", "b"])
        meta.add("new_array", "c")
        self.assertEqual(meta["new_array"], "c")
        self.assertEqual(meta.getArray("new_array"), ["a", "b", "c"])
        meta["new_array"] = [1, 2, 3]
        self.assertEqual(meta.getArray("new_array"), [1, 2, 3])
        meta["empty_array"] = []
        self.assertEqual(meta.getArray("empty_array"), [])
        with self.assertRaises(KeyError):
            meta["empty_array"]

        meta["meta"] = 5
        meta["meta"] = TaskMetadata()
        self.assertIsInstance(meta["meta"], TaskMetadata)
        meta["meta.a.b"] = "deep"
        self.assertEqual(meta["meta.a.b"], "deep")
        self.assertIsInstance(meta["meta.a"], TaskMetadata)

        meta.add("via_scalar", 22)
        self.assertEqual(meta["via_scalar"], 22)

        del meta["test"]
        self.assertNotIn("test", meta)
        del meta["new.int"]
        self.assertNotIn("new.int", meta)
        self.assertIn("new", meta)
        with self.assertRaises(KeyError):
            del meta["test2"]
        with self.assertRaises(KeyError) as cm:
            # Check that deleting a hierarchy that is not present also
            # reports the correct key.
            del meta["new.a.b.c"]
        self.assertIn("new.a.b.c", str(cm.exception))

        with self.assertRaises(KeyError) as cm:
            # Something that doesn't exist at all.
            meta["something.a.b"]
        # Ensure that the full key hierarchy is reported in the error message.
        self.assertIn("something.a.b", str(cm.exception))

        with self.assertRaises(KeyError) as cm:
            # Something that does exist at level 2 but not further down.
            meta["new.str.a"]
        # Ensure that the full key hierarchy is reported in the error message.
        self.assertIn("new.str.a", str(cm.exception))

        with self.assertRaises(KeyError) as cm:
            # Something that only exists at level 1.
            meta["new.str3"]
        # Ensure that the full key hierarchy is reported in the error message.
        self.assertIn("new.str3", str(cm.exception))

        with self.assertRaises(KeyError) as cm:
            # Something that only exists at level 1 but as an array.
            meta.getArray("new.str3")
        # Ensure that the full key hierarchy is reported in the error message.
        self.assertIn("new.str3", str(cm.exception))

        with self.assertRaises(ValueError):
            meta.add("new", 1)

        with self.assertRaises(KeyError):
            meta[42]

        with self.assertRaises(KeyError):
            meta["not.present"]

        with self.assertRaises(KeyError):
            meta["not_present"]

        with self.assertRaises(KeyError):
            meta.getScalar("not_present")

        with self.assertRaises(KeyError):
            meta.getArray("not_present")

    def testValidation(self):
        """Test that validation works."""
        meta = TaskMetadata()

        class BadThing:
            pass

        with self.assertRaises(ValueError):
            meta["bad"] = BadThing()

        with self.assertRaises(ValueError):
            meta["bad_list"] = [BadThing()]

        meta.add("int", 4)
        with self.assertRaises(ValueError):
            meta.add("int", "string")

        with self.assertRaises(ValueError):
            meta.add("mapping", {})

        with self.assertRaises(ValueError):
            meta.add("int", ["string", "array"])

        with self.assertRaises(ValueError):
            meta["mixed"] = [1, "one"]

    def test_nan(self):
        """Check that NaN round trips as a NaN."""
        meta = TaskMetadata()
        meta["nan"] = float("NaN")
        new_meta = TaskMetadata.model_validate_json(meta.model_dump_json())
        self.assertTrue(math.isnan(new_meta["nan"]))

    def testDict(self):
        """Construct a TaskMetadata from a dictionary."""
        d = {"a": "b", "c": 1, "d": [1, 2], "e": {"f": "g", "h": {"i": [3, 4]}}}

        meta = TaskMetadata.from_dict(d)
        self.assertEqual(meta["a"], "b")
        self.assertEqual(meta["e.f"], "g")
        self.assertEqual(meta.getArray("d"), [1, 2])
        self.assertEqual(meta["e.h.i"], 4)

        d2 = meta.to_dict()
        self.assertEqual(d2, d)

        j = meta.model_dump_json()
        meta2 = TaskMetadata.model_validate_json(j)
        self.assertEqual(meta2, meta)

        # Round trip.
        meta3 = TaskMetadata.from_metadata(meta)
        self.assertEqual(meta3, meta)

        # Add a new element that would be a single-element array.
        # This will not equate as equal because from_metadata will move
        # the item to the scalar part of the model and pydantic does not
        # see them as equal.
        meta3.add("e.new", 5)
        meta4 = TaskMetadata.from_metadata(meta3)
        self.assertNotEqual(meta4, meta3)
        self.assertEqual(meta4["e.new"], meta3["e.new"])
        del meta4["e.new"]
        del meta3["e.new"]
        self.assertEqual(meta4, meta3)

    @unittest.skipIf(not numpy, "Numpy is required for this test.")
    def testNumpy(self):
        meta = TaskMetadata()
        meta["int"] = numpy.int64(42)
        self.assertEqual(meta["int"], 42)
        self.assertEqual(type(meta["int"]), int)

        meta["float"] = numpy.float64(3.14)
        self.assertEqual(meta["float"], 3.14)
        self.assertEqual(type(meta["float"]), float)

        meta.add("floatArray", [numpy.float64(1.5), numpy.float64(3.0)])
        self.assertEqual(meta.getArray("floatArray"), [1.5, 3.0])
        self.assertEqual(type(meta["floatArray"]), float)

        meta.add("intArray", [numpy.int64(1), numpy.int64(3)])
        self.assertEqual(meta.getArray("intArray"), [1, 3])
        self.assertEqual(type(meta["intArray"]), int)

        with self.assertRaises(ValueError):
            meta.add("mixed", [1.5, numpy.float64(4.5)])

        with self.assertRaises(ValueError):
            meta["numpy"] = numpy.zeros(5)

    def test_get_set_dict(self):
        """Test the get_dict and set_dict methods."""
        obj = TaskMetadata()
        d1 = {"one": 1, "two": 2.0, "three": True, "four": {"a": 4, "b": "B"}, "five": {}}
        obj.set_dict("d", d1)
        obj.set_dict("e", {})
        d2 = obj.get_dict("d")
        # Keys with empty-dict values may or may not be round-tripped.
        self.assertGreaterEqual(d2.keys(), {"one", "two", "three", "four"})
        self.assertLessEqual(d2.keys(), {"one", "two", "three", "four", "five"})
        self.assertEqual(d2["one"], d1["one"])
        self.assertEqual(d2["two"], d1["two"])
        self.assertEqual(d2["three"], d1["three"])
        self.assertEqual(d2["four"], d1["four"])
        self.assertEqual(d2.get("five", {}), d1["five"])
        # Empty dict may or may not have been added, and retrieving it or
        # a key that was never added yields an empty dict.
        self.assertEqual(obj.get_dict("e"), {})
        self.assertEqual(obj.get_dict("f"), {})


if __name__ == "__main__":
    unittest.main()
