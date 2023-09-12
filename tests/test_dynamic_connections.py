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

import unittest
from collections.abc import Callable
from types import MappingProxyType

from lsst.pipe.base import PipelineTaskConfig, PipelineTaskConnections
from lsst.pipe.base.connectionTypes import Input, Output, PrerequisiteInput

# Keyword arguments for defining our lone test connection in its class.
DEFAULT_CONNECTION_KWARGS = dict(
    doc="field docs",
    name="unconfigured",
    dimensions=(),
    storageClass="Dummy",
)

# Keyword arguments for making our lone test connection post-configuration.
# We always rename the dataset type via configuration to make sure those
# changes are never dropped.
RENAMED_CONNECTION_KWARGS = DEFAULT_CONNECTION_KWARGS.copy()
RENAMED_CONNECTION_KWARGS["name"] = "configured"


class TestDynamicConnectionsClass(unittest.TestCase):
    """Test modifying connections in derived __init__ implementations."""

    def build_dynamic_connections(
        self, init_callback: Callable[[PipelineTaskConnections], None] = None
    ) -> PipelineTaskConnections:
        """Define and construct a connections class instance with a callback
        run in ``__init__``.

        Parameters
        ----------
        init_callback
            Callback to invoke with the `PipelineTaskConnections` instance
            as its only argument.  Return value is ignored.

        Returns
        -------
        connections : `PipelineTaskConnections`
            Constructed connections instance.
        """

        class ExampleConnections(PipelineTaskConnections, dimensions=()):
            the_connection = Input(**DEFAULT_CONNECTION_KWARGS)

            def __init__(self, config: ExampleConfig):
                # Calling super() is harmless but now unnecessary, so don't do
                # it to make sure that works.  Old code calling it is fine.
                if init_callback is not None:
                    init_callback(self)

        class ExampleConfig(PipelineTaskConfig, pipelineConnections=ExampleConnections):
            pass

        config = ExampleConfig()
        config.connections.the_connection = RENAMED_CONNECTION_KWARGS["name"]
        return ExampleConnections(config=config)

    def test_freeze_after_construction(self):
        connections = self.build_dynamic_connections()
        self.assertIsInstance(connections.dimensions, frozenset)
        self.assertIsInstance(connections.inputs, frozenset)
        self.assertIsInstance(connections.prerequisiteInputs, frozenset)
        self.assertIsInstance(connections.outputs, frozenset)
        self.assertIsInstance(connections.initInputs, frozenset)
        self.assertIsInstance(connections.initOutputs, frozenset)
        self.assertIsInstance(connections.allConnections, MappingProxyType)

    def test_change_attr_after_construction(self):
        connections = self.build_dynamic_connections()
        with self.assertRaises(TypeError):
            connections.the_connection = PrerequisiteInput(**RENAMED_CONNECTION_KWARGS)

    def test_delete_attr_after_construction(self):
        connections = self.build_dynamic_connections()
        with self.assertRaises(TypeError):
            del connections.the_connection

    def test_change_dimensions(self):
        def callback(instance):
            instance.dimensions = {"new", "dimensions"}
            instance.the_connection = Input(
                doc=instance.the_connection.doc,
                name=instance.the_connection.name,
                dimensions={"new", "dimensions"},
                storageClass=instance.the_connection.storageClass,
            )

        connections = self.build_dynamic_connections(callback)
        self.assertEqual(connections.dimensions, {"new", "dimensions"})
        self.assertIsInstance(connections.dimensions, frozenset)
        self.assertEqual(connections.inputs, {"the_connection"})
        self.assertEqual(connections.allConnections.keys(), {"the_connection"})
        updated_connection_kwargs = RENAMED_CONNECTION_KWARGS.copy()
        updated_connection_kwargs["dimensions"] = {"new", "dimensions"}
        self.assertEqual(connections.allConnections["the_connection"], Input(**updated_connection_kwargs))
        self.assertEqual(connections.the_connection, Input(**updated_connection_kwargs))

    def test_change_connection_type(self):
        def callback(instance):
            instance.the_connection = PrerequisiteInput(
                doc=instance.the_connection.doc,
                name=instance.the_connection.name,
                dimensions=instance.the_connection.dimensions,
                storageClass=instance.the_connection.storageClass,
            )

        connections = self.build_dynamic_connections(callback)
        self.assertEqual(connections.inputs, set())
        self.assertEqual(connections.prerequisiteInputs, {"the_connection"})
        self.assertEqual(connections.allConnections.keys(), {"the_connection"})
        self.assertEqual(
            connections.allConnections["the_connection"], PrerequisiteInput(**RENAMED_CONNECTION_KWARGS)
        )
        self.assertEqual(connections.the_connection, PrerequisiteInput(**RENAMED_CONNECTION_KWARGS))

    def test_change_connection_type_twice(self):
        def callback(instance):
            instance.the_connection = PrerequisiteInput(
                doc=instance.the_connection.doc,
                name=instance.the_connection.name,
                dimensions=instance.the_connection.dimensions,
                storageClass=instance.the_connection.storageClass,
            )
            instance.the_connection = Output(
                doc=instance.the_connection.doc,
                name=instance.the_connection.name,
                dimensions=instance.the_connection.dimensions,
                storageClass=instance.the_connection.storageClass,
            )

        connections = self.build_dynamic_connections(callback)
        self.assertEqual(connections.inputs, set())
        self.assertEqual(connections.prerequisiteInputs, set())
        self.assertEqual(connections.outputs, {"the_connection"})
        self.assertEqual(connections.allConnections.keys(), {"the_connection"})
        self.assertEqual(connections.allConnections["the_connection"], Output(**RENAMED_CONNECTION_KWARGS))
        self.assertEqual(connections.the_connection, Output(**RENAMED_CONNECTION_KWARGS))

    def test_remove_from_set(self):
        def callback(instance):
            instance.inputs.remove("the_connection")
            # We can't make this remove corresponding attribute or the entry in
            # allConnections *immediately* without using a custom set class for
            # 'inputs' etc, which we haven't bothered to do, because even that
            # wouldn't be enough to have additions to those sets update the
            # attributes and allConnections.  Instead updates to those happen
            # after __init__.

        connections = self.build_dynamic_connections(callback)
        self.assertEqual(connections.inputs, set())
        self.assertEqual(connections.allConnections, {})
        with self.assertRaises(AttributeError):
            connections.the_connection

    def test_delete_attr(self):
        def callback(instance):
            del instance.the_connection
            # This updates the corresponding entry from the inputs set and
            # the allConnections dict.
            self.assertEqual(instance.inputs, set())
            self.assertEqual(instance.allConnections, {})

        connections = self.build_dynamic_connections(callback)
        self.assertEqual(connections.inputs, set())
        self.assertEqual(connections.allConnections, {})
        with self.assertRaises(AttributeError):
            connections.the_connection

    def test_delete_attr_twice(self):
        def callback(instance):
            del instance.the_connection
            with self.assertRaises(AttributeError):
                del instance.the_connection

        self.build_dynamic_connections(callback)

    def test_change_connection_type_then_remove_from_set(self):
        def callback(instance):
            instance.the_connection = PrerequisiteInput(
                doc=instance.the_connection.doc,
                name=instance.the_connection.name,
                dimensions=instance.the_connection.dimensions,
                storageClass=instance.the_connection.storageClass,
            )
            instance.prerequisiteInputs.remove("the_connection")

        connections = self.build_dynamic_connections(callback)
        self.assertEqual(connections.inputs, set())
        self.assertEqual(connections.prerequisiteInputs, set())
        self.assertEqual(connections.allConnections, {})
        with self.assertRaises(AttributeError):
            connections.the_connection
        with self.assertRaises(KeyError):
            connections.allConnections["the_connection"]

    def test_change_connection_type_then_delete_attr(self):
        def callback(instance):
            instance.the_connection = PrerequisiteInput(
                doc=instance.the_connection.doc,
                name=instance.the_connection.name,
                dimensions=instance.the_connection.dimensions,
                storageClass=instance.the_connection.storageClass,
            )
            del instance.the_connection
            # This updates the corresponding entry from the inputs set and
            # the allConnections dict.
            self.assertEqual(instance.inputs, set())
            self.assertEqual(instance.prerequisiteInputs, set())
            self.assertEqual(instance.allConnections, {})
            with self.assertRaises(AttributeError):
                instance.the_connection
            with self.assertRaises(KeyError):
                instance.allConnections["the_connection"]

        connections = self.build_dynamic_connections(callback)
        self.assertEqual(connections.inputs, set())
        self.assertEqual(connections.prerequisiteInputs, set())
        self.assertEqual(connections.allConnections, {})
        with self.assertRaises(AttributeError):
            connections.the_connection
        with self.assertRaises(KeyError):
            connections.allConnections["the_connection"]

    def test_add_new_connection(self):
        new_connection = Output(
            name="new_dataset_type",
            doc="new connection_docs",
            storageClass="Dummy",
            dimensions=(),
        )

        def callback(instance):
            instance.new_connection = new_connection
            self.assertEqual(instance.outputs, {"new_connection"})
            self.assertEqual(instance.allConnections.keys(), {"new_connection", "the_connection"})
            self.assertIs(instance.new_connection, new_connection)
            self.assertIs(instance.allConnections["new_connection"], new_connection)

        connections = self.build_dynamic_connections(callback)
        self.assertEqual(connections.outputs, {"new_connection"})
        self.assertEqual(connections.allConnections.keys(), {"new_connection", "the_connection"})
        self.assertIs(connections.new_connection, new_connection)
        self.assertIs(connections.allConnections["new_connection"], new_connection)

    def test_add_and_change_new_connection(self):
        new_connection = Output(
            name="new_dataset_type",
            doc="new connection_docs",
            storageClass="Dummy",
            dimensions=(),
        )
        changed_connection = PrerequisiteInput(
            name="new_dataset_type",
            doc="new connection_docs",
            storageClass="Dummy",
            dimensions=(),
        )

        def callback(instance):
            instance.new_connection = new_connection
            self.assertEqual(instance.outputs, {"new_connection"})
            self.assertEqual(instance.allConnections.keys(), {"new_connection", "the_connection"})
            self.assertIs(instance.new_connection, new_connection)
            self.assertIs(instance.allConnections["new_connection"], new_connection)
            instance.new_connection = changed_connection
            self.assertEqual(instance.outputs, set())
            self.assertEqual(instance.allConnections.keys(), {"new_connection", "the_connection"})
            self.assertIs(instance.new_connection, changed_connection)
            self.assertIs(instance.allConnections["new_connection"], changed_connection)

        connections = self.build_dynamic_connections(callback)
        self.assertEqual(connections.outputs, set())
        self.assertEqual(connections.allConnections.keys(), {"new_connection", "the_connection"})
        self.assertIs(connections.new_connection, changed_connection)
        self.assertIs(connections.allConnections["new_connection"], changed_connection)

    def test_add_and_remove_new_connection(self):
        new_connection = Output(
            name="new_dataset_type",
            doc="new connection_docs",
            storageClass="Dummy",
            dimensions=(),
        )

        def callback(instance):
            instance.new_connection = new_connection
            self.assertEqual(instance.outputs, {"new_connection"})
            self.assertEqual(instance.allConnections.keys(), {"new_connection", "the_connection"})
            self.assertIs(instance.new_connection, new_connection)
            self.assertIs(instance.allConnections["new_connection"], new_connection)
            del instance.new_connection
            self.assertEqual(instance.outputs, set())
            self.assertEqual(instance.allConnections.keys(), {"the_connection"})
            with self.assertRaises(AttributeError):
                instance.new_connection
            with self.assertRaises(KeyError):
                instance.allConnections["new_connection"]

        connections = self.build_dynamic_connections(callback)
        self.assertEqual(connections.outputs, set())
        self.assertEqual(connections.allConnections.keys(), {"the_connection"})
        with self.assertRaises(AttributeError):
            connections.new_connection
        with self.assertRaises(KeyError):
            connections.allConnections["new_connection"]


if __name__ == "__main__":
    unittest.main()
