# This file is part of ctrl_mpexec.
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

import shutil
import tempfile
import unittest
from typing import TYPE_CHECKING

import lsst.daf.butler.tests as butlerTests
import lsst.pex.config as pexConfig
from lsst.ctrl.mpexec import TaskFactory
from lsst.pipe.base import PipelineGraph, PipelineTaskConfig, PipelineTaskConnections, connectionTypes

if TYPE_CHECKING:
    from lsst.daf.butler import Butler, DatasetRef

# Storage class to use for tests of fakes.
_FAKE_STORAGE_CLASS = "StructuredDataDict"


class FakeConnections(PipelineTaskConnections, dimensions=set()):
    """Fake connections class used for testing."""

    initInput = connectionTypes.InitInput(name="fakeInitInput", doc="test", storageClass=_FAKE_STORAGE_CLASS)
    initOutput = connectionTypes.InitOutput(
        name="fakeInitOutput", doc="test", storageClass=_FAKE_STORAGE_CLASS
    )
    input = connectionTypes.Input(
        name="fakeInput", doc="test", storageClass=_FAKE_STORAGE_CLASS, dimensions=set()
    )
    output = connectionTypes.Output(
        name="fakeOutput", doc="test", storageClass=_FAKE_STORAGE_CLASS, dimensions=set()
    )


class FakeConfig(PipelineTaskConfig, pipelineConnections=FakeConnections):
    """Config class used along with fake connections class."""

    widget = pexConfig.Field(dtype=float, doc="test", default=1.0)


def mockTaskClass():
    """Record calls to ``__call__``.

    A class placeholder that records calls to __call__.
    """
    mock = unittest.mock.Mock(__name__="_TaskMock", _DefaultName="FakeTask", ConfigClass=FakeConfig)
    return mock


class TaskFactoryTestCase(unittest.TestCase):
    """Tests for `TaskFactory`."""

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()

        tmp = tempfile.mkdtemp()
        cls.addClassCleanup(shutil.rmtree, tmp, ignore_errors=True)
        cls.repo = butlerTests.makeTestRepo(tmp)
        butlerTests.addDatasetType(cls.repo, "fakeInitInput", set(), _FAKE_STORAGE_CLASS)
        butlerTests.addDatasetType(cls.repo, "fakeInitOutput", set(), _FAKE_STORAGE_CLASS)
        butlerTests.addDatasetType(cls.repo, "fakeInput", set(), _FAKE_STORAGE_CLASS)
        butlerTests.addDatasetType(cls.repo, "fakeOutput", set(), _FAKE_STORAGE_CLASS)

    def setUp(self) -> None:
        super().setUp()

        self.factory = TaskFactory()
        self.constructor = mockTaskClass()

    @staticmethod
    def _alteredConfig() -> FakeConfig:
        config = FakeConfig()
        config.widget = 42.0
        return config

    @staticmethod
    def _dummyCatalog() -> dict:
        return {}

    def _tempButler(self) -> tuple[Butler, dict[str, DatasetRef]]:
        butler = butlerTests.makeTestCollection(self.repo, uniqueId=self.id())
        catalog = self._dummyCatalog()
        refs = {}
        for dataset_type in ("fakeInitInput", "fakeInitOutput", "fakeInput", "fakeOutput"):
            refs[dataset_type] = butler.put(catalog, dataset_type)
        return butler, refs

    def testDefaultConfigLabel(self) -> None:
        pipeline_graph = PipelineGraph()
        task_node = pipeline_graph.add_task(None, self.constructor)
        butler, _ = self._tempButler()
        self.factory.makeTask(task_node, butler=butler, initInputRefs=[])
        self.constructor.assert_called_with(config=FakeConfig(), initInputs={}, name="FakeTask")

    def testAllArgs(self) -> None:
        config = self._alteredConfig()
        pipeline_graph = PipelineGraph()
        task_node = pipeline_graph.add_task("no-name", self.constructor, config=config)
        butler, refs = self._tempButler()
        self.factory.makeTask(task_node, butler=butler, initInputRefs=[refs["fakeInitInput"]])
        catalog = butler.get("fakeInitInput")
        self.constructor.assert_called_with(config=config, initInputs={"initInput": catalog}, name="no-name")


if __name__ == "__main__":
    unittest.main()
