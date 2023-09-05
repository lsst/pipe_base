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

import os
import unittest
from tempfile import NamedTemporaryFile
from textwrap import dedent

import lsst.pipe.base as pipeBase


class PipelineLoadSubsetTest(unittest.TestCase):
    """Tests for loading subsets of pipelines from YAML files."""

    def setUp(self):
        frozen_pipeline = dedent(
            """
            description: Frozen Pipeline
            tasks:
              isr:
                class: lsst.pipe.base.tests.mocks.DynamicTestPipelineTask
                config:
                  python: |
                    from lsst.pipe.base.tests.mocks import DynamicConnectionConfig
                    config.dimensions = ['exposure', 'detector']
                    config.inputs['input_image'] = DynamicConnectionConfig(
                      dataset_type_name='raw',
                      dimensions={'exposure', 'detector'},
                    )
                    config.outputs['output_image'] = DynamicConnectionConfig(
                      dataset_type_name='detrended',
                      dimensions={'exposure', 'detector'},
                    )
              calibrate:
                class: lsst.pipe.base.tests.mocks.DynamicTestPipelineTask
                config:
                  python: |
                    from lsst.pipe.base.tests.mocks import DynamicConnectionConfig
                    config.dimensions = ['visit', 'detector']
                    config.inputs['input_image'] = DynamicConnectionConfig(
                      dataset_type_name='detrended',
                      dimensions={'exposure', 'detector'},
                    )
                    config.outputs['output_image'] = DynamicConnectionConfig(
                      dataset_type_name='pvi',
                      dimensions={'visit', 'detector'},
                    )
              makeWarp:
                class: lsst.pipe.base.tests.mocks.DynamicTestPipelineTask
                config:
                  python: |
                    from lsst.pipe.base.tests.mocks import DynamicConnectionConfig
                    config.dimensions = ['patch', 'visit']
                    config.inputs['input_image'] = DynamicConnectionConfig(
                      dataset_type_name='pvi',
                      dimensions={'visit', 'detector'},
                    )
                    config.outputs['output_image'] = DynamicConnectionConfig(
                      dataset_type_name='warp',
                      dimensions={'visit', 'patch'},
                    )
              assembleCoadd:
                class: lsst.pipe.base.tests.mocks.DynamicTestPipelineTask
                config:
                  python: |
                    from lsst.pipe.base.tests.mocks import DynamicConnectionConfig
                    config.dimensions = ['patch', 'band']
                    config.inputs['input_image'] = DynamicConnectionConfig(
                      dataset_type_name='warp',
                      dimensions={'visit', 'patch'},
                    )
                    config.outputs['output_image'] = DynamicConnectionConfig(
                      dataset_type_name='coadd',
                      dimensions={'patch', 'band'},
                    )
            """
        )
        self.temp_pipeline_name = ""
        while not self.temp_pipeline_name:
            self.temp_pipeline = NamedTemporaryFile()
            self.temp_pipeline.write(frozen_pipeline.encode())
            self.temp_pipeline.flush()
            if not os.path.exists(self.temp_pipeline.name):
                self.temp_pipeline.close()
            else:
                self.temp_pipeline_name = self.temp_pipeline.name

    def tearDown(self):
        self.temp_pipeline.close()

    def testLoadList(self):
        """Test loading a specific list of labels."""
        labels = ("isr", "makeWarp")
        path = os.path.expandvars(f"{self.temp_pipeline_name}#{','.join(labels)}")
        pipeline = pipeBase.Pipeline.fromFile(path)
        self.assertEqual(set(labels), pipeline._pipelineIR.tasks.keys())

    def testLoadSingle(self):
        """Test loading a specific label."""
        label = "calibrate"
        path = os.path.expandvars(f"{self.temp_pipeline_name}#{label}")
        pipeline = pipeBase.Pipeline.fromFile(path)
        self.assertEqual(set((label,)), pipeline._pipelineIR.tasks.keys())

    def testLoadBoundedRange(self):
        """Test loading a bounded range."""
        path = os.path.expandvars(f"{self.temp_pipeline_name}#calibrate..assembleCoadd")
        pipeline = pipeBase.Pipeline.fromFile(path)
        self.assertEqual(
            {"calibrate", "makeWarp", "assembleCoadd"},
            pipeline._pipelineIR.tasks.keys(),
        )

    def testLoadUpperBound(self):
        """Test loading a range that only has an upper bound."""
        path = os.path.expandvars(f"{self.temp_pipeline_name}#..makeWarp")
        pipeline = pipeBase.Pipeline.fromFile(path)
        self.assertEqual(
            {"isr", "calibrate", "makeWarp"},
            pipeline._pipelineIR.tasks.keys(),
        )

    def testLoadLowerBound(self):
        """Test loading a range that only has a lower bound."""
        path = os.path.expandvars(f"{self.temp_pipeline_name}#makeWarp..")
        pipeline = pipeBase.Pipeline.fromFile(path)
        self.assertEqual(
            {"makeWarp", "assembleCoadd"},
            pipeline._pipelineIR.tasks.keys(),
        )

    def testLabelChecks(self):
        # test a bad list
        path = os.path.expandvars(f"{self.temp_pipeline_name}#FakeLabel")
        with self.assertRaises(ValueError):
            pipeBase.Pipeline.fromFile(path)

        # test a bad end label
        path = os.path.expandvars(f"{self.temp_pipeline_name}#..FakeEndLabel")
        with self.assertRaises(ValueError):
            pipeBase.Pipeline.fromFile(path)

        # test a bad begin label
        path = os.path.expandvars(f"{self.temp_pipeline_name}#FakeBeginLabel..")
        with self.assertRaises(ValueError):
            pipeBase.Pipeline.fromFile(path)

    def testContractRemoval(self):
        path = os.path.expandvars(f"{self.temp_pipeline_name}")
        pipeline = pipeBase.Pipeline.fromFile(path)
        contract = pipeBase.pipelineIR.ContractIR("'visit' in calibrate.dimensions", None)
        pipeline._pipelineIR.contracts.append(contract)
        pipeline = pipeline.subsetFromLabels(pipeBase.LabelSpecifier(labels={"isr"}))
        self.assertEqual(len(pipeline._pipelineIR.contracts), 0)


if __name__ == "__main__":
    unittest.main()
