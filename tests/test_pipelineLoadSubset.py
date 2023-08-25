import os
import unittest

from tempfile import NamedTemporaryFile
from textwrap import dedent

import lsst.pipe.base as pipeBase
import lsst.utils.tests


class PipelineLoadSubsetTest(unittest.TestCase):
    def setUp(self):
        frozen_pipeline = dedent("""
            description: Frozen Pipeline
            instrument: lsst.obs.subaru.HyperSuprimeCam
            tasks:
              isr:
                class: lsst.ip.isr.IsrTask
              charImage:
                class: lsst.pipe.tasks.characterizeImage.CharacterizeImageTask
              calibrate:
                class: lsst.pipe.tasks.calibrate.CalibrateTask
              makeWarpTask:
                class: lsst.pipe.tasks.makeWarp.MakeWarpTask
                config:
                - python: config.warpAndPsfMatch.psfMatch.kernel['AL'].alardSigGauss = [1.0, 2.0,
                    4.5]
                  matchingKernelSize: 29
                  makePsfMatched: true
                  modelPsf.defaultFwhm: 7.7
                  doApplyExternalPhotoCalib: false
                  doApplyExternalSkyWcs: false
                  doApplySkyCorr: false
                  doWriteEmptyWarps: true
              assembleCoadd:
                class: lsst.pipe.tasks.assembleCoadd.CompareWarpAssembleCoaddTask
              detection:
                class: lsst.pipe.tasks.multiBand.DetectCoaddSourcesTask
              mergeDetections:
                class: lsst.pipe.tasks.mergeDetections.MergeDetectionsTask
                config:
                - priorityList:
                  - i
                  - r
              deblend:
                class: lsst.pipe.tasks.deblendCoaddSourcesPipeline.DeblendCoaddSourcesSingleTask
              measure:
                class: lsst.pipe.tasks.multiBand.MeasureMergedCoaddSourcesTask
                config:
                - inputCatalog: deblendedFlux
                - doAddFootprints: false
              mergeMeasurements:
                class: lsst.pipe.tasks.mergeMeasurements.MergeMeasurementsTask
                config:
                - priorityList:
                  - i
                  - r
              forcedPhotCcd:
                class: lsst.meas.base.forcedPhotCcd.ForcedPhotCcdTask
                config:
                - doApplyExternalPhotoCalib: false
                  doApplyExternalSkyWcs: false
                  doApplySkyCorr: false
              forcedPhotCoadd:
                class: lsst.drp.tasks.forcedPhotCoadd.ForcedPhotCoaddTask
            """)
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
        """This function tests loading a specific list of labels
        """
        labels = ("charImage", "calibrate", "makeWarpTask")
        path = os.path.expandvars(f"{self.temp_pipeline_name}#{','.join(labels)}")
        pipeline = pipeBase.Pipeline.fromFile(path)
        self.assertEqual(set(labels), pipeline._pipelineIR.tasks.keys())

    def testLoadSingle(self):
        """This function tests loading a specific label
        """
        label = "charImage"
        path = os.path.expandvars(f"{self.temp_pipeline_name}#{label}")
        pipeline = pipeBase.Pipeline.fromFile(path)
        self.assertEqual(set((label,)), pipeline._pipelineIR.tasks.keys())

    def testLoadBoundedRange(self):
        """This function tests loading a bounded range
        """
        path = os.path.expandvars(f"{self.temp_pipeline_name}#charImage..assembleCoadd")
        pipeline = pipeBase.Pipeline.fromFile(path)
        self.assertEqual(set(('charImage', 'calibrate', 'makeWarpTask', 'assembleCoadd')),
                         pipeline._pipelineIR.tasks.keys())

    def testLoadUpperBound(self):
        """This function tests loading a range that only has an upper bound
        """
        path = os.path.expandvars(f"{self.temp_pipeline_name}#..assembleCoadd")
        pipeline = pipeBase.Pipeline.fromFile(path)
        self.assertEqual(set(('isr', 'charImage', 'calibrate', 'makeWarpTask', 'assembleCoadd')),
                         pipeline._pipelineIR.tasks.keys())

    def testLoadLowerBound(self):
        """This function tests loading a range that only has an upper bound
        """
        path = os.path.expandvars(f"{self.temp_pipeline_name}#mergeDetections..")
        pipeline = pipeBase.Pipeline.fromFile(path)
        self.assertEqual(set(('mergeDetections', 'deblend', 'measure', 'mergeMeasurements', 'forcedPhotCcd',
                             'forcedPhotCoadd')),
                         pipeline._pipelineIR.tasks.keys())

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
        contract = pipeBase.pipelineIR.ContractIR("forcedPhotCcd.doApplyExternalPhotoCalib == False", None)
        pipeline._pipelineIR.contracts.append(contract)
        pipeline = pipeline.subsetFromLabels(pipeBase.LabelSpecifier(labels=set(("isr",))))
        self.assertEqual(len(pipeline._pipelineIR.contracts), 0)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
