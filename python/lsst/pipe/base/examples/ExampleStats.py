from __future__ import absolute_import, division, print_function

import lsst.pex.config as pexConfig

from lsst.afw.image import MaskU
import lsst.afw.math as afwMath
import lsst.pex.config as pexConfig
import lsst.pipe.base as pipeBase
import lsst.pipe.base.super_task as super_task
from lsst.pipe.base.super_task import SuperTask




class ExampleMeanConfig(pexConfig.Config):
    """!Configuration for ExampleSigmaClippedStatsTask
    """
    badMaskPlanes = pexConfig.ListField(
        dtype = str,
        doc = "Mask planes that, if set, the associated pixel should not be included in the coaddTempExp.",
        default = ("EDGE",),
    )
    numSigmaClip = pexConfig.Field(
        doc = "number of sigmas at which to clip data",
        dtype = float,
        default = 3.0,
    )
    numIter = pexConfig.Field(
        doc = "number of iterations of sigma clipping",
        dtype = int,
        default = 2,
    )


class ExampleStdConfig(pexConfig.Config):
    """!Configuration for ExampleSigmaClippedStatsTask
    """
    badMaskPlanes = pexConfig.ListField(
        dtype = str,
        doc = "Mask planes that, if set, the associated pixel should not be included in the coaddTempExp.",
        default = ("EDGE",),
    )
    numSigmaClip = pexConfig.Field(
        doc = "number of sigmas at which to clip data",
        dtype = float,
        default = 3.0,
    )
    numIter = pexConfig.Field(
        doc = "number of iterations of sigma clipping",
        dtype = int,
        default = 2,
    )

@super_task.wrapclass(super_task.wraprun)
class ExampleMeanTask(SuperTask):

    ConfigClass = ExampleMeanConfig
    _default_name = "exampleMean"

    def __init__(self, *args, **kwargs):

        super(ExampleMeanTask, self).__init__(*args, **kwargs)
        #basetask.Task.__init__(self, *args, **kwargs)


    @pipeBase.timeMethod
    def execute(self, dataRef):
        self.log.info("Processing data ID %s" % (dataRef.dataId,))
        self._badPixelMask = MaskU.getPlaneBitMask(self.config.badMaskPlanes)
        self._statsControl = afwMath.StatisticsControl()
        self._statsControl.setNumSigmaClip(self.config.numSigmaClip)
        self._statsControl.setNumIter(self.config.numIter)
        self._statsControl.setAndMask(self._badPixelMask)

        calExp = dataRef.get("raw")
        maskedImage = calExp.getMaskedImage()
        return self.run(maskedImage)



    def run(self, maskedImage):

        statObj = afwMath.makeStatistics(maskedImage, afwMath.MEANCLIP | afwMath.STDEVCLIP | afwMath.ERRORS,
            self._statsControl)
        mean, meanErr = statObj.getResult(afwMath.MEANCLIP)
        self.log.info("clipped mean=%0.2f; meanErr=%0.2f" % (mean, meanErr))

        self.output= pipeBase.Struct(
            mean = mean,
            meanErr = meanErr,
        )
        return self.output



@super_task.wrapclass(super_task.wraprun)
class ExampleStdTask(SuperTask):

    ConfigClass = ExampleStdConfig
    _default_name = "exampleStd"

    def __init__(self, *args, **kwargs):

        super(ExampleStdTask, self).__init__(*args, **kwargs)


    @pipeBase.timeMethod
    def execute(self, dataRef):
        self.log.info("Processing data ID %s" % (dataRef.dataId,))
        self._badPixelMask = MaskU.getPlaneBitMask(self.config.badMaskPlanes)
        self._statsControl = afwMath.StatisticsControl()
        self._statsControl.setNumSigmaClip(self.config.numSigmaClip)
        self._statsControl.setNumIter(self.config.numIter)
        self._statsControl.setAndMask(self._badPixelMask)
        calExp = dataRef.get("raw")
        maskedImage = calExp.getMaskedImage()
        return self.run(maskedImage)


    def run(self, maskedImage):
        statObj = afwMath.makeStatistics(maskedImage, afwMath.MEANCLIP | afwMath.STDEVCLIP | afwMath.ERRORS,
            self._statsControl)
        stdDev, stdDevErr = statObj.getResult(afwMath.STDEVCLIP)
        self.log.info("stdDev=%0.2f; stdDevErr=%0.2f" % \
            (stdDev, stdDevErr))
        self.output = pipeBase.Struct(
            stdDev = stdDev,
            stdDevErr = stdDevErr,
        )
        return self.output



