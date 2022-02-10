import numpy as np

import lsst.pipe.base as pipeBase
import lsst.pex.config as pexConfig
import lsst.afw.table as afwTable
import lsst.afw.image as afwImage
from lsst.geom import Point2I

from lsst.pipe.base import connectionTypes


class ApertureTaskConnections(pipeBase.PipelineTaskConnections, dimensions=("visit", "detector", "band")):
    exposure = connectionTypes.Input(
        doc="Input exposure to make measurements on",
        dimensions=("visit", "detector", "band"),
        storageClass="ExposureF",
        name="calexp",
    )
    inputCatalog = connectionTypes.Input(
        doc="Input catalog with existing measurements",
        dimensions=("visit", "detector", "band"),
        storageClass="SourceCatalog",
        name="src",
    )
    outputCatalog = connectionTypes.Output(
        doc="Aperture measurements",
        dimensions=("visit", "detector", "band"),
        storageClass="SourceCatalog",
        name="customAperture",
    )


class ApertureTaskConfig(pipeBase.PipelineTaskConfig, pipelineConnections=ApertureTaskConnections):
    apRad = pexConfig.Field(doc="Radius of aperture", dtype=int, default=4)


class ApertureTask(pipeBase.PipelineTask):

    ConfigClass = ApertureTaskConfig
    _DefaultName = "apertureDemoTask"

    def __init__(self, config: pexConfig.Config, *args, **kwargs):
        super().__init__(config=config, *args, **kwargs)
        self.apRad = self.config.apRad

        self.outputSchema = afwTable.SourceTable.makeMinimalSchema()
        self.apKey = self.outputSchema.addField("apFlux", type=np.float64, doc="Ap flux measured")

        self.outputCatalog = afwTable.SourceCatalog(self.outputSchema)

    def run(self, exposure: afwImage.Exposure, inputCatalog: afwTable.SourceCatalog) -> pipeBase.Struct:
        # set dimension cutouts to 3 times the apRad times 2 (for diameter)
        dimensions = (3 * self.apRad * 2, 3 * self.apRad * 2)

        # Get indexes for each pixel
        indy, indx = np.indices(dimensions)

        # Loop over each record in the catalog
        for source in inputCatalog:
            # Create an aperture and measure the flux
            center = Point2I(source.getCentroid())
            center = (center.getY(), center.getX())
            # Create a cutout
            stamp = exposure.image.array[
                center[0] - 3 * self.apRad : center[0] + 3 * self.apRad,
                center[1] - 3 * self.apRad : center[1] + 3 * self.apRad,
            ]
            mask = ((indy - center[0]) ** 2 + (indx - center[0]) ** 2) ** 0.5 < self.apRad
            flux = np.sum(stamp * mask)

            # Add a record to the output catalog
            tmpRecord = self.outputCatalog.addNew()
            tmpRecord.set(self.apKey, flux)

        return pipeBase.Struct(outputCatalog=self.outputCatalog)
