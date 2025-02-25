import numpy as np

import lsst.afw.image as afwImage
import lsst.afw.table as afwTable
import lsst.pex.config as pexConfig
import lsst.pipe.base as pipeBase
from lsst.geom import Point2I
from lsst.pipe.base import connectionTypes  # noqa: F401


class ApertureTaskConfig(pexConfig.Config):
    apRad = pexConfig.Field(doc="Radius of aperture", dtype=int, default=4)


class ApertureTask(pipeBase.Task):
    ConfigClass = ApertureTaskConfig
    _DefaultName = "apertureDemoTask"

    def __init__(self, config: pexConfig.Config, *args, **kwargs):
        super().__init__(config, *args, **kwargs)
        self.apRad = self.config.apRad

        self.outputSchema = afwTable.SourceTable.makeMinimalSchema()
        self.apKey = self.outputSchema.addField("apFlux", type=np.float64, doc="Ap flux measured")

    def run(self, exposure: afwImage.Exposure, inputCatalog: afwTable.SourceCatalog) -> pipeBase.Struct:
        # set dimension cutouts to 3 times the apRad times 2 (for diameter)
        dimensions = (3 * self.apRad * 2, 3 * self.apRad * 2)

        # Get indexes for each pixel
        indy, indx = np.indices(dimensions)

        outputCatalog = afwTable.SourceCatalog(self.outputSchema)
        outputCatalog.reserve(len(inputCatalog))

        # Loop over each record in the catalog
        for source in inputCatalog:
            # Create an aperture and measure the flux
            center = Point2I(source.getCentroid())
            center = (center.getY(), center.getX())

            stamp = exposure.image.array[
                center[0] - 3 * self.apRad : center[0] + 3 * self.apRad,
                center[1] - 3 * self.apRad : center[1] + 3 * self.apRad,
            ]
            mask = ((indy - center[0]) ** 2 + (indx - center[0]) ** 2) ** 0.5 < self.apRad
            flux = np.sum(stamp * mask)

            # Add a record to the output catalog
            tmpRecord = outputCatalog.addNew()
            tmpRecord.set(self.apKey, flux)

        return pipeBase.Struct(outputCatalog)
