from typing import Mapping, Optional

import lsst.afw.image as afwImage
import lsst.afw.math as afwMath
import lsst.afw.table as afwTable
import lsst.pex.config as pexConfig
import lsst.pipe.base as pipeBase
import numpy as np
from lsst.geom import Point2I
from lsst.pipe.base import connectionTypes


class ApertureTaskConnections(
    pipeBase.PipelineTaskConnections,
    defaultTemplates={"outputName": "customAperture"},
    dimensions=("visit", "detector", "band"),
):
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
    background = connectionTypes.Input(
        doc="Background model for the exposure",
        storageClass="Background",
        name="calexpBackground",
        dimensions=("visit", "detector", "band"),
    )
    inputCatalog = connectionTypes.Input(
        doc="Input catalog with existing measurements",
        dimensions=(
            "visit",
            "detector",
            "band",
        ),
        storageClass="SourceCatalog",
        name="src",
    )
    outputCatalog = connectionTypes.Output(
        doc="Aperture measurements",
        dimensions=("visit", "detector", "band"),
        storageClass="SourceCatalog",
        name="{outputName}",
    )
    outputSchema = connectionTypes.InitOutput(
        doc="Schema created in Aperture PipelineTask",
        storageClass="SourceCatalog",
        name="{outputName}_schema",
    )

    def __init__(self, *, config=None):
        super().__init__(config=config)

        if config.doLocalBackground is False:
            self.inputs.remove("background")


class ApertureTaskConfig(pipeBase.PipelineTaskConfig, pipelineConnections=ApertureTaskConnections):
    apRad = pexConfig.Field(doc="Radius of aperture", dtype=int, default=4)
    doLocalBackground = pexConfig.Field(
        doc="Should the background be added " "before doing photometry", dtype=bool, default=False
    )


class ApertureTask(pipeBase.PipelineTask):
    ConfigClass = ApertureTaskConfig
    _DefaultName = "apertureDemoTask"

    def __init__(self, config: pexConfig.Config, initInput: Mapping, *args, **kwargs):
        super().__init__(config=config, *args, **kwargs)
        self.apRad = self.config.apRad
        inputSchema = initInput["inputSchema"].schema

        # Create a camera mapper to create a copy of the input schema
        self.mapper = afwTable.SchemaMapper(inputSchema)
        self.mapper.addMinimalSchema(inputSchema, True)

        # Add the new field
        self.apKey = self.mapper.editOutputSchema().addField(
            "apFlux", type=np.float64, doc="Ap flux measured"
        )

        # Get the output schema
        self.schema = self.mapper.getOutputSchema()

        # create the catalog in which new measurements will be stored
        self.outputCatalog = afwTable.SourceCatalog(self.schema)

        # Put the outputSchema into a SourceCatalog container. This var name
        # matches an initOut so will be persisted
        self.outputSchema = afwTable.SourceCatalog(self.schema)

    def run(
        self,
        exposure: afwImage.Exposure,
        inputCatalog: afwTable.SourceCatalog,
        background: Optional[afwMath.BackgroundList] = None,
    ) -> pipeBase.Struct:
        # If a background is supplied, add it back to the image so local
        # background subtraction can be done.
        if background is not None:
            exposure.image.array += background.image

        # Add in all the records from the input catalog into what will be the
        # output catalog
        self.outputCatalog.extend(inputCatalog, mapper=self.mapper)

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
            distance = ((indy - center[0]) ** 2 + (indx - center[0]) ** 2) ** 0.5
            mask = distance < self.apRad
            flux = np.sum(stamp * mask)

            # Do local background subtraction
            if background is not None:
                outerAn = distance < 2.5 * self.apRad
                innerAn = distance < 1.5 * self.apRad
                annulus = outerAn - innerAn
                localBackground = np.mean(exposure.image.array * annulus)
                flux -= np.sum(mask) * localBackground

            # Set the flux field of this source
            source.set(self.apKey, flux)

        return pipeBase.Struct(outputCatalog=self.outputCatalog)
