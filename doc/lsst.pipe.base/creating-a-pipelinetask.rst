.. _pipe-base-creating-a-pipelinetask:

#######################
Creating a PipelineTask
#######################

So you want to build a |PipelineTask|, where should you begin? Right here in this guide!
This guide will create an example PipelineTask to measure aperture photometry, using
progressively more features of Gen3 middleware. So let's begin.

From Task to PipelineTask
=========================
A |PipelineTask| is at its heart just a |Task|, so the best place to start in creating a task is to create a
Config class.

.. code-block:: python

    import numpy as np

    import lsst.pipe.base as pipeBase
    import lsst.pex.config as pexConfig
    import lsst.afw.table as afwTable
    import lsst.afw.image as afwImage

    from lsst.pipe.base import connectionTypes


    class ApertureTaskConfig(pexConfig.Config):
        apRad = pexConfig.Field(doc="Radius of aperture", dtype=int, default=4)

Next, create a |Task| calss that performs the measurements:

.. code-block:: python

    class ApertureTask(pipeBase.Task):

        ConfigClass = ApertureTaskConfig
        _DefaultName = "apertureDemoTask"

        def __init__(self, config: pexConfig.Config, *args, **kwargs):
            super().__init__(config, *args, **kwargs)
            self.apRad = self.config.apRad

            self.outputSchema = afwTable.SourceTable.makeMinimalSchema()
            self.apKey = self.outputSchema.addField("apFlux", type=np.float64,
                                                    doc="Ap flux measured")

            self.outputCatalog = afwTable.SourceCatalog(self.outputSchema)

        def run(self, exposure: afwImage.Exposure,
                inputCatalog: afwTable.SourceCatalog) -> pipeBase.Struct:
            # set dimension cutouts to 3 times the apRad times 2 (for diameter)
            dimensions = (3*self.apRad*2, 3*self.apRad*2)

            # Get indexes for each pixel
            indy, indx = np.indices(dimensions)

            # Loop over each record in the catalog
            for source in inputCatalog:
                # Create an aperture and measure the flux
                center = Point2I(source.getCentroid())
                center = (center.getY(), center.getX())

                stamp = exposure.image.array[center[0]-3*self.apRad: center[0]+3*self.apRad,
                                            center[1]-3*self.apRad: center[1]+3*self.apRad]
                mask = ((indy - center[0])**2
                        + (indx - center[0])**2)**0.5 < self.apRad
                flux = np.sum(stamp*mask)

                # Add a record to the output catalog
                tmpRecord = self.outputCatalog.addNew()
                tmpRecord.set(self.apKey, flux)

            return self.outputCatalog

So now you have a task that takes an exposure and inputCatalog in its run
method and returns a new output catalog with apertures measured. As with all
Tasks, this will work well for in-memory data products, but you want to read and
write datasets so you will need to write a PipelineTask. To do this we need to
have our task inherit from PipelineTask primitives instead of base objects.
Let's start by converting our Config class.

.. code-block:: python

    class ApertureTaskConfig(pipeBase.PipelineTaskConfig):
        ...

Now however when we try and import the module containing this config, an
exception is thrown complaining about a missing PipelineTaskConnections class.
In generation 3 middleware Tasks that do IO need to declare what kind of
data-products they need, what kind they will produce, and what identifiers are
used to fetch that data. Additionally PipelineTaskConnections defined a unit of
work over which the task will operate, such as measurements on coadds will work
on an individual Tracts, Patches, and Filter. By declaring this information, the
generation 3 middleware is able to orchestrate the loading, saving, and running
of one or more tasks. With that in mind, let's create a
`~lsst.pipe.base.PipelineTaskConnections` class for our new Task. You would
expect that this task is going to work on individual ccds, looking at processed
calexps and associated measurement catalogs (that were produced by some previous
task).

.. code-block:: python

    class ApertureTaskConnections(pipeBase.PipelineTaskConnections,
                                dimensions=("visit", "detector", "band")):,
        exposure = connectionTypes.Input(doc="Input exposure to make measurements "
                                              "on",
                                         dimensions=("visit", "detector"),
                                         storageClass="ExposureF",
                                         name="calexp")
        inputCatalog = connectionTypes.Input(doc="Input catalog with existing "
                                                  "measurements",
                                             dimensions=("visit", "detector",
                                                         "band",),
                                             storageClass="SourceCatalog",
                                             name="src")
        outputCatalog = connectionTypes.Output(doc="Aperture measurements",
                                               dimensions=("visit", "detector",
                                                           "band"),
                                               storageClass="SourceCatalog",
                                               name="customAperture")

So what is going on here? The first thing that happens is that
ApertureTaskConnections inherits from PipelineTaskConnections, that is fairly
standard. What might be new syntax for you is the ``dimensions=...``. This is
how we tell the connections class what unit of work a Task that uses this
connection class will operate on. The unit of work for a |PipelineTask| is
known as a ``quantum``. In the case of our Task, it will work on a ``visit`` (a
single image taken by the camera), ``detector`` (an individual ccd out of the
camera's mosaic), ``band`` (an abstract notion of a filter, say r band, that is
not tied to the exact band passes of an individual telescope filter).

Next, take a look at the fields defined on your new connection class. These
are defined in a similar way as defining a configuration class, but instead
of using `~lsst.pex.config.Field` types from `lsst.pex.config`,
connection classes make use of connection types defined in
`lsst.pipe.base.connectionTypes`. These connections define the inputs and outputs that
a |PipelineTask| will expect to make use of. Each of these connections documents
what the connection is, what dimensions represent this data product (in this
case they are the same as the task itself, in the
:ref:`PipelineTask-processing-multiple-data-sets` section we will cover when
they are different), what kind of storage class represents this data type on
disk, and the name of the data type itself. In this connections class you have
defined two inputs and an output. The inputs the Task will make use of are
``calexp`` calibrated exposures and ``src`` catalogs, both of which are produced
by the `CalibratePipelineTask` during single frame processing. Our Task will
produce a new SourceCatalog of aperture measurements and save it out with a
dataset type we have named ``customAperture``.

So now you have a connections class, how do you use it? The good news is that
you only need to make one small change to our ``ApertureTaskConfig``, passing
the connections class in as a parameter to the configuration class declaration.

.. code-block:: python

    class ApertureTaskConfig(pipeBase.PipelineTaskConfig,
                             pipelineConnections=ApertureTaskConnections):
        ...

That's it. All the rest of the Config class stays the same. Below, examples
demonstrate what this change does, and how to use it, but first take a
look at what changes when you turn a Task into a PipelineTask.

.. code-block:: python

    class ApertureTask(pipeBase.PipelineTask):

        ...

        def run(self, exposure: afwImage.Exposure,
                inputCatalog: afwTable.SourceCatalog) -> pipeBase.Struct:
                ...
            return pipeBase.Struct(outputCatalog=self.outputCatalog)

In a simple PipelineTask like this, these are all the changes that need to be
made. Firstly the base class to is changed to `PipelineTask`. This inheritance
provides all the base machinery that the middleware will need to run
this task. The second change you need to make a task into a `PipelineTask` is
to change the signature of the run method. A run method in a PipelineTask must
return a `lsst.pipe.base.Struct` object whose field names correspond to the
names of the outputs defined in the connection class. In our connection class
we defined the output collection with the identifier ``outputCatalog``, so in
our returned `~lsst.pipe.base.Struct` has a field with that name as well.
Another thing worth highlighting, though it was not a change that was made, is
the names of the arguments to the run method. These names also must (and do)
correspond to the identifiers used for the input connections. The names of the
variables of the inputs and outputs are how the |PipelineTask| activator maps
connections into the in-memory data products that the algorithm requires.

The complete source integrating these changes can be used in :ref:`pipeline-appendix-a`.


Arguments and returns for \_\_init\_\_
======================================

In this exercise you will make this task a little bit more advanced. Instead
of creating a new catalog that only contains the aperture flux measurements,
you will create a catalog that contains all the input data, and add an
additional column for the aperture measurements. To do this you will need the
schema for the input catalog in the init method so that you can correctly
construct our output catalog to contain all the appropriate fields. This is
accomplished by using the InitInput connection type in the connection class.
Data products specified with this connection type will be provided to the
``__init__`` method of a task in a dictionary named ``initInputs`` when the task
is executed. Take a look what the connection class looks like.

.. code-block:: python

    class ApertureTaskConnections(pipeBase.PipelineTaskConnections,
                                  dimensions=("visit", "detector", "band")):
        inputSchema = connectionTypes.InitInput(doc="Schema associated with a src catalog",
                                                storageClass="SourceCatalog",
                                                name="src_schema")
        ...

    class ApertureTask(pipeBase.PipelineTask):

        ...

        def __init__(self, config: pexConfig.Config, initInput: Mapping,
                     *args, **kwargs):
            ...
            inputSchema = initInput['inputSchema'].schema

            # Create a camera mapper to create a copy of the input schema
            self.mapper = afwTable.SchemaMapper(inputSchema)
            self.mapper.addMinimalSchema(inputSchema, True)

            # Add the new field
            self.apKey = self.mapper.editOutputSchema().addField("apFlux",
                                                                 type=np.float64,
                                                                 doc="Ap flux
                                                                     "measured")

            # Get the output schema
            self.schema = self.mapper.getOutputSchema()

            # create the catalog in which new measurements will be stored
            self.outputCatalog = afwTable.SourceCatalog(self.schema)

        def run(self, exposure: afwImage.Exposure,
                inputCatalog: afwTable.SourceCatalog
                ) -> pipeBase.Struct:
            # Add in all the records from the input catalog into what will be the
            # output catalog
            self.outputCatalog.extend(inputCatalog, mapper=self.mapper)

            # set dimension cutouts to 3 times the apRad times 2 (for diameter)
            dimensions = (3*self.apRad*2, 3*self.apRad*2)

            # Get indexes for each pixel
            indy, indx = np.indices(dimensions)

            # Loop over each record in the catalog
            for source in inputCatalog:
                # Create an aperture and measure the flux
                center = Point2I(source.getCentroid())
                center = (center.getY(), center.getX())
                # Create a cutout
                stamp = exposure.image.array[center[0]-3*self.apRad: center[0]+3*self.apRad,
                                            center[1]-3*self.apRad: center[1]+3*self.apRad]
                mask = ((indy - center[0])**2
                        + (indx - center[0])**2)**0.5 < self.apRad
                flux = np.sum(stamp*mask)

                # Set the flux field of this source
                source.set(self.apKey, flux)

            return pipeBase.Struct(outputCatalog=self.outputCatalog)

These changes allow you to load in and use schemas to initialize your task before
any actual data is loaded to be passed to the algorithm code located in the
``run`` method. Inside the ``run`` method, the output catalog copies all the
records from the input into itself. The loop can the go over the output
catalog, and insert the new measurements right into the output catalog.

One thing to note about `~lsst.pipe.base.connectionTypes.InitInput` connections
is that they do not take any dimensions. This is because the sort of data loaded
will correspond to a given data-set type produced by a task, and not by
(possibly multiple) executions of a run method over data-sets that have
dimensions. In other words, these data-sets are unique to the task itself and
not tied to the unit of work that the task operates on.

In the same way the input schema from some previous stage of processing was
added, the output schema for your task should also be persisted so some other
|PipelineTask| can make use of it. To do this you should add an
`~lsst.pipe.base.connectionTypes.InitOutput` connection to your connection
class.

.. code-block:: python

    class ApertureTaskConnections(pipeBase.PipelineTaskConnections,
                                  dimensions=("visit", "detector",
                                              "band", "skymap")):
        ...
        outputSchema = connectionTypes.InitOutput(doc="Schema created in Aperture PipelineTask",
                                                  storageClass="SourceCatalog",
                                                  name="customAperture_schema")

        ...


    class ApertureTask(pipeBase.PipelineTask):

        ...

        def __init__(self, config: pexConfig.Config, initInput:Mapping,
                     *args, **kwargs):
            ...
            # Get the output schema
            self.schema = mapper.getOutputSchema()

            # Create the output catalog
            self.outputCatalog = afwTable.SourceCatalog(self.schema)

            # Put the outputSchema into a SourceCatalog container. This var name
            # matches an initOut so will be persisted
            self.outputSchema = afwTable.SourceCatalog(self.schema)

In the init method we associate the variable we would like to output with a
name that matches the variable name used in the connection class. The
activator uses this shared name to know that variable should be persisted.

The complete updated example can be found in :ref:`pipeline-appendix-b`.

Optional Datasets
==================
Sometimes it is useful to have a task that optionally uses a data-set. In the
case of the example task you have been building, this might be a background
model that was previously removed. You may want your task to add back in the
background so that it can do a new local background estimate. To start add
the background data-set to our connection class like you did for your other
data-sets.

.. code-block:: python

    class ApertureTaskConnections(pipeBase.PipelineTaskConnections,
                                  dimensions=("visit", "detector", "band")):
        ...
        background = connectionTypes.Input(doc="Background model for the exposure",
                                           storageClass="Background",
                                           name="calexpBackground",
                                           dimensions=("visit", "detector", "band"))
        ...

Now your `PipelineTask` will load the background each time the task is run.
How do you make this optional? First, add a configuration field in your
config class to allow the user to specify if it is to be loaded like thus

.. code-block:: python

    class ApertureTaskConfig(pipeBase.PipelineTaskConfig,
                             pipelineConnections=ApertureTaskConnections):
        ...
        doLocalBackground = pexConfig.Field(doc="Should the background be added "
                                                "before doing photometry",
                                            dtype=bool, default=False)

The ``__init__`` method of the connection class is given an instance of the
task's configuration class after all overrides have been applied. This provides
you an opportunity change the behavior of the connection class according to
various configuration options.

.. code-block:: python

    class ApertureTaskConnections(pipeBase.PipelineTaskConnections,
                                  dimensions=("visit", "detector",
                                              "band", "skymap")):
        ...
        background = ct.Input(doc="Background model for the exposure",
                              storageClass="Background",
                              name="calexpBackground",
                              dimensions=("visit", "detector", "band",
                                          "skymap"))
        ...

        def __init__(self, *, config=None):
            super().__init__(config=config)

            if config.doLocalBackground is False:
                self.inputs.remove("background")

Your connection class now looks at the value of ``doLocalBackground`` on the
``config`` object and if it is ``False``, removes it from the connection
instances list of input connections. Connection classes keep track of what
connections are defined in sets. Each set contains the variable names of a
connection, and the sets themselves are identified by the type of connection
they contain. In the example you are modifying the set of input connections. The
names for each of the sets are as follows:

* ``initInputs``
* ``initOutputs``
* ``inputs``
* ``prerequisiteInputs``
* ``outputs``

The last step in modifying your task will be to update the ``run`` method to
take into account that a background may or may not be supplied.

.. code-block:: python

    ...
    import typing

    import lsst.afw.math as afwMath

    ...

    class ApertureTask(pipeBase.PipelineTask):

        ...

        def run(self, exposure: afwImage.Exposure,
                inputCatalog: afwTable.SourceCatalog,
                background: typing.Optional[afwMath.BackgroundList] = None
                ) -> pipeBase.Struct:
            # If a background is supplied, add it back to the image so local
            # background subtraction can be done.
            if background is not None:
                exposure.image.array += background.image

            ...

            # Loop over each record in the catalog
            for source in outputCatalog:

                ...
                distance = ((indy - center[0])**2
                            + (indx - center[0])**2)**0.5
                mask = distance < self.apRad
                flux = np.sum(stamp*mask)

                # Do local background subtraction
                if background is not None:
                    outerAn = distance < 2.5*self.apRad
                    innerAn = distance < 1.5*self.apRad
                    annulus = outerAn - innerAn
                    localBackground = np.mean(exposure.image.array*annulus)
                    flux -= np.sum(mask)*localBackground

                ...

            return pipeBase.Struct(outputCatalog=self.outputCatalogs)

The ``run`` method now takes an argument named ``background``, which defaults to
a value of `None`. If the connection is removed from the connection class, there
will be no argument passed to run with that name. Conversely, when the
connection is present, the background is un-persisted by the butler, and is
passed on to the run method. The body of the run method checks if the background
has been passed, and if so adds it back in and does a local background
subtraction.

To bring this all together, see :ref:`pipeline-appendix-c`


----------------------------------------
Dataset name configuration and templates
----------------------------------------
Now that you have the option to control results of processing with a
configuration option (turning on and off local background subtraction) it may
be useful for a user who turns on local background subtraction to change the
name of the data-set produced so as to tell what the configuration was
without looking at the persisted configurations. The user may make a
configuration override file that looks something like the following:

.. code-block:: python

    config.doLocalBackground = True
    config.connections.outputSchema = "customAperture_localBg_schema"
    config.connections.outputCatalog = "customAperture_localBg"

This config file introduces the special attribute ``connections`` that exists
on every `PipelineTaskConfig` class. This attribute is dynamically built from
the linked `~lsst.pipe.base.PipelineTaskConnections` class. The ``connections``
attribute is a sub-config that has `~lsst.pex.config.Field`\ s corresponding to
the variable names of the connections, with values of those fields corresponding
to the name of a connection. So by default ``config.connections.outputCatalog``
would be ``customAperture`` and ``config.connections.exposure`` would be
``calexp`` etc. Assigning to these `~lsst.pex.config.Field`\ s has the effect of
changing the name of the data-set type defined in the connection.

In this config file you are changing the name of the data-set type that will
be persisted to include the information that local background subtraction was
done. It is interesting to note that there are no hard coded data-set type
names that must be adhered to, the user is free to pick any name. The only
consequence of changing a dataset type name, is that any downstream code
that is to use the output data-set must have its default name changed to
match. As an aside, ``pipetask`` requires that the first time a
data-set name is used the activator command is run with the
`--register-dataset-types` switch. This is to prevent accidental typos
becoming new data-set types.

Looking at the config file, there are two different `~lsst.pex.config.Field`\
s that are being set to the same value. In the case of other tasks this
number may even be higher. This leads not only to the issue of config
overrides needing to potentially be lengthy, but that there may be a typo,
and the fields will be set inconsistently. |PipelineTask| tasks address this by
providing a way to template data-set type names.

.. code-block:: python

    class ApertureTaskConnections(pipeBase.PipelineTaskConnections,
                                  defaultTemplates={"outputName": "customAperture"},
                                  dimensions=("visit", "detector", "band")):
        ...


        outputCatalog = connectionTypes.Output(doc="Aperture measurements",
                                               dimensions=("visit", "detector", "band"),
                                               storageClass="SourceCatalog",
                                               name="{outputName}")
        ...
        outputSchema = connectionTypes.InitOutput(doc="Schema created in Aperture PipelineTask",
                                                  storageClass="SourceCatalog",
                                                  name="{outputName}_schema")


In the modified connection class, the ``outputSchema`` and ``outputCatalog``
connections now have python format strings, which are referred to with the
template name ``outputName``. This template will be formatted with a specified
string to become a dataset type name prior to any data being read and supplied
to the task. You may have noticed the class declaration also has a new argument:
``defaultTemplates``.  The strings defined inside ``defaultTemplates`` are what
will be used to format the name string if a user does not provide any overrides.
The defaults are supplied as a Python dictionary of template identifiers as
keys, and default strings as values. If there are any templates used in a
connection class, you must supply a default template for each template
identifier. A `TypeError` will be thrown if you attempt to import a module
containing a `~lsst.pipe.base.PipelineTaskConnections` class that does not have
defaults for all the defined templates.

With these changes, have a look at how your configuration override file changes.

.. code-block:: python

    config.doLocalBackground = True
    config.connections.outputName = "customAperture_localBg"

The ``connections`` sub-config now contains a `~lsst.pex.config.Field` called
``outputName``, the same as your template identifier. Each template identifier
will have a corresponding field on the `connections` sub-config. Setting the
value on these configs has the effect of setting the templates wherever
they are used.

Setting a template config field does not preclude you from also setting the
name of a dataset type directly. This may be useful in |PipelineTask|\ s
with templates used in lots of places. Though not needed in this example,
such a config would look something like the following:

.. code-block:: python

    config.doLocalBackground = True
    config.connections.outputName = "customAperture_localBg"
    config.connections.outputSchema = "different_name_schema"

View the complete code in :ref:`pipeline-appendix-d`


Prerequisite inputs
-------------------
Some tasks make use of datasets that are created outside the processing
environment of the Science Pipelines. These may be things like reference
catalogs, bright star masks, and some calibrations. To account for this
PipelineTasks have a special type of connection called,
`~lsst.pipe.base.connectionTypes.PrerequisiteInput`. This type of input tells
the execution system, that this is a special type of dataset, and not to expect
it to be produced anywhere in a processing pipeline. If this dataset is not
found, the system will raise a hard error and tell you the dataset type is
missing instead of inferring that there is some processing step that is missing.

These connections are specified the same way as any input, but with a different
connection type name. In the example task you are creating, you will add in a
prerequisite type on a mask, that is assumed to be human-created. If a source
center happens to fall in a masked area, no aperture photometry will be
performed. Because this is almost the same as an Input connection type, the
complete updated example is shown in :ref:`pipeline-appendix-e`, in lieu of
building it up a piece at a time.


Prerequisite inputs have an additional attribute named ``lookupFunction``. This
attribute is optional and can be used to manually lookup datasets associated
with a prerequisite input. Using a ``lookupFunction`` is an advanced feature
that should only be needed very rarely. As such, they are beyond the scope of
this tutorial, and are only brought up for completeness sake.


.. _PipelineTask-processing-multiple-data-sets:

Processing multiple data-sets of the same dataset type
------------------------------------------------------
The dimensions you have used in your task up to this point have specified that
the unit of processing is to be done on individual detectors on a per-visit
basis. This makes sense, as this is a natural way to parallelize this data.
However, sometimes it is useful to consider an entire focal plane at a time,
or some other larger scale concept, like a tract. By changing the dimensions
of the unit of processing for your task, you change what sort of inputs your
task will expect. These changes are reflected in the connections class as
follows.

.. code-block:: python

    class ApertureTaskConnections(
        pipeBase.PipelineTaskConnections,
        defaultTemplates={"outputName": "customAperture"},
        dimensions=("visit", "band"),
    ):
        exposures = connectionTypes.Input(
            doc="Input exposure to make measurements on",
            dimensions=("visit", "detector", "band"),
            storageClass="ExposureF",
            name="calexp",
            multiple=True,
        )
        backgrounds = connectionTypes.Input(
            doc="Background model for the exposure",
            storageClass="Background",
            name="calexpBackground",
            dimensions=("visit", "detector", "band"),
            multiple=True,
        )
        inputCatalogs = connectionTypes.Input(
            doc="Input catalog with existing measurements",
            dimensions=(
                "visit",
                "detector",
                "band",
            ),
            storageClass="SourceCatalog",
            name="src",
            multiple=True,
        )
        outputCatalogs = connectionTypes.Output(
            doc="Aperture measurements",
            dimensions=("visit", "detector", "band"),
            storageClass="SourceCatalog",
            name="{outputName}",
            multiple=True,
        )
        ...
        areaMasks = connectionTypes.PrerequisiteInput(
            doc="A mask of areas to be ignored",
            storageClass="Mask",
            dimensions=("visit", "detector", "band"),
            name="ApAreaMask",
            multiple=True,
        )
        ...

The dimensions of your `ApertureTaskConnections` class are now ``visit`` and
``band``. However, all of your input data-sets are themselves still defined
over each of these dimensions, and also ``detector``. That is to say you get
one ``calexp`` for ever unique combination of ``exposure``\ 's dimensions.
Because the tasks's dimensions are a more inclusive set of dimensions (less
specified) you should expect that for a given unit of processing, there will be
multiple values for each of the input data-set types along the ``detector``
dimension. For example, in LSST there will be 189 detectors in each visit. You
indicate to the execution framework that you expect there to be a list of
datasets for each connection by adding ``multiple=True`` to its declaration.
This ensures the values passed will be inside of a list container.

As a caveat, depending on the exact data that has been ingested/processed,
there may only be one data-set that matches this combination of dimensions
(i.e. only one raw was ingested for a visit) but the ``multiple`` flag will
still ensure that the system passes this one data-set along inside contained
inside a list. This ensures a uniform api to program against. Make note that
the connection variable names change to add an `s` on connections marked with
`multi` to reflect that they will potentially contain multiple values.

With this in mind, go ahead and make the changes needed accommodate multiple
data-sets in each inputs to the run method. Because this task is inherently
parallel over detectors, these modifications are not the most natural way to
code this behavior, but are done to demonstrate how to make use of the
``multi`` flag for situations that are not so trivial.

.. code-block:: python

    class ApertureTask(pipeBase.PipelineTask):

        ...

        def run(self, exposures: List[afwImage.Exposure],
                inputCatalogs: List[afwTable.SourceCatalog],
                areaMasks: List[afwImage.Mask],
                backgrounds: Optional[typing.List[afwMath.BackgroundList]] = None
                ) -> pipeBase.Struct:
            # Track the length of each catalog as to know which exposure to use
            # in later processing
            cumulativeLength = 0
            lengths = []

            # Add in all the input catalogs into the output catalog
            for inCat in inputCatalogs:
                self.outputCatalog.extend(inCat, mapper=self.mapper)
                lengths.append(len(inCat)+cumulativeLength)
                cumulativeLength += len(inCat)

            ...

            # track which image is being used
            imageIndex = 0
            exposure = exposures[imageIndex]
            areaMask = areaMasks[imageIndex]
            background = areaMasks[imageIndex]

            # Loop over each record in the catalog
            for i, source in enumerate(self.outputCatalog):
                # get the associated exposure
                if i >= lengths[imageIndex]:
                    # only update if this is not the last index
                    if imageIndex < len(lengths) - 1:
                        imageIndex += 1
                    exposure = exposures[imageIndex]
                    areaMask = areaMasks[imageIndex]
                    background = areaMasks[imageIndex]
                # If a background is supplied, add it back to the image so local
                # background subtraction can be done.
                if backgrounds is not None:
                    exposure.image.array += background.image

                ...

                # Skip measuring flux if the center of a source is in a masked
                # pixel
                if areaMask.array[center[0], center[1]] != 0:
                    ...

                ...

                # Do local background subtraction
                if backgrounds is not None:
                    ...

                ...

            return pipeBase.Struct(outputCatalog=self.outputCatalog)

The signature of the run method now reflects the change in the names of the
connections. Next, the output catalog must be extended to include all the
supplied input catalogs. When the loop proceeds through the list of output
sources, it checks if that source as come from the next inputCatalog in the
list. If it has then a counter reflecting which image arrays to use is
updated to match. The specifics of this code are not as important as the way
in which they use the variables supplied to run, namely that the arguments
are now lists of variables that each need handled.

The complete example can be found in :ref:`pipeline-appendix-f`.

----------------
Deferred Loading
----------------

An astute eye will notice that this code will potentially consume a large
amount of memory. Loading in every catalog, image, background, and mask for
an entire LSST focal plain will put a lot of pressure on the memory of the
computer running this code. Fortunately the middleware gives you a way to
lighten this load. You can add an argument to a connection in our connection
class that informs the middleware to not load the data at the time of
running, but supply a variable that allows a task to load the data when the
task needs it. This argument is called ``deferLoad``, and takes a boolean
value which is by default false. Take a look at the connection class with
this in place.

.. code-block:: python

    class ApertureTaskConnections(
        pipeBase.PipelineTaskConnections,
        defaultTemplates={"outputName": "customAperture"},
        dimensions=("visit", "band"),
    ):
        exposures = connectionTypes.Input(
            doc="Input exposure to make measurements on",
            dimensions=("visit", "detector", "band"),
            storageClass="ExposureF",
            name="calexp",
            multiple=True,
            deferLoad=True,
        )
        backgrounds = connectionTypes.Input(
            doc="Background model for the exposure",
            storageClass="Background",
            name="calexpBackground",
            dimensions=("visit", "detector", "band"),
            multiple=True,
            deferLoad=True,
        )
        inputCatalogs = connectionTypes.Input(
            doc="Input catalog with existing measurements",
            dimensions=(
                "visit",
                "detector",
                "band",
            ),
            storageClass="SourceCatalog",
            name="src",
            multiple=True,
            deferLoad=True,
        )
        ...
        areaMasks = connectionTypes.PrerequisiteInput(
            doc="A mask of areas to be ignored",
            storageClass="Mask",
            dimensions=("visit", "detector", "band"),
            name="ApAreaMask",
            multiple=True,
            deferLoad=True,
        )
        ...

Take a look at how the `run` method changes to make use of this.


.. code-block:: python

    class ApertureTask(pipeBase.PipelineTask):
        ...
        def run(self, exposures: List[afwImage.Exposure],
                inputCatalogs: List[afwTable.SourceCatalog],
                areaMasks: List[afwImage.Mask],
                backgrounds: Optional[typing.List[afwMath.BackgroundList]] = None
                ) -> pipeBase.Struct:
            ...

            # track which image is being used
            imageIndex = 0
            exposure = exposures[imageIndex].get()
            areaMask = areaMasks[imageIndex].get()
            background = areaMasks[imageIndex].get()

            # Loop over each record in the catalog
            for i, source in enumerate(self.outputCatalog):
                # get the associated exposure
                if i >= lengths[imageIndex]:
                    # only update if this is not the last index
                    if imageIndex < len(lengths) - 1:
                        imageIndex += 1
                    exposure = exposures[imageIndex].get()
                    areaMask = areaMasks[imageIndex].get()
                    background = areaMasks[imageIndex].get()
                ...

            return pipeBase.Struct(outputCatalog=self.outputCatalog)

In this modified `run` method the only code addition is the use of the `get`
method on input arguments. When a connection is marked with `deferLoad`, the
middleware will supply an `~lsst.daf.butler.DeferredDatasetHandle`. This handle
has a `get` method which loads and returns the object specified by the
handle. The ``get``` method also optionally supports a `parameters` argument
that can be used in the same manor as a normal `~lsst.daf.butler.Butler`
``get`` call. This allows things like fetching only part of an image, loading
only the wcs from an exposure, etc. See `~lsst.daf.butler.Butler`
documentation for more info on the parameters argument.

:ref:`pipeline-appendix-g` is now the complete example for this code.

.. _PipelineTask-processing-altering-what-is-processed:

---------------------------------------------
Checking and altering quanta before execution
---------------------------------------------

When a `QuantumGraph` is first created, the tasks it will run are given an opportunity to make limited adjustments and raise exceptions if a quantum does not meet its needs in more subtle ways, via a call to `PipelineTaskConnections.adjustQuantum`.
This method is also called just before each quantum is executed, since predecessor quanta may not have actually produced some of the outputs they were predicted to, and that will change the inputs that are available to the current quantum.

For the vast majority of `PipelineTask` subclasses, the default implementation provided by the base class should be adequate, and even classes that wish to override it should always delegate to `super`, as the base class implementation performs the checks implied by the ``multiple`` and ``minumum`` arguments to connection fields.
Overriding `~PipelineTaskConnections.adjustQuantum` is most useful for tasks that have more than one connection with ``multiple=True``, and the data IDs of these connections are closely related, such as when the presence of one input dataset implies the production of a corresponding output dataset.
That's exactly what should happens with `ApertureTask` now; it should produce an ``outputCatalog`` dataset for a data ID only when all input datasets for that data ID are present:

.. code-block:: python

    class ApertureTaskConnections(pipeBase.PipelineTaskConnections,
                                defaultTemplates={"outputName":"customAperture"},
                                dimensions=("visit", "detector", "band")):
        ...

        def adjustQuantum(self, inputs, outputs, label, data_id):
            # Find the data IDs common to all multiple=True inputs.
            input_names = ("exposures", "inputCatalogs", "backgrounds")
            inputs_by_data_id = []
            for name in input_names:
                inputs_by_data_id.append(
                    {ref.dataId: ref for ref in inputs[name][1]}
                )
            # Intersection looks messy because dict_keys only supports |.
            # not an "intersection" method.
            data_ids_to_keep = functools.reduce(
                operator.__and__,
                (d.keys() for d in inputs_by_data_id)
            )
            # Pull out just the DatasetRefs that are in common in the inputs
            # and order them consistently (note that consistent ordering is not
            # automatic).
            adjusted_inputs = {}
            for name, refs in zip(input_names, inputs_by_data_id):
                adjusted_inputs[name] = (
                    inputs[name][0],
                    [refs[data_id] for data_id in data_ids_to_keep],
                )
                # Also update the full dict of inputs, so we can pass it to
                # super() later.
                inputs[name] = adjusted_inputs[name]
            # Do the same for the outputs.
            outputs_by_data_id = {
                ref.dataId: ref for ref in outputs["outputCatalogs"][1]
            }
            adjusted_outputs = {
                "outputCatalogs": (
                    outputs["outputCatalogs"][0],
                    [outputs_by_data_id[data_id]
                     for data_id in data_ids_to_keep]
                )
            }
            outputs["outputCatalogs"] = adjusted_outputs["outputCatalogs"]
            # Delegate to super(); ignore results because they are guaranteed
            # to be empty.
            super().adjustQuantum(inputs, outputs, label, data_id)
            return adjusted_inputs, adjusted_outputs

See the documentation for `~PipelineTaskConnections.adjustQuantum` for additional details on overriding it.

The aggregated task can be seen in :ref:`pipeline-appendix-h`

-------------------------
Overriding Task execution
-------------------------

Overriding the `PipelineTask` method ``runQuantum`` is another advanced tool.
This method is used in task execution and is supplied identifiers for input
dataset references to be used in processing, and output dataset references
for data-sets the middleware frame work expects to be written at the end of
processing. The ``runQuantum`` method is responsible for fetching inputs,
calling the ``run`` method, and writing outputs using the supplied data ids.
After an introduction to the default implementation of ``runQuantum``, this
guide will talk about variations that will all accomplish the same thing, but
in different ways. This will hopefully give some introduction to what is
possible with `runQuantum` and some ideas as to why you many need to override
it. Take a look at how ``runQuantum`` is defined in ``PipelineTask``.

.. code-block:: python

    class PipelineTask(Task):
        ...

        def runQuantum(self, butlerQC: ButlerQuantumContext,
                       inputRefs: InputQuantizedConnection,
                       outputRefs: OutputQuantizedConnection):
            inputs = butlerQC.get(inputRefs)
            outputs = self.run(**inputs)
            butlerQC.put(outputs, outputRefs)

        ...

If this looks pretty straight forward to you, then great! This function is
tightly packed, but short to make the barrier to overloading it as low as
possible.

The first argument is named ``butlerQC`` which may cause you to
wonder how it relates to a butler. If you are paying attention, you
might know from the type annotation that the QC stands for QuantumContext,
but that does not really give many clues does it? A `ButlerQuantumContext`
object is simply a `~lsst.daf.butler.Butler` that has special information
about the unit of data your task will be processing, i.e. the quantum, as well
as extra functionality attached to it.

`~lsst.daf.butler.DatasetRef`\ s that can be used to interact with specific
data-sets managed by the butler. See the description of the
`InputQuantizedConnection` type in the section
:ref:`PipelineTask-processing-altering-what-is-processed` for more
information on `QuantizedConnection`\ s, noting that
`OutputQuantizedConnection` functions in the same manner but with output
`lsst.daf.butler.DatasetRef`\ s.

The ``butlerQC`` object has a ``get`` method that knows how understand the
structure of an `InputQuantizedConnection` and load in all of the inputs
supplied in the inputRefs. However, the ``get`` method also understand
`list`\ s or single instances of `~lsst.daf.butler.DatasetRef`\ s. Likewise
the ``butlerQC`` object also has a ``put`` method that mirrors all the
capabilities of the ``get`` method, put for putting outputs into the butler.

For examples sake, add a `runQuantum` method to your photometry task that
loads in all the input references one connection at a time. Your task only
expects to write a single data-set out, so the `runQuantum` will also put
with that single `lsst.daf.butler.DatasetRef`.

.. code-block:: python

    class ApertureTask(pipeBase.PipelineTask):
        ...

        def runQuantum(self, butlerQC: pipeBase.ButlerQuantumContext,
                    inputRefs: pipeBase.InputQuantizedConnection,
                    outputRefs: pipeBase.OutputQuantizedConnection):
            inputs = {}
            for name, refs in inputRefs:
                inputs[name] = butlerQC.get(refs)
            output = self.run(**inputs)
            butlerQC.put(output, outputRefs.OutputCatalog)

Overriding ``runQuantum`` also provides the opportunity to do a transformation
on input data, or some other related calculation. This allows the ``run``
method to have a convenient interface for user interaction within a notebook
or shell, but still match the types of input `PipelineTask`\ s will get when
run by the middleware system.

To demonstrate this, modify the ``runQuantum`` and ``run`` methods in such a
way that the output catalog of the task is already pre-populated with all of
the input catalogs. The user then only needs to supply the lengths of each of
the input catalogs that went in to creating the output catalog. This change
is likely not worth doing in a production `PipelineTask` but is perfect for
demoing the concepts here.

.. code-block:: python

        ...

        def runQuantum(self, butlerQC: pipeBase.ButlerQuantumContext,
                    inputRefs: pipeBase.InputQuantizedConnection,
                    outputRefs: pipeBase.OutputQuantizedConnection):
            inputs = {}
            for name, refs in inputRefs:
                inputs[name] = butlerQC.get(refs)

            # Record the lengths of each input catalog
            lengths = []

            # Remove the input catalogs from the list of inputs to the run method
            inputCatalogs = inputs.pop('inputCatalogs')

            # Add in all the input catalogs into the output catalog
            cumulativeLength = 0
            for inCatHandle in inputCatalogs:
                inCat = inCatHandle.get()
                lengths.append(len(inCat)+cumulativeLength)
                cumulativeLength += len(inCat)
                self.outputCatalog.extend(inCat, mapper=self.mapper)

            # Add the catalog lengths to the inputs to the run method
            inputs['lengths'] = lengths
            output = self.run(**inputs)
            butlerQC.put(output, outputRefs.OutputCatalog)

        def run(self, exposures: List[afwImage.Exposure],
                lengths: List[int],
                areaMasks: List[afwImage.Mask],
                backgrounds: Union[None, typing.List[afwMath.BackgroundList]] = None
                ) -> pipeBase.Struct:
            ...

Adding additional logic to a `PipelineTask` in this way is very powerful, but
should be using sparingly and with great thought. Putting logic in
`runQuantum` not only makes it more difficult to follow the flow of the
algorithm, but creates the need for duplication of that logic in contexts
where the `run` method is called outside of `PipelineTask` execution.

See :ref:`pipeline-appendix-i` for the complete example.

.. _pipeline-appendix-a:

Appendix A
==========
.. literalinclude:: /_static/pipe_base/PipelineTask_Examples/aperturePipelineTaskV1.py
    :language: python
    :linenos:


.. _pipeline-appendix-b:

Appendix B
==========
.. literalinclude:: /_static/pipe_base/PipelineTask_Examples/aperturePipelineTaskV2.py
    :language: python
    :linenos:

.. _pipeline-appendix-c:

Appendix C
==========
.. literalinclude:: /_static/pipe_base/PipelineTask_Examples/aperturePipelineTaskV3.py
    :language: python
    :linenos:

.. _pipeline-appendix-d:

Appendix D
==========
.. literalinclude:: /_static/pipe_base/PipelineTask_Examples/aperturePipelineTaskV4.py
    :language: python
    :linenos:

.. _pipeline-appendix-e:

Appendix E
==========
.. literalinclude:: /_static/pipe_base/PipelineTask_Examples/aperturePipelineTaskV5.py
    :language: python
    :linenos:

.. _pipeline-appendix-f:

Appendix F
==========
.. literalinclude:: /_static/pipe_base/PipelineTask_Examples/aperturePipelineTaskV6.py
    :language: python
    :linenos:

.. _pipeline-appendix-g:

Appendix G
==========
.. literalinclude:: /_static/pipe_base/PipelineTask_Examples/aperturePipelineTaskV7.py
    :language: python
    :linenos:

.. _pipeline-appendix-h:

Appendix H
==========
.. literalinclude:: /_static/pipe_base/PipelineTask_Examples/aperturePipelineTaskV8.py
    :language: python
    :linenos:

.. _pipeline-appendix-i:

Appendix I
==========
.. literalinclude:: /_static/pipe_base/PipelineTask_Examples/aperturePipelineTaskV9.py
    :language: python
    :linenos:

.. |PipelineTask| replace:: `~lsst.pipe.base.PipelineTask`
.. |Task| replace:: `~lsst.pipe.base.Task`
.. |Config| replace:: `~lsst.pipe.base.Config`
