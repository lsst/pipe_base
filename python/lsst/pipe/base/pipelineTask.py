# This file is part of pipe_base.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
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

"""This module defines PipelineTask class and related methods.
"""

__all__ = ["DatasetTypeDescriptor", "PipelineTask"]  # Classes in this module

from lsst.daf.butler import DatasetType
from .config import (InputDatasetConfig, OutputDatasetConfig,
                     InitInputDatasetConfig, InitOutputDatasetConfig)
from .task import Task


class ScalarError(TypeError):
    """Exception raised when dataset type is configured as scalar
    but there are multiple DataIds in a Quantum for that dataset.

    Parameters
    ----------
    key : `str`
        Name of the configuration field for dataset type.
    numDataIds : `int`
        Actual number of DataIds in a Quantum for this dataset type.
    """
    def __init__(self, key, numDataIds):
        super().__init__(("Expected scalar for output dataset field {}, "
                          "received {} DataIds").format(key, numDataIds))


class DatasetTypeDescriptor:
    """Description of an unnormalized proto-DatasetType and its relationship to
    a PipelineTask.

    This class contains the information needed to construct a `DatasetType`
    (once a `DimensionUniverse` is available) and all relevant options that are
    used by PipelineTask. Typically this is derived from configuration classes,
    but sub-classes of PipelineTask can also define additional DatasetTypes
    that are not part of the task configuration.

    Parameters
    ----------
    name : `str`
        Name of the dataset type.
    dimensionNames: `~collections.abc.Set` of `str`
        Names of the dimensions used to identify datasets of this type.
    storageClassName: `str`
        Name of the `~lsst.daf.butler.StorageClass` for this dataset type.
    scalar : `bool`
        `True` if this is a scalar dataset.
    manualLoad : `bool`
        `True` if this dataset will be manually loaded by a concrete
        `PipelineTask` instead of loaded automatically by the base class.
    """

    def __init__(self, name, dimensionNames, storageClassName, scalar, manualLoad):
        self._name = name
        self._dimensionNames = dimensionNames
        self._storageClassName = storageClassName
        self._scalar = scalar
        self._manualLoad = manualLoad

    @classmethod
    def fromConfig(cls, datasetConfig):
        """Make DatasetTypeDescriptor instance from configuration object.

        Parameters
        ----------
        datasetConfig : `lsst.pex.config.Config`
            Instance of one the `InputDatasetConfig`, `OutputDatasetConfig`,
            `InitInputDatasetConfig`, or `InitOutputDatasetConfig` types

        Returns
        -------
        descriptor : `DatasetTypeDescriptor`
        """
        # Use scalar=True for Init dataset types
        scalar = getattr(datasetConfig, 'scalar', True)
        manualLoad = getattr(datasetConfig, 'manualLoad', False)
        return cls(name=datasetConfig.name, dimensionNames=datasetConfig.dimensions,
                   storageClassName=datasetConfig.storageClass, scalar=scalar, manualLoad=manualLoad)

    def makeDatasetType(self, universe):
        """Construct a true `DatasetType` instance with normalized dimensions.

        Parameters
        ----------
        universe : `lsst.daf.butler.DimensionUniverse`
            Set of all known dimensions to be used to normalize the dimension
            names specified in config.

        Returns
        -------
        datasetType : `DatasetType`
            The `DatasetType` defined by this descriptor.
        """
        return DatasetType(self._name,
                           universe.extract(self._dimensionNames),
                           self._storageClassName)

    @property
    def name(self):
        """Name of the dataset type (`str`).
        """
        return self._name

    @property
    def scalar(self):
        """`True` if this is a scalar dataset.
        """
        return self._scalar

    @property
    def manualLoad(self):
        """`True` if the task will handle loading the data
        """
        return self._manualLoad


class PipelineTask(Task):
    """Base class for all pipeline tasks.

    This is an abstract base class for PipelineTasks which represents an
    algorithm executed by framework(s) on data which comes from data butler,
    resulting data is also stored in a data butler.

    PipelineTask inherits from a `pipe.base.Task` and uses the same
    configuration mechanism based on `pex.config`. PipelineTask sub-class
    typically implements `run()` method which receives Python-domain data
    objects and returns `pipe.base.Struct` object with resulting data.
    `run()` method is not supposed to perform any I/O, it operates entirely
    on in-memory objects. `runQuantum()` is the method (can be re-implemented
    in sub-class) where all necessary I/O is performed, it reads all input
    data from data butler into memory, calls `run()` method with that data,
    examines returned `Struct` object and saves some or all of that data back
    to data butler. `runQuantum()` method receives `daf.butler.Quantum`
    instance which defines all input and output datasets for a single
    invocation of PipelineTask.

    Subclasses must be constructable with exactly the arguments taken by the
    PipelineTask base class constructor, but may support other signatures as
    well.

    Attributes
    ----------
    canMultiprocess : bool, True by default (class attribute)
        This class attribute is checked by execution framework, sub-classes
        can set it to ``False`` in case task does not support multiprocessing.

    Parameters
    ----------
    config : `pex.config.Config`, optional
        Configuration for this task (an instance of ``self.ConfigClass``,
        which is a task-specific subclass of `PipelineTaskConfig`).
        If not specified then it defaults to `self.ConfigClass()`.
    log : `lsst.log.Log`, optional
        Logger instance whose name is used as a log name prefix, or ``None``
        for no prefix.
    initInputs : `dict`, optional
        A dictionary of objects needed to construct this PipelineTask, with
        keys matching the keys of the dictionary returned by
        `getInitInputDatasetTypes` and values equivalent to what would be
        obtained by calling `Butler.get` with those DatasetTypes and no data
        IDs.  While it is optional for the base class, subclasses are
        permitted to require this argument.
    """

    canMultiprocess = True

    def __init__(self, *, config=None, log=None, initInputs=None, **kwargs):
        super().__init__(config=config, log=log, **kwargs)

    def getInitOutputDatasets(self):
        """Return persistable outputs that are available immediately after
        the task has been constructed.

        Subclasses that operate on catalogs should override this method to
        return the schema(s) of the catalog(s) they produce.

        It is not necessary to return the PipelineTask's configuration or
        other provenance information in order for it to be persisted; that is
        the responsibility of the execution system.

        Returns
        -------
        datasets : `dict`
            Dictionary with keys that match those of the dict returned by
            `getInitOutputDatasetTypes` values that can be written by calling
            `Butler.put` with those DatasetTypes and no data IDs. An empty
            `dict` should be returned by tasks that produce no initialization
            outputs.
        """
        return {}

    @classmethod
    def getInputDatasetTypes(cls, config):
        """Return input dataset type descriptors for this task.

        Default implementation finds all fields of type `InputDatasetConfig`
        in configuration (non-recursively) and uses them for constructing
        `DatasetTypeDescriptor` instances. The names of these fields are used
        as keys in returned dictionary. Subclasses can override this behavior.

        Parameters
        ----------
        config : `Config`
            Configuration for this task. Typically datasets are defined in
            a task configuration.

        Returns
        -------
        Dictionary where key is the name (arbitrary) of the input dataset
        and value is the `DatasetTypeDescriptor` instance. Default
        implementation uses configuration field name as dictionary key.
        """
        return cls.getDatasetTypes(config, InputDatasetConfig)

    @classmethod
    def getOutputDatasetTypes(cls, config):
        """Return output dataset type descriptors for this task.

        Default implementation finds all fields of type `OutputDatasetConfig`
        in configuration (non-recursively) and uses them for constructing
        `DatasetTypeDescriptor` instances. The keys of these fields are used
        as keys in returned dictionary. Subclasses can override this behavior.

        Parameters
        ----------
        config : `Config`
            Configuration for this task. Typically datasets are defined in
            a task configuration.

        Returns
        -------
        Dictionary where key is the name (arbitrary) of the output dataset
        and value is the `DatasetTypeDescriptor` instance. Default
        implementation uses configuration field name as dictionary key.
        """
        return cls.getDatasetTypes(config, OutputDatasetConfig)

    @classmethod
    def getPrerequisiteDatasetTypes(cls, config):
        """Return the local names of input dataset types that should be
        assumed to exist instead of constraining what data to process with
        this task.

        Usually, when running a `PipelineTask`, the presence of input datasets
        constrains the processing to be done (as defined by the `QuantumGraph`
        generated during "preflight").  "Prerequisites" are special input
        datasets that do not constrain that graph, but instead cause a hard
        failure when missing.  Calibration products and reference catalogs
        are examples of dataset types that should usually be marked as
        prerequisites.

        Parameters
        ----------
        config : `Config`
            Configuration for this task. Typically datasets are defined in
            a task configuration.

        Returns
        -------
        prerequisite : `~collections.abc.Set` of `str`
            The keys in the dictionary returned by `getInputDatasetTypes` that
            represent dataset types that should be considered prerequisites.
            Names returned here that are not keys in that dictionary are
            ignored; that way, if a config option removes an input dataset type
            only `getInputDatasetTypes` needs to be updated.
        """
        return frozenset()

    @classmethod
    def getInitInputDatasetTypes(cls, config):
        """Return dataset type descriptors that can be used to retrieve the
        ``initInputs`` constructor argument.

        Datasets used in initialization may not be associated with any
        Dimension (i.e. their data IDs must be empty dictionaries).

        Default implementation finds all fields of type
        `InitInputInputDatasetConfig` in configuration (non-recursively) and
        uses them for constructing `DatasetTypeDescriptor` instances. The
        names of these fields are used as keys in returned dictionary.
        Subclasses can override this behavior.

        Parameters
        ----------
        config : `Config`
            Configuration for this task. Typically datasets are defined in
            a task configuration.

        Returns
        -------
        Dictionary where key is the name (arbitrary) of the input dataset
        and value is the `DatasetTypeDescriptor` instance. Default
        implementation uses configuration field name as dictionary key.

        When the task requires no initialization inputs, should return an
        empty dict.
        """
        return cls.getDatasetTypes(config, InitInputDatasetConfig)

    @classmethod
    def getInitOutputDatasetTypes(cls, config):
        """Return dataset type descriptors that can be used to write the
        objects returned by `getOutputDatasets`.

        Datasets used in initialization may not be associated with any
        Dimension (i.e. their data IDs must be empty dictionaries).

        Default implementation finds all fields of type
        `InitOutputDatasetConfig` in configuration (non-recursively) and uses
        them for constructing `DatasetTypeDescriptor` instances. The names of
        these fields are used as keys in returned dictionary. Subclasses can
        override this behavior.

        Parameters
        ----------
        config : `Config`
            Configuration for this task. Typically datasets are defined in
            a task configuration.

        Returns
        -------
        Dictionary where key is the name (arbitrary) of the output dataset
        and value is the `DatasetTypeDescriptor` instance. Default
        implementation uses configuration field name as dictionary key.

        When the task produces no initialization outputs, should return an
        empty dict.
        """
        return cls.getDatasetTypes(config, InitOutputDatasetConfig)

    @classmethod
    def getDatasetTypes(cls, config, configClass):
        """Return dataset type descriptors defined in task configuration.

        This method can be used by other methods that need to extract dataset
        types from task configuration (e.g. `getInputDatasetTypes` or
        sub-class methods).

        Parameters
        ----------
        config : `Config`
            Configuration for this task. Typically datasets are defined in
            a task configuration.
        configClass : `type`
            Class of the configuration object which defines dataset type.

        Returns
        -------
        Dictionary where key is the name (arbitrary) of the output dataset
        and value is the `DatasetTypeDescriptor` instance. Default
        implementation uses configuration field name as dictionary key.
        Returns empty dict if configuration has no fields with the specified
        ``configClass``.
        """
        dsTypes = {}
        for key, value in config.items():
            if isinstance(value, configClass):
                dsTypes[key] = DatasetTypeDescriptor.fromConfig(value)
        return dsTypes

    @classmethod
    def getPerDatasetTypeDimensions(cls, config):
        """Return any Dimensions that are permitted to have different values
        for different DatasetTypes within the same quantum.

        Parameters
        ----------
        config : `Config`
            Configuration for this task.

        Returns
        -------
        dimensions : `~collections.abc.Set` of `Dimension` or `str`
            The dimensions or names thereof that should be considered
            per-DatasetType.

        Notes
        -----
        Any Dimension declared to be per-DatasetType by a PipelineTask must
        also be declared to be per-DatasetType by other PipelineTasks in the
        same Pipeline.

        The classic example of a per-DatasetType dimension is the
        ``CalibrationLabel`` dimension that maps to a validity range for
        master calibrations.  When running Instrument Signature Removal, one
        does not care that different dataset types like flat, bias, and dark
        have different validity ranges, as long as those validity ranges all
        overlap the relevant observation.
        """
        return frozenset()

    def adaptArgsAndRun(self, inputData, inputDataIds, outputDataIds, butler):
        """Run task algorithm on in-memory data.

        This method is called by `runQuantum` to operate on input in-memory
        data and produce coressponding output in-memory data. It receives
        arguments which are dictionaries with input data and input/output
        DataIds. Many simple tasks do not need to know DataIds so default
        implementation of this method calls `run` method passing input data
        objects as keyword arguments. Most simple tasks will implement `run`
        method, more complex tasks that need to know about output DataIds
        will override this method instead.

        All three arguments to this method are dictionaries with keys equal
        to the name of the configuration fields for dataset type. If dataset
        type is configured with ``scalar`` fiels set to ``True`` then it is
        expected that only one dataset appears on input or output for that
        dataset type and dictionary value will be a single data object or
        DataId. Otherwise if ``scalar`` is ``False`` (default) then value
        will be a list (even if only one item is in the list).

        The method returns `Struct` instance with attributes matching the
        configuration fields for output dataset types. Values stored in
        returned struct are single object if ``scalar`` is ``True`` or
        list of objects otherwise. If tasks produces more than one object
        for some dataset type then data objects returned in ``struct`` must
        match in count and order corresponding DataIds in ``outputDataIds``.

        Parameters
        ----------
        inputData : `dict`
            Dictionary whose keys are the names of the configuration fields
            describing input dataset types and values are Python-domain data
            objects (or lists of objects) retrieved from data butler.
        inputDataIds : `dict`
            Dictionary whose keys are the names of the configuration fields
            describing input dataset types and values are DataIds (or lists
            of DataIds) that task consumes for corresponding dataset type.
            DataIds are guaranteed to match data objects in ``inputData``
        outputDataIds : `dict`
            Dictionary whose keys are the names of the configuration fields
            describing output dataset types and values are DataIds (or lists
            of DataIds) that task is to produce for corresponding dataset
            type.

        Returns
        -------
        struct : `Struct`
            Standard convention is that this method should return `Struct`
            instance containing all output data. Struct attribute names
            should correspond to the names of the configuration fields
            describing task output dataset types. If something different
            is returned then `saveStruct` method has to be re-implemented
            accordingly.
        """
        return self.run(**inputData)

    def run(self, **kwargs):
        """Run task algorithm on in-memory data.

        This method should be implemented in a subclass unless tasks overrides
        `adaptArgsAndRun` to do something different from its default
        implementation. With default implementation of `adaptArgsAndRun` this
        method will receive keyword arguments whose names will be the same as
        names of configuration fields describing input dataset types. Argument
        values will be data objects retrieved from data butler. If a dataset
        type is configured with ``scalar`` field set to ``True`` then argument
        value will be a single object, otherwise it will be a list of objects.

        If the task needs to know its input or output DataIds then it has to
        override `adaptArgsAndRun` method instead.

        Returns
        -------
        struct : `Struct`
            See description of `adaptArgsAndRun` method.

        Examples
        --------
        Typical implementation of this method may look like::

            def run(self, input, calib):
                # "input", "calib", and "output" are the names of the config fields

                # Assuming that input/calib datasets are `scalar` they are simple objects,
                # do something with inputs and calibs, produce output image.
                image = self.makeImage(input, calib)

                # If output dataset is `scalar` then return object, not list
                return Struct(output=image)

        """
        raise NotImplementedError("run() is not implemented")

    def runQuantum(self, quantum, butler):
        """Execute PipelineTask algorithm on single quantum of data.

        Typical implementation of this method will use inputs from quantum
        to retrieve Python-domain objects from data butler and call
        `adaptArgsAndRun` method on that data. On return from
        `adaptArgsAndRun` this method will extract data from returned
        `Struct` instance and save that data to butler.

        The `Struct` returned from `adaptArgsAndRun` is expected to contain
        data attributes with the names equal to the names of the
        configuration fields defining output dataset types. The values of
        the data attributes must be data objects corresponding to
        the DataIds of output dataset types. All data objects will be
        saved in butler using DataRefs from Quantum's output dictionary.

        This method does not return anything to the caller, on errors
        corresponding exception is raised.

        Parameters
        ----------
        quantum : `Quantum`
            Object describing input and output corresponding to this
            invocation of PipelineTask instance.
        butler : object
            Data butler instance.

        Raises
        ------
        `ScalarError` if a dataset type is configured as scalar but receives
        multiple DataIds in `quantum`. Any exceptions that happen in data
        butler or in `adaptArgsAndRun` method.
        """

        def makeDataRefs(descriptors, refMap):
            """Generate map of DatasetRefs and DataIds.

            Given a map of DatasetTypeDescriptor and a map of Quantum
            DatasetRefs makes maps of DataIds and and DatasetRefs.
            For scalar dataset types unpacks DatasetRefs and DataIds.

            Parameters
            ----------
            descriptors : `dict`
                Map of (dataset key, DatasetTypeDescriptor).
            refMap : `dict`
                Map of (dataset type name, DatasetRefs).

            Returns
            -------
            dataIds : `dict`
                Map of (dataset key, DataIds)
            dataRefs : `dict`
                Map of (dataset key, DatasetRefs)

            Raises
            ------
            ScalarError
                Raised if dataset type is configured as scalar but more than
                one DatasetRef exists for it.
            """
            dataIds = {}
            dataRefs = {}
            for key, descriptor in descriptors.items():
                datasetType = descriptor.makeDatasetType(butler.registry.dimensions)
                keyDataRefs = refMap[datasetType.name]
                keyDataIds = [dataRef.dataId for dataRef in keyDataRefs]
                if descriptor.scalar:
                    # unpack single-item lists
                    if len(keyDataRefs) != 1:
                        raise ScalarError(key, len(keyDataRefs))
                    keyDataRefs = keyDataRefs[0]
                    keyDataIds = keyDataIds[0]
                dataIds[key] = keyDataIds
                if not descriptor.manualLoad:
                    dataRefs[key] = keyDataRefs
            return dataIds, dataRefs

        # lists of DataRefs/DataIds for input datasets
        descriptors = self.getInputDatasetTypes(self.config)
        inputDataIds, inputDataRefs = makeDataRefs(descriptors, quantum.predictedInputs)

        # get all data from butler
        inputs = {}
        for key, dataRefs in inputDataRefs.items():
            if isinstance(dataRefs, list):
                inputs[key] = [butler.get(dataRef) for dataRef in dataRefs]
            else:
                inputs[key] = butler.get(dataRefs)
        del inputDataRefs

        # lists of DataRefs/DataIds for output datasets
        descriptors = self.getOutputDatasetTypes(self.config)
        outputDataIds, outputDataRefs = makeDataRefs(descriptors, quantum.outputs)

        # call run method with keyword arguments
        struct = self.adaptArgsAndRun(inputs, inputDataIds, outputDataIds, butler)

        # store produced ouput data
        self.saveStruct(struct, outputDataRefs, butler)

    def saveStruct(self, struct, outputDataRefs, butler):
        """Save data in butler.

        Convention is that struct returned from ``run()`` method has data
        field(s) with the same names as the config fields defining
        output DatasetTypes. Subclasses may override this method to implement
        different convention for `Struct` content or in case any
        post-processing of data may be needed.

        Parameters
        ----------
        struct : `Struct`
            Data produced by the task packed into `Struct` instance
        outputDataRefs : `dict`
            Dictionary whose keys are the names of the configuration fields
            describing output dataset types and values are lists of DataRefs.
            DataRefs must match corresponding data objects in ``struct`` in
            number and order.
        butler : object
            Data butler instance.
        """
        structDict = struct.getDict()
        descriptors = self.getOutputDatasetTypes(self.config)
        for key in descriptors.keys():
            dataList = structDict[key]
            dataRefs = outputDataRefs[key]
            if not isinstance(dataRefs, list):
                # scalar outputs, make them lists again
                dataRefs = [dataRefs]
                dataList = [dataList]
            # TODO: check that data objects and data refs are aligned
            for dataRef, data in zip(dataRefs, dataList):
                butler.put(data, dataRef.datasetType.name, dataRef.dataId)

    def getResourceConfig(self):
        """Return resource configuration for this task.

        Returns
        -------
        Object of type `~config.ResourceConfig` or ``None`` if resource
        configuration is not defined for this task.
        """
        return getattr(self.config, "resources", None)
