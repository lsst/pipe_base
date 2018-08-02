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

__all__ = ["PipelineTask"]  # Classes in this module

from lsst.daf.butler import DatasetType, StorageClassFactory
from .config import (InputDatasetConfig, OutputDatasetConfig,
                     InitInputDatasetConfig, InitOutputDatasetConfig)
from .task import Task


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

    def __init__(self, config=None, log=None, initInputs=None):
        super().__init__(config=config, log=log,)

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
        """Return input dataset types for this task.

        Default implementation finds all fields of type `InputDatasetConfig`
        in configuration (non-recursively) and uses them for constructing
        `DatasetType` instances. The keys of these fields are used as keys
        in returned dictionary. Subclasses can override this behavior.

        Parameters
        ----------
        config : `Config`
            Configuration for this task. Typically datasets are defined in
            a task configuration.

        Returns
        -------
        Dictionary where key is the name (arbitrary) of the input dataset
        and value is the `butler.DatasetType` instance. Default
        implementation uses configuration field name as dictionary key.
        """
        return cls.getDatasetTypes(config, InputDatasetConfig)

    @classmethod
    def getOutputDatasetTypes(cls, config):
        """Return output dataset types for this task.

        Default implementation finds all fields of type `OutputDatasetConfig`
        in configuration (non-recursively) and uses them for constructing
        `DatasetType` instances. The keys of these fields are used as keys
        in returned dictionary. Subclasses can override this behavior.

        Parameters
        ----------
        config : `Config`
            Configuration for this task. Typically datasets are defined in
            a task configuration.

        Returns
        -------
        Dictionary where key is the name (arbitrary) of the output dataset
        and value is the `butler.DatasetType` instance. Default
        implementation uses configuration field name as dictionary key.
        """
        return cls.getDatasetTypes(config, OutputDatasetConfig)

    @classmethod
    def getInitInputDatasetTypes(cls, config):
        """Return dataset types that can be used to retrieve the
        ``initInputs`` constructor argument.

        Datasets used in initialization may not be associated with any
        DataUnits (i.e. their data IDs must be empty dictionaries).

        Default implementation finds all fields of type
        `InitInputInputDatasetConfig` in configuration (non-recursively) and
        uses them for constructing `DatasetType` instances. The keys of these
        fields are used as keys in returned dictionary. Subclasses can
        override this behavior.

        Parameters
        ----------
        config : `Config`
            Configuration for this task. Typically datasets are defined in
            a task configuration.

        Returns
        -------
        Dictionary where key is the name (arbitrary) of the input dataset
        and value is the `butler.DatasetType` instance. Default
        implementation uses configuration field name as dictionary key.

        When the task requires no initialization inputs, should return an
        empty dict.
        """
        return cls.getDatasetTypes(config, InitInputDatasetConfig)

    @classmethod
    def getInitOutputDatasetTypes(cls, config):
        """Return dataset types that can be used to write the objects
        returned by `getOutputDatasets`.

        Datasets used in initialization may not be associated with any
        DataUnits (i.e. their data IDs must be empty dictionaries).

        Default implementation finds all fields of type
        `InitOutputDatasetConfig` in configuration (non-recursively) and uses
        them for constructing `DatasetType` instances. The keys of these
        fields are used as keys in returned dictionary. Subclasses can
        override this behavior.

        Parameters
        ----------
        config : `Config`
            Configuration for this task. Typically datasets are defined in
            a task configuration.

        Returns
        -------
        Dictionary where key is the name (arbitrary) of the output dataset
        and value is the `butler.DatasetType` instance. Default
        implementation uses configuration field name as dictionary key.

        When the task produces no initialization outputs, should return an
        empty dict.
        """
        return cls.getDatasetTypes(config, InitOutputDatasetConfig)

    @classmethod
    def getDatasetTypes(cls, config, configClass):
        """Return dataset types defined in task configuration .

        This method can be used by other methods that need to extract dataset
        types from task configuration (e.g. :py:method:`getInputDatasetTypes`
        or sub-class methods).

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
        and value is the `butler.DatasetType` instance. Default
        implementation uses configuration field name as dictionary key.

        When the task produces no initialization outputs, should return an
        empty dict.
        """
        dsTypes = {}
        for key, value in config.items():
            if isinstance(value, configClass):
                dsTypes[key] = cls.makeDatasetType(value)
        return dsTypes

    def adaptArgsAndRun(self, inputData, inputDataIds, outputDataIds):
        """Run task algorithm on in-memory data.

        This method is called by `runQuantum` to operate on input in-memory
        data and produce coressponding output in-memory data. It receives
        arguments which are dictionaries with input data and input/output
        DataIds. Many simple tasks do not need to know DataIds so default
        implementation of this method calls `run` method passing input data
        objects as keyword arguments. Most simple tasks will implement `run`
        method, more complex tasks that need to know about output DataIds
        will override this method instead.

        The method returns `Struct` instance with attributes matching the
        configuration fields for output dataset types. Values stored in
        returned struct are lists, if tasks produces more than one object
        for some dataset type then data objects returned in ``struct`` must
        match in count and order corresponding DataIds in ``outputDataIds``.

        Parameters
        ----------
        inputData : `dict`
            Dictionary whose keys are the names of the configuration fields
            describing input dataset types and values are lists of
            Python-domain data objects retrieved from data butler.
        inputDataIds : `dict`
            Dictionary whose keys are the names of the configuration fields
            describing input dataset types and values are lists of DataIds
            (units) that task consumes for corresponding dataset type.
            DataIds are guaranteed to match data objects in ``inputData``
        outputDataIds : `dict`
            Dictionary whose keys are the names of the configuration fields
            describing output dataset types and values are lists of DataIds
            (units) that task is to produce for corresponding dataset type.

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
        values will be lists of the data object retrieved from data butler.
        If the task also needs to know its output DataIds then it needs to
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

                # Do something with inputs and calibs lists, produce output image.
                assert len(input) == 1 and len(calib) == 1
                image = self.makeImage(input[0], calib[0])

                return Struct(output=[image])

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
        the data attributes must be lists of data objects corresponding to
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
        Any exceptions that happen in data butler or in `adaptArgsAndRun`
        method.
        """
        # get all data from butler
        inputDataIds = {}
        inputs = {}
        for key, value in self.config.items():
            if isinstance(value, InputDatasetConfig):
                dataRefs = quantum.predictedInputs[value.name]
                inputDataIds[key] = [dataRef.dataId for dataRef in dataRefs]
                inputs[key] = [butler.get(dataRef.datasetType.name, dataRef.dataId)
                               for dataRef in dataRefs]

        # lists of DataRefs/DataIds for output datasets
        outputDataRefs = {}
        outputDataIds = {}
        for key, value in self.config.items():
            if isinstance(value, OutputDatasetConfig):
                dataRefs = quantum.outputs[value.name]
                outputDataRefs[key] = dataRefs
                outputDataIds[key] = [dataRef.dataId for dataRef in dataRefs]

        # call run method with keyword arguments
        struct = self.adaptArgsAndRun(inputs, inputDataIds, outputDataIds)

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
        for key, value in self.config.items():
            if isinstance(value, OutputDatasetConfig):
                dataList = structDict[key]
                dataRefs = outputDataRefs[key]
                # TODO: check that data objects and data refs are aligned
                for dataRef, data in zip(dataRefs, dataList):
                    butler.put(data, dataRef.datasetType.name, dataRef.dataId)

    @classmethod
    def makeDatasetType(cls, dsConfig):
        """Create new instance of the `DatasetType` from task config.

        Parameters
        ----------
        dsConfig : `pexConfig.Config`
            Instance of `InputDatasetConfig`, `OutputDatasetConfig`,
            `InitInputDatasetConfig`, or `InitOutputDatasetConfig`.

        Returns
        -------
        `butler.DatasetType` instance.
        """
        # map storage class name to storage class
        storageClass = StorageClassFactory().getStorageClass(dsConfig.storageClass)

        return DatasetType(name=dsConfig.name,
                           dataUnits=dsConfig.units,
                           storageClass=storageClass)

    def getResourceConfig(self):
        """Return resource configuration for this task.

        Returns
        -------
        Object of type `~config.ResourceConfig` or ``None`` if resource
        configuration is not defined for this task.
        """
        return getattr(self.config, "resources", None)
