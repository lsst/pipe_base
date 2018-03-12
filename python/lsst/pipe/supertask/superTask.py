#
# LSST Data Management System
# Copyright 2016-2018 AURA/LSST.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
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
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.
#

"""
This module defines SuperTask class and related methods.
"""

from __future__ import absolute_import, division, print_function

__all__ = ["SuperTask"]  # Classes in this module

import lsst.afw.table as afwTable
from lsst.pipe.base.task import Task, TaskError
from lsst.daf.butler.core.datasets import DatasetType
from lsst.daf.butler.core.storageClass import StorageClassFactory
import lsst.daf.butler.core.units as coreUnits
from .config import InputDatasetConfig, OutputDatasetConfig


class SuperTask(Task):
    """Base class for all SuperTasks.

    This is an abstract base class for SuperTasks which represents an
    algorithm executed by SuperTask framework(s) on data which comes
    from data butler, resulting data is also stored in a data butler.

    SuperTask inherits from a `pipe.base.Task` and uses the same configuration
    mechanism based on `pex.config`. SuperTask sub-class typically implements
    `run()` method which receives Python-domain data objects and returns
    `pipe.base.Struct` object with resulting data. `run()` method is not
    supposed to perform any I/O, it operates entirely on in-memory objects.
    `runQuantum()` is the method (also to be implemented in sub-class) where
    all necessary I/O is performed, it reads all input data from data butler
    into memory, calls `run()` method with that data, examines returned
    `Struct` object and saves some or all of that data back to data butler.
    `runQuantum()` method receives `Quantum` instance which defines all input
    and output datasets for a single invocation of SuperTask.

    Attributes
    ----------
    canMultiprocess : bool, True by default (class attribute)
        This class attribute is checked by execution framework, sub-classes
        can set it to `False` in case task does not support multiprocessing.

    Parameters
    ----------
    config : `pex.config.Config`, optional
        Configuration for this task (an instance of self.ConfigClass,
        which is a task-specific subclass of lsst.pex.config.Config).
        If not specified then it defaults to `self.ConfigClass()`.
    log : `lsst.log.Log`, optional
        Logger instance whose name is used as a log name prefix,
        or None for no prefix. Ignored if parentTask specified, in which case
        parentTask.log's name is used as a prefix.
    """

    canMultiprocess = True

    # TODO: temporary hack, I think factory should be global
    storageClassFactory = StorageClassFactory()

    def __init__(self, config=None, log=None):
        super().__init__(config=config, log=log)

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
        Dictionary where key is the name (arbitrary) of the input dataset and
        value is the `butler.core.datasets.DatasetType` instance. Default
        implementation uses configuration field name as dictionary key.
        """
        dsTypes = {}
        for key, value in config.items():
            if isinstance(value, InputDatasetConfig):
                dsTypes[key] = cls.makeDatasetType(value)
        return dsTypes

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
        Dictionary where key is the name (arbitrary) of the output dataset and
        value is the `butler.core.datasets.DatasetType` instance. Default
        implementation uses configuration field name as dictionary key.
        """
        dsTypes = {}
        for key, value in config.items():
            if isinstance(value, OutputDatasetConfig):
                dsTypes[key] = cls.makeDatasetType(value)
        return dsTypes

    def run(self, *args, **kwargs):
        """Run task algorithm on in-memory data.

        This function is the one that actually operates on the data and usually
        returning a `Struct` with the produced results. This method will be
        overridden by every subclass. It operates on in-memory data structures
        (or data proxies) and cannot access any external data such as data
        butler or databases. All interaction with external data happens in
        `runQuantum` method.

        With default implementation of `runQuantum()` this method will
        receive keyword arguments whose names will be the same as names
        of configuration fields describing input and output dataset types.
        For input dataset types argument values will be lists of the data
        object retrieved from data butler. For output dataset types argument
        values will be the lists of units from DataRefs in a Quantum.
        """
        raise NotImplementedError("run() is not implemented")

    def runQuantum(self, quantum, butler):
        """Execute SuperTask algorithm on single quantum of data.

        Typical implementation of this method will use inputs from quantum
        to retrieve Python-domain objects from data butler and call `run()`
        method on that data. On return from `run()` this method will
        extract data from returned `Struct` instance and save that data
        to butler.

        Default implementaion retrieves all input data in quantum graph
        and calls `run()` method with keyword arguments where name of the
        keyword argument is the same as the name of the configuration field
        defining input DatasetType. Additionally it also passes keyword
        arguments that correspond to output dataset types, each keyword
        argument will have the list of units for corresponding output DataRefs.

        The `Struct` returned from `run()` is expected to contain data
        attributes with the names equal to the names of the configuration
        fields defining output dataset types. The values of the data
        attributes must be lists of data bjects corresponding to the units
        passed as keyword arguments. All data objects will be saved in butler
        using DataRefs from Quantum's output dictionary.

        This method does not return anything to the caller, on errors
        corresponding exception is raised.

        Parameters
        ----------
        quantum : `Quantum`
            Object describing input and output corresponding to this
            invocation of SuperTask instance.
        butler : object
            Data butler instance.

        Raises
        ------
        Any exceptions that happen in data butler or in `run()` method.
        """
        # get all data from butler
        inputs = {}
        for key, value in self.config.items():
            if isinstance(value, InputDatasetConfig):
                dataRefs = quantum.predictedInputs[value.name]
                inputs[key] = [butler.get(dataRef) for dataRef in dataRefs]

        # lists of units for output datasets
        outUnits = {}
        for key, value in self.config.items():
            if isinstance(value, OutputDatasetConfig):
                dataRefs = quantum.outputs[value.name]
                outUnits[key] = [dataRef.dataId for dataRef in dataRefs]

        # call run method with keyword arguments
        struct = self.run(**inputs, **outUnits)

        # save data in butler, convention is that returned struct
        # has data field(s) with the same names as the config fields
        # defining DatasetTypes
        structDict = struct.getDict()
        for key, value in self.config.items():
            if isinstance(value, OutputDatasetConfig):
                dataList = structDict[key]
                dataRefs = quantum.outputs[value.name]
                # TODO: check that data objects and data refs are aligned
                for dataRef, data in zip(dataRefs, dataList):
                    butler.put(dataRef, data)

    @classmethod
    def makeDatasetType(cls, dsConfig):
        """Create new instance of the `DatasetType` from task config.

        Parameters
        ----------
        dsConfig : `pexConfig.Config`
            Instance of the `supertask.InputDatasetConfig` or
            `supertask.OutputDatasetConfig`

        Returns
        -------
        `butler.core.datasets.DatasetType` instance.

        Raises
        ------
        `KeyError` is raised if unit configuration uses incorrect unit name.
        """
        # make a dict of {className: UnitCLass} for all unit classes
        unitsDict = dict((k, v) for k, v in vars(coreUnits).items()
                         if isinstance(v, type) and issubclass(v, coreUnits.DataUnit))
        # map unit names to classes, this will throw if unit name is unknown
        units = [unitsDict[unit] for unit in dsConfig.units]
        # make unit set
        units = coreUnits.DataUnitSet(units)

        # map storage class name to storage class
        storageClass = cls.storageClassFactory.getStorageClass(dsConfig.storageClass)

        return DatasetType(name=dsConfig.name, dataUnits=units, storageClass=storageClass)

    def write_config(self, butler, clobber=False, do_backup=True):
        """Write the configuration used for processing the data, or check that
        an existing one is equal to the new one if present.

        Parameters
        ----------
        butler : `Butler`
            data butler used to write the config. The config is written to
            dataset type ``self._get_config_name()``.
        clobber : bool, optional
            Boolean flag that controls what happens if a config already has
            been saved. If True then overwrite the existing config, otherwise
            (default) raise `TaskError` if this config does not match the existing
            config.
        do_backup : bool, optional
            If True then make backup copy when overwriting dataset.
        """
        config_name = self._get_config_name()
        if config_name is None:
            return
        if clobber:
            butler.put(self.config, config_name, doBackup=do_backup)
        elif butler.datasetExists(config_name):
            # this may be subject to a race condition; see #2789
            try:
                old_config = butler.get(config_name, immediate=True)
            except Exception as exc:
                raise type(exc)("Unable to read stored config file %s (%s); consider using --clobber-config" %
                                (config_name, exc))

            def output(msg): return self.log.fatal("Comparing configuration: " + msg)

            if not self.config.compare(old_config, shortcut=False, output=output):
                raise TaskError(
                    ("Config does not match existing task config %r on disk; tasks configurations " +
                     "must be consistent within the same output repo (override with --clobber-config)") %
                    (config_name,))
        else:
            butler.put(self.config, config_name)

    def write_schemas(self, butler, clobber=False, do_backup=True):
        """Write the schemas returned by `getAllSchemaCatalogs` method.

        If `clobber` is False and an existing schema does not match current
        schema, then some schemas may have been saved successfully and others
        may not, and there is no easy way to tell which is which.

        Parameters
        ----------
        butler : `Butler`
            Data butler used to write the schema. Each schema is written to
            the dataset type specified as the key in the dict returned by
            `getAllSchemaCatalogs`.
        clobber : bool, optional
            Boolean flag that controls what happens if a schema already has
            been saved. If True then overwrite the existing schema, otherwise
            (default) raise `TaskError` if this schema does not match the
            existing schema.
        do_backup : bool, optional
            If True then make backup copy when overwriting dataset.
        """
        for dataset, catalog in self.getAllSchemaCatalogs().items():
            schema_dataset = dataset + "_schema"
            if clobber:
                self.log.info("Writing schema %s", schema_dataset)
                butler.put(catalog, schema_dataset, doBackup=do_backup)
            elif butler.datasetExists(schema_dataset):
                self.log.info("Getting schema %s", schema_dataset)
                old_schema = butler.get(schema_dataset, immediate=True).getSchema()
                if not old_schema.compare(catalog.getSchema(), afwTable.Schema.IDENTICAL):
                    raise TaskError(
                        ("New schema does not match schema %r on disk; schemas must be " +
                         " consistent within the same output repo (override with --clobber-config)") %
                        (dataset,))
            else:
                self.log.info("Writing schema %s" % schema_dataset)
                butler.put(catalog, schema_dataset)

    def get_resource_config(self):
        """Return resource configuration for this task.

        Returns
        -------
        Object of type `resource_config.ResourceConfig`or None if resource configuration
        is not defined for this task.
        """
        return getattr(self.config, "resources", None)

    def _get_config_name(self):
        """Return the name of the config dataset type, or None if config is
        not to be persisted.

        The name may depend on the config; that is why this is not a class method.
        """
        return self.getName() + "_config"
