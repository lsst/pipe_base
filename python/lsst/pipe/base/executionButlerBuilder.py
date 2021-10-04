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
from __future__ import annotations

__all__ = ("buildExecutionButler", )

import io

from collections import defaultdict
import itertools
from typing import Callable, DefaultDict, Mapping, Optional, Set, Tuple, Iterable, List, Union

from lsst.daf.butler import (DatasetRef, DatasetType, Butler, DataCoordinate, ButlerURI, Config)
from lsst.utils.introspection import get_class_of
from lsst.daf.butler.transfers import RepoExportContext
from lsst.daf.butler.core.repoRelocation import BUTLER_ROOT_TAG

from .graph import QuantumGraph, QuantumNode
from .pipeline import PipelineDatasetTypes

DataSetTypeMap = Mapping[DatasetType, Set[DataCoordinate]]


def _accumulate(
    graph: QuantumGraph,
    dataset_types: PipelineDatasetTypes,
) -> Tuple[Set[DatasetRef], DataSetTypeMap]:
    # accumulate the DatasetRefs that will be transferred to the execution
    # registry

    # exports holds all the existing data that will be migrated to the
    # execution butler
    exports: Set[DatasetRef] = set()

    # inserts is the mapping of DatasetType to dataIds for what is to be
    # inserted into the registry. These are the products that are expected
    # to be produced during processing of the QuantumGraph
    inserts: DefaultDict[DatasetType, Set[DataCoordinate]] = defaultdict(set)

    # Add inserts for initOutputs (including initIntermediates); these are
    # defined fully by their DatasetType, because they have no dimensions, and
    # they are by definition not resolved.  initInputs are part of Quantum and
    # that's the only place the graph stores the dataset IDs, so we process
    # them there even though each Quantum for a task has the same ones.
    for dataset_type in itertools.chain(dataset_types.initIntermediates, dataset_types.initOutputs):
        inserts[dataset_type].add(DataCoordinate.makeEmpty(dataset_type.dimensions.universe))

    n: QuantumNode
    for quantum in (n.quantum for n in graph):
        for attrName in ("initInputs", "inputs", "outputs"):
            attr: Mapping[DatasetType, Union[DatasetRef, List[DatasetRef]]] = getattr(quantum, attrName)

            for type, refs in attr.items():
                # This if block is because init inputs has a different
                # signature for its items
                if not isinstance(refs, list):
                    refs = [refs]
                # iterate over all the references, if it has an id, it
                # means it exists and should be exported, if not it should
                # be inserted into the new registry
                for ref in refs:
                    if ref.id is not None:
                        exports.add(ref)
                    else:
                        if ref.isComponent():
                            # We can't insert a component, and a component will
                            # be part of some other upstream dataset, so it
                            # should be safe to skip them here
                            continue
                        inserts[type].add(ref.dataId)
    return exports, inserts


def _discoverCollections(butler: Butler, collections: Iterable[str]) -> set[str]:
    # Recurse through any discovered collections to make sure all collections
    # are exported. This exists because I ran into a situation where some
    # collections were not properly being discovered and exported. This
    # method may be able to be removed in the future if collection export
    # logic changes
    collections = set(collections)
    while True:
        discoveredCollections = set(butler.registry.queryCollections(collections, flattenChains=True,
                                                                     includeChains=True))
        if len(discoveredCollections) > len(collections):
            collections = discoveredCollections
        else:
            break
    return collections


def _export(butler: Butler, collections: Optional[Iterable[str]], exports: Set[DatasetRef],
            inserts: DataSetTypeMap) -> io.StringIO:
    # This exports the datasets that exist in the input butler using
    # daf butler objects, however it reaches in deep and does not use the
    # public methods so that it can export it to a string buffer and skip
    # disk access.
    yamlBuffer = io.StringIO()
    # Yaml is hard coded, since the class controls both ends of the
    # export/import
    BackendClass = get_class_of(butler._config["repo_transfer_formats", "yaml", "export"])
    backend = BackendClass(yamlBuffer)
    exporter = RepoExportContext(butler.registry, butler.datastore, backend, directory=None, transfer=None)
    exporter.saveDatasets(exports)

    # Need to ensure that the dimension records for outputs are
    # transferred.
    for _, dataIds in inserts.items():
        exporter.saveDataIds(dataIds)

    # Look for any defined collection, if not get the defaults
    if collections is None:
        collections = butler.registry.defaults.collections

    # look up all collections associated with those inputs, this follows
    # all chains to make sure everything is properly exported
    for c in _discoverCollections(butler, collections):
        exporter.saveCollection(c)
    exporter._finish()

    # reset the string buffer to the beginning so the read operation will
    # actually *see* the data that was exported
    yamlBuffer.seek(0)
    return yamlBuffer


def _setupNewButler(butler: Butler, outputLocation: ButlerURI, dirExists: bool) -> Butler:
    # Set up the new butler object at the specified location
    if dirExists:
        # Remove the existing table, if the code got this far and this exists
        # clobber must be true
        executionRegistry = outputLocation.join("gen3.sqlite3")
        if executionRegistry.exists():
            executionRegistry.remove()
    else:
        outputLocation.mkdir()

    # Copy the existing butler config, modifying the location of the
    # registry to the specified location.
    # Preserve the root path from the existing butler so things like
    # file data stores continue to look at the old location.
    config = Config(butler._config)
    config["root"] = outputLocation.geturl()
    config["allow_put_of_predefined_dataset"] = True
    config["registry", "db"] = "sqlite:///<butlerRoot>/gen3.sqlite3"

    # Remove any namespace that may be set in main registry.
    config.pop(("registry", "namespace"), None)

    # record the current root of the datastore if it is specified relative
    # to the butler root
    if config.get(("datastore", "root")) == BUTLER_ROOT_TAG:
        config["datastore", "root"] = butler._config.configDir.geturl()
    config["datastore", "trust_get_request"] = True

    # Requires that we use the dimension configuration from the original
    # butler and not use the defaults.
    config = Butler.makeRepo(root=outputLocation, config=config,
                             dimensionConfig=butler.registry.dimensions.dimensionConfig,
                             overwrite=True, forceConfigRoot=False)

    # Return a newly created butler
    return Butler(config, writeable=True)


def _import(yamlBuffer: io.StringIO,
            newButler: Butler,
            inserts: DataSetTypeMap,
            run: str,
            butlerModifier: Optional[Callable[[Butler], Butler]]
            ) -> Butler:
    # This method takes the exports from the existing butler, imports
    # them into the newly created butler, and then inserts the datasets
    # that are expected to be produced.

    # import the existing datasets using "split" mode. "split" is safe
    # because execution butler is assumed to be able to see all the file
    # locations that the main datastore can see. "split" supports some
    # absolute URIs in the datastore.
    newButler.import_(filename=yamlBuffer, format="yaml", reuseIds=True, transfer="split")

    # If there is modifier callable, run it to make necessary updates
    # to the new butler.
    if butlerModifier is not None:
        newButler = butlerModifier(newButler)

    # Register datasets to be produced and insert them into the registry
    for dsType, dataIds in inserts.items():
        newButler.registry.registerDatasetType(dsType)
        newButler.registry.insertDatasets(dsType, dataIds, run)

    return newButler


def buildExecutionButler(butler: Butler,
                         graph: QuantumGraph,
                         outputLocation: Union[str, ButlerURI],
                         run: str,
                         *,
                         clobber: bool = False,
                         butlerModifier: Optional[Callable[[Butler], Butler]] = None,
                         collections: Optional[Iterable[str]] = None
                         ) -> Butler:
    r"""buildExecutionButler is a function that is responsible for exporting
    input `QuantumGraphs` into a new minimal `~lsst.daf.butler.Butler` which
    only contains datasets specified by the `QuantumGraph`. These datasets are
    both those that already exist in the input `~lsst.daf.butler.Butler`, and
    those that are expected to be produced during the execution of the
    `QuantumGraph`.

    Parameters
    ----------
    butler : `lsst.daf.butler.Bulter`
        This is the existing `~lsst.daf.butler.Butler` instance from which
        existing datasets will be exported. This should be the
        `~lsst.daf.butler.Butler` which was used to create any `QuantumGraphs`
        that will be converted with this object.
    graph : `QuantumGraph`
        Graph containing nodes that are to be exported into an execution
        butler
    outputLocation : `str` or `~lsst.daf.butler.ButlerURI`
        URI Location at which the execution butler is to be exported. May be
        specified as a string or a ButlerURI instance.
    run : `str` optional
        The run collection that the exported datasets are to be placed in. If
        None, the default value in registry.defaults will be used.
    clobber : `bool`, Optional
        By default a butler will not be created if a file or directory
        already exists at the output location. If this is set to `True`
        what is at the location will be deleted prior to running the
        export. Defaults to `False`
    butlerModifier : `~typing.Callable`, Optional
        If supplied this should be a callable that accepts a
        `~lsst.daf.butler.Butler`, and returns an instantiated
        `~lsst.daf.butler.Butler`. This callable may be used to make any
        modifications to the `~lsst.daf.butler.Butler` desired. This
        will be called after importing all datasets that exist in the input
        `~lsst.daf.butler.Butler` but prior to inserting Datasets expected
        to be produced. Examples of what this method could do include
        things such as creating collections/runs/ etc.
    collections : `~typing.Iterable` of `str`, Optional
        An iterable of collection names that will be exported from the input
        `~lsst.daf.butler.Butler` when creating the execution butler. If not
        supplied the `~lsst.daf.butler.Butler`\ 's `~lsst.daf.butler.Registry`
        default collections will be used.

    Returns
    -------
    executionButler : `lsst.daf.butler.Butler`
        An instance of the newly created execution butler

    Raises
    ------
    FileExistsError
        Raised if something exists in the filesystem at the specified output
        location and clobber is `False`
    NotADirectoryError
        Raised if specified output URI does not correspond to a directory
    """
    # We know this must refer to a directory.
    outputLocation = ButlerURI(outputLocation, forceDirectory=True)

    # Do this first to Fail Fast if the output exists
    if (dirExists := outputLocation.exists()) and not clobber:
        raise FileExistsError("Cannot create a butler at specified location, location exists")
    if not outputLocation.isdir():
        raise NotADirectoryError("The specified output URI does not appear to correspond to a directory")

    # Gather all DatasetTypes from the Python and check any that already exist
    # in the registry for consistency.  This does not check that all dataset
    # types here exist, because they might want to register dataset types
    # later.  It would be nice to also check that, but to that we would need to
    # be told whether they plan to register dataset types later (DM-30845).
    dataset_types = PipelineDatasetTypes.fromPipeline(graph.iterTaskGraph(), registry=butler.registry)

    exports, inserts = _accumulate(graph, dataset_types)
    yamlBuffer = _export(butler, collections, exports, inserts)

    newButler = _setupNewButler(butler, outputLocation, dirExists)

    return _import(yamlBuffer, newButler, inserts, run, butlerModifier)
