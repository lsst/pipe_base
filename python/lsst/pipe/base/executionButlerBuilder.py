# This file is part of pipe_base.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
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

__all__ = ("buildExecutionButler",)

import io
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping

from lsst.daf.butler import Butler, Config, DatasetRef, DatasetType, Registry
from lsst.daf.butler.direct_butler import DirectButler
from lsst.daf.butler.registry import ConflictingDefinitionError, MissingDatasetTypeError
from lsst.daf.butler.repo_relocation import BUTLER_ROOT_TAG
from lsst.daf.butler.transfers import RepoExportContext
from lsst.resources import ResourcePath, ResourcePathExpression
from lsst.utils.introspection import get_class_of

from .graph import QuantumGraph

DataSetTypeRefMap = Mapping[DatasetType, set[DatasetRef]]


def _validate_dataset_type(
    candidate: DatasetType, previous: dict[str | DatasetType, DatasetType], registry: Registry
) -> DatasetType:
    """Check the dataset types and return a consistent variant if there are
    different compatible options.

    Parameters
    ----------
    candidate : `lsst.daf.butler.DatasetType`
        The candidate dataset type.
    previous : `dict` [ `str` | `~lsst.daf.butler.DatasetType`, \
            `~lsst.daf.butler.DatasetType`]
        Previous dataset types found, indexed by name and also by
        dataset type. The latter provides a quick way of returning a
        previously checked dataset type.
    registry : `lsst.daf.butler.Registry`
        Main registry whose dataset type registration should override the
        given one if it exists.

    Returns
    -------
    datasetType : `lsst.daf.butler.DatasetType`
        The dataset type to be used. This can be different from the
        given ``candidate`` if a previous dataset type was encountered
        with the same name and this one is compatible with it.

    Raises
    ------
    ConflictingDefinitionError
        Raised if a candidate dataset type has the same name as one
        previously encountered but is not compatible with it.

    Notes
    -----
    This function ensures that if a dataset type is given that has the
    same name as a previously encountered dataset type but differs solely
    in a way that is interchangeable (through a supported storage class)
    then we will always return the first dataset type encountered instead
    of the new variant.  We assume that the butler will handle the
    type conversion itself later.
    """
    # First check that if we have previously vetted this dataset type.
    # Return the vetted form immediately if we have.
    checked = previous.get(candidate)
    if checked:
        return checked

    # Have not previously encountered this dataset type.
    name = candidate.name
    if prevDsType := previous.get(name):
        # Check compatibility. For now assume both directions have to
        # be acceptable.
        if prevDsType.is_compatible_with(candidate) and candidate.is_compatible_with(prevDsType):
            # Ensure that if this dataset type is used again we will return
            # the version that we were first given with this name. Store
            # it for next time and return the previous one.
            previous[candidate] = prevDsType
            return prevDsType
        else:
            raise ConflictingDefinitionError(
                f"Dataset type incompatibility in graph: {prevDsType} not compatible with {candidate}"
            )

    # We haven't seen this dataset type in this graph before, but it may
    # already be in the registry.
    try:
        registryDsType = registry.getDatasetType(name)
        previous[candidate] = registryDsType
        return registryDsType
    except MissingDatasetTypeError:
        pass
    # Dataset type is totally new.  Store it by name and by dataset type so
    # it will be validated immediately next time it comes up.
    previous[name] = candidate
    previous[candidate] = candidate
    return candidate


def _accumulate(
    butler: Butler,
    graph: QuantumGraph,
) -> tuple[set[DatasetRef], DataSetTypeRefMap]:
    # accumulate the DatasetRefs that will be transferred to the execution
    # registry

    # exports holds all the existing data that will be migrated to the
    # execution butler
    exports: set[DatasetRef] = set()

    # inserts is the mapping of DatasetType to dataIds for what is to be
    # inserted into the registry. These are the products that are expected
    # to be produced during processing of the QuantumGraph
    inserts: DataSetTypeRefMap = defaultdict(set)

    # It is possible to end up with a graph that has different storage
    # classes attached to the same dataset type name. This is okay but
    # must we must ensure that only a single dataset type definition is
    # accumulated in the loop below.  This data structure caches every dataset
    # type encountered and stores the compatible alternative.
    datasetTypes: dict[str | DatasetType, DatasetType] = {}

    # Find the initOutput refs.
    initOutputRefs = list(graph.globalInitOutputRefs())
    for task_def in graph.iterTaskGraph():
        task_refs = graph.initOutputRefs(task_def)
        if task_refs:
            initOutputRefs.extend(task_refs)

    for ref in initOutputRefs:
        dataset_type = ref.datasetType
        if dataset_type.component() is not None:
            dataset_type = dataset_type.makeCompositeDatasetType()
        dataset_type = _validate_dataset_type(dataset_type, datasetTypes, butler.registry)
        inserts[dataset_type].add(ref)

    # Output references may be resolved even if they do not exist. Find all
    # actually existing refs.
    check_refs: set[DatasetRef] = set()
    for quantum in (n.quantum for n in graph):
        for attrName in ("initInputs", "inputs", "outputs"):
            attr: Mapping[DatasetType, DatasetRef | list[DatasetRef]] = getattr(quantum, attrName)
            for refs in attr.values():
                # This if block is because init inputs has a different
                # signature for its items
                if not isinstance(refs, list | tuple):
                    refs = [refs]
                for ref in refs:
                    if ref.isComponent():
                        ref = ref.makeCompositeRef()
                    check_refs.add(ref)
    exist_map = butler._exists_many(check_refs, full_check=False)
    existing_ids = {ref.id for ref, exists in exist_map.items() if exists}
    del exist_map

    for quantum in (n.quantum for n in graph):
        for attrName in ("initInputs", "inputs", "outputs"):
            attr = getattr(quantum, attrName)

            for type, refs in attr.items():
                if not isinstance(refs, list | tuple):
                    refs = [refs]
                if type.component() is not None:
                    type = type.makeCompositeDatasetType()
                type = _validate_dataset_type(type, datasetTypes, butler.registry)
                # iterate over all the references, if it exists and should be
                # exported, if not it should be inserted into the new registry
                for ref in refs:
                    # Component dataset ID is the same as its parent ID, so
                    # checking component in existing_ids works OK.
                    if ref.id in existing_ids:
                        # If this is a component we want the composite to be
                        # exported.
                        if ref.isComponent():
                            ref = ref.makeCompositeRef()
                        # Make sure we export this with the registry's dataset
                        # type, since transfer_from doesn't handle storage
                        # class differences (maybe it should, but it's not
                        # bad to be defensive here even if that changes).
                        if type != ref.datasetType:
                            ref = ref.overrideStorageClass(type.storageClass)
                            assert ref.datasetType == type, "Dataset types should not differ in other ways."
                        exports.add(ref)
                    else:
                        if ref.isComponent():
                            # We can't insert a component, and a component will
                            # be part of some other upstream dataset, so it
                            # should be safe to skip them here
                            continue
                        inserts[type].add(ref)

    return exports, inserts


def _discoverCollections(butler: Butler, collections: Iterable[str]) -> set[str]:
    # Recurse through any discovered collections to make sure all collections
    # are exported. This exists because I ran into a situation where some
    # collections were not properly being discovered and exported. This
    # method may be able to be removed in the future if collection export
    # logic changes
    collections = set(collections)
    while True:
        discoveredCollections = set(
            butler.registry.queryCollections(collections, flattenChains=True, includeChains=True)
        )
        if len(discoveredCollections) > len(collections):
            collections = discoveredCollections
        else:
            break
    return collections


def _export(
    butler: DirectButler, collections: Iterable[str] | None, inserts: DataSetTypeRefMap
) -> io.StringIO:
    # This exports relevant dimension records and collections using daf butler
    # objects, however it reaches in deep and does not use the public methods
    # so that it can export it to a string buffer and skip disk access.  This
    # does not export the datasets themselves, since we use transfer_from for
    # that.
    yamlBuffer = io.StringIO()
    # Yaml is hard coded, since the class controls both ends of the
    # export/import
    BackendClass = get_class_of(butler._config["repo_transfer_formats", "yaml", "export"])
    backend = BackendClass(yamlBuffer, universe=butler.dimensions)
    exporter = RepoExportContext(butler, backend, directory=None, transfer=None)

    # Need to ensure that the dimension records for outputs are
    # transferred.
    for _, refs in inserts.items():
        exporter.saveDataIds([ref.dataId for ref in refs])

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


def _setupNewButler(
    butler: DirectButler,
    outputLocation: ResourcePath,
    dirExists: bool,
    datastoreRoot: ResourcePath | None = None,
) -> Butler:
    """Set up the execution butler

    Parameters
    ----------
    butler : `Butler`
        The original butler, upon which the execution butler is based.
    outputLocation : `~lsst.resources.ResourcePath`
        Location of the execution butler.
    dirExists : `bool`
        Does the ``outputLocation`` exist, and if so, should it be clobbered?
    datastoreRoot : `~lsst.resources.ResourcePath`, optional
        Path for the execution butler datastore. If not specified, then the
        original butler's datastore will be used.

    Returns
    -------
    execution_butler : `Butler`
        Execution butler.
    """
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
    config["registry", "db"] = "sqlite:///<butlerRoot>/gen3.sqlite3"

    # Remove any namespace that may be set in main registry.
    config.pop(("registry", "namespace"), None)

    # Obscore manager cannot be used with execution butler.
    config.pop(("registry", "managers", "obscore"), None)

    # record the current root of the datastore if it is specified relative
    # to the butler root
    if datastoreRoot is not None:
        config["datastore", "root"] = datastoreRoot.geturl()
    elif config.get(("datastore", "root")) == BUTLER_ROOT_TAG and butler._config.configDir is not None:
        config["datastore", "root"] = butler._config.configDir.geturl()
    config["datastore", "trust_get_request"] = True

    # Requires that we use the dimension configuration from the original
    # butler and not use the defaults.
    config = Butler.makeRepo(
        root=outputLocation,
        config=config,
        dimensionConfig=butler.dimensions.dimensionConfig,
        overwrite=True,
        forceConfigRoot=False,
    )

    # Return a newly created butler
    return Butler.from_config(config, writeable=True)


def _import(
    yamlBuffer: io.StringIO,
    newButler: Butler,
    inserts: DataSetTypeRefMap,
    run: str | None,
    butlerModifier: Callable[[Butler], Butler] | None,
) -> Butler:
    # This method takes the exports from the existing butler, imports
    # them into the newly created butler, and then inserts the datasets
    # that are expected to be produced.

    # import the existing datasets using "split" mode. "split" is safe
    # because execution butler is assumed to be able to see all the file
    # locations that the main datastore can see. "split" supports some
    # absolute URIs in the datastore.
    newButler.import_(filename=yamlBuffer, format="yaml", transfer="split")

    # If there is modifier callable, run it to make necessary updates
    # to the new butler.
    if butlerModifier is not None:
        newButler = butlerModifier(newButler)

    # Register datasets to be produced and insert them into the registry
    for dsType, refs in inserts.items():
        # Storage class differences should have already been resolved by calls
        # _validate_dataset_type in _export, resulting in the Registry dataset
        # type whenever that exists.
        newButler.registry.registerDatasetType(dsType)
        newButler.registry._importDatasets(refs)

    return newButler


def buildExecutionButler(
    butler: DirectButler,
    graph: QuantumGraph,
    outputLocation: ResourcePathExpression,
    run: str | None,
    *,
    clobber: bool = False,
    butlerModifier: Callable[[Butler], Butler] | None = None,
    collections: Iterable[str] | None = None,
    datastoreRoot: ResourcePathExpression | None = None,
    transfer: str = "auto",
) -> Butler:
    r"""Create an execution butler.

    Responsible for exporting
    input `QuantumGraph`\s into a new minimal `~lsst.daf.butler.Butler` which
    only contains datasets specified by the `QuantumGraph`.

    These datasets are both those that already exist in the input
    `~lsst.daf.butler.Butler`, and those that are expected to be produced
    during the execution of the `QuantumGraph`.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        This is the existing `~lsst.daf.butler.Butler` instance from
        which existing datasets will be exported. This should be the
        `~lsst.daf.butler.Butler` which was used to create any `QuantumGraphs`
        that will be converted with this object.
    graph : `QuantumGraph`
        Graph containing nodes that are to be exported into an execution
        butler.
    outputLocation : convertible to `~lsst.resources.ResourcePath`
        URI Location at which the execution butler is to be exported. May be
        specified as a string or a `~lsst.resources.ResourcePath` instance.
    run : `str`, optional
        The run collection that the exported datasets are to be placed in. If
        None, the default value in registry.defaults will be used.
    clobber : `bool`, Optional
        By default a butler will not be created if a file or directory
        already exists at the output location. If this is set to `True`
        what is at the location will be deleted prior to running the
        export. Defaults to `False`.
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
    datastoreRoot : convertible to `~lsst.resources.ResourcePath`, Optional
        Root directory for datastore of execution butler. If `None`, then the
        original butler's datastore will be used.
    transfer : `str`
        How (and whether) the input datasets should be added to the execution
        butler datastore. This should be a ``transfer`` string recognized by
        :func:`lsst.resources.ResourcePath.transfer_from`.
        ``"auto"`` means to ``"copy"`` if the ``datastoreRoot`` is specified.

    Returns
    -------
    executionButler : `lsst.daf.butler.Butler`
        An instance of the newly created execution butler.

    Raises
    ------
    FileExistsError
        Raised if something exists in the filesystem at the specified output
        location and clobber is `False`.
    NotADirectoryError
        Raised if specified output URI does not correspond to a directory.
    """
    # Now require that if run is given it must match the graph run.
    if run and graph.metadata and run != (graph_run := graph.metadata.get("output_run")):
        raise ValueError(f"The given run, {run!r}, does not match that specified in the graph, {graph_run!r}")

    # We know this must refer to a directory.
    outputLocation = ResourcePath(outputLocation, forceDirectory=True)
    if datastoreRoot is not None:
        datastoreRoot = ResourcePath(datastoreRoot, forceDirectory=True)

    # Do this first to Fail Fast if the output exists
    if (dirExists := outputLocation.exists()) and not clobber:
        raise FileExistsError("Cannot create a butler at specified location, location exists")
    if not outputLocation.isdir():
        raise NotADirectoryError("The specified output URI does not appear to correspond to a directory")

    exports, inserts = _accumulate(butler, graph)
    yamlBuffer = _export(butler, collections, inserts)

    newButler = _setupNewButler(butler, outputLocation, dirExists, datastoreRoot)

    newButler = _import(yamlBuffer, newButler, inserts, run, butlerModifier)

    if transfer == "auto" and datastoreRoot is not None:
        transfer = "copy"

    # Transfer the existing datasets directly from the source butler.
    newButler.transfer_from(
        butler,
        exports,
        transfer=transfer,
        skip_missing=False,  # Everything should exist.
        register_dataset_types=True,
        transfer_dimensions=True,
    )

    return newButler
