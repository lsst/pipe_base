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

"""Shim classes that provide (limited) Gen2 Butler interfaces to Gen3
data repositories.

All of the classes here still operate on Gen3 data IDs - the shim layer
essentially assumes that Gen2 Tasks treat the data ID as an opaque blob, which
is usually (but not always true).  When it isn't, the best approach now is
probably to use the GENERATION class attribute on the Butler classes to
special-case code for each generation.
"""

__all__ = ("ShimButler", "ShimButlerSubset", "ShimDataRef")

from lsst.daf.butler import StorageClassFactory
from lsst.daf.persistence import NoResults


class ShimButler:
    """A shim for a Gen2 `~lsst.daf.persistence.Butler` with a Gen3
    `~lsst.daf.butler.Butler` backend.

    Parameters
    ----------
    butler3 : `lsst.daf.butler.Butler`
        Generation 3 Butler instance.
    """

    GENERATION = 2.5
    """This is a Generation 2 shim for a Generation3 Butler.
    """

    def __init__(self, butler3):
        self._butler3 = butler3

    def _makeDataId(self, dataId=None, **rest):
        """Construct a full data ID by merging the given arguments with the
        internal data ID.
        """
        fullDataId = dict()
        if dataId is not None:
            fullDataId.update(dataId)
        fullDataId.update(rest)
        return fullDataId

    def _translateDatasetType(self, datasetType):
        if "_" in datasetType:
            if datasetType.endswith("_md"):
                return f"{datasetType[:-3]}.metadata"
            for component in StorageClassFactory().getStorageClass("Exposure").components:
                suffix = f"_{component}"
                if datasetType.endswith(suffix):
                    return "{}.{}".format(datasetType[: -len(suffix)], component)
        return datasetType

    def datasetExists(self, datasetType, dataId=None, write=False, **rest):
        """Check whether a datataset exists in the repository.

        Parameters
        ----------
        datasetType : `str`
            Name of the Gen2 dataset type.
        dataId : `dict` or `~lsst.daf.butler.DataId`, optional
            A Generation 3 data ID that identifies the dataset.
        write : `bool`
            This option is provided for compatibility with
            `lsst.daf.persistence.Butler`, but must be `False`.
        rest
            Additional key-value pairs to augment the given data ID.

        Returns
        -------
        exists : `bool`
            `True` if the dataset is present in the repository, `False`
            otherwise.
        """
        if write:
            raise NotImplementedError("ShimButler cannot implement datasetExists with 'write=True'")
        datasetType = self._translateDatasetType(datasetType)
        try:
            return self._butler3.datasetExists(datasetType, self._makeDataId(dataId, **rest))
        except LookupError:
            # Gen3 datasetExists raises if Dataset is not present in Registry;
            # Gen2 does not distinguish between present in Datastore and
            # present in Registry.
            return False

    def get(self, datasetType, dataId=None, immediate=True, **rest):
        """Retrieve a dataset.

        Parameters
        ----------
        datasetType : `str`
            Name of the Gen2 dataset type.
        dataId : `dict` or `~lsst.daf.butler.DataId`, optional
            A Generation 3 data ID that identifies the dataset.
        immediate : `bool`
            This option is provided for compatibility with
            `lsst.daf.persistence.Butler`, but is ignored.
        rest
            Additional key-value pairs to augment the given data ID.

        Returns
        -------
        dataset
            Retrieved object.

        Raises
        ------
        `~lsst.daf.persistence.NoResults`
            Raised if the dataset does not exist.
        """
        datasetType = self._translateDatasetType(datasetType)
        fullDataId = self._makeDataId(dataId, **rest)
        if datasetType.endswith("_sub"):
            import lsst.afw.image

            datasetType = datasetType[: -len("_sub")]
            parameters = dict(bbox=fullDataId.pop("bbox"))
            origin = fullDataId.pop("imageOrigin", lsst.afw.image.PARENT)
            parameters["origin"] = origin
        else:
            parameters = {}
        try:
            return self._butler3.get(datasetType, fullDataId, parameters=parameters)
        except (FileNotFoundError, LookupError) as err:
            raise NoResults(str(err), datasetType, fullDataId)

    def put(self, obj, datasetType, dataId=None, doBackup=False, **rest):
        """Write a dataset.

        Parameters
        ----------
        obj
            Object to write.
        datasetType : `str`
            Name of the Gen2 dataset type.
        dataId : `dict` or `~lsst.daf.butler.DataId`, optional
            A Generation 3 data ID that identifies the dataset.
        doBackup : `bool`
            This option is provided for compatibility with
            `lsst.daf.persistence.Butler`, but must be `False`.
        rest
            Additional key-value pairs to augment the given data ID.
        """
        if doBackup:
            raise NotImplementedError("ShimButler cannot implement put with 'doBackup=True'")
        datasetType = self._translateDatasetType(datasetType)
        self._butler3.put(obj, datasetType, self._makeDataId(dataId, **rest))

    def dataRef(self, datasetType, level=None, dataId=None, **rest):
        """Return a DataRef associated with the given dataset type and data ID.

        Parameters
        ----------
        datasetType : `str`
            Name of the dataset type.
        dataId : `dict` or `~lsst.daf.butler.DataId`, optional
            A Generation 3 data ID that identifies the dataset.
        level
            This option is provided for compatibility with
            `lsst.daf.persistence.Butler`, but must be `None`.
        rest
            Additional key-value pairs to augment the given data ID.
        """
        if level is not None:
            raise NotImplementedError("ShimButler cannot implement dataRef with 'level != None'")
        fullDataId = {}
        if dataId is not None:
            fullDataId.update(dataId)
        fullDataId.update(rest)
        return next(iter(ShimButlerSubset(self, datasetType, [fullDataId])))


class ShimButlerSubset:
    """A shim for a Gen2 `~lsst.daf.persistence.ButlerSubset` with a Gen3
    `~lsst.daf.butler.Butler` backend.

    Parameters
    ----------
    butler : `ShimButler`
        Butler shim instance.
    datasetType : `str`
        Name of the dataset type.
    dataIds : iterable of `dict` or `~lsst.daf.butler.DataId`
        Generation 3 data IDs that define the data in this subset.
    """

    GENERATION = 2.5
    """This is a Generation 2 shim for a Generation3 Butler.
    """

    def __init__(self, butler, datasetType, dataIds):
        self.butler = butler
        self.datasetType = datasetType
        self._dataIds = tuple(dataIds)

    def __len__(self):
        return len(self._dataIds)

    def __iter__(self):
        for dataId in self._dataIds:
            yield ShimDataRef(self, dataId)


class ShimDataRef:
    """A shim for a Gen2 `~lsst.daf.persistence.ButlerDataRef` with a Gen3
    `~lsst.daf.butler.Butler` backend.

    Parameters
    ----------
    butlerSubset : `ShimButlerSubset`
        ButlerSubset shim instance.  Sets the butler and default dataset type
        used by the Dataref.
    dataId : `dict` or `~lsst.daf.butler.DataId`
        Generation 3 data ID associated with this reference.
    """

    GENERATION = 2.5
    """This is a Generation 2 shim for a Generation3 Butler.
    """

    def __init__(self, butlerSubset, dataId):
        self.butlerSubset = butlerSubset
        self.dataId = dataId

    def get(self, datasetType=None, **rest):
        """Retrieve a dataset.

        Parameters
        ----------
        datasetType : `str`, optional.
            Name of the dataset type.  Defaults to the dataset type used to
            construct the `ShimButlerSubset`.
        rest
            Additional arguments forwarded to `ShimButler.get`.

        Returns
        -------
        dataset
            Retrieved object.

        Raises
        ------
        `~lsst.daf.persistence.NoResults`
            Raised if the dataset does not exist.
        """
        if datasetType is None:
            datasetType = self.butlerSubset.datasetType
        return self.butlerSubset.butler.get(datasetType, self.dataId, **rest)

    def put(self, obj, datasetType=None, doBackup=False, **rest):
        """Write a dataset.

        Parameters
        ----------
        obj
            Object to write.
        datasetType : `str`, optional
            Name of the dataset type.  Defaults to the dataset type used to
            construct the `ShimButlerSubset`.
        doBackup : `bool`
            This option is provided for compatibility with
            `lsst.daf.persistence.ButlerDataRef`, but must be `False`.
        rest
            Additional arguments forwarded to `ShimButler.put`.
        """
        if datasetType is None:
            datasetType = self.butlerSubset.datasetType
        self.butlerSubset.butler.put(obj, datasetType, self.dataId, doBackup=doBackup, **rest)

    def datasetExists(self, datasetType=None, write=False, **rest):
        """Check whether a datataset exists in the repository.

        Parameters
        ----------
        datasetType : `str`, optional
            Name of the dataset type.  Defaults to the dataset type used to
            construct the `ShimButlerSubset`.
        write : `bool`
            This option is provided for compatibility with
            `lsst.daf.persistence.ButlerDataRef`, but must be `False`.
        rest
            Additional arguments forwarded to `ShimButler.datasetExists`.

        Returns
        -------
        exists : `bool`
            `True` if the dataset is present in the repository, `False`
            otherwise.
        """
        if datasetType is None:
            datasetType = self.butlerSubset.datasetType
        return self.butlerSubset.butler.datasetExists(datasetType, self.dataId, write=write, **rest)

    def getButler(self):
        """Return the (shim) Butler used by this DataRef."""
        return self.butlerSubset.butler
