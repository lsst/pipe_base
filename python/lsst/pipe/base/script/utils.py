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

__all__ = ["filter_by_dataset_type_glob", "filter_by_existence"]

import re
from collections.abc import Collection

from lsst.daf.butler import Butler, DatasetRef
from lsst.daf.butler.utils import globToRegex
from lsst.utils.logging import getLogger
from lsst.utils.timer import time_this

_LOG = getLogger(__name__)


def _matches_dataset_type(dataset_type_name: str, regexes: list[str | re.Pattern]) -> bool:
    for regex in regexes:
        if isinstance(regex, str):
            if dataset_type_name == regex:
                return True
        elif regex.search(dataset_type_name):
            return True
    return False


def filter_by_dataset_type_glob(
    refs: Collection[DatasetRef], dataset_types: tuple[str, ...]
) -> Collection[DatasetRef]:
    """Filter the refs based on requested dataset types.

    Parameters
    ----------
    refs : `collections.abc.Collection` [ `lsst.daf.butler.DatasetRef` ]
        Datasets to be filtered.
    dataset_types : `tuple` [ `str`, ...]
        Dataset type names or globs to use for filtering. Empty tuple implies
        no filtering.

    Returns
    -------
    filtered : `collections.abc.Collection` [ `lsst.daf.butler.DatasetRef` ]
        Filter datasets.
    """
    regexes = globToRegex(dataset_types)
    if regexes is ...:
        # Nothing to do.
        return refs

    return {ref for ref in refs if _matches_dataset_type(ref.datasetType.name, regexes)}


def filter_by_existence(butler: Butler, refs: Collection[DatasetRef]) -> Collection[DatasetRef]:
    """Filter out the refs that the butler already knows exist.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        Butler in which to check existence of given datarefs.
    refs : `collections.abc.Collection` [ `lsst.daf.butler.DatasetRef` ]
        Datasets to be filtered.

    Returns
    -------
    filtered : `collections.abc.Collection` [ `lsst.daf.butler.DatasetRef` ]
        Filter datasets.
    """
    _LOG.verbose("Filtering out datasets already known to the target butler...")
    with time_this(log=_LOG, msg="Completed checking existence"):
        existence = butler._datastore.knows_these(refs)
        filtered = [ref for ref in existence if not existence[ref]]
    _LOG.verbose(
        "After filtering out those already in the target butler, number of datasets to transfer: %d",
        len(filtered),
    )

    return filtered
