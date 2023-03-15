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

__all__ = ("NodeAttributeOptions",)

import dataclasses
from typing import Literal


@dataclasses.dataclass
class NodeAttributeOptions:
    """Struct holding options for how to display and possibly merge nodes."""

    dimensions: Literal["full"] | Literal["concise"] | Literal[False] | None
    """Options for displaying dimensions.

    Possible values include:

    - ``"full"``: report fully-expanded dimensions.
    - ``"concise"``: report only dimensions that are not required or implied
      dependencies of any reported dimension.
    - `False`: do not report dimensions at all.
    - `None`: context-dependent default behavior.
    """

    task_classes: Literal["full"] | Literal["concise"] | Literal[False] | None
    """Options for displaying task types.

    Possible values include:

    - ``"full"``: report the fully-qualified task class name.
    - ``"concise"``: report the task class name with no module or package.
    - `False`: do not report task classes at all.
    - `None`: context-dependent default behavior.
    """

    storage_classes: bool | None
    """Options for displaying dataset type storage classes.

    Possible values include:

    - `True`: report storage classes.
    - `False`: do not report storage classes.
    - `None`: context-dependent default behavior.
    """

    def __bool__(self) -> bool:
        return bool(self.dimensions or self.storage_classes or self.task_classes)

    def checked(self, is_resolved: bool) -> NodeAttributeOptions:
        """Check these options against a pipeline graph's resolution status and
        fill in defaults.

        Parameters
        ----------
        is_resolved : `bool`
            Whether the pipeline graph to be displayed is resolved
            (`PipelineGraph.is_fully_resolved`).

        Returns
        -------
        options : `NodeAttributeOptions`
            Options with all `None` values in ``self`` filled in.  Concise
            reporting is used by default if the graph is resolved.

        Raises
        ------
        ValueError
            Raised if an attribute is explicitly requested but the graph is not
            fully resolved.
        """
        if self.dimensions and not is_resolved:
            raise ValueError("Cannot show dimensions unless they have been resolved.")
        if self.storage_classes and not is_resolved:
            raise ValueError("Cannot show storage classes unless they have been resolved.")
        return NodeAttributeOptions(
            dimensions=(
                self.dimensions if self.dimensions is not None else ("concise" if is_resolved else False)
            ),
            task_classes=(
                self.task_classes if self.task_classes is not None else ("concise" if is_resolved else False)
            ),
            storage_classes=(self.storage_classes if self.storage_classes is not None else is_resolved),
        )
