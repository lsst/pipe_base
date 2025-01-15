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

from collections.abc import Iterator, Sequence
from contextlib import contextmanager

from lsst.daf.butler import Butler, DatasetId, DatasetRef, DatasetType
from pyspark.sql import DataFrame, SparkSession


@contextmanager
def create_spark_context(butler: Butler) -> Iterator[BuilderSparkContext]:
    session = SparkSession.builder.getOrCreate()
    try:
        yield BuilderSparkContext(session, butler)
    finally:
        session.stop()


class BuilderSparkContext:
    def __init__(self, session: SparkSession, butler: Butler):
        self.session = session
        self._butler = butler

    def get_datasets(self, dataset_type_name: str, collections: Sequence[str]) -> DataFrame:
        with self._butler.query() as query:
            result = query.join_dataset_search(dataset_type_name, collections).general(
                [],
                dataset_fields={dataset_type_name: ["dataset_id", "run", "collection", "timespan"]},
                find_first=False,
            )
            columns = result._spec.get_result_columns()
            rows = []
            for input_row in result:
                output_row = {}
                for col in columns:
                    name = col.logical_table if col.field is None else col.field
                    input_value = input_row[str(col)]
                    if name == "timespan":
                        if input_value is not None:
                            output_row["timespan_begin"] = input_value.nsec[0]
                            output_row["timespan_end"] = input_value.nsec[1]
                    elif name == "dataset_id":
                        output_row[name] = input_value.bytes
                    else:
                        output_row[name] = input_value
                rows.append(output_row)

        return self.session.createDataFrame(rows)

    def convert_datasets_to_refs(
        self, dataset_type: DatasetType, datasets: DataFrame
    ) -> Iterator[DatasetRef]:
        for row in datasets.toLocalIterator():
            yield DatasetRef(
                dataset_type, row.asDict(), row["run"], id=DatasetId(bytes=bytes(row["dataset_id"]))
            )
