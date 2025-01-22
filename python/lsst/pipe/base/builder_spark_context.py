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

import os
from collections.abc import Iterator, Sequence
from contextlib import contextmanager

import lsst.sphgeom
import pyarrow
from lsst.daf.butler import (
    Butler,
    DataCoordinate,
    DatasetId,
    DatasetRef,
    DatasetType,
    Dimension,
    DimensionGroup,
)
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import BinaryType, BooleanType, LongType, StringType, StructField, StructType


@contextmanager
def create_spark_context(butler: Butler) -> Iterator[BuilderSparkContext]:
    session = SparkSession.builder.config(
        # Propagate library path to workers, so that lsst.sphgeom can find its
        # C++ library.
        "spark.executorEnv.DYLD_LIBRARY_PATH",
        os.environ["DYLD_LIBRARY_PATH"],
    ).getOrCreate()
    try:
        yield BuilderSparkContext(session, butler)
    finally:
        session.stop()


class BuilderSparkContext:
    def __init__(self, session: SparkSession, butler: Butler):
        self.session = session
        self._butler = butler
        self._dimension_records: dict[str, DataFrame] = {}

    def get_datasets(self, dataset_type_name: str, collections: Sequence[str]) -> DataFrame:
        with self._butler.query() as query:
            result = query.join_dataset_search(dataset_type_name, collections).general(
                [],
                dataset_fields={dataset_type_name: ["dataset_id", "run", "collection", "timespan"]},
                find_first=False,
            )
            columns = result._spec.get_result_columns()

            schema = []
            for col in columns:
                spec = columns.get_column_spec(col.logical_table, col.field)
                name = col.logical_table if col.field is None else col.field
                if spec.type == "int":
                    schema.append(StructField(name, LongType(), False))
                elif spec.type == "string":
                    schema.append(StructField(name, StringType(), False))
                elif spec.type == "uuid":
                    schema.append(StructField(name, BinaryType(), False))
                elif spec.type == "timespan":
                    schema.append(StructField("timespan_begin", LongType(), True))
                    schema.append(StructField("timespan_end", LongType(), True))
                else:
                    raise TypeError(f"Unhandled Butler data type {spec.type}")

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

        return self.session.createDataFrame(rows, schema=StructType(schema))

    def get_dimension_records(self, dimension_name: str) -> DataFrame:
        if (df := self._dimension_records.get(dimension_name)) is not None:
            return df

        dimension = self._butler.dimensions[dimension_name]

        with self._butler.query() as query:
            pages = (page.to_arrow() for page in query.dimension_records(dimension_name).iter_table_pages())
            table = pyarrow.concat_tables(pages)
        df = self.session.createDataFrame(table.to_pandas())

        # Use the table name for the ID column.  Other tables will
        # reference the column this same way.
        if isinstance(dimension, Dimension):
            primary_key_name = dimension.primary_key.name
            df = df.withColumnRenamed(primary_key_name, dimension_name)

        self._dimension_records[dimension_name] = df
        df.show()
        return df

    def join(
        self,
        left_df: DataFrame,
        left_dimensions: DimensionGroup,
        right_df: DataFrame,
        right_dimensions: DimensionGroup,
    ) -> DataFrame:
        join_dimensions = left_dimensions.intersection(right_dimensions)
        join_columns = list(join_dimensions.required)

        if (
            len(left_dimensions.spatial) == 0
            or len(right_dimensions.spatial) == 0
            or left_dimensions.spatial == right_dimensions.spatial
        ):
            # We can join directly using the data ID.  No spatial join is
            # needed because one of the dimensions is not spatial, or they are
            # from the same spatial family.
            if len(join_columns) == 0:
                # No data ID columns are available to join -- we have to use
                # the cartesian product.
                return left_df.crossJoin(right_df)
            else:
                return left_df.join(right_df, on=join_columns)

        # Both dimension groups have spatial dimensions, but they are from
        # different families.  That means we need to do a spatial join using
        # the regions from the dimension records.
        left_df = self._add_region_column(left_dimensions, left_df)
        right_df = self._add_region_column(right_dimensions, right_df)
        # TODO: this join is extremely inefficient because it requires a test
        # for every pair-wise combination of regions.  It can be optimized
        # using a strategy like the real Butler query system, computing htm
        # pixels or some other representation for fast overlap checks.
        if len(join_columns) == 0:
            # No data ID columns are available to join -- we have to use the
            # cartesian product.
            joined = left_df.crossJoin(right_df)
        else:
            joined = left_df.join(right_df, on=join_columns)

        return joined.where(_region_overlaps(left_df["region"], right_df["region"]))

    def _add_region_column(self, dimensions: DimensionGroup, df: DataFrame) -> DataFrame:
        # The logic is more complicated if a dimension group has more than one
        # spatial dimension, so that case is not handled yet.
        assert len(dimensions.spatial) == 1

        region_dimension = dimensions.region_dimension
        if region_dimension == "htm7":
            return df.withColumn("region", _htm_pixel_region("htm7"))
        else:
            dimension_records = self.get_dimension_records(region_dimension)
            join_columns = list(dimensions.universe[region_dimension].required.names)
            return df.join(dimension_records, on=join_columns).select(df["*"], dimension_records["region"])

    def convert_datasets_to_refs(
        self, dataset_type: DatasetType, datasets: DataFrame
    ) -> Iterator[DatasetRef]:
        for row in datasets.toLocalIterator():
            yield _to_ref(dataset_type, row)

    def convert_datasets_to_coordinate_and_refs(
        self, dimensions: DimensionGroup, dataset_type: DatasetType, datasets: DataFrame
    ) -> Iterator[tuple[DataCoordinate, DatasetRef]]:
        for row in datasets.toLocalIterator():
            yield _to_data_coordinate(dimensions, row), _to_ref(dataset_type, row)


def _to_ref(dataset_type: DatasetType, row: Row) -> DatasetRef:
    return DatasetRef(dataset_type, row.asDict(), row["run"], id=DatasetId(bytes=bytes(row["dataset_id"])))


def _to_data_coordinate(dimensions: DimensionGroup, row: Row) -> DataCoordinate:
    return DataCoordinate.standardize(row.asDict(), dimensions=dimensions)


@udf(returnType=BooleanType())
def _region_overlaps(a: bytes, b: bytes) -> bool:
    region_a = lsst.sphgeom.Region.decode(bytes(a))
    region_b = lsst.sphgeom.Region.decode(bytes(b))
    return not (region_a.relate(region_b) & lsst.sphgeom.DISJOINT)


@udf(returnType=BinaryType())
def _htm_pixel_region(pixel: int) -> bytes:
    return lsst.sphgeom.HtmPixelization(7).pixel(pixel).encode()
