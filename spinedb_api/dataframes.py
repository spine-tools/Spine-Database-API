######################################################################################################################
# Copyright (C) 2017-2022 Spine project consortium
# Copyright Spine Database API contributors
# This file is part of Spine Database API.
# Spine Database API is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser
# General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################
"""This module offers a Pandas interoperability layer to ``spinedb_api``.

.. warning::

  This is highly experimental API.

The main access points here are :func:`to_dataframe` which converts a parameter value item to a dataframe,
and :py:func:`add_or_update_from` which creates and updates data in a database mapping based on dataframe input.
Additionally, :func:`fetch_as_dataframe` grants direct-query access to parameter values.
It is somewhat more involved to use than the other functions
but may be faster since it relies on database queries only
bypassing the complex caching in database mapping.
It might be useful if you do not need to write back to the database.

The dataframes returned by the :func:`to_dataframe` and :func:`fetch_as_dataframe` functions
have the following structure:

.. list-table:: Dataframe columns left-to-right
  :header-rows: 1

  * - Column
    - Content
  * - entity_class_name
    - class names
  * - <dimension 1>
    - "leaf" entity names (bynames)
  * - ...
    - more entity bynames
  * - parameter_definition_name
    - parameter names
  * - alternative_name
    - alternative names
  * - <index 1>
    - parameter indices, e.g. time stamps
  * - ...
    - more parameter indices
  * - value
    - "leaf" values

For example, say we want to get *unit_capacity* time series
for a relationship between *power_plant_a* and *electricity_node*
and plot it.
This is straightforward with :func:`to_dataframe`::

    import matplotlib.pyplot as plt
    from spinedb_api import DatabaseMapping
    from spinedb_api.dataframes import to_dataframe

    with DatabaseMapping(url) as db_map:
        value_item = db_map.get_parameter_value_item(
            entity_class_name="unit__to_node",
            entity_byname=("power_plant_a", "electricity_node"),
            parameter_definition_name="unit_capacity",
            alternative_name="Base",
        )
        df = to_dataframe(value_item)

    figure, axes = plt.subplots()
    df.plot(x="t", y="value", ax=axes)
    plt.show()

.. note::

  The ``ignore_year`` and ``repeat`` attributes are stored in the ``attrs`` attribute
  of the dataframe if it contains time series.

The dataframe can be used to add new values or update existing values in a database,
proven that the target entities, parameter definitions and alternatives exist already.
For example, using ``df`` from above::

    with DatabaseMapping(target_url) as db_map:
        add_or_update_from(df, db_map)
        db_map.commit_session("Added unit_capacity value.")

.. warning::

  Time series are currently added/updated as :class:`spinedb_api.parameter_value.Map` values
  rather than as :class:`spinedb_api.parameter_value.TimeSeriesFixedResolution`,
  because :func:`add_or_update_from` does not implement dataframe -> time series transformation yet.

To use :func:`fetch_as_dataframe` instead of :func:`to_dataframe`,
:class:`FetchedMaps` needs to be instantiated
and a special `SQLAlchemy <docs.sqlalchemy.org/en/14/>`_ query prepared::

    with DatabaseMapping(url) as db_map:
        maps = FetchedMaps.fetch(db_map)
        query = parameter_value_sq(db_map)
        final_query = (
            db_map.query(query)
                .filter(query.c.entity_class_name == "node")
                .filter(query.c.parameter_definition_name=="state_coeff")
                .filter(query.c.alternative_name=="Base")
        ).subquery()
        df = fetch_as_dataframe(db_map, final_query, maps)
"""
from __future__ import annotations
import collections
from typing import Any, Union
import pandas as pd
import pyarrow
from sqlalchemy.sql import Subquery
from spinedb_api import DatabaseMapping, Map, SpineDBAPIError
from spinedb_api.arrow_value import from_database
from spinedb_api.db_mapping_base import PublicItem
from spinedb_api.parameter_value import NON_ZERO_RANK_TYPES, RANK_1_TYPES, VALUE_TYPES, DateTime

SpineScalarValue = Union[float, str, bool]
SpineValue = Union[SpineScalarValue, None, pyarrow.RecordBatch]
IdToIdMap = dict[int, int]
IdToIdListMap = dict[int, list[int]]
IdToListValueMap = dict[int, tuple[bytes, str]]
IdToNameMap = dict[int, str]
IdToNameAndClassMap = dict[int, tuple[str, int]]


_BASIC_COLUMNS = ["entity_class_name", "parameter_definition_name", "alternative_name"]


def to_dataframe(item: PublicItem) -> pd.DataFrame:
    """Converts parameter value item to dataframe."""
    item = item.mapped_item
    db_map = item.db_map
    entity = db_map.mapped_table("entity").find_item_by_id(item["entity_id"])
    entity_class = db_map.mapped_table("entity_class").find_item_by_id(item["entity_class_id"])
    value = item["arrow_value"]
    row_map = {"entity_class_name": pd.Series(entity["entity_class_name"], dtype="category")}
    row_map.update(
        {
            class_name: pd.Series([element_name], dtype="string")
            for class_name, element_name in zip(entity_class["entity_class_byname"], entity["entity_byname"])
        }
    )
    row_map.update(
        {
            "parameter_definition_name": pd.Series(item["parameter_definition_name"], dtype="category"),
            "alternative_name": pd.Series(item["alternative_name"], dtype="category"),
            "value": [_record_batch_to_dataframe(value)],
            "type": [item["type"]],
        }
    )
    dataframe = pd.DataFrame(row_map)
    return _expand_values(dataframe)


def _dataframe_to_value(
    dataframe: pd.DataFrame, value_columns: list[str], class_bynames: dict[str, tuple[str]]
) -> pd.Series:
    data = dataframe.loc[0, _BASIC_COLUMNS].to_dict()
    byname_columns = list(class_bynames[dataframe.loc[0, "entity_class_name"]])
    byname = tuple(dataframe.loc[0, byname_columns])
    data["entity_byname"] = byname
    if len(value_columns) == 1:
        data["parsed_value"] = _scalar_to_parsed_value(dataframe.loc[0, value_columns[0]])
    else:
        data["parsed_value"] = _to_parsed_value(dataframe.loc[:, value_columns])
    return pd.Series(data)


def _last_columns_to_map(dataframe: pd.DataFrame) -> Map:
    return Map(
        dataframe.iloc[:, -2].transform(_scalar_to_parsed_value).array,
        dataframe.iloc[:, -1].transform(_scalar_to_parsed_value).array,
        index_name=dataframe.columns[-2],
    )


def _scalar_to_parsed_value(scalar: Any) -> Any:
    if isinstance(scalar, pd.Timestamp):
        return DateTime(scalar.to_pydatetime())
    return scalar


def _to_parsed_value(value_frame: pd.DataFrame) -> Any:
    while value_frame.shape[1] > 2:
        new_last_column = value_frame.groupby(list(value_frame.columns[:-2]), sort=False).apply(
            _last_columns_to_map, include_groups=False
        )
        value_frame = pd.DataFrame(new_last_column).reset_index()
    return _last_columns_to_map(value_frame)


def add_or_update_from(dataframe: pd.DataFrame, db_map: DatabaseMapping) -> None:
    """Adds or updates parameter value items from dataframe.

    The dataframe is expected to contain at least ``entity_class_name``,
    ``parameter_definition_name``, ``alternative_name`` and ``value`` columns,
    and a column for each 0-dimensional entity from which entity bynames can be composed.
    Any additional column is considered as value index.

    The database mapping must contain the target entity, parameter definition and alternative
    before this operation.
    This helps in finding typos in the dataframe.
    """
    class_bynames = {}
    unique_bynames = set()
    entity_class_table = db_map.mapped_table("entity_class")
    for class_name in dataframe.loc[:, "entity_class_name"].unique():
        entity_class = db_map.item(entity_class_table, name=class_name)
        if not entity_class:
            raise SpineDBAPIError(f"no such entity class '{class_name}'")
        elemental_names = entity_class["entity_class_byname"]
        class_bynames[class_name] = elemental_names
        unique_bynames.update(elemental_names)
    unique_bynames = list(unique_bynames)
    basic_and_entity_columns = _BASIC_COLUMNS + unique_bynames
    value_groups = dataframe.groupby(basic_and_entity_columns, sort=False)
    value_columns = [column for column in dataframe.columns if column not in set(basic_and_entity_columns)]
    aggregated = value_groups[dataframe.columns].apply(
        _dataframe_to_value, value_columns=value_columns, class_bynames=class_bynames
    )
    for row in aggregated.itertuples(index=False):
        _, _, error = db_map.add_update_item("parameter_value", **row._asdict())
        if error:
            raise SpineDBAPIError(error)


class FetchedMaps:
    """A 'cache' class that holds information required to build a dataframe with :py:func:`fetch_as_dataframe`."""

    def __init__(
        self,
        list_value_map: IdToListValueMap,
        entity_class_name_map: IdToNameMap,
        entity_name_and_class_map: IdToNameAndClassMap,
        entity_element_map: IdToIdListMap,
        entity_dimension_map: IdToIdMap,
    ):
        self.list_value_map = list_value_map
        self.entity_class_name_map = entity_class_name_map
        self.entity_name_and_class_map = entity_name_and_class_map
        self.entity_element_map = entity_element_map
        self.entity_dimension_map = entity_dimension_map

    @classmethod
    def fetch(cls, db_map: DatabaseMapping) -> FetchedMaps:
        """Instantiates a :py:class:`FetchedMaps` with data queried from a database."""
        return cls(
            _fetch_list_value_map(db_map),
            _fetch_entity_class_name_map(db_map),
            _fetch_entity_name_and_class_map(db_map),
            _fetch_entity_element_map(db_map),
            _fetch_entity_dimension_map(db_map),
        )


def parameter_value_sq(db_map: DatabaseMapping) -> Subquery:
    """Returns basic parameter value subquery required by :py:func:`fetch_as_dataframe`."""
    return (
        db_map.query(
            db_map.entity_class_sq.c.id.label("entity_class_id"),
            db_map.entity_class_sq.c.name.label("entity_class_name"),
            db_map.entity_sq.c.id.label("entity_id"),
            db_map.parameter_definition_sq.c.name.label("parameter_definition_name"),
            db_map.alternative_sq.c.name.label("alternative_name"),
            db_map.parameter_value_sq.c.value,
            db_map.parameter_value_sq.c.type,
            db_map.parameter_value_sq.c.list_value_id,
        )
        .join(
            db_map.parameter_definition_sq,
            db_map.parameter_definition_sq.c.id == db_map.parameter_value_sq.c.parameter_definition_id,
        )
        .join(db_map.entity_sq, db_map.parameter_value_sq.c.entity_id == db_map.entity_sq.c.id)
        .join(
            db_map.entity_class_sq,
            db_map.parameter_definition_sq.c.entity_class_id == db_map.entity_class_sq.c.id,
        )
        .join(db_map.alternative_sq, db_map.parameter_value_sq.c.alternative_id == db_map.alternative_sq.c.id)
        .subquery()
    )


def _fetch_list_value_map(db_map: DatabaseMapping) -> IdToListValueMap:
    return {
        row.id: (row.value, row.type)
        for row in db_map.query(db_map.list_value_sq.c.id, db_map.list_value_sq.c.value, db_map.list_value_sq.c.type)
    }


def _fetch_entity_name_and_class_map(db_map: DatabaseMapping) -> IdToNameAndClassMap:
    return {
        row.id: (row.name, row.class_id)
        for row in db_map.query(
            db_map.entity_sq.c.id,
            db_map.entity_sq.c.name,
            db_map.entity_sq.c.class_id,
        )
    }


def _expand_ids_recursive(entity_id: int, element_map: IdToIdListMap) -> list[int]:
    expanded = []
    for element_id in element_map[entity_id]:
        if element_id not in element_map:
            expanded.append(element_id)
            continue
        expanded.extend(_expand_ids_recursive(element_id, element_map))
    return expanded


def _fetch_entity_element_map(db_map: DatabaseMapping) -> IdToIdListMap:
    element_table = pd.DataFrame(
        db_map.query(
            db_map.entity_element_sq.c.entity_id,
            db_map.entity_element_sq.c.element_id,
            db_map.entity_element_sq.c.position,
        ).order_by(db_map.entity_element_sq.c.entity_id, db_map.entity_element_sq.c.position)
    )
    if element_table.empty:
        return {}
    element_map: IdToIdListMap = {}
    for entity_id, element_group in element_table.groupby(["entity_id"], sort=False):
        element_map[entity_id[0]] = element_group["element_id"]
    return {entity_id: _expand_ids_recursive(entity_id, element_map) for entity_id in element_map}


def _fetch_entity_dimension_map(db_map: DatabaseMapping) -> IdToIdMap:
    return {
        row.id: row.class_id
        for row in db_map.query(
            db_map.entity_sq.c.id,
            db_map.entity_sq.c.class_id,
        )
    }


def _fetch_entity_class_name_map(db_map: DatabaseMapping) -> IdToNameMap:
    return {row.id: row.name for row in db_map.query(db_map.entity_class_sq.c.id, db_map.entity_class_sq.c.name)}


def _resolve_elements(
    dataframe: pd.DataFrame,
    entity_class_name_map: IdToNameMap,
    entity_name_and_class_map: IdToNameAndClassMap,
    entity_element_map: IdToIdListMap,
) -> pd.DataFrame:
    groups = dataframe.groupby("entity_class_id", sort=False)
    resolved_groups = []
    for _, group in groups:
        resolved_columns = _resolve_elements_for_single_class(
            group["entity_id"], entity_class_name_map, entity_name_and_class_map, entity_element_map
        )
        group_frame = group.drop(columns=["entity_class_id", "entity_id"])
        column_names = _unique_series_names(resolved_columns)
        for name, column in zip(reversed(column_names), reversed(resolved_columns)):
            group_frame.insert(1, name, column)
        resolved_groups.append(group_frame)
    return pd.concat(resolved_groups)


def _resolve_elements_for_single_class(
    entity_id_series: pd.Series,
    entity_class_name_map: IdToNameMap,
    entity_name_and_class_map: IdToNameAndClassMap,
    entity_element_map: IdToIdListMap,
) -> list[pd.Series]:
    element_series = {}
    for entity_id in entity_id_series:
        try:
            elements = entity_element_map[entity_id]
        except KeyError:
            elements = [entity_id]
        for position, element_id in enumerate(elements):
            entity_name, class_id = entity_name_and_class_map[element_id]
            class_name = entity_class_name_map[class_id]
            series = element_series.setdefault((class_name, position), [])
            series.append(entity_name)
    return [
        pd.Series(entities, name=class_name, dtype="string") for (class_name, _), entities in element_series.items()
    ]


def _unique_series_names(series_list: list[pd.Series]) -> list[str]:
    name_counts = collections.Counter()
    for series in series_list:
        name_counts[series.name] += 1
    names = []
    rename_counts = collections.Counter()
    for series in series_list:
        count = name_counts[series.name]
        if count == 1:
            names.append(series.name)
        else:
            rename_counts[series.name] += 1
            names.append(series.name + f"_{rename_counts[series.name]}")
    return names


def _convert_values_from_database(dataframe_row: pd.Series, list_value_map: IdToListValueMap) -> pd.Series:
    if dataframe_row.iloc[-1] is not None:
        value, value_type = list_value_map[dataframe_row[-1]]
    else:
        value = dataframe_row.iloc[-3]
        value_type = dataframe_row.iloc[-2]
    value = _record_batch_to_dataframe(from_database(value, value_type))
    return pd.Series({"value": value, "type": value_type})


def _expand_values(dataframe: pd.DataFrame) -> pd.DataFrame:
    grouped = dataframe.groupby("type", sort=False)
    expanded = []
    attributes = {}
    for expandable_type in NON_ZERO_RANK_TYPES:
        try:
            group = grouped.indices[expandable_type]
        except KeyError:
            continue
        block_of_single_type = dataframe.iloc[group, :]
        for i in range(block_of_single_type.shape[0]):
            row = block_of_single_type.iloc[i : i + 1, :]
            value = row.iat[0, -2]
            row = row.drop(columns=["value", "type"])
            left = pd.concat(value.shape[0] * [row], ignore_index=True)
            expanded.append(pd.concat((left, value), axis="columns"))
            attributes.update(value.attrs)
    for non_expandable_type in VALUE_TYPES - NON_ZERO_RANK_TYPES:
        try:
            group = grouped.indices[non_expandable_type]
        except KeyError:
            continue
        non_expanded = dataframe.iloc[group]
        expanded.append(non_expanded.drop(columns=["type"]))
    expanded = pd.concat(expanded, ignore_index=True)
    expanded.attrs = attributes
    if expanded.columns.get_loc("value") == expanded.shape[1] - 1:
        return expanded
    y_column = expanded.pop("value")
    return pd.concat((expanded, y_column), axis="columns")


def _record_batch_to_dataframe(x: SpineValue) -> Union[SpineScalarValue, None, pd.DataFrame]:
    if isinstance(x, pyarrow.RecordBatch):
        dataframe = x.to_pandas()
        attributes = {}
        for column in x.schema.names:
            metadata = x.schema.field(column).metadata
            if metadata is not None:
                attributes[column] = {key.decode(): value.decode() for key, value in metadata.items()}
        if attributes:
            dataframe.attrs = attributes
        return dataframe
    return x


def fetch_as_dataframe(db_map: DatabaseMapping, value_sq: Subquery, fetched_maps: FetchedMaps) -> pd.DataFrame:
    """Fetches parameter values from database returning them as dataframe."""
    dataframe = pd.DataFrame(db_map.query(value_sq))
    if dataframe.empty:
        return dataframe
    dataframe["entity_class_name"] = dataframe["entity_class_name"].astype("category")
    dataframe["parameter_definition_name"] = dataframe["parameter_definition_name"].astype("category")
    dataframe["alternative_name"] = dataframe["alternative_name"].astype("category")
    value_series = dataframe.apply(
        _convert_values_from_database, axis="columns", result_type="expand", args=(fetched_maps.list_value_map,)
    )
    dataframe = dataframe.drop(columns=["value", "type", "list_value_id"])
    dataframe = pd.concat((dataframe, value_series), axis="columns")
    dataframe = _resolve_elements(
        dataframe,
        fetched_maps.entity_class_name_map,
        fetched_maps.entity_name_and_class_map,
        fetched_maps.entity_element_map,
    )
    return _expand_values(dataframe)
