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
"""This module offers a Pandas interoperability layer to `spinedb_api`.

.. note::

  This is highly experimental API.

The functions here work at the database query level with as little overhead as possible.
"""
import collections
import pandas as pd
import pyarrow
from spinedb_api import Map
from spinedb_api.arrow_value import from_database
from spinedb_api.parameter_value import RANK_1_TYPES, VALUE_TYPES


class FetchedMaps:
    def __init__(
        self, list_value_map, entity_class_name_map, entity_name_and_class_map, entity_element_map, entity_dimension_map
    ):
        self.list_value_map = list_value_map
        self.entity_class_name_map = entity_class_name_map
        self.entity_name_and_class_map = entity_name_and_class_map
        self.entity_element_map = entity_element_map
        self.entity_dimension_map = entity_dimension_map

    @classmethod
    def fetch(cls, db_map):
        return cls(
            fetch_list_value_map(db_map),
            fetch_entity_class_name_map(db_map),
            fetch_entity_name_and_class_map(db_map),
            fetch_entity_element_map(db_map),
            fetch_entity_dimension_map(db_map),
        )


def parameter_value_sq(db_map):
    return (
        db_map.query(
            db_map.entity_class_sq.c.id.label("entity_class_id"),
            db_map.parameter_definition_sq.c.name.label("parameter_definition_name"),
            db_map.entity_sq.c.id.label("entity_id"),
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


def fetch_list_value_map(db_map):
    return {
        row.id: (row.value, row.type)
        for row in db_map.query(db_map.list_value_sq.c.id, db_map.list_value_sq.c.value, db_map.list_value_sq.c.type)
    }


def fetch_entity_name_and_class_map(db_map):
    return {
        row.id: (row.name, row.class_id)
        for row in db_map.query(
            db_map.entity_sq.c.id,
            db_map.entity_sq.c.name,
            db_map.entity_sq.c.class_id,
        )
    }


def _expand_ids_iterative(entity_id, element_map):
    expanded = []
    for element_id in element_map[entity_id]:
        if element_id not in element_map:
            expanded.append(element_id)
            continue
        expanded.extend(_expand_ids_iterative(element_id, element_map))
    return expanded


def fetch_entity_element_map(db_map):
    element_table = pd.DataFrame(
        db_map.query(
            db_map.entity_element_sq.c.entity_id,
            db_map.entity_element_sq.c.element_id,
            db_map.entity_element_sq.c.position,
        ).order_by(db_map.entity_element_sq.c.entity_id, db_map.entity_element_sq.c.position)
    )
    if element_table.empty:
        return {}
    element_map = {}
    for entity_id, element_group in element_table.groupby(["entity_id"], sort=False):
        element_map[entity_id[0]] = element_group["element_id"]
    return {entity_id: _expand_ids_iterative(entity_id, element_map) for entity_id in element_map}


def fetch_entity_dimension_map(db_map):
    return {
        row.id: row.class_id
        for row in db_map.query(
            db_map.entity_sq.c.id,
            db_map.entity_sq.c.class_id,
        )
    }


def fetch_entity_class_name_map(db_map):
    return {row.id: row.name for row in db_map.query(db_map.entity_class_sq.c.id, db_map.entity_class_sq.c.name)}


def resolve_elements(dataframe, entity_class_name_map, entity_name_and_class_map, entity_element_map):
    groups = dataframe.groupby("entity_class_id", sort=False)
    resolved_groups = []
    for _, group in groups:
        resolved_columns = resolve_elements_for_single_class(
            group["entity_id"], entity_class_name_map, entity_name_and_class_map, entity_element_map
        )
        group_frame = group.drop(columns=["entity_class_id", "entity_id"])
        column_names = unique_series_names(resolved_columns)
        for name, column in zip(reversed(column_names), reversed(resolved_columns)):
            group_frame.insert(0, name, column)
        resolved_groups.append(group_frame)
    return pd.concat(resolved_groups)


def resolve_elements_for_single_class(
    entity_id_series, entity_class_name_map, entity_name_and_class_map, entity_element_map
):
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
    return [pd.Series(entities, name=class_name) for (class_name, _), entities in element_series.items()]


def unique_series_names(series_list):
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


def convert_values_from_database(dataframe_row, list_value_map):
    if dataframe_row.iloc[-1] is not None:
        value, value_type = list_value_map[dataframe_row[-1]]
    else:
        value = dataframe_row.iloc[-3]
        value_type = dataframe_row.iloc[-2]
    value = from_database(value, value_type)
    if isinstance(value, pyarrow.RecordBatch):
        value = value.to_pandas()
    return pd.Series({"value": value, "type": value_type})


def expand_values(dataframe):
    """Expands parsed values in a dataframe.

    Consumes the 'type' column.

    Args:
        dataframe (pd.DataFrame): dataframe to expand

    Returns:
        pd.DataFrame: expanded dataframe
    """
    grouped = dataframe.groupby("type", sort=False)
    expanded = []
    rank_n_types = {Map.type_()}
    for expandable_type in RANK_1_TYPES | rank_n_types:
        try:
            group = grouped.indices[expandable_type]
        except KeyError:
            continue
        for row in dataframe.iloc[group].itertuples(index=False):
            left = row._asdict()
            del left["type"]
            value = left.pop("value")
            left = pd.DataFrame(value.shape[0] * [left])
            expanded.append(pd.concat((left, value), axis="columns"))
    for non_expandable_type in VALUE_TYPES - RANK_1_TYPES - rank_n_types:
        try:
            group = grouped.indices[non_expandable_type]
        except KeyError:
            continue
        non_expanded = dataframe.iloc[group]
        expanded.append(non_expanded.drop(columns=["type"]))
    expanded = pd.concat(expanded, ignore_index=True)
    if expanded.columns.get_loc("value") == expanded.shape[1] - 1:
        return expanded
    y_column = expanded.pop("value")
    return pd.concat((expanded, y_column), axis="columns")


def expand_real_value(x):
    if isinstance(x, pyarrow.RecordBatch):
        return x.to_pandas()
    return x


def fetch_as_dataframe(db_map, value_sq, fetched_maps):
    """Fetches parameter values from database returning them as dataframe.

    Args:
        db_map (DatabaseMapping): database map
        value_sq (Subquery): parameter value subquery
        fetched_maps (FetchedMaps): extra data needed to construct the dataframe

    Returns:
        pd.DataFrame: value and all its dimensions in a dataframe
    """
    dataframe = pd.DataFrame(db_map.query(value_sq))
    if dataframe.empty:
        return dataframe
    value_series = dataframe.apply(
        convert_values_from_database, axis="columns", result_type="expand", args=(fetched_maps.list_value_map,)
    )
    dataframe = dataframe.drop(columns=["value", "type", "list_value_id"])
    dataframe = pd.concat((dataframe, value_series), axis="columns")
    dataframe = resolve_elements(
        dataframe,
        fetched_maps.entity_class_name_map,
        fetched_maps.entity_name_and_class_map,
        fetched_maps.entity_element_map,
    )
    return expand_values(dataframe)
