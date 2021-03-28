######################################################################################################################
# Copyright (C) 2017-2021 Spine project consortium
# This file is part of Spine Database API.
# Spine Database API is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser
# General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

"""
Framework for exporting a database to Excel file.

:author: P. Vennström (VTT), A. Soininen (VTT), M. Marin (KTH)
:date:   31.1.2020
"""

from spinedb_api.export_mapping.export_mapping import (
    Position,
    ExportKey,
    AlternativeMapping,
    AlternativeDescriptionMapping,
    ObjectClassMapping,
    ObjectGroupMapping,
    ObjectMapping,
    FixedValueMapping,
    ScenarioMapping,
    ScenarioAlternativeMapping,
    ScenarioBeforeAlternativeMapping,
    ScenarioDescriptionMapping,
    ParameterDefinitionMapping,
    ParameterValueIndexMapping,
    ParameterValueTypeMapping,
    ParameterValueMapping,
    ExpandedParameterValueMapping,
    RelationshipClassMapping,
    RelationshipMapping,
    RelationshipObjectMapping,
)
from .excel_writer import ExcelWriter
from .writer import write

# FIXME: Use multiple sheets if data doesn't fit

MAX_MAP_DIM_COUNT = 3


class ExcelWriterWithPreamble(ExcelWriter):
    _preamble = {}

    def start_table(self, table_name, title_key):
        """See base class."""
        if super().start_table(table_name, title_key):
            self._preamble = self._make_preamble(table_name, title_key)
            return True
        return False

    @staticmethod
    def _make_preamble(table_name, title_key):
        if table_name in ("alternative", "scenario", "scenario_alternative"):
            return {"sheet_type": table_name}
        class_row = title_key[ExportKey.CLASS_ROW_CACHE]
        class_name = class_row.name
        if table_name.endswith(",group"):
            return {"sheet_type": "object_group", "class_name": class_name}
        object_class_name_list = title_key.get(ExportKey.OBJECT_CLASS_NAME_LIST)
        if object_class_name_list is None:
            entity_type = "object"
            entity_dim_count = 1
        else:
            entity_type = "relationship"
            entity_dim_count = len(object_class_name_list)
        preamble = {
            "sheet_type": "entity",
            "entity_type": entity_type,
            "class_name": class_name,
            "entity_dim_count": entity_dim_count,
        }
        value_type = title_key.get(ExportKey.PARAMETER_VALUE_TYPE)
        if value_type is not None:
            preamble["value_type"] = value_type.type_
            preamble["index_dim_count"] = value_type.dimension_count
        return preamble

    def _create_current_sheet(self):
        super()._create_current_sheet()
        if not self._preamble:
            return
        for row in self._preamble.items():
            self._current_sheet.append(row)
        self._current_sheet.append([])


def export_spine_database_to_xlsx(db_map, filepath):
    """Writes data from a Spine database into an excel file.

    Args:
        db_map (spinedb_api.DatabaseMapping): database mapping.
        filepath (str): destination path.
    """
    mappings = []
    mappings.append(_make_alternative_mapping())
    mappings.append(_make_scenario_mapping())
    mappings.append(_make_scenario_alternative_mapping())
    mappings.extend(_make_object_group_mappings(db_map))
    mappings.extend(_make_parameter_value_mappings(db_map))
    writer = ExcelWriterWithPreamble(filepath)
    write(db_map, writer, *mappings)


def _make_alternative_mapping():
    root_mapping = FixedValueMapping(Position.table_name, value="alternative")
    alternative_mapping = root_mapping.child = AlternativeMapping(0, header="alternative")
    alternative_mapping.child = AlternativeDescriptionMapping(1, header="description")
    return root_mapping


def _make_scenario_mapping():
    root_mapping = FixedValueMapping(Position.table_name, value="scenario")
    scenario_mapping = root_mapping.child = ScenarioMapping(0, header="scenario")
    scenario_mapping.child = ScenarioDescriptionMapping(1, header="description")
    return root_mapping


def _make_scenario_alternative_mapping():
    root_mapping = FixedValueMapping(Position.table_name, value="scenario_alternative")
    scenario_mapping = root_mapping.child = ScenarioMapping(0, header="scenario")
    alternative_mapping = scenario_mapping.child = ScenarioAlternativeMapping(1, header="alternative")
    alternative_mapping.child = ScenarioBeforeAlternativeMapping(2, header="before alternative")
    return root_mapping


def _make_object_group_mappings(db_map):
    for obj_cls in db_map.query(db_map.object_class_sq):
        root_mapping = ObjectClassMapping(Position.table_name, filter_re=obj_cls.name)
        group_mapping = root_mapping.child = FixedValueMapping(Position.table_name, value="group")
        object_mapping = group_mapping.child = ObjectMapping(1, header="member")
        object_mapping.child = ObjectGroupMapping(0, header="group")
        yield root_mapping


def _make_scalar_parameter_value_mapping(alt_pos=1):
    alternative_mapping = AlternativeMapping(alt_pos, header="alternative")
    param_def_mapping = alternative_mapping.child = ParameterDefinitionMapping(-1)
    type_mapping = param_def_mapping.child = ParameterValueTypeMapping(Position.table_name, filter_re="single_value")
    type_mapping.child = ParameterValueMapping(420)
    return alternative_mapping


def _make_indexed_parameter_value_mapping(alt_pos=-2, filter_re="array|time_pattern|time_series", dim_count=1):
    alternative_mapping = AlternativeMapping(alt_pos, header="alternative")
    param_def_mapping = alternative_mapping.child = ParameterDefinitionMapping(alt_pos - 1)
    type_mapping = param_def_mapping.child = ParameterValueTypeMapping(Position.table_name, filter_re=filter_re)
    parent_mapping = type_mapping
    for k in range(dim_count):
        index_mapping = parent_mapping.child = ParameterValueIndexMapping(k, header="index")
        index_mapping.set_ignorable()
        parent_mapping = index_mapping
    parent_mapping.child = ExpandedParameterValueMapping(420)
    return alternative_mapping


def _make_object_mapping(object_class, pivoted=False):
    root_mapping = ObjectClassMapping(Position.table_name, filter_re=f"^{object_class.name}$", group_fn="one_or_none")
    pos = 0 if not pivoted else -1
    root_mapping.child = ObjectMapping(pos, header=object_class.name)
    return root_mapping


def _make_object_scalar_parameter_value_mapping(object_class):
    root_mapping = _make_object_mapping(object_class)
    object_mapping = root_mapping.child
    object_mapping.child = _make_scalar_parameter_value_mapping(alt_pos=1)
    return root_mapping


def _make_object_indexed_parameter_value_mapping(object_class):
    root_mapping = _make_object_mapping(object_class, pivoted=True)
    object_mapping = root_mapping.child
    object_mapping.child = _make_indexed_parameter_value_mapping(alt_pos=-2)
    return root_mapping


def _make_object_map_parameter_value_mapping(object_class, dim_count):
    root_mapping = _make_object_mapping(object_class, pivoted=True)
    object_mapping = root_mapping.child
    filter_re = f"{dim_count}d_map"
    object_mapping.child = _make_indexed_parameter_value_mapping(alt_pos=-2, filter_re=filter_re, dim_count=dim_count)
    return root_mapping


def _make_relationship_mapping(relationship_class, pivoted=False):
    root_mapping = RelationshipClassMapping(
        Position.table_name, filter_re=f"^{relationship_class.name}$", group_fn="one_or_none"
    )
    relationship_mapping = root_mapping.child = RelationshipMapping(Position.hidden)
    parent_mapping = relationship_mapping
    for d, class_name in enumerate(relationship_class.object_class_name_list.split(",")):
        if pivoted:
            d = -(d + 1)
        object_mapping = parent_mapping.child = RelationshipObjectMapping(d, header=class_name)
        parent_mapping = object_mapping
    return root_mapping


def _make_relationship_scalar_parameter_value_mapping(relationship_class):
    root_mapping = _make_relationship_mapping(relationship_class)
    parent_mapping = root_mapping.flatten()[-1]
    d = len(relationship_class.object_class_name_list.split(","))
    parent_mapping.child = _make_scalar_parameter_value_mapping(alt_pos=d)
    return root_mapping


def _make_relationship_indexed_parameter_value_mapping(relationship_class):
    root_mapping = _make_relationship_mapping(relationship_class, pivoted=True)
    parent_mapping = root_mapping.flatten()[-1]
    d = len(relationship_class.object_class_name_list.split(",")) + 1
    parent_mapping.child = _make_indexed_parameter_value_mapping(alt_pos=-d)
    return root_mapping


def _make_relationship_map_parameter_value_mapping(relationship_class, dim_count):
    root_mapping = _make_relationship_mapping(relationship_class, pivoted=True)
    parent_mapping = root_mapping.flatten()[-1]
    d = len(relationship_class.object_class_name_list.split(",")) + 1
    filter_re = f"{dim_count}d_map"
    parent_mapping.child = _make_indexed_parameter_value_mapping(alt_pos=-d, filter_re=filter_re, dim_count=dim_count)
    return root_mapping


def _make_parameter_value_mappings(db_map):
    for object_class in db_map.query(db_map.object_class_sq):
        yield _make_object_mapping(object_class)
        yield _make_object_scalar_parameter_value_mapping(object_class)
        yield _make_object_indexed_parameter_value_mapping(object_class)
        for k in range(1, MAX_MAP_DIM_COUNT + 1):
            yield _make_object_map_parameter_value_mapping(object_class, k)
    for relationship_class in db_map.query(db_map.wide_relationship_class_sq):
        yield _make_relationship_mapping(relationship_class)
        yield _make_relationship_scalar_parameter_value_mapping(relationship_class)
        yield _make_relationship_indexed_parameter_value_mapping(relationship_class)
        for k in range(1, MAX_MAP_DIM_COUNT + 1):
            yield _make_relationship_map_parameter_value_mapping(relationship_class, k)
