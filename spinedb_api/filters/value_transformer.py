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
Provides a database query manipulator that applies mathematical transformations to parameter values.

:author: A. Soininen
:date:   20.5.2021
"""
import json
from functools import partial

from sqlalchemy import case
from sqlalchemy.sql.expression import label
from ..parameter_value import TRANSFORM_TAG, VALUE_TRANSFORMS

VALUE_TRANSFORMER_TYPE = "value_transformer"
VALUE_TRANSFORMER_SHORTHAND_TAG = "value_transform"


def apply_value_transform_to_parameter_value_sq(db_map, instructions):
    """
    Applies renaming to parameter definition subquery.

    Args:
        db_map (DatabaseMappingBase): a database map
        instructions (dict): mapping from entity class name to mapping from parameter name to list of
            instructions
    """
    state = _ValueTransformerState(db_map, instructions)
    transform = partial(_make_parameter_value_transforming_sq, state=state)
    db_map.override_parameter_value_sq_maker(transform)


def value_transformer_config(instructions):
    """
    Creates a config dict for transformer.

    Args:
        instructions (dict): mapping from entity class name to mapping from parameter name to list of
            instructions

    Returns:
        dict: transformer configuration
    """
    return {"type": VALUE_TRANSFORMER_TYPE, "instructions": instructions}


def value_transformer_from_dict(db_map, config):
    """
    Applies value transformer manipulator to given database map.

    Args:
        db_map (DatabaseMappingBase): target database map
        config (dict): transformer configuration
    """
    apply_value_transform_to_parameter_value_sq(db_map, config["instructions"])


def value_transformer_config_to_shorthand(config):
    """
    Makes a shorthand string from transformer configuration.

    Args:
        config (dict): transformer configuration

    Returns:
        str: a shorthand string
    """
    shorthand = ""
    instructions = config["instructions"]
    for class_name, param_instructions in instructions.items():
        for param_name, instruction_list in param_instructions.items():
            shorthand = shorthand + ":" + class_name
            shorthand = shorthand + ":" + param_name
            for instruction in instruction_list:
                shorthand = shorthand + ":" + instruction["operation"]
                for key, value in instruction.items():
                    if key == "operation":
                        continue
                    shorthand = shorthand + ":" + key + ":" + str(value)
                shorthand = shorthand + ":end"
    return VALUE_TRANSFORMER_SHORTHAND_TAG + shorthand


def value_transformer_shorthand_to_config(shorthand):
    """
    Makes configuration dictionary out of a shorthand string.

    Args:
        shorthand (str): a shorthand string

    Returns:
        dict: value transformer configuration
    """
    tokens = shorthand.split(":")[1:]
    instructions = dict()
    while tokens:
        class_name = tokens.pop(0)
        param_name = tokens.pop(0)
        instruction = {"operation": tokens.pop(0)}
        while True:
            key = tokens.pop(0)
            if key == "end":
                break
            instruction[key] = float(tokens.pop(0))
        instructions.setdefault(class_name, {}).setdefault(param_name, []).append(instruction)
    return value_transformer_config(instructions)


class _ValueTransformerState:
    def __init__(self, db_map, instructions):
        """
        Args:
            db_map (DatabaseMappingBase): a database map
            instructions (dict): mapping from entity class name to mapping from parameter name to list of
                instructions
        """
        self.original_parameter_value_sq = db_map.parameter_value_sq
        self.instructions = {
            id_: (
                bytes(TRANSFORM_TAG + json.dumps(param_instructions) + TRANSFORM_TAG, "UTF8") if instructions else None
            )
            for id_, param_instructions in self._ids(db_map, instructions).items()
        }

    @staticmethod
    def _ids(db_map, instructions):
        """Searches the database for applicable parameter definition ids.

        Args:
            db_map (DatabaseMappingBase): a database map
            instructions (dict): mapping from entity class name to mapping from parameter name to list of
                instructions

        Returns:
            dict: mapping from parameter definition ids to list of instructions
        """
        class_names = set(instructions.keys())
        param_names = set(name for class_instructions in instructions.values() for name in class_instructions)
        id_to_names = {
            (definition_row.entity_class_name, definition_row.parameter_name): definition_row.id
            for definition_row in db_map.query(db_map.entity_parameter_definition_sq).filter(
                db_map.entity_parameter_definition_sq.c.entity_class_name.in_(class_names)
                & db_map.entity_parameter_definition_sq.c.parameter_name.in_(param_names)
            )
        }
        return {
            id_: instructions[path[0]][path[1]] for path, id_ in id_to_names.items() if path[1] in instructions[path[0]]
        }


def _make_parameter_value_transforming_sq(db_map, state):
    """
    Returns subquery which applies transformations to parameter values.

    Args:
        db_map (DatabaseMappingBase): a database map
        state (_ValueTransformerState): state

    Returns:
        Alias: a value transforming parameter value subquery
    """
    subquery = state.original_parameter_value_sq
    if not state.instructions:
        return subquery
    cases = [
        (subquery.c.parameter_definition_id == id_, instructions + subquery.c.value)
        for id_, instructions in state.instructions.items()
    ]
    new_parameter_value = case(cases, else_=subquery.c.value)

    object_class_case = case(
        [(db_map.entity_class_sq.c.type_id == db_map.object_class_type, subquery.c.entity_class_id)], else_=None
    )
    rel_class_case = case(
        [(db_map.entity_class_sq.c.type_id == db_map.relationship_class_type, subquery.c.entity_class_id)], else_=None
    )
    object_entity_case = case(
        [(db_map.entity_sq.c.type_id == db_map.object_entity_type, subquery.c.entity_id)], else_=None
    )
    rel_entity_case = case(
        [(db_map.entity_sq.c.type_id == db_map.relationship_entity_type, subquery.c.entity_id)], else_=None
    )
    parameter_value_sq = (
        db_map.query(
            subquery.c.id.label("id"),
            subquery.c.parameter_definition_id,
            subquery.c.entity_class_id,
            subquery.c.entity_id,
            label("object_class_id", object_class_case),
            label("relationship_class_id", rel_class_case),
            label("object_id", object_entity_case),
            label("relationship_id", rel_entity_case),
            new_parameter_value.label("value"),
            subquery.c.commit_id.label("commit_id"),
            subquery.c.alternative_id,
        )
        .join(db_map.entity_sq, db_map.entity_sq.c.id == subquery.c.entity_id)
        .join(db_map.entity_class_sq, db_map.entity_class_sq.c.id == subquery.c.entity_class_id)
        .subquery()
    )
    return parameter_value_sq
