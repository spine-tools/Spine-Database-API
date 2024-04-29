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

"""
Provides a database query manipulator that applies mathematical transformations to parameter values.

"""
from functools import partial
from numbers import Number
from sqlalchemy import case, literal, Integer, LargeBinary, String
from sqlalchemy.sql.expression import select, cast, union_all

from ..exception import SpineDBAPIError
from ..helpers import LONGTEXT_LENGTH
from ..parameter_value import from_database, IndexedValue, to_database, Map

VALUE_TRANSFORMER_TYPE = "value_transformer"
VALUE_TRANSFORMER_SHORTHAND_TAG = "value_transform"


def apply_value_transform_to_parameter_value_sq(db_map, instructions):
    """
    Applies renaming to parameter definition subquery.

    Args:
        db_map (DatabaseMapping): a database map
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
        db_map (DatabaseMapping): target database map
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
            for instruction in instruction_list:
                shorthand = shorthand + ":" + class_name
                shorthand = shorthand + ":" + param_name
                operation = instruction["operation"]
                shorthand = shorthand + ":" + operation
                if operation == "multiply":
                    shorthand = shorthand + ":" + str(instruction["rhs"])
                elif operation == "generate_index":
                    shorthand = shorthand + ":" + instruction["expression"]
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
        operation = tokens.pop(0)
        instruction = {"operation": operation}
        if operation == "multiply":
            instruction["rhs"] = float(tokens.pop(0))
        elif operation == "generate_index":
            instruction["expression"] = tokens.pop(0)
        instructions.setdefault(class_name, {}).setdefault(param_name, []).append(instruction)
    return value_transformer_config(instructions)


class _ValueTransformerState:
    def __init__(self, db_map, instructions):
        """
        Args:
            db_map (DatabaseMapping): a database map
            instructions (dict): mapping from entity class name to parameter name to list of instructions
        """
        self.original_parameter_value_sq = db_map.parameter_value_sq
        self.transformed = self._transform(db_map, instructions)

    @staticmethod
    def _transform(db_map, instructions):
        """Transforms applicable parameter values for caching.

        Args:
            db_map (DatabaseMapping): a database map
            instructions (dict): mapping from entity class name to parameter name to list of instructions

        Returns:
            dict: mapping from parameter value ids to transformed values
        """
        class_names = set(instructions.keys())
        param_names = set(name for class_instructions in instructions.values() for name in class_instructions)
        definition_ids = {
            definition_row.id
            for definition_row in db_map.query(db_map.entity_parameter_definition_sq).filter(
                db_map.entity_parameter_definition_sq.c.entity_class_name.in_(class_names)
                & db_map.entity_parameter_definition_sq.c.parameter_name.in_(param_names)
            )
        }
        transformed = dict()
        for value_row in db_map.query(db_map.entity_parameter_value_sq).filter(
            db_map.entity_parameter_value_sq.c.parameter_id.in_(definition_ids)
        ):
            # definition_ids may contain class-parameter name combinations that don't exist in instructions.
            param_instructions = instructions[value_row.entity_class_name].get(value_row.parameter_name)
            if param_instructions is not None:
                transformed[value_row.id] = to_database(
                    _transform(from_database(value_row.value, value_row.type), param_instructions)
                )
        return transformed


def _make_parameter_value_transforming_sq(db_map, state):
    """
    Returns subquery which applies transformations to parameter values.

    Args:
        db_map (DatabaseMapping): a database map
        state (_ValueTransformerState): state

    Returns:
        Alias: a value transforming parameter value subquery
    """
    subquery = state.original_parameter_value_sq
    if not state.transformed:
        return subquery
    transformed_rows = [(id_, value, type_) for id_, (value, type_) in state.transformed.items()]
    # Little optimization: SqlAlchemy can infer types from the first row, so we need to use cast only on that.
    statements = [
        select(
            [
                cast(literal(transformed_rows[0][0]), Integer()).label("id"),
                cast(literal(transformed_rows[0][1]), LargeBinary(LONGTEXT_LENGTH)).label("transformed_value"),
                cast(literal(transformed_rows[0][2]), String()).label("transformed_type"),
            ]
        )
    ]
    statements += [select([literal(i), literal(v), literal(t)]) for i, v, t in transformed_rows[1:]]
    temp_sq = union_all(*statements).alias("transformed_values")
    new_value = case([(temp_sq.c.transformed_value != None, temp_sq.c.transformed_value)], else_=subquery.c.value)
    new_type = case([(temp_sq.c.transformed_type != None, temp_sq.c.transformed_type)], else_=subquery.c.type)
    parameter_value_sq = (
        db_map.query(
            subquery.c.id.label("id"),
            subquery.c.parameter_definition_id,
            subquery.c.entity_class_id,
            subquery.c.entity_id,
            new_value.label("value"),
            new_type.label("type"),
            subquery.c.list_value_id,
            subquery.c.alternative_id,
            subquery.c.commit_id.label("commit_id"),
        )
        .join(temp_sq, subquery.c.id == temp_sq.c.id, isouter=True)
        .join(db_map.entity_sq, db_map.entity_sq.c.id == subquery.c.entity_id)
        .join(db_map.entity_class_sq, db_map.entity_class_sq.c.id == subquery.c.entity_class_id)
        .subquery()
    )
    return parameter_value_sq


def _transform(value, instructions):
    """Transforms a value according to instructions.

    Args:
        value (Any): value to transform
        instructions (list of dict): transformation instructions

    Returns:
        Any: transformed value
    """

    for instruction in instructions:
        operation = instruction["operation"]
        value = _VALUE_TRANSFORMS[operation](value, instruction)
    return value


def _negate(value, instruction):
    """Negates a value.

    Args:
        value (Any): value to negate
        instruction (dict): instruction for the operation

    Returns:
        Any: negated value
    """
    if isinstance(value, Number):
        return -value
    if isinstance(value, IndexedValue):
        for i, element in enumerate(value.values):
            value.values[i] = _negate(element, instruction)
        return value
    return value


def _invert(value, instruction):
    """Calculates the reciprocal of a value.

    Args:
        value (Any): value to invert
        instruction (dict): instruction for the operation

    Returns:
        Any: reciprocal of value
    """
    if isinstance(value, Number):
        return 1.0 / value
    if isinstance(value, IndexedValue):
        for i, element in enumerate(value.values):
            value.values[i] = _invert(element, instruction)
        return value
    return value


def _multiply(value, instruction):
    """Multiplies a value.

    Args:
        value (Any): value to multiply
        instruction (dict): instruction for the operation

    Returns:
        Any: multiplied value
    """
    if isinstance(value, Number):
        return instruction["rhs"] * value
    if isinstance(value, IndexedValue):
        for i, element in enumerate(value.values):
            value.values[i] = _multiply(element, instruction)
        return value
    return value


def _generate_index(value, instruction):
    """Converts value to Map and generates new indexes for the first dimension.

    Args:
        value (IndexedValue): value to modify
        instruction (dict): rename instruction

    Returns:
        Map or Any: modified value or value as-is if it does not have indexes
    """
    if not isinstance(value, IndexedValue):
        return value
    if len(value) == 0:
        return Map([], [], str)
    try:
        compiled = compile(instruction["expression"], "<string>", "eval")
    except (SyntaxError, ValueError):
        raise SpineDBAPIError("Failed to compile index generator expression.")
    generate_index = partial(eval, compiled, {})
    try:
        indexes = [generate_index({"i": i}) for i in range(1, len(value) + 1)]  # pylint: disable=eval-used
    except (AttributeError, NameError, ValueError):
        raise SpineDBAPIError("Failed to evaluate index generator expression.")
    if len(indexes) != len(set(indexes)):
        raise SpineDBAPIError(f"Expression '{instruction['expression']}' does not generate unique indexes.")
    return Map(indexes, value.values)


_VALUE_TRANSFORMS = {"generate_index": _generate_index, "invert": _invert, "multiply": _multiply, "negate": _negate}
