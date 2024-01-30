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
""" Provides a database query manipulator that renames database items. """
from functools import partial
from sqlalchemy import case


ENTITY_CLASS_RENAMER_TYPE = "entity_class_renamer"
ENTITY_CLASS_RENAMER_SHORTHAND_TAG = "entity_class_rename"
PARAMETER_RENAMER_TYPE = "parameter_renamer"
PARAMETER_RENAMER_SHORTHAND_TAG = "parameter_rename"


def apply_renaming_to_entity_class_sq(db_map, name_map):
    """
    Applies renaming to entity class subquery.

    Args:
        db_map (DatabaseMapping): a database map
        name_map (dict): a map from old name to new name
    """
    state = _EntityClassRenamerState(db_map, name_map)
    renaming = partial(_make_renaming_entity_class_sq, state=state)
    db_map.override_entity_class_sq_maker(renaming)


def entity_class_renamer_config(**renames):
    """
    Creates a config dict for renamer.

    Args:
        **renames: keyword is the old name, value is the new name

    Returns:
        dict: renamer configuration
    """
    return {"type": ENTITY_CLASS_RENAMER_TYPE, "name_map": dict(renames)}


def entity_class_renamer_from_dict(db_map, config):
    """
    Applies entity class renamer manipulator to given database map.

    Args:
        db_map (DatabaseMapping): target database map
        config (dict): renamer configuration
    """
    apply_renaming_to_entity_class_sq(db_map, config["name_map"])


def entity_class_renamer_config_to_shorthand(config):
    """
    Makes a shorthand string from renamer configuration.

    Args:
        config (dict): renamer configuration

    Returns:
        str: a shorthand string
    """
    shorthand = ""
    for old_name, new_name in config["name_map"].items():
        shorthand = shorthand + ":" + old_name + ":" + new_name
    return ENTITY_CLASS_RENAMER_SHORTHAND_TAG + shorthand


def entity_class_renamer_shorthand_to_config(shorthand):
    """
    Makes configuration dictionary out of a shorthand string.

    Args:
        shorthand (str): a shorthand string

    Returns:
        dict: renamer configuration
    """
    names = shorthand.split(":")
    name_map = {}
    for old_name, new_name in zip(names[1::2], names[2::2]):
        name_map[old_name] = new_name
    return entity_class_renamer_config(**name_map)


def apply_renaming_to_parameter_definition_sq(db_map, name_map):
    """
    Applies renaming to parameter definition subquery.

    Args:
        db_map (DatabaseMapping): a database map
        name_map (dict): a map from old name to new name
    """
    state = _ParameterRenamerState(db_map, name_map)
    renaming = partial(_make_renaming_parameter_definition_sq, state=state)
    db_map.override_parameter_definition_sq_maker(renaming)


def parameter_renamer_config(renames):
    """
    Creates a config dict for renamer.

    Args:
        renames (dict): mapping from entity class name to mapping from parameter name to new name

    Returns:
        dict: renamer configuration
    """
    return {"type": PARAMETER_RENAMER_TYPE, "name_map": renames}


def parameter_renamer_from_dict(db_map, config):
    """
    Applies parameter renamer manipulator to given database map.

    Args:
        db_map (DatabaseMapping): target database map
        config (dict): renamer configuration
    """
    apply_renaming_to_parameter_definition_sq(db_map, config["name_map"])


def parameter_renamer_config_to_shorthand(config):
    """
    Makes a shorthand string from renamer configuration.

    Args:
        config (dict): renamer configuration

    Returns:
        str: a shorthand string
    """
    shorthand = ""
    for class_name, renaming in config["name_map"].items():
        for old_name, new_name in renaming.items():
            shorthand = shorthand + ":" + class_name + ":" + old_name + ":" + new_name
    return PARAMETER_RENAMER_SHORTHAND_TAG + shorthand


def parameter_renamer_shorthand_to_config(shorthand):
    """
    Makes configuration dictionary out of a shorthand string.

    Args:
        shorthand (str): a shorthand string

    Returns:
        dict: renamer configuration
    """
    names = shorthand.split(":")
    name_map = {}
    for class_name, old_name, new_name in zip(names[1::3], names[2::3], names[3::3]):
        name_map.setdefault(class_name, {})[old_name] = new_name
    return parameter_renamer_config(name_map)


class _EntityClassRenamerState:
    def __init__(self, db_map, name_map):
        """
        Args:
            db_map (DatabaseMapping): a database map
            name_map (dict): a mapping from original name to a new name.
        """
        name_map = {old: new for old, new in name_map.items() if old != new}
        self.id_to_name = self._ids(db_map, name_map)
        self.original_entity_class_sq = db_map.entity_class_sq

    @staticmethod
    def _ids(db_map, name_map):
        """
        Args:
            db_map (DatabaseMapping): a database map
            name_map (dict): a mapping from original name to a new name

        Returns:
            dict: a mapping from entity class id to a new name
        """
        names = set(name_map.keys())
        return {
            class_row.id: name_map[class_row.name]
            for class_row in db_map.query(db_map.entity_class_sq).filter(db_map.entity_class_sq.c.name.in_(names)).all()
        }


def _make_renaming_entity_class_sq(db_map, state):
    """
    Returns an entity class subquery which renames classes.

    Args:
        db_map (DatabaseMapping): a database map
        state (_EntityClassRenamerState):

    Returns:
        Alias: a renaming entity class subquery
    """
    subquery = state.original_entity_class_sq
    if not state.id_to_name:
        return subquery
    cases = [(subquery.c.id == id, new_name) for id, new_name in state.id_to_name.items()]
    new_class_name = case(cases, else_=subquery.c.name)  # if not in the name map, just keep the original name
    entity_class_sq = db_map.query(
        subquery.c.id,
        new_class_name.label("name"),
        subquery.c.description,
        subquery.c.display_order,
        subquery.c.display_icon,
        subquery.c.hidden,
        subquery.c.active_by_default,
    ).subquery()
    return entity_class_sq


class _ParameterRenamerState:
    def __init__(self, db_map, name_map):
        """
        Args:
            db_map (DatabaseMapping): a database map
            name_map (dict): mapping from entity class name to mapping from parameter name to new name
        """
        self.id_to_name = self._ids(db_map, name_map)
        self.original_parameter_definition_sq = db_map.parameter_definition_sq

    @staticmethod
    def _ids(db_map, name_map):
        """
        Args:
            db_map (DatabaseMapping): a database map
            name_map (dict): a mapping from original name to a new name

        Returns:
            dict: a mapping from entity class id to a new name
        """
        class_names = set(name_map.keys())
        param_names = set(old_name for renaming in name_map.values() for old_name in renaming)
        id_to_names = {
            (definition_row.entity_class_name, definition_row.parameter_name): definition_row.id
            for definition_row in db_map.query(db_map.entity_parameter_definition_sq).filter(
                db_map.entity_parameter_definition_sq.c.entity_class_name.in_(class_names)
                & db_map.entity_parameter_definition_sq.c.parameter_name.in_(param_names)
            )
        }
        return {id_: name_map[path[0]][path[1]] for path, id_ in id_to_names.items() if path[1] in name_map[path[0]]}


def _make_renaming_parameter_definition_sq(db_map, state):
    """
    Returns an entity class subquery which renames parameters.

    Args:
        db_map (DatabaseMapping): a database map
        state (_ParameterRenamerState):

    Returns:
        Alias: a renaming parameter definition subquery
    """
    subquery = state.original_parameter_definition_sq
    if not state.id_to_name:
        return subquery
    cases = [(subquery.c.id == id, new_name) for id, new_name in state.id_to_name.items()]
    new_parameter_name = case(cases, else_=subquery.c.name)  # if not in the name map, just keep the original name
    parameter_definition_sq = db_map.query(
        subquery.c.id,
        new_parameter_name.label("name"),
        subquery.c.description,
        subquery.c.entity_class_id,
        subquery.c.default_value,
        subquery.c.default_type,
        subquery.c.list_value_id,
        subquery.c.commit_id,
        subquery.c.parameter_value_list_id,
    ).subquery()
    return parameter_definition_sq
