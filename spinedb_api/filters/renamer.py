######################################################################################################################
# Copyright (C) 2017 - 2020 Spine project consortium
# This file is part of Spine Database API.
# Spine Database API is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser
# General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

"""
Provides a database query manipulator that renames database items.

:author: A. Soininen
:date:   2.10.2020
"""
from functools import partial
from sqlalchemy import case


def apply_renaming_to_entity_class_sq(db_map, name_map):
    """
    Applies renaming to entity class subquery.

    Args:
        db_map (DatabaseMappingBase): a database map
        name_map (dict): a map from old name to new name
    """
    state = _EntityClassRenamerState(db_map, name_map)
    renaming = partial(_make_renaming_entity_class_sq, state=state)
    db_map.override_entity_class_sq_maker(renaming)


class _EntityClassRenamerState:
    def __init__(self, db_map, name_map):
        """
        Args:
            db_map (DatabaseMappingBase): a database map
            name_map (dict): a mapping from original name to a new name.
        """
        self.id_to_name = self._ids(db_map, name_map)
        self.original_entity_class_sq = db_map.entity_class_sq

    @staticmethod
    def _ids(db_map, name_map):
        """
        Args:
            db_map (DatabaseMappingBase): a database map
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
        db_map (DatabaseMappingBase): a database map
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
        subquery.c.type_id,
        new_class_name.label("name"),
        subquery.c.description,
        subquery.c.display_order,
        subquery.c.display_icon,
        subquery.c.hidden,
        subquery.c.commit_id,
    ).subquery()
    return entity_class_sq
