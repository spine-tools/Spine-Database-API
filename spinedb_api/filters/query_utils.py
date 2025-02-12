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
from sqlalchemy import and_, func, or_


def filter_by_active_elements(db_map, query, ext_entity_sq):
    """Applies a filter to given subquery that drops incomplete multidimensional entities.

    'Incomplete' means entities that have elements that are inactive,
    i.e. are filtered out because entity alternative/active_by_default is set to False.

    Args:
        db_map (DatabaseMapping): database map
        query (Query): query to apply the filter to
        ext_entity_sq (Alias): extended entity subquery

    Returns:
        Alias: filtered subquery
    """
    ext_entity_element_count_sq = (
        db_map.query(
            db_map.entity_element_sq.c.entity_id,
            func.count(db_map.entity_element_sq.c.element_id).label("element_count"),
        )
        .group_by(db_map.entity_element_sq.c.entity_id)
        .subquery()
    )
    ext_entity_class_dimension_count_sq = (
        db_map.query(
            db_map.entity_class_dimension_sq.c.entity_class_id,
            func.count(db_map.entity_class_dimension_sq.c.dimension_id).label("dimension_count"),
        )
        .group_by(db_map.entity_class_dimension_sq.c.entity_class_id)
        .subquery()
    )
    return (
        query.outerjoin(
            ext_entity_element_count_sq,
            ext_entity_element_count_sq.c.entity_id == ext_entity_sq.c.id,
        )
        .outerjoin(
            ext_entity_class_dimension_count_sq,
            ext_entity_class_dimension_count_sq.c.entity_class_id == ext_entity_sq.c.class_id,
        )
        .filter(
            or_(
                and_(
                    ext_entity_element_count_sq.c.element_count == None,
                    ext_entity_class_dimension_count_sq.c.dimension_count == None,
                ),
                ext_entity_element_count_sq.c.element_count == ext_entity_class_dimension_count_sq.c.dimension_count,
            )
        )
    )
