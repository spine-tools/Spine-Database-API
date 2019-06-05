#############################################################################
# Copyright (C) 2017 - 2018 VTT Technical Research Centre of Finland
#
# This file is part of Spine Database API.
#
# Spine Spine Database API is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#############################################################################

"""Provides :class:`.DatabaseMappingQueryMixin`.

:author: Manuel Marin (KTH)
:date:   11.8.2018
"""
# TODO: Consider returning lists (by callling `all()` on the resulting query)
# TODO: Maybe handle errors in queries
# TODO: Improve docstrings

from sqlalchemy import false, distinct, func, or_


class DatabaseMappingQueryMixin:
    """Provides methods to perform standard queries (SELECT statements) on a Spine db.
    """

    def __init__(self, *args, **kwargs):
        """Initialize class."""
        super().__init__(*args, **kwargs)

    def object_class_list(self, id_list=None, ordered=True):
        """Return object classes ordered by display order."""
        qry = self.query(self.object_class_sq)
        if id_list is not None:
            qry = qry.filter(self.object_class_sq.c.id.in_(id_list))
        if ordered:
            qry = qry.order_by(self.object_class_sq.c.display_order)
        return qry

    def object_list(self, id_list=None, class_id=None):
        """Return objects, optionally filtered by class id."""
        qry = self.query(self.object_sq)
        if id_list is not None:
            qry = qry.filter(self.object_sq.c.id.in_(id_list))
        if class_id is not None:
            qry = qry.filter(self.object_sq.c.class_id == class_id)
        return qry

    def relationship_class_list(self, id=None, ordered=True):
        """Return all relationship classes optionally filtered by id."""
        qry = self.query(self.relationship_class_sq)
        if id is not None:
            qry = qry.filter(self.relationship_class_sq.c.id == id)
        if ordered:
            qry = qry.order_by(self.relationship_class_sq.c.id, self.relationship_class_sq.c.dimension)
        return qry

    def wide_relationship_class_list(self, id_list=None, object_class_id=None):
        """Return list of relationship classes in wide format involving a given object class."""
        qry = self.query(self.wide_relationship_class_sq)
        if id_list is not None:
            qry = qry.filter(self.wide_relationship_class_sq.c.id.in_(id_list))
        if object_class_id is not None:
            qry = qry.filter(
                or_(
                    self.wide_relationship_class_sq.c.object_class_id_list.like(f"%,{object_class_id},%"),
                    self.wide_relationship_class_sq.c.object_class_id_list.like(f"{object_class_id},%"),
                    self.wide_relationship_class_sq.c.object_class_id_list.like(f"%,{object_class_id}"),
                    self.wide_relationship_class_sq.c.object_class_id_list == object_class_id,
                )
            )
        return qry

    def relationship_list(self, id=None, ordered=True):
        """Return relationships, optionally filtered by id."""
        qry = self.query(self.relationship_sq)
        if id is not None:
            qry = qry.filter(self.relationship_sq.c.id == id)
        if ordered:
            qry = qry.order_by(self.relationship_sq.c.id, self.relationship_sq.c.dimension)
        return qry

    def wide_relationship_list(self, id_list=None, class_id=None, object_id=None):
        """Return list of relationships in wide format involving a given relationship class and object."""
        qry = self.query(self.wide_relationship_sq)
        if id_list is not None:
            qry = qry.filter(self.wide_relationship_sq.c.id.in_(id_list))
        if class_id is not None:
            qry = qry.filter(self.wide_relationship_sq.c.class_id == class_id)
        if object_id is not None:
            qry = qry.filter(
                or_(
                    self.wide_relationship_sq.c.object_id_list.like(f"%,{object_id},%"),
                    self.wide_relationship_sq.c.object_id_list.like(f"{object_id},%"),
                    self.wide_relationship_sq.c.object_id_list.like(f"%,{object_id}"),
                    self.wide_relationship_sq.c.object_id_list == object_id,
                )
            )
        return qry

    def parameter_definition_list(self, id_list=None, object_class_id=None, relationship_class_id=None):
        """Return parameter definitions."""
        qry = self.query(self.parameter_definition_sq)
        if id_list is not None:
            qry = qry.filter(self.parameter_definition_sq.c.id.in_(id_list))
        if object_class_id is not None:
            qry = qry.filter(self.parameter_definition_sq.c.object_class_id == object_class_id)
        if relationship_class_id is not None:
            qry = qry.filter(self.parameter_definition_sq.c.relationship_class_id == relationship_class_id)
        return qry

    def object_parameter_definition_list(self, object_class_id=None, parameter_definition_id=None):
        """Return object classes and their parameters."""
        qry = self.query(self.object_parameter_definition_sq)
        if object_class_id:
            qry = qry.filter(self.object_parameter_definition_sq.c.object_class_id == object_class_id)
        if parameter_definition_id:
            qry = qry.filter(self.object_parameter_definition_sq.c.id == parameter_id)
        return qry

    def relationship_parameter_definition_list(self, relationship_class_id=None, parameter_definition_id=None):
        """Return relationship classes and their parameters."""
        qry = self.query(self.relationship_parameter_definition_sq)
        if relationship_class_id:
            qry = qry.filter(self.relationship_parameter_definition_sq.c.relationship_class_id == relationship_class_id)
        if parameter_definition_id:
            qry = qry.filter(self.relationship_parameter_definition_sq.c.id == parameter_id)
        return qry

    def wide_object_parameter_definition_list(self, object_class_id_list=None, parameter_definition_id_list=None):
        """Return object classes and their parameter definitions in wide format."""
        qry = self.query(
            self.object_class_sq.c.id.label("object_class_id"),
            self.object_class_sq.c.name.label("object_class_name"),
            self.parameter_definition_sq.c.id.label("parameter_definition_id"),
            self.parameter_definition_sq.c.name.label("parameter_name"),
        ).filter(self.object_class_sq.c.id == self.parameter_definition_sq.c.object_class_id)
        if object_class_id_list is not None:
            qry = qry.filter(self.object_class_sq.c.id.in_(object_class_id_list))
        if parameter_definition_id_list is not None:
            qry = qry.filter(self.parameter_definition_sq.c.id.in_(parameter_definition_id_list))
        subqry = qry.subquery()
        return self.query(
            subqry.c.object_class_id,
            subqry.c.object_class_name,
            func.group_concat(subqry.c.parameter_definition_id).label("parameter_definition_id_list"),
            func.group_concat(subqry.c.parameter_name).label("parameter_name_list"),
        ).group_by(subqry.c.object_class_id, subqry.c.object_class_name)

    def wide_relationship_parameter_definition_list(
        self, relationship_class_id_list=None, parameter_definition_id_list=None
    ):
        """Return relationship classes and their parameter definitions in wide format."""
        qry = self.query(
            self.RelationshipClass.id.label("relationship_class_id"),
            self.RelationshipClass.name.label("relationship_class_name"),
            self.parameter_definition_sq.c.id.label("parameter_definition_id"),
            self.parameter_definition_sq.c.name.label("parameter_name"),
        ).filter(self.RelationshipClass.id == self.parameter_definition_sq.c.relationship_class_id)
        if relationship_class_id_list is not None:
            qry = qry.filter(self.RelationshipClass.id.in_(relationship_class_id_list))
        if parameter_definition_id_list is not None:
            qry = qry.filter(self.parameter_definition_sq.c.id.in_(parameter_definition_id_list))
        subqry = qry.subquery()
        return self.query(
            subqry.c.relationship_class_id,
            subqry.c.relationship_class_name,
            func.group_concat(subqry.c.parameter_definition_id).label("parameter_definition_id_list"),
            func.group_concat(subqry.c.parameter_name).label("parameter_name_list"),
        ).group_by(subqry.c.relationship_class_id, subqry.c.relationship_class_name)

    def parameter_value_list(self, id_list=None, object_id=None, relationship_id=None):
        """Return parameter values."""
        qry = self.query(self.parameter_value_sq)
        if id_list is not None:
            qry = qry.filter(self.parameter_value_sq.c.id.in_(id_list))
        if object_id:
            qry = qry.filter(self.parameter_value_sq.c.object_id == object_id)
        if relationship_id:
            qry = qry.filter(self.parameter_value_sq.c.relationship_id == relationship_id)
        return qry

    def object_parameter_value_list(self, parameter_name=None):
        """Return objects and their parameter values."""
        qry = self.query(self.object_parameter_value_sq)
        if parameter_name:
            qry = qry.filter(self.object_parameter_value_sq.c.parameter_name == parameter_name)
        return qry

    def relationship_parameter_value_list(self, parameter_name=None):
        """Return relationships and their parameter values."""
        qry = self.query(self.relationship_parameter_value_sq)
        if parameter_name:
            qry = qry.filter(self.relationship_parameter_value_sq.c.parameter_name == parameter_name)
        return qry

    def parameter_tag_list(self, id_list=None, tag_list=None):
        """Return list of parameter tags."""
        qry = self.query(self.parameter_tag_sq)
        if id_list is not None:
            qry = qry.filter(self.parameter_tag_sq.c.id.in_(id_list))
        if tag_list is not None:
            qry = qry.filter(self.parameter_tag_sq.c.tag.in_(tag_list))
        return qry

    def parameter_definition_tag_list(self, id_list=None):
        """Return list of parameter definition tags."""
        qry = self.query(self.parameter_definition_tag_sq)
        if id_list is not None:
            qry = qry.filter(self.parameter_definition_tag_sq.c.id.in_(id_list))
        return qry

    def wide_parameter_definition_tag_list(self, parameter_definition_id=None):
        """Return list of parameter tags in wide format for a given parameter definition."""
        qry = self.query(self.wide_parameter_definition_tag_sq)
        if parameter_definition_id:
            qry = qry.filter(self.wide_parameter_definition_tag_sq.c.parameter_definition_id == parameter_definition_id)
        return qry

    def wide_parameter_tag_definition_list(self, parameter_tag_id=None):
        """Return list of parameter tags (including the NULL tag) and their definitions in wide format.
        """
        qry = self.query(self.wide_parameter_tag_definition_sq)
        if parameter_tag_id:
            qry = qry.filter(self.wide_parameter_tag_definition_sq.c.parameter_tag_id == parameter_tag_id)
        return qry

    def parameter_value_list_list(self, id_list=None):
        """Return list of parameter value lists."""
        qry = self.query(self.parameter_value_list_sq)
        if id_list is not None:
            qry = qry.filter(self.parameter_value_list_sq.c.id.in_(id_list))
        return qry

    def wide_parameter_value_list_list(self, id_list=None):
        """Return list of parameter value lists and their elements in wide format."""
        qry = self.query(self.wide_parameter_value_list_sq)
        if id_list is not None:
            qry = qry.filter(self.wide_parameter_value_list_sq.c.id.in_(id_list))
        return qry

    def object_parameter_definition_fields(self):
        """Return object parameter fields."""
        return [x["name"] for x in self.object_parameter_definition_list().column_descriptions]

    def relationship_parameter_definition_fields(self):
        """Return relationship parameter fields."""
        return [x["name"] for x in self.relationship_parameter_definition_list().column_descriptions]

    def object_parameter_value_fields(self):
        """Return object parameter value fields."""
        return [x["name"] for x in self.object_parameter_value_list().column_descriptions]

    def relationship_parameter_value_fields(self):
        """Return relationship parameter value fields."""
        return [x["name"] for x in self.relationship_parameter_value_list().column_descriptions]

    def _empty_list(self):
        return self.query(false()).filter(false())
