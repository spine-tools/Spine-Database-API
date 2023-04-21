######################################################################################################################
# Copyright (C) 2017-2022 Spine project consortium
# This file is part of Spine Database API.
# Spine Database API is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser
# General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

"""Provides :class:`.DatabaseMappingQueryMixin`.

"""
# TODO: Deprecate and drop this module

from sqlalchemy import func, or_


class DatabaseMappingQueryMixin:
    """Provides methods to perform standard queries (``SELECT`` statements) on a Spine db."""

    def object_class_list(self, id_list=None, ordered=True):
        """Return all records from the :meth:`object_class_sq <.DatabaseMappingBase.object_class_sq>` subquery.

        :param id_list: If present, only return records where ``id`` is in this list.
        :param bool ordered: if True, order the result by the ``display_order`` field.

        :rtype: :class:`~sqlalchemy.orm.query.Query`
        """
        qry = self.query(self.object_class_sq)
        if id_list is not None:
            qry = qry.filter(self.object_class_sq.c.id.in_(id_list))
        if ordered:
            qry = qry.order_by(self.object_class_sq.c.display_order)
        return qry

    def object_list(self, id_list=None, class_id=None):
        """Return all records from the :meth:`object_sq <.DatabaseMappingBase.object_sq>` subquery.

        :param id_list: If present, only return records where ``id`` is in this list.
        :param int class_id: If present, only return records where ``class_id`` is equal to this.

        :rtype: :class:`~sqlalchemy.orm.query.Query`
        """
        qry = self.query(self.object_sq)
        if id_list is not None:
            qry = qry.filter(self.object_sq.c.id.in_(id_list))
        if class_id is not None:
            qry = qry.filter(self.object_sq.c.class_id == class_id)
        return qry

    def wide_relationship_class_list(self, id_list=None, object_class_id=None):
        """Return all records from the
        :meth:`wide_relationship_class_sq <.DatabaseMappingBase.wide_relationship_class_sq>` subquery.

        :param id_list: If present, only return records where ``id`` is in this list.
        :param int object_class_id: If present, only return records where ``object_class_id`` is equal to this.

        :rtype: :class:`~sqlalchemy.orm.query.Query`
        """
        qry = self.query(self.wide_relationship_class_sq)
        if id_list is not None:
            qry = qry.filter(self.wide_relationship_class_sq.c.id.in_(id_list))
        if object_class_id is not None:
            qry = qry.filter(
                or_(
                    self.wide_relationship_class_sq.c.object_class_id_list.like(f"%,{object_class_id},%"),
                    self.wide_relationship_class_sq.c.object_class_id_list.like(f"{object_class_id},%"),
                    self.wide_relationship_class_sq.c.object_class_id_list.like(f"%,{object_class_id}"),
                    self.wide_relationship_class_sq.c.object_class_id_list == str(object_class_id),
                )
            )
        return qry

    def wide_relationship_list(self, id_list=None, class_id=None, object_id=None):
        """Return all records from the
        :meth:`wide_relationship_sq <.DatabaseMappingBase.wide_relationship_sq>` subquery.

        :param id_list: If present, only return records where ``id`` is in this list.
        :param int class_id: If present, only return records where ``class_id`` is equal to this.
        :param int object_id: If present, only return records where ``object_id`` is equal to this.

        :rtype: :class:`~sqlalchemy.orm.query.Query`
        """
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
        """Return all records from the
        :meth:`parameter_definition_sq <.DatabaseMappingBase.parameter_definition_sq>` subquery.

        :param id_list: If present, only return records where ``id`` is in this list.
        :param int object_class_id: If present, only return records where ``object_class_id`` is equal to this.
        :param int relationship_class_id: If present, only return records where ``relationship_class_id``
            is equal to this.

        :rtype: :class:`~sqlalchemy.orm.query.Query`
        """
        qry = self.query(self.parameter_definition_sq)
        if id_list is not None:
            qry = qry.filter(self.parameter_definition_sq.c.id.in_(id_list))
        if object_class_id is not None:
            # to do make sure type is object
            qry = qry.filter(self.parameter_definition_sq.c.object_class_id == object_class_id)
        if relationship_class_id is not None:
            # to do make sure type is relationship
            qry = qry.filter(self.parameter_definition_sq.c.relationship_class_id == relationship_class_id)
        return qry

    def object_parameter_definition_list(self, object_class_id=None, parameter_definition_id=None):
        """Return all records from the
        :meth:`object_parameter_definition_sq <.DatabaseMappingBase.object_parameter_definition_sq>` subquery.

        :param int object_class_id: If present, only return records where ``object_class_id`` is equal to this.
        :param int parameter_definition_id: If present, only return records where ``id`` is in this list.

        :rtype: :class:`~sqlalchemy.orm.query.Query`
        """
        qry = self.query(self.object_parameter_definition_sq)
        if object_class_id:
            qry = qry.filter(self.object_parameter_definition_sq.c.object_class_id == object_class_id)
        if parameter_definition_id:
            qry = qry.filter(self.object_parameter_definition_sq.c.id == parameter_definition_id)
        return qry

    def relationship_parameter_definition_list(self, relationship_class_id=None, parameter_definition_id=None):
        """Return all records from the
        :meth:`relationship_parameter_definition_sq <.DatabaseMappingBase.relationship_parameter_definition_sq>`
        subquery.

        :param int relationship_class_id: If present, only return records where ``relationship_class_id``
            is equal to this.
        :param int parameter_definition_id: If present, only return records where ``id`` is in this list.

        :rtype: :class:`~sqlalchemy.orm.query.Query`
        """
        qry = self.query(self.relationship_parameter_definition_sq)
        if relationship_class_id:
            qry = qry.filter(self.relationship_parameter_definition_sq.c.relationship_class_id == relationship_class_id)
        if parameter_definition_id:
            qry = qry.filter(self.relationship_parameter_definition_sq.c.id == parameter_definition_id)
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
            self.relationship_class_sq.c.id.label("relationship_class_id"),
            self.relationship_class_sq.c.name.label("relationship_class_name"),
            self.parameter_definition_sq.c.id.label("parameter_definition_id"),
            self.parameter_definition_sq.c.name.label("parameter_name"),
        ).filter(self.relationship_class_sq.c.id == self.parameter_definition_sq.c.relationship_class_id)
        if relationship_class_id_list is not None:
            qry = qry.filter(self.relationship_class_sq.c.id.in_(relationship_class_id_list))
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
        """Return all records from the
        :meth:`parameter_value_sq <.DatabaseMappingBase.parameter_value_sq>` subquery.

        :param id_list: If present, only return records where ``id`` is in this list.
        :param int object_id: If present, only return records where ``object_id`` is equal to this.
        :param int relationship_id: If present, only return records where ``relationship_id`` is equal to this.

        :rtype: :class:`~sqlalchemy.orm.query.Query`
        """
        qry = self.query(self.parameter_value_sq)
        if id_list is not None:
            qry = qry.filter(self.parameter_value_sq.c.id.in_(id_list))
        if object_id:
            qry = qry.filter(self.parameter_value_sq.c.object_id == object_id)
        if relationship_id:
            qry = qry.filter(self.parameter_value_sq.c.relationship_id == relationship_id)
        return qry

    def object_parameter_value_list(self, parameter_name=None):
        """Return all records from the
        :meth:`object_parameter_value_sq <.DatabaseMappingBase.object_parameter_value_sq>` subquery.

        :param str parameter_name: If present, only return records where ``parameter_name`` is equal to this.

        :rtype: :class:`~sqlalchemy.orm.query.Query`
        """
        qry = self.query(self.object_parameter_value_sq)
        if parameter_name:
            qry = qry.filter(self.object_parameter_value_sq.c.parameter_name == parameter_name)
        return qry

    def relationship_parameter_value_list(self, parameter_name=None):
        """Return all records from the
        :meth:`relationship_parameter_value_sq <.DatabaseMappingBase.relationship_parameter_value_sq>` subquery.

        :param str parameter_name: If present, only return records where ``parameter_name`` is equal to this.

        :rtype: :class:`~sqlalchemy.orm.query.Query`
        """
        qry = self.query(self.relationship_parameter_value_sq)
        if parameter_name:
            qry = qry.filter(self.relationship_parameter_value_sq.c.parameter_name == parameter_name)
        return qry

    def parameter_value_list_list(self, id_list=None):
        """Return all records from the
        :meth:`parameter_value_list_sq <.DatabaseMappingBase.parameter_value_list_sq>` subquery.

        :param id_list: If present, only return records where ``id`` is in this list.

        :rtype: :class:`~sqlalchemy.orm.query.Query`
        """
        qry = self.query(self.parameter_value_list_sq)
        if id_list is not None:
            qry = qry.filter(self.parameter_value_list_sq.c.id.in_(id_list))
        return qry

    def wide_parameter_value_list_list(self, id_list=None):
        """Return all records from the
        :meth:`wide_parameter_value_list_sq <.DatabaseMappingBase.wide_parameter_value_list_sq>` subquery.

        :param id_list: If present, only return records where ``id`` is in this list.

        :rtype: :class:`~sqlalchemy.orm.query.Query`
        """
        qry = self.query(self.wide_parameter_value_list_sq)
        if id_list is not None:
            qry = qry.filter(self.wide_parameter_value_list_sq.c.id.in_(id_list))
        return qry

    def object_parameter_definition_fields(self):
        """Return names of columns that would be returned by :meth:`object_parameter_definition_list`."""
        return [x["name"] for x in self.object_parameter_definition_list().column_descriptions]

    def relationship_parameter_definition_fields(self):
        """Return names of columns that would be returned by :meth:`relationship_parameter_definition_list`."""
        return [x["name"] for x in self.relationship_parameter_definition_list().column_descriptions]

    def object_parameter_value_fields(self):
        """Return names of columns that would be returned by :meth:`object_parameter_value_list`."""
        return [x["name"] for x in self.object_parameter_value_list().column_descriptions]

    def relationship_parameter_value_fields(self):
        """Return names of columns that would be returned by :meth:`relationship_parameter_value_list`."""
        return [x["name"] for x in self.relationship_parameter_value_list().column_descriptions]

    def alternative_list(self):
        """Return names of columns that would be returned by :meth:`relationship_parameter_value_list`."""
        return self.query(self.alternative_sq)
