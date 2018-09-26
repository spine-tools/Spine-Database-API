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

"""
Classes to handle the Spine database object relational mapping.

:author: Manuel Marin (KTH)
:date:   11.8.2018
"""

import time
import logging
import sqlite3
from .database_mapping import DatabaseMapping
from sqlalchemy import create_engine, MetaData, Table, Column, select, func, inspect, or_, and_
from sqlalchemy.types import Integer
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.ext.horizontal_shard import ShardedSession
from sqlalchemy.orm import Session, sessionmaker, Query
from sqlalchemy.orm.session import make_transient
from sqlalchemy.pool import StaticPool
from sqlalchemy.exc import NoSuchTableError, DBAPIError, DatabaseError, OperationalError
from sqlalchemy.schema import ForeignKey
from .exception import SpineDBAPIError, TableNotFoundError
from .helpers import custom_generate_relationship, attr_dict
from datetime import datetime, timezone

# TODO: Consider using methods from the super class `DatabaseMapping` whenever possible
# For example, while querying tables we can get the super query, filter out touched items
# and then union with the diff query.
# But this needs polishing `DatabaseMapping`

class TempDatabaseMapping(DatabaseMapping):
    """A mapping to a temporary copy of a db."""
    def __init__(self, db_url, username=None):
        """Initialize class."""
        tic = time.clock()
        super().__init__(db_url, username)
        # Diff Base and tables
        self.DiffBase = None
        self.DiffObjectClass = None
        self.DiffObject = None
        self.DiffRelationshipClass = None
        self.DiffRelationship = None
        self.DiffParameter = None
        self.DiffParameterValue = None
        # Next ids
        id = self.session.query(func.max(self.ObjectClass.id)).scalar()
        self.next_object_class_id = id + 1 if id else 1
        id = self.session.query(func.max(self.Object.id)).scalar()
        self.next_object_id = id + 1 if id else 1
        id = self.session.query(func.max(self.RelationshipClass.id)).scalar()
        self.next_relationship_class_id = id + 1 if id else 1
        id = self.session.query(func.max(self.Relationship.id)).scalar()
        self.next_relationship_id = id + 1 if id else 1
        id = self.session.query(func.max(self.Parameter.id)).scalar()
        self.next_parameter_id = id + 1 if id else 1
        id = self.session.query(func.max(self.ParameterValue.id)).scalar()
        self.next_parameter_value_id = id + 1 if id else 1
        self.new_item_id = {
            "object_class": set(),
            "object": set(),
            "relationship_class": set(),
            "relationship": set(),
            "parameter": set(),
            "parameter_value": set(),
        }
        self.dirty_item_id = {
            "object_class": set(),
            "object": set(),
            "relationship_class": set(),
            "relationship": set(),
            "parameter": set(),
            "parameter_value": set(),
        }
        self.removed_item_id = {
            "object_class": set(),
            "object": set(),
            "relationship_class": set(),
            "relationship": set(),
            "parameter": set(),
            "parameter_value": set(),
        }
        # List of items that are either dirty, or removed
        self.touched_item_id = {
            "object_class": set(),
            "object": set(),
            "relationship_class": set(),
            "relationship": set(),
            "parameter": set(),
            "parameter_value": set(),
        }
        self.create_and_reflect_diff_tables()
        toc = time.clock()
        logging.debug("Temp mapping created in {} seconds".format(toc - tic))

    def create_and_reflect_diff_tables(self):
        """Create difference tables in both orig and diff db.
        """
        # Create
        metadata = MetaData(bind=self.engine)
        diff_tables = list()
        for t in self.Base.metadata.sorted_tables:
            if t.name.startswith('diff_'):
                continue
            diff_columns = list()
            for column in t.columns:
                diff_columns.append(column.copy())
            diff_table = Table(
                "diff_" + t.name, metadata,
                *diff_columns)
            diff_tables.append(diff_table)
        metadata.drop_all(tables=diff_tables)
        for diff_table in diff_tables:
            diff_table.create(self.engine)
        # Reflect
        self.DiffBase = automap_base(metadata=metadata)
        self.DiffBase.prepare(generate_relationship=custom_generate_relationship)
        try:
            self.DiffObjectClass = self.DiffBase.classes.diff_object_class
            self.DiffObject = self.DiffBase.classes.diff_object
            self.DiffRelationshipClass = self.DiffBase.classes.diff_relationship_class
            self.DiffRelationship = self.DiffBase.classes.diff_relationship
            self.DiffParameter = self.DiffBase.classes.diff_parameter
            self.DiffParameterValue = self.DiffBase.classes.diff_parameter_value
        except NoSuchTableError as table:
            raise TableNotFoundError(table)
        except AttributeError as table:
            raise TableNotFoundError(table)

    def single_object_class(self, id=None, name=None):
        """Return a single object class given the id or name."""
        qry = self.object_class_list()
        if id:
            return qry.filter(or_(self.ObjectClass.id == id, self.DiffObjectClass.id == id))
        if name:
            return qry.filter(or_(self.ObjectClass.name == name, self.DiffObjectClass.name == name))
        return self.empty_list()

    def single_object(self, id=None, name=None):
        """Return a single object given the id or name."""
        qry = self.object_list()
        if id:
            return qry.filter(or_(self.Object.id == id, self.DiffObject.id == id))
        if name:
            return qry.filter(or_(self.Object.name == name, self.DiffObject.name == name))
        return self.empty_list()

    def single_wide_relationship_class(self, id=None, name=None):
        """Return a single relationship class in wide format given the id or name."""
        subqry = self.wide_relationship_class_list().subquery()
        qry = self.session.query(
            subqry.c.id,
            subqry.c.object_class_id_list,
            subqry.c.object_class_name_list,
            subqry.c.name
        )
        if id:
            return qry.filter(subqry.c.id == id)
        if name:
            return qry.filter(subqry.c.name == name)
        return self.empty_list()

    def single_wide_relationship(self, id=None, name=None, class_id=None, object_id_list=None, object_name_list=None):
        """Return a single relationship in wide format given the id or name."""
        subqry = self.wide_relationship_list().subquery()
        qry = self.session.query(
            subqry.c.id,
            subqry.c.class_id,
            subqry.c.object_id_list,
            subqry.c.object_name_list,
            subqry.c.name
        )
        if id:
            return qry.filter(subqry.c.id == id)
        if name:
            return qry.filter(subqry.c.name == name)
        if class_id:
            qry = qry.filter(subqry.c.class_id == class_id)
            if object_id_list:
                return qry.filter(subqry.c.object_id_list == object_id_list)
            if object_name_list:
                return qry.filter(subqry.c.object_name_list == object_name_list)
        return self.empty_list()

    def single_parameter(self, id=None, name=None):
        """Return parameter corresponding to id."""
        qry = self.parameter_list()
        if id:
            return qry.filter(or_(self.Parameter.id == id, self.DiffParameter.id == id))
        if name:
            return qry.filter(or_(self.Parameter.name == name, self.DiffParameter.name == name))
        return self.empty_list()

    def single_object_parameter(self, id):
        """Return object class and the parameter corresponding to id."""
        return self.object_parameter_list().filter(or_(self.Parameter.id == id, self.DiffParameter.id == id))

    def single_relationship_parameter(self, id):
        """Return relationship class and the parameter corresponding to id."""
        return self.relationship_parameter_list().filter(or_(self.Parameter.id == id, self.DiffParameter.id == id))

    def single_parameter_value(self, id=None):
        """Return parameter value corresponding to id."""
        if id:
            return self.parameter_value_list().\
                filter(or_(self.ParameterValue.id == id, self.DiffParameterValue.id == id))
        return self.empty_list()

    def single_object_parameter_value(self, id=None, parameter_id=None, object_id=None):
        """Return object and the parameter value, either corresponding to id,
        or to parameter_id and object_id.
        """
        qry = self.object_parameter_value_list()
        if id:
            return qry.filter(or_(self.ParameterValue.id == id, self.DiffParameterValue.id == id))
        if parameter_id and object_id:
            return qry.filter(or_(
                and_(
                    self.ParameterValue.parameter_id == parameter_id,
                    self.ParameterValue.object_id == object_id),
                and_(
                    self.DiffParameterValue.parameter_id == parameter_id,
                    self.DiffParameterValue.object_id == object_id)))
        return self.empty_list()

    def single_relationship_parameter_value(self, id):
        """Return relationship and the parameter value corresponding to id."""
        return self.relationship_parameter_value_list().\
            filter(or_(self.ParameterValue.id == id, self.DiffParameterValue.id == id))

    def object_class_list(self):
        """Return object classes ordered by display order."""
        qry = self.session.query(
            self.ObjectClass.id.label("id"),
            self.ObjectClass.name.label("name"),
            self.ObjectClass.display_order.label("display_order"),
        ).filter(~self.ObjectClass.id.in_(self.touched_item_id["object_class"]))
        diff_qry = self.session.query(
            self.DiffObjectClass.id.label("id"),
            self.DiffObjectClass.name.label("name"),
            self.DiffObjectClass.display_order.label("display_order"),
        )
        return qry.union_all(diff_qry).order_by(self.ObjectClass.display_order)

    def object_list(self, class_id=None):
        """Return objects, optionally filtered by class id."""
        qry = self.session.query(
            self.Object.id.label('id'),
            self.Object.class_id.label('class_id'),
            self.Object.name.label('name'),
        ).filter(~self.Object.id.in_(self.touched_item_id["object"]))
        diff_qry = self.session.query(
            self.DiffObject.id.label('id'),
            self.DiffObject.class_id.label('class_id'),
            self.DiffObject.name.label('name'),
        )
        if class_id:
            qry = qry.filter_by(class_id=class_id)
            diff_qry = diff_qry.filter_by(class_id=class_id)
        return qry.union_all(diff_qry)

    def wide_relationship_class_list(self, object_class_id=None):
        """Return list of relationship classes in wide format involving a given object class."""
        object_class_list = self.object_class_list().subquery()
        qry = self.session.query(
            self.RelationshipClass.id.label('id'),
            self.RelationshipClass.object_class_id.label('object_class_id'),
            object_class_list.c.name.label('object_class_name'),
            self.RelationshipClass.name.label('name')
        ).filter(self.RelationshipClass.object_class_id == object_class_list.c.id).\
        filter(~self.RelationshipClass.id.in_(self.touched_item_id["relationship_class"]))
        diff_qry = self.session.query(
            self.DiffRelationshipClass.id.label('id'),
            self.DiffRelationshipClass.object_class_id.label('object_class_id'),
            object_class_list.c.name.label('object_class_name'),
            self.DiffRelationshipClass.name.label('name')
        ).filter(self.DiffRelationshipClass.object_class_id == object_class_list.c.id)
        if object_class_id:
            qry = qry.filter(self.RelationshipClass.id.in_(
                self.session.query(self.RelationshipClass.id).\
                    filter_by(object_class_id=object_class_id).distinct()))
            diff_qry = diff_qry.filter(self.DiffRelationshipClass.id.in_(
                self.session.query(self.DiffRelationshipClass.id).\
                    filter_by(object_class_id=object_class_id).distinct()))
        subqry = qry.union_all(diff_qry).subquery()
        return self.session.query(
            subqry.c.id,
            func.group_concat(subqry.c.object_class_id).label('object_class_id_list'),
            func.group_concat(subqry.c.object_class_name).label('object_class_name_list'),
            subqry.c.name
        ).group_by(subqry.c.id)

    def wide_relationship_list(self, class_id=None, object_id=None):
        """Return list of relationships in wide format involving a given relationship class and object."""
        object_list = self.object_list().subquery()
        qry = self.session.query(
            self.Relationship.id.label('id'),
            self.Relationship.class_id.label('class_id'),
            self.Relationship.object_id.label('object_id'),
            object_list.c.name.label('object_name'),
            self.Relationship.name.label('name')
        ).filter(self.Relationship.object_id == object_list.c.id).\
        filter(~self.Relationship.id.in_(self.touched_item_id["relationship"]))
        diff_qry = self.session.query(
            self.DiffRelationship.id.label('id'),
            self.DiffRelationship.class_id.label('class_id'),
            self.DiffRelationship.object_id.label('object_id'),
            object_list.c.name.label('object_name'),
            self.DiffRelationship.name.label('name')
        ).filter(self.DiffRelationship.object_id == object_list.c.id)
        if class_id:
            qry = qry.filter(self.Relationship.id.in_(
                self.session.query(self.Relationship.id).filter_by(class_id=class_id).distinct()))
            diff_qry = diff_qry.filter(self.DiffRelationship.id.in_(
                self.session.query(self.DiffRelationship.id).filter_by(class_id=class_id).distinct()))
        if object_id:
            qry = qry.filter(self.Relationship.id.in_(
                self.session.query(self.Relationship.id).filter_by(object_id=object_id).distinct()))
            diff_qry = diff_qry.filter(self.DiffRelationship.id.in_(
                self.session.query(self.DiffRelationship.id).filter_by(object_id=object_id).distinct()))
        subqry = qry.union_all(diff_qry).subquery()
        return self.session.query(
            subqry.c.id,
            subqry.c.class_id,
            func.group_concat(subqry.c.object_id).label('object_id_list'),
            func.group_concat(subqry.c.object_name).label('object_name_list'),
            subqry.c.name
        ).group_by(subqry.c.id)

    def parameter_list(self, object_class_id=None, relationship_class_id=None):
        """Return parameters."""
        qry = self.session.query(
            self.Parameter.id.label('id'),
            self.Parameter.name.label('name'),
            self.Parameter.relationship_class_id.label('relationship_class_id'),
            self.Parameter.object_class_id.label('object_class_id'),
            self.Parameter.can_have_time_series.label('can_have_time_series'),
            self.Parameter.can_have_time_pattern.label('can_have_time_pattern'),
            self.Parameter.can_be_stochastic.label('can_be_stochastic'),
            self.Parameter.default_value.label('default_value'),
            self.Parameter.is_mandatory.label('is_mandatory'),
            self.Parameter.precision.label('precision'),
            self.Parameter.minimum_value.label('minimum_value'),
            self.Parameter.maximum_value.label('maximum_value')
        ).filter(~self.Parameter.id.in_(self.touched_item_id["parameter"]))
        diff_qry = self.session.query(
            self.DiffParameter.id.label('id'),
            self.DiffParameter.name.label('name'),
            self.DiffParameter.relationship_class_id.label('relationship_class_id'),
            self.DiffParameter.object_class_id.label('object_class_id'),
            self.DiffParameter.can_have_time_series.label('can_have_time_series'),
            self.DiffParameter.can_have_time_pattern.label('can_have_time_pattern'),
            self.DiffParameter.can_be_stochastic.label('can_be_stochastic'),
            self.DiffParameter.default_value.label('default_value'),
            self.DiffParameter.is_mandatory.label('is_mandatory'),
            self.DiffParameter.precision.label('precision'),
            self.DiffParameter.minimum_value.label('minimum_value'),
            self.DiffParameter.maximum_value.label('maximum_value'))
        if object_class_id:
            qry = qry.filter_by(object_class_id=object_class_id)
            diff_qry = diff_qry.filter_by(object_class_id=object_class_id)
        if relationship_class_id:
            qry = qry.filter_by(object_class_id=object_class_id)
            diff_qry = diff_qry.filter_by(object_class_id=object_class_id)
        return qry.union_all(diff_qry)

    def object_parameter_list(self, object_class_id=None, parameter_id=None):
        """Return object classes and their parameters."""
        object_class_list = self.object_class_list().subquery()
        qry = self.session.query(
            self.Parameter.id.label('parameter_id'),
            object_class_list.c.name.label('object_class_name'),
            self.Parameter.name.label('parameter_name'),
            self.Parameter.can_have_time_series,
            self.Parameter.can_have_time_pattern,
            self.Parameter.can_be_stochastic,
            self.Parameter.default_value,
            self.Parameter.is_mandatory,
            self.Parameter.precision,
            self.Parameter.minimum_value,
            self.Parameter.maximum_value
        ).filter(object_class_list.c.id == self.Parameter.object_class_id).\
        filter(~self.Parameter.id.in_(self.touched_item_id["parameter"]))
        diff_qry = self.session.query(
            self.DiffParameter.id.label('parameter_id'),
            object_class_list.c.name.label('object_class_name'),
            self.DiffParameter.name.label('parameter_name'),
            self.DiffParameter.can_have_time_series,
            self.DiffParameter.can_have_time_pattern,
            self.DiffParameter.can_be_stochastic,
            self.DiffParameter.default_value,
            self.DiffParameter.is_mandatory,
            self.DiffParameter.precision,
            self.DiffParameter.minimum_value,
            self.DiffParameter.maximum_value
        ).filter(object_class_list.c.id == self.DiffParameter.object_class_id)
        if object_class_id:
            qry = qry.filter(self.Parameter.object_class_id == object_class_id)
            diff_qry = diff_qry.filter(self.DiffParameter.object_class_id == object_class_id)
        if parameter_id:
            qry = qry.filter(self.Parameter.id == parameter_id)
            diff_qry = diff_qry.filter(self.DiffParameter.id == parameter_id)
        return qry.union_all(diff_qry).order_by(self.Parameter.id, self.DiffParameter.id)

    def relationship_parameter_list(self, relationship_class_id=None, parameter_id=None):
        """Return relationship classes and their parameters."""
        wide_relationship_class_list = self.wide_relationship_class_list().subquery()
        qry = self.session.query(
            self.Parameter.id.label('parameter_id'),
            wide_relationship_class_list.c.name.label('relationship_class_name'),
            wide_relationship_class_list.c.object_class_name_list,
            self.Parameter.name.label('parameter_name'),
            self.Parameter.can_have_time_series,
            self.Parameter.can_have_time_pattern,
            self.Parameter.can_be_stochastic,
            self.Parameter.default_value,
            self.Parameter.is_mandatory,
            self.Parameter.precision,
            self.Parameter.minimum_value,
            self.Parameter.maximum_value
        ).filter(self.Parameter.relationship_class_id == wide_relationship_class_list.c.id).\
        filter(~self.Parameter.id.in_(self.touched_item_id["parameter"]))
        diff_qry = self.session.query(
            self.DiffParameter.id.label('parameter_id'),
            wide_relationship_class_list.c.name.label('relationship_class_name'),
            wide_relationship_class_list.c.object_class_name_list,
            self.DiffParameter.name.label('parameter_name'),
            self.DiffParameter.can_have_time_series,
            self.DiffParameter.can_have_time_pattern,
            self.DiffParameter.can_be_stochastic,
            self.DiffParameter.default_value,
            self.DiffParameter.is_mandatory,
            self.DiffParameter.precision,
            self.DiffParameter.minimum_value,
            self.DiffParameter.maximum_value
        ).filter(self.DiffParameter.relationship_class_id == wide_relationship_class_list.c.id)
        if relationship_class_id:
            qry = qry.filter(self.Parameter.relationship_class_id == relationship_class_id)
            diff_qry = diff_qry.filter(self.DiffParameter.relationship_class_id == relationship_class_id)
        if parameter_id:
            qry = qry.filter(self.Parameter.id == parameter_id)
            diff_qry = diff_qry.filter(self.DiffParameter.id == parameter_id)
        return qry.union_all(diff_qry).order_by(self.Parameter.id, self.DiffParameter.id)

    def parameter_value_list(self, object_id=None, relationship_id=None):
        """Return parameter values."""
        qry = self.session.query(
            self.ParameterValue.id,
            self.ParameterValue.parameter_id,
            self.ParameterValue.object_id,
            self.ParameterValue.relationship_id,
            self.ParameterValue.index,
            self.ParameterValue.value,
            self.ParameterValue.json,
            self.ParameterValue.expression,
            self.ParameterValue.time_pattern,
            self.ParameterValue.time_series_id,
            self.ParameterValue.stochastic_model_id
        ).filter(~self.ParameterValue.id.in_(self.touched_item_id["parameter_value"]))
        diff_qry = self.session.query(
            self.DiffParameterValue.id,
            self.DiffParameterValue.parameter_id,
            self.DiffParameterValue.object_id,
            self.DiffParameterValue.relationship_id,
            self.DiffParameterValue.index,
            self.DiffParameterValue.value,
            self.DiffParameterValue.json,
            self.DiffParameterValue.expression,
            self.DiffParameterValue.time_pattern,
            self.DiffParameterValue.time_series_id,
            self.DiffParameterValue.stochastic_model_id)
        if object_id:
            qry = qry.filter_by(object_id=object_id)
            diff_qry = diff_qry.filter_by(object_id=object_id)
        if relationship_id:
            qry = qry.filter_by(relationship_id=relationship_id)
            diff_qry = diff_qry.filter_by(relationship_id=relationship_id)
        return qry.union_all(diff_qry)

    def object_parameter_value_list(self, parameter_name=None):
        """Return objects and their parameter values."""
        parameter_list = self.parameter_list().subquery()
        object_class_list = self.object_class_list().subquery()
        object_list = self.object_list().subquery()
        qry = self.session.query(
            self.ParameterValue.id.label('parameter_value_id'),
            object_class_list.c.name.label('object_class_name'),
            object_list.c.name.label('object_name'),
            parameter_list.c.name.label('parameter_name'),
            self.ParameterValue.index,
            self.ParameterValue.value,
            self.ParameterValue.json,
            self.ParameterValue.expression,
            self.ParameterValue.time_pattern,
            self.ParameterValue.time_series_id,
            self.ParameterValue.stochastic_model_id
        ).filter(parameter_list.c.id == self.ParameterValue.parameter_id).\
        filter(self.ParameterValue.object_id == object_list.c.id).\
        filter(parameter_list.c.object_class_id == object_class_list.c.id).\
        filter(~self.ParameterValue.id.in_(self.touched_item_id["parameter_value"]))
        diff_qry = self.session.query(
            self.DiffParameterValue.id.label('parameter_value_id'),
            object_class_list.c.name.label('object_class_name'),
            object_list.c.name.label('object_name'),
            parameter_list.c.name.label('parameter_name'),
            self.DiffParameterValue.index,
            self.DiffParameterValue.value,
            self.DiffParameterValue.json,
            self.DiffParameterValue.expression,
            self.DiffParameterValue.time_pattern,
            self.DiffParameterValue.time_series_id,
            self.DiffParameterValue.stochastic_model_id
        ).filter(parameter_list.c.id == self.DiffParameterValue.parameter_id).\
        filter(self.DiffParameterValue.object_id == object_list.c.id).\
        filter(self.DiffParameter.object_class_id == object_class_list.c.id)
        if parameter_name:
            qry = qry.filter(parameter_list.c.name == parameter_name)
            diff_qry = diff_qry.filter(parameter_list.c.name == parameter_name)
        return qry.union_all(diff_qry)

    def relationship_parameter_value_list(self, parameter_name=None):
        """Return relationships and their parameter values."""
        parameter_list = self.parameter_list().subquery()
        wide_relationship_class_list = self.wide_relationship_class_list().subquery()
        wide_relationship_list = self.wide_relationship_list().subquery()
        qry = self.session.query(
            self.ParameterValue.id.label('parameter_value_id'),
            wide_relationship_class_list.c.name.label('relationship_class_name'),
            wide_relationship_list.c.object_name_list,
            parameter_list.c.name.label('parameter_name'),
            self.ParameterValue.index,
            self.ParameterValue.value,
            self.ParameterValue.json,
            self.ParameterValue.expression,
            self.ParameterValue.time_pattern,
            self.ParameterValue.time_series_id,
            self.ParameterValue.stochastic_model_id
        ).filter(parameter_list.c.id == self.ParameterValue.parameter_id).\
        filter(self.ParameterValue.relationship_id == wide_relationship_list.c.id).\
        filter(parameter_list.c.relationship_class_id == wide_relationship_class_list.c.id).\
        filter(~self.ParameterValue.id.in_(self.touched_item_id["parameter_value"]))
        diff_qry = self.session.query(
            self.DiffParameterValue.id.label('parameter_value_id'),
            wide_relationship_class_list.c.name.label('relationship_class_name'),
            wide_relationship_list.c.object_name_list,
            parameter_list.c.name.label('parameter_name'),
            self.DiffParameterValue.index,
            self.DiffParameterValue.value,
            self.DiffParameterValue.json,
            self.DiffParameterValue.expression,
            self.DiffParameterValue.time_pattern,
            self.DiffParameterValue.time_series_id,
            self.DiffParameterValue.stochastic_model_id
        ).filter(parameter_list.c.id == self.DiffParameterValue.parameter_id).\
        filter(self.DiffParameterValue.relationship_id == wide_relationship_list.c.id).\
        filter(parameter_list.c.relationship_class_id == wide_relationship_class_list.c.id)
        if parameter_name:
            qry = qry.filter(parameter_list.c.name == parameter_name)
            diff_qry = diff_qry.filter(parameter_list.c.name == parameter_name)
        return qry.union_all(diff_qry)

    def add_object_class(self, **kwargs):
        """Add object class to database.

        Returns:
            object_class (KeyedTuple): the object class now with the id
        """
        try:
            id = self.next_object_class_id
            item = self.DiffObjectClass(id=id, **kwargs)
            self.session.add(item)
            self.session.commit()
            self.new_item_id["object_class"].add(id)
            self.next_object_class_id += 1
            return self.single_object_class(id=id).one_or_none()
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while inserting object class '{}': {}".format(kwargs['name'], e.orig.args)
            raise SpineDBAPIError(msg)

    def add_object(self, **kwargs):
        """Add object to database.

        Returns:
            object_ (KeyedTuple): the object now with the id
        """
        try:
            id = self.next_object_id
            item = self.DiffObject(id=id, **kwargs)
            self.session.add(item)
            self.session.commit()
            self.new_item_id["object"].add(id)
            self.next_object_id += 1
            return self.single_object(id=id).one_or_none()
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while inserting object '{}': {}".format(kwargs['name'], e.orig.args)
            raise SpineDBAPIError(msg)

    def add_wide_relationship_class(self, **kwargs):
        """Add relationship class to database.

        Args:
            kwargs (dict): the relationship class in wide format

        Returns:
            wide_relationship_class (KeyedTuple): the relationship class now with the id
        """
        try:
            id = self.next_relationship_class_id
            item_list = list()
            for dimension, object_class_id in enumerate(kwargs['object_class_id_list']):
                kwargs = {
                    'id': id,
                    'dimension': dimension,
                    'object_class_id': object_class_id,
                    'name': kwargs['name']
                }
                item = self.DiffRelationshipClass(**kwargs)
                item_list.append(item)
            self.session.add_all(item_list)
            self.session.commit()
            self.new_item_id["relationship_class"].add(id)
            self.next_relationship_class_id += 1
            return self.single_wide_relationship_class(id=id).one_or_none()
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while inserting relationship class '{}': {}".format(kwargs['name'], e.orig.args)
            raise SpineDBAPIError(msg)

    def add_wide_relationship(self, **kwargs):
        """Add relationship to database.

        Args:
            kwargs (dict): the relationship in wide format

        Returns:
            wide_relationship (KeyedTuple): the relationship now with the id
        """
        try:
            id = self.next_relationship_id
            item_list = list()
            for dimension, object_id in enumerate(kwargs['object_id_list']):
                kwargs = {
                    'id': id,
                    'class_id': kwargs['class_id'],
                    'dimension': dimension,
                    'object_id': object_id,
                    'name': kwargs['name']
                }
                item = self.DiffRelationship(**kwargs)
                item_list.append(item)
            self.session.add_all(item_list)
            self.session.commit()
            self.new_item_id["relationship"].add(id)
            self.next_relationship_id += 1
            return self.single_wide_relationship(id=id).one_or_none()
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while inserting relationship '{}': {}".format(kwargs['name'], e.orig.args)
            raise SpineDBAPIError(msg)

    def add_parameter(self, **kwargs):
        """Add parameter to database.

        Returns:
            An instance of self.Parameter if successful, None otherwise
        """
        try:
            id = self.next_parameter_id
            item = self.DiffParameter(id=id, **kwargs)
            self.session.add(item)
            self.session.commit()
            self.new_item_id["parameter"].add(id)
            self.next_parameter_id += 1
            return self.single_parameter(id=id).one_or_none()
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while inserting parameter '{}': {}".format(kwargs['name'], e.orig.args)
            raise SpineDBAPIError(msg)

    def add_parameter_value(self, **kwargs):
        """Add parameter value to database.

        Returns:
            An instance of self.ParameterValue if successful, None otherwise
        """
        try:
            id = self.next_parameter_value_id
            item = self.DiffParameterValue(id=id, **kwargs)
            self.session.add(item)
            self.session.commit()
            self.new_item_id["parameter_value"].add(id)
            self.next_parameter_value_id += 1
            return self.single_parameter_value(id=id).one_or_none()
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while inserting parameter value: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def rename_object_class(self, id, new_name):
        """Rename object class."""
        try:
            diff_item = self.session.query(self.DiffObjectClass).filter_by(id=id).one_or_none()
            if diff_item:
                diff_item.name = new_name
            else:
                item = self.session.query(self.ObjectClass).filter_by(id=id).one_or_none()
                if not item:
                    return None
                kwargs = attr_dict(item)
                kwargs['name'] = new_name
                diff_item = self.DiffObjectClass(**kwargs)
                self.session.add(diff_item)
            self.session.commit()
            self.touched_item_id["object_class"].add(id)
            self.dirty_item_id["object_class"].add(id)
            return self.single_object_class(id=id).one_or_none()
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while renaming object class '{}': {}".format(diff_item.name, e.orig.args)
            raise SpineDBAPIError(msg)

    def rename_object(self, id, new_name):
        """Rename object."""
        try:
            diff_item = self.session.query(self.DiffObject).filter_by(id=id).one_or_none()
            if diff_item:
                diff_item.name = new_name
            else:
                item = self.session.query(self.Object).filter_by(id=id).one_or_none()
                if not item:
                    return None
                kwargs = attr_dict(item)
                kwargs['name'] = new_name
                diff_item = self.DiffObject(**kwargs)
                self.session.add(diff_item)
            self.session.commit()
            self.touched_item_id["object"].add(id)
            self.dirty_item_id["object"].add(id)
            return self.single_object(id=id).one_or_none()
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while renaming object '{}': {}".format(diff_item.name, e.orig.args)
            raise SpineDBAPIError(msg)

    def rename_relationship_class(self, id, new_name):
        """Rename relationship class."""
        try:
            diff_item_list = self.session.query(self.DiffRelationshipClass).filter_by(id=id)
            if diff_item_list.count():
                for diff_item in diff_item_list:
                    diff_item.name = new_name
            else:
                item_list = self.session.query(self.RelationshipClass).filter_by(id=id)
                diff_item_list = list()
                for item in item_list:
                    kwargs = attr_dict(item)
                    kwargs['name'] = new_name
                    diff_item = self.DiffRelationshipClass(**kwargs)
                    diff_item_list.append(diff_item)
                self.session.add_all(diff_item_list)
            self.session.commit()
            self.touched_item_id["relationship_class"].add(id)
            self.dirty_item_id["relationship_class"].add(id)
            return self.single_wide_relationship_class(id=id).one_or_none()
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while renaming relationship class: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def rename_relationship(self, id, new_name):
        """Rename relationship."""
        try:
            diff_item_list = self.session.query(self.DiffRelationship).filter_by(id=id)
            if diff_item_list.count():
                for diff_item in diff_item_list:
                    diff_item.name = new_name
            else:
                item_list = self.session.query(self.Relationship).filter_by(id=id)
                diff_item_list = list()
                for item in item_list:
                    kwargs = attr_dict(item)
                    kwargs['name'] = new_name
                    diff_item = self.DiffRelationship(**kwargs)
                    diff_item_list.append(diff_item)
                self.session.add_all(diff_item_list)
            self.session.commit()
            self.touched_item_id["relationship"].add(id)
            self.dirty_item_id["relationship"].add(id)
            return self.single_wide_relationship(id=id).one_or_none()
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while renaming relationship: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def update_parameter(self, id, field_name, new_value):
        """Update parameter."""
        try:
            diff_item = self.session.query(self.DiffParameter).filter_by(id=id).one_or_none()
            if diff_item:
                setattr(diff_item, field_name, new_value)
            else:
                item = self.session.query(self.Parameter).filter_by(id=id).one_or_none()
                if not item:
                    return None
                kwargs = attr_dict(item)
                kwargs[field_name] = new_value
                diff_item = self.DiffParameter(**kwargs)
                self.session.add(diff_item)
            self.session.commit()
            self.touched_item_id["parameter"].add(id)
            self.dirty_item_id["parameter"].add(id)
            return self.single_parameter(id=id).one_or_none()
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while updating parameter '{}': {}".format(diff_item.name, e.orig.args)
            raise SpineDBAPIError(msg)

    def update_parameter_value(self, id, field_name, new_value):
        """Update parameter value."""
        try:
            diff_item = self.session.query(self.DiffParameterValue).filter_by(id=id).one_or_none()
            if diff_item:
                setattr(diff_item, field_name, new_value)
            else:
                item = self.session.query(self.ParameterValue).filter_by(id=id).one_or_none()
                if not item:
                    return None
                kwargs = attr_dict(item)
                kwargs[field_name] = new_value
                diff_item = self.DiffParameterValue(**kwargs)
                self.session.add(diff_item)
            self.session.commit()
            self.touched_item_id["parameter_value"].add(id)
            self.dirty_item_id["parameter_value"].add(id)
            return self.single_parameter(id=id).one_or_none()
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while updating parameter value: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def remove_object_class(self, id):
        """Remove object class."""
        diff_item = self.session.query(self.DiffObjectClass).filter_by(id=id).one_or_none()
        if diff_item:
            try:
                self.session.delete(diff_item)
                self.session.commit()
                return True
            except DBAPIError as e:
                self.session.rollback()
                msg = "DBAPIError while removing object class '{}': {}".format(diff_item.name, e.orig.args)
                raise SpineDBAPIError(msg)
        self.removed_item_id["object_class"].add(id)
        self.touched_item_id["object_class"].add(id)
        for item in self.session.query(self.Object.id).filter_by(class_id=id):
            self.touched_item_id["object"].add(item.id)
        id_list = [x.id for x in self.session.query(self.RelationshipClass.id).filter_by(object_class_id=id)]
        for item in self.session.query(self.RelationshipClass.id).filter(self.RelationshipClass.id.in_(id_list)):
            self.touched_item_id["relationship_class"].add(item.id)
        for item in self.session.query(self.Parameter.id).filter_by(object_class_id=id):
            self.touched_item_id["parameter"].add(item.id)
        return True

    def remove_object(self, id):
        """Remove object."""
        diff_item = self.session.query(self.DiffObject).filter_by(id=id).one_or_none()
        if diff_item:
            try:
                self.session.delete(diff_item)
                self.session.commit()
                return True
            except DBAPIError as e:
                self.session.rollback()
                msg = "DBAPIError while removing object '{}': {}".format(diff_item.name, e.orig.args)
                raise SpineDBAPIError(msg)
        self.removed_item_id["object"].add(id)
        self.touched_item_id["object"].add(id)
        id_list = [x.id for x in self.session.query(self.Relationship.id).filter_by(object_id=id)]
        for item in self.session.query(self.Relationship.id).filter(self.Relationship.id.in_(id_list)):
            self.touched_item_id["relationship"].add(item.id)
        for item in self.session.query(self.ParameterValue.id).filter_by(object_id=id):
            self.touched_item_id["parameter_value"].add(item.id)
        return True

    def remove_relationship_class(self, id):
        """Remove relationship class."""
        diff_item = self.session.query(self.DiffRelationshipClass).filter_by(id=id).one_or_none()
        if diff_item:
            try:
                self.session.delete(diff_item)
                self.session.commit()
                return True
            except DBAPIError as e:
                self.session.rollback()
                msg = "DBAPIError while removing relationship class '{}': {}".format(diff_item.name, e.orig.args)
                raise SpineDBAPIError(msg)
        self.removed_item_id["relationship_class"].add(id)
        self.touched_item_id["relationship_class"].add(id)
        id_list = [x.id for x in self.session.query(self.Relationship.id).filter_by(class_id=id)]
        for item in self.session.query(self.Relationship.id).filter(self.Relationship.id.in_(id_list)):
            self.touched_item_id["relationship"].add(item.id)
        for item in self.session.query(self.Parameter.id).filter_by(relationship_class_id=id):
            self.touched_item_id["parameter"].add(item.id)
        return True

    def remove_relationship(self, id):
        """Remove relationship."""
        diff_item = self.session.query(self.DiffRelationship).filter_by(id=id).one_or_none()
        if diff_item:
            try:
                self.session.delete(diff_item)
                self.session.commit()
                return True
            except DBAPIError as e:
                self.session.rollback()
                msg = "DBAPIError while removing relationship '{}': {}".format(diff_item.name, e.orig.args)
                raise SpineDBAPIError(msg)
        self.removed_item_id["relationship"].add(id)
        self.touched_item_id["relationship"].add(id)
        for item in self.session.query(self.ParameterValue.id).filter_by(relationship_id=id):
            self.touched_item_id["parameter_value"].add(item.id)
        return True

    def remove_parameter(self, id):
        """Remove parameter."""
        diff_item = self.session.query(self.DiffParameter).filter_by(id=id).one_or_none()
        if diff_item:
            try:
                self.session.delete(diff_item)
                self.session.commit()
                return True
            except DBAPIError as e:
                self.session.rollback()
                msg = "DBAPIError while removing parameter '{}': {}".format(diff_item.name, e.orig.args)
                raise SpineDBAPIError(msg)
        self.removed_item_id["parameter"].add(id)
        self.touched_item_id["parameter"].add(id)
        for item in self.session.query(self.ParameterValue.id).filter_by(parameter_id=id):
            self.touched_item_id["parameter_value"].add(item.id)
        return True

    def remove_parameter_value(self, id):
        """Remove parameter value."""
        diff_item = self.session.query(self.DiffParameterValue).filter_by(id=id).one_or_none()
        if diff_item:
            try:
                self.session.delete(diff_item)
                self.session.commit()
                return True
            except DBAPIError as e:
                self.session.rollback()
                msg = "DBAPIError while removing parameter value '{}': {}".format(diff_item.name, e.orig.args)
                raise SpineDBAPIError(msg)
        self.removed_item_id["parameter_value"].add(id)
        self.touched_item_id["parameter_value"].add(id)

    def commit_session(self, comment):
        """Commit changes to source database."""
        try:
            user = self.username
            date = datetime.now(timezone.utc)
            commit = self.Commit(comment=comment, date=date, user=user)
            self.session.add(commit)
            self.session.flush()
            # Add new
            new_items = list()
            for id in self.new_item_id["object_class"]:
                item = self.session.query(self.DiffObjectClass).filter_by(id=id).one_or_none()
                if not item:
                    continue # TODO: ...or scream?
                kwargs = attr_dict(item)
                kwargs['commit_id'] = commit.id
                new_item = self.ObjectClass(**kwargs)
                new_items.append(new_item)
            for id in self.new_item_id["object"]:
                item = self.session.query(self.DiffObject).filter_by(id=id).one_or_none()
                if not item:
                    continue
                kwargs = attr_dict(item)
                kwargs['commit_id'] = commit.id
                new_item = self.Object(**kwargs)
                new_items.append(new_item)
            for id in self.new_item_id["relationship_class"]:
                for item in self.session.query(self.DiffRelationshipClass).filter_by(id=id):
                    kwargs = attr_dict(item)
                    kwargs['commit_id'] = commit.id
                    new_item = self.RelationshipClass(**kwargs)
                    new_items.append(new_item)
            for id in self.new_item_id["relationship"]:
                for item in self.session.query(self.DiffRelationship).filter_by(id=id):
                    kwargs = attr_dict(item)
                    kwargs['commit_id'] = commit.id
                    new_item = self.Relationship(**kwargs)
                    new_items.append(new_item)
            for id in self.new_item_id["parameter"]:
                item = self.session.query(self.DiffParameter).filter_by(id=id).one_or_none()
                kwargs = attr_dict(item)
                kwargs['commit_id'] = commit.id
                new_item = self.Parameter(**kwargs)
                new_items.append(new_item)
            for id in self.new_item_id["parameter_value"]:
                item = self.session.query(self.DiffParameterValue).filter_by(id=id).one_or_none()
                kwargs = attr_dict(item)
                kwargs['commit_id'] = commit.id
                new_item = self.ParameterValue(**kwargs)
                new_items.append(new_item)
            self.session.add_all(new_items)
            # Merge dirty
            dirty_items = list()
            for id in self.dirty_item_id["object_class"]:
                item = self.session.query(self.DiffObjectClass).filter_by(id=id).one_or_none()
                if not item:
                    continue
                kwargs = attr_dict(item)
                kwargs['commit_id'] = commit.id
                dirty_item = self.ObjectClass(**kwargs)
                dirty_items.append(dirty_item)
            for id in self.dirty_item_id["object"]:
                item = self.session.query(self.DiffObject).filter_by(id=id).one_or_none()
                if not item:
                    continue
                kwargs = attr_dict(item)
                kwargs['commit_id'] = commit.id
                dirty_item = self.Object(**kwargs)
                dirty_items.append(dirty_item)
            for id in self.dirty_item_id["relationship_class"]:
                for item in self.session.query(self.RelationshipClass).filter_by(id=id):
                    kwargs = attr_dict(item)
                    kwargs['commit_id'] = commit.id
                    dirty_item = self.RelationshipClass(**kwargs)
                    dirty_items.append(dirty_item)
            for id in self.dirty_item_id["relationship"]:
                for item in self.session.query(self.Relationship).filter_by(id=id):
                    kwargs = attr_dict(item)
                    kwargs['commit_id'] = commit.id
                    dirty_item = self.Relationship(**kwargs)
                    dirty_items.append(dirty_item)
            for id in self.dirty_item_id["parameter"]:
                item = self.session.query(self.DiffParameter).filter_by(id=id).one_or_none()
                kwargs = attr_dict(item)
                kwargs['commit_id'] = commit.id
                dirty_item = self.Parameter(**kwargs)
                dirty_items.append(dirty_item)
            for id in self.dirty_item_id["parameter_value"]:
                item = self.session.query(self.DiffParameterValue).filter_by(id=id).one_or_none()
                kwargs = attr_dict(item)
                kwargs['commit_id'] = commit.id
                dirty_item = self.ParameterValue(**kwargs)
                dirty_items.append(dirty_item)
            self.session.flush()
            for dirty_item in dirty_items:
                self.session.merge(dirty_item)
            # Remove removed
            removed_items = list()
            for id in self.removed_item_id["object_class"]:
                removed_item = self.session.query(self.ObjectClass).filter_by(id=id).one_or_none()
                removed_items.append(removed_item)
            for id in self.removed_item_id["object"]:
                item = self.session.query(self.Object).filter_by(id=id).one_or_none()
                removed_items.append(removed_item)
            for id in self.removed_item_id["relationship_class"]:
                for removed_item in self.session.query(self.RelationshipClass).filter_by(id=id):
                    removed_items.append(removed_item)
            for id in self.removed_item_id["relationship"]:
                for removed_item in self.session.query(self.Relationship).filter_by(id=id):
                    removed_items.append(removed_item)
            for id in self.removed_item_id["parameter"]:
                removed_item = self.session.query(self.Parameter).filter_by(id=id).one_or_none()
                removed_items.append(removed_item)
            for id in self.removed_item_id["parameter_value"]:
                removed_item = self.session.query(self.ParameterValue).filter_by(id=id).one_or_none()
                removed_items.append(removed_item)
            for removed_item in removed_items:
                self.session.delete(removed_item)
            self.session.commit()
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while commiting changes: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def rollback_session(self):
        # TODO: just reload the diff database, and clear all dicts
        pass
