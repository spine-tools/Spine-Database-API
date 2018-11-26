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
from .database_mapping import DatabaseMapping
from sqlalchemy import MetaData, Table, Column, Integer, String, func, or_, and_, event
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.exc import NoSuchTableError, DBAPIError
from sqlalchemy.sql.schema import UniqueConstraint, PrimaryKeyConstraint, ForeignKeyConstraint, CheckConstraint
from .exception import SpineDBAPIError, SpineTableNotFoundError, SpineIntegrityError
from .helpers import custom_generate_relationship, attr_dict
from datetime import datetime, timezone


# TODO: improve docstrings

class DiffDatabaseMapping(DatabaseMapping):
    """A class to handle changes made to a db in a graceful way.
    In a nutshell, it works by creating a new bunch of tables to hold differences
    with respect to original tables.
    """
    def __init__(self, db_url, username=None, create_all=True, warm_up=False):
        """Initialize class."""
        super().__init__(db_url, username=username, create_all=False)
        # Diff meta, Base and tables
        self.diff_prefix = None
        self.diff_metadata = None
        self.DiffBase = None
        self.DiffCommit = None
        self.DiffObjectClass = None
        self.DiffObject = None
        self.DiffRelationshipClass = None
        self.DiffRelationship = None
        self.DiffParameter = None
        self.DiffParameterValue = None
        self.NextId = None
        # Diff dictionaries
        self.new_item_id = {}
        self.dirty_item_id = {}
        self.removed_item_id = {}
        self.touched_item_id = {}
        # Initialize stuff
        self.init_diff_dicts()
        if create_all:
            self.create_engine_and_session()
            self.create_mapping()
            self.create_diff_tables_and_mapping()
            self.init_next_id()
            # self.create_triggers()
        # NOTE: this was intended to remove lag when running the first operation
        # But actually the lag was caused by orphan diff tables...
        if warm_up:
            user = self.username
            date = datetime.now(timezone.utc)
            comment = "warming up"
            diff_commit = self.DiffCommit(comment=comment, date=date, user=user)
            self.session.add(diff_commit)

    def has_pending_changes(self):
        """Return True if there are uncommitted changes. Otherwise return False."""
        if any([v for v in self.new_item_id.values()]):
            return True
        if any([v for v in self.touched_item_id.values()]):
            return True
        return False

    def init_diff_dicts(self):
        """Initialize dictionaries holding the differences."""
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
        # Items that we don't want to read from the original tables (either dirty, or removed)
        self.touched_item_id = {
            "object_class": set(),
            "object": set(),
            "relationship_class": set(),
            "relationship": set(),
            "parameter": set(),
            "parameter_value": set(),
        }

    def create_diff_tables_and_mapping(self):
        """Create tables to hold differences and the corresponding mapping using an automap_base."""
        # Tables...
        # TODO: handle the case where username is None
        self.diff_prefix = "diff_" + self.username + datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S") + "_"
        self.diff_metadata = MetaData()
        diff_tables = list()
        for t in self.Base.metadata.sorted_tables:
            if t.name.startswith("diff_" + self.username):
                continue
            if t.name == 'next_id':
                continue
            # Copy columns
            diff_columns = [c.copy() for c in t.columns]
            # Copy constraints.
            # NOTE: Is this needed? since we're going to do all checks by hand
            # TODO: check if there's a better, less hacky way.
            # TODO: Also beware of duplicating constraint names, might not work well
            diff_constraints = list()
            for constraint in t.constraints:
                if type(constraint) in (UniqueConstraint, CheckConstraint):
                    diff_constraints.append(constraint)
                elif type(constraint) == ForeignKeyConstraint:
                    foreign_key_constraint = ForeignKeyConstraint(
                        constraint.column_keys,
                        [self.diff_prefix + c.target_fullname for c in constraint.elements],
                        ondelete=constraint.ondelete,
                        onupdate=constraint.onupdate
                    )
                    diff_constraints.append(foreign_key_constraint)
            # Create table
            args = diff_columns + diff_constraints
            diff_table = Table(
                self.diff_prefix + t.name, self.diff_metadata, *args)
        self.diff_metadata.drop_all(self.engine)
        self.diff_metadata.create_all(self.engine)
        # Mapping...
        self.DiffBase = automap_base(metadata=self.diff_metadata)
        self.DiffBase.prepare(generate_relationship=custom_generate_relationship)
        try:
            self.DiffCommit = getattr(self.DiffBase.classes, self.diff_prefix + "commit")
            self.DiffObjectClass = getattr(self.DiffBase.classes, self.diff_prefix + "object_class")
            self.DiffObject = getattr(self.DiffBase.classes, self.diff_prefix + "object")
            self.DiffRelationshipClass = getattr(self.DiffBase.classes, self.diff_prefix + "relationship_class")
            self.DiffRelationship = getattr(self.DiffBase.classes, self.diff_prefix + "relationship")
            self.DiffParameter = getattr(self.DiffBase.classes, self.diff_prefix + "parameter")
            self.DiffParameterValue = getattr(self.DiffBase.classes, self.diff_prefix + "parameter_value")
        except NoSuchTableError as table:
            self.close()
            raise SpineTableNotFoundError(table)
        except AttributeError as table:
            self.close()
            raise SpineTableNotFoundError(table)

    def init_next_id(self):
        """Create next_id table if not exists and map it."""
        # TODO: Does this work? WHat happens if there's already a next_id table with a different definition?
        # Next id table
        metadata = MetaData()
        next_id_table = Table(
            "next_id", metadata,
            Column('user', String, primary_key=True),
            Column('date', String, primary_key=True),
            Column('object_class_id', Integer),
            Column('object_id', Integer),
            Column('relationship_class_id', Integer),
            Column('relationship_id', Integer),
            Column('parameter_id', Integer),
            Column('parameter_value_id', Integer)
        )
        next_id_table.create(self.engine, checkfirst=True)
        # Mapping...
        Base = automap_base(metadata=metadata)
        Base.prepare()
        try:
            self.NextId = Base.classes.next_id
        except NoSuchTableError as table:
            self.close()
            raise SpineTableNotFoundError(table)
        except AttributeError as table:
            self.close()
            raise SpineTableNotFoundError(table)

    def create_triggers(self):
        """Create ad-hoc triggers.
        NOTE: Not in use at the moment. Cascade delete is implemented in the `remove_items` method.
        TODO: is there a way to synch this with our CREATE TRIGGER statements
        from `helpers.create_new_spine_database`?
        """
        super().create_triggers()
        @event.listens_for(self.DiffObjectClass, 'after_delete')
        def receive_after_object_class_delete(mapper, connection, object_class):
            @event.listens_for(self.session, "after_flush", once=True)
            def receive_after_flush(session, context):
                id_list = session.query(self.DiffRelationshipClass.id).\
                    filter_by(object_class_id=object_class.id).distinct()
                item_list = session.query(self.DiffRelationshipClass).\
                    filter(self.DiffRelationshipClass.id.in_(id_list))
                for item in item_list:
                    session.delete(item)
        @event.listens_for(self.DiffObject, 'after_delete')
        def receive_after_object_delete(mapper, connection, object_):
            @event.listens_for(self.session, "after_flush", once=True)
            def receive_after_flush(session, context):
                id_list = session.query(self.DiffRelationship.id).filter_by(object_id=object_.id).distinct()
                item_list = session.query(self.DiffRelationship).filter(self.DiffRelationship.id.in_(id_list))
                for item in item_list:
                    session.delete(item)

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
        # TODO: If this method is identical to the one in the super class, just remove it
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
        # TODO: If this method is identical to the one in the super class, just remove it
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

    # TODO: Try and make some sense into these single_ methods...
    # Why are the arguments and behavior so irregular?
    def single_parameter(self, id=None, name=None):
        """Return parameter corresponding to id."""
        if id:
            return self.parameter_list().\
                filter(or_(self.Parameter.id == id, self.DiffParameter.id == id))
        if name:
            return self.parameter_list().\
                filter(or_(self.Parameter.name == name, self.DiffParameter.name == name))
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

    def object_class_list(self, id_list=set(), ordered=True):
        """Return object classes ordered by display order."""
        qry = super().object_class_list(id_list=id_list, ordered=False).\
            filter(~self.ObjectClass.id.in_(self.touched_item_id["object_class"]))
        diff_qry = self.session.query(
            self.DiffObjectClass.id.label("id"),
            self.DiffObjectClass.name.label("name"),
            self.DiffObjectClass.display_order.label("display_order"),
            self.DiffObjectClass.description.label("description"))
        if id_list:
            diff_qry = diff_qry.filter(self.DiffObjectClass.id.in_(id_list))
        qry = qry.union_all(diff_qry)
        if ordered:
            qry = qry.order_by(self.ObjectClass.display_order, self.DiffObjectClass.display_order)
        return qry

    def object_list(self, id_list=set(), class_id=None):
        """Return objects, optionally filtered by class id."""
        qry = super().object_list(id_list=id_list, class_id=class_id).\
            filter(~self.Object.id.in_(self.touched_item_id["object"]))
        diff_qry = self.session.query(
            self.DiffObject.id.label('id'),
            self.DiffObject.class_id.label('class_id'),
            self.DiffObject.name.label('name'),
            self.DiffObject.description.label("description"))
        if id_list:
            diff_qry = diff_qry.filter(self.DiffObject.id.in_(id_list))
        if class_id:
            diff_qry = diff_qry.filter_by(class_id=class_id)
        return qry.union_all(diff_qry)

    def relationship_class_list(self, id=None, ordered=True):
        """Return all relationship classes optionally filtered by id."""
        qry = super().relationship_class_list(id=id, ordered=False).\
            filter(~self.RelationshipClass.id.in_(self.touched_item_id["relationship_class"]))
        diff_qry = self.session.query(
            self.DiffRelationshipClass.id.label('id'),
            self.DiffRelationshipClass.dimension.label('dimension'),
            self.DiffRelationshipClass.object_class_id.label('object_class_id'),
            self.DiffRelationshipClass.name.label('name')
        )
        if id:
            diff_qry = diff_qry.filter_by(id=id)
        qry = qry.union_all(diff_qry)
        if ordered:
            qry = qry.order_by(
                self.RelationshipClass.id, self.RelationshipClass.dimension,
                self.DiffRelationshipClass.id, self.DiffRelationshipClass.dimension)
        return qry

    def wide_relationship_class_list(self, id_list=set(), object_class_id=None):
        """Return list of relationship classes in wide format involving a given object class."""
        object_class_list = self.object_class_list(ordered=False).subquery()
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
        if id_list:
            qry = qry.filter(self.RelationshipClass.id.in_(id_list))
            diff_qry = diff_qry.filter(self.DiffRelationshipClass.id.in_(id_list))
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

    def wide_relationship_list(self, id_list=set(), class_id=None, object_id=None):
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
        if id_list:
            qry = qry.filter(self.Relationship.id.in_(id_list))
            diff_qry = diff_qry.filter(self.DiffRelationship.id.in_(id_list))
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

    def parameter_list(self, id_list=set(), object_class_id=None, relationship_class_id=None):
        """Return parameters."""
        qry = super().parameter_list(
            id_list=id_list,
            object_class_id=object_class_id,
            relationship_class_id=relationship_class_id
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
        if id_list:
            diff_qry = diff_qry.filter(self.DiffParameter.id.in_(id_list))
        if object_class_id:
            diff_qry = diff_qry.filter_by(object_class_id=object_class_id)
        if relationship_class_id:
            diff_qry = diff_qry.filter_by(relationship_class_id=relationship_class_id)
        return qry.union_all(diff_qry)

    def object_parameter_list(self, object_class_id=None, parameter_id=None):
        """Return object classes and their parameters."""
        object_class_list = self.object_class_list().subquery()
        qry = self.session.query(
            self.Parameter.id.label('id'),
            object_class_list.c.id.label('object_class_id'),
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
            self.DiffParameter.id.label('id'),
            object_class_list.c.id.label('object_class_id'),
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
            self.Parameter.id.label('id'),
            wide_relationship_class_list.c.id.label('relationship_class_id'),
            wide_relationship_class_list.c.name.label('relationship_class_name'),
            wide_relationship_class_list.c.object_class_id_list,
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
            self.DiffParameter.id.label('id'),
            wide_relationship_class_list.c.id.label('relationship_class_id'),
            wide_relationship_class_list.c.name.label('relationship_class_name'),
            wide_relationship_class_list.c.object_class_id_list,
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

    def parameter_value_list(self, id_list=set(), object_id=None, relationship_id=None):
        """Return parameter values."""
        qry = super().parameter_value_list(
            id_list=id_list,
            object_id=object_id,
            relationship_id=relationship_id
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
        if id_list:
            diff_qry = diff_qry.filter(self.DiffParameterValue.id.in_(id_list))
        if object_id:
            diff_qry = diff_qry.filter_by(object_id=object_id)
        if relationship_id:
            diff_qry = diff_qry.filter_by(relationship_id=relationship_id)
        return qry.union_all(diff_qry)

    def object_parameter_value_list(self, object_class_id=None, parameter_name=None):
        """Return objects and their parameter values."""
        parameter_list = self.parameter_list().subquery()
        object_class_list = self.object_class_list().subquery()
        object_list = self.object_list().subquery()
        qry = self.session.query(
            self.ParameterValue.id.label('id'),
            object_class_list.c.id.label('object_class_id'),
            object_class_list.c.name.label('object_class_name'),
            object_list.c.id.label('object_id'),
            object_list.c.name.label('object_name'),
            parameter_list.c.id.label('parameter_id'),
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
            self.DiffParameterValue.id.label('id'),
            object_class_list.c.id.label('object_class_id'),
            object_class_list.c.name.label('object_class_name'),
            object_list.c.id.label('object_id'),
            object_list.c.name.label('object_name'),
            parameter_list.c.id.label('parameter_id'),
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
        filter(parameter_list.c.object_class_id == object_class_list.c.id)
        if object_class_id:
            qry = qry.filter(object_class_list.c.id == object_class_id)
            diff_qry = diff_qry.filter(object_class_list.c.id == object_class_id)
        if parameter_name:
            qry = qry.filter(parameter_list.c.name == parameter_name)
            diff_qry = diff_qry.filter(parameter_list.c.name == parameter_name)
        return qry.union_all(diff_qry)

    def relationship_parameter_value_list(self, relationship_class_id=None, parameter_name=None):
        """Return relationships and their parameter values."""
        parameter_list = self.parameter_list().subquery()
        wide_relationship_class_list = self.wide_relationship_class_list().subquery()
        wide_relationship_list = self.wide_relationship_list().subquery()
        qry = self.session.query(
            self.ParameterValue.id.label('id'),
            wide_relationship_class_list.c.id.label('relationship_class_id'),
            wide_relationship_class_list.c.name.label('relationship_class_name'),
            wide_relationship_class_list.c.object_class_id_list,
            wide_relationship_class_list.c.object_class_name_list,
            wide_relationship_list.c.id.label('relationship_id'),
            wide_relationship_list.c.object_id_list,
            wide_relationship_list.c.object_name_list,
            parameter_list.c.id.label('parameter_id'),
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
            self.DiffParameterValue.id.label('id'),
            wide_relationship_class_list.c.id.label('relationship_class_id'),
            wide_relationship_class_list.c.name.label('relationship_class_name'),
            wide_relationship_class_list.c.object_class_id_list,
            wide_relationship_class_list.c.object_class_name_list,
            wide_relationship_list.c.id.label('relationship_id'),
            wide_relationship_list.c.object_id_list,
            wide_relationship_list.c.object_name_list,
            parameter_list.c.id.label('parameter_id'),
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
        if relationship_class_id:
            qry = qry.filter(wide_relationship_class_list.c.id == relationship_class_id)
            diff_qry = diff_qry.filter(wide_relationship_class_list.c.id == relationship_class_id)
        if parameter_name:
            qry = qry.filter(parameter_list.c.name == parameter_name)
            diff_qry = diff_qry.filter(parameter_list.c.name == parameter_name)
        return qry.union_all(diff_qry)

    # TODO: Find out why we don't need to say, e.g., ~self.DiffParameter.id.in_(valued_parameter_ids)
    def unvalued_object_parameter_list(self, object_id):
        """Return parameters that do not have a value for given object."""
        object_ = self.single_object(id=object_id).one_or_none()
        if not object_:
            return self.empty_list()
        valued_parameters = self.parameter_value_list(object_id=object_id)
        return self.parameter_list(object_class_id=object_.class_id).\
            filter(~self.Parameter.id.in_([x.parameter_id for x in valued_parameters]))

    def unvalued_object_list(self, parameter_id):
        """Return objects for which given parameter does not have a value."""
        parameter = self.single_parameter(parameter_id).one_or_none()
        if not parameter:
            return self.empty_list()
        valued_object_ids = self.session.query(self.ParameterValue.object_id).\
            filter_by(parameter_id=parameter_id)
        return self.object_list().filter_by(class_id=parameter.object_class_id).\
            filter(~self.Object.id.in_(valued_object_ids))

    def unvalued_relationship_parameter_list(self, relationship_id):
        """Return parameters that do not have a value for given relationship."""
        relationship = self.single_wide_relationship(id=relationship_id).one_or_none()
        if not relationship:
            return self.empty_list()
        valued_parameters = self.parameter_value_list(relationship_id=relationship_id)
        return self.parameter_list(relationship_class_id=relationship.class_id).\
            filter(~self.Parameter.id.in_([x.parameter_id for x in valued_parameters]))

    def unvalued_relationship_list(self, parameter_id):
        """Return relationships for which given parameter does not have a value."""
        parameter = self.single_parameter(parameter_id).one_or_none()
        if not parameter:
            return self.empty_list()
        valued_relationship_ids = self.session.query(self.ParameterValue.relationship_id).\
            filter_by(parameter_id=parameter_id)
        return self.wide_relationship_list().filter_by(class_id=parameter.relationship_class_id).\
            filter(~self.Relationship.id.in_(valued_relationship_ids))

    def check_object_classes_for_insert(self, *kwargs_list):
        """Check that object classes respect integrity constraints for an insert operation."""
        checked_kwargs_list = list()
        object_class_list = [{"name": x.name} for x in self.object_class_list()]
        for kwargs in kwargs_list:
            self.check_object_class(kwargs, object_class_list)
            checked_kwargs_list.append(kwargs)
            # If the check passes, append kwargs to `object_class_list` for next iteration.
            object_class_list.append({"name": kwargs["name"]})
        return checked_kwargs_list

    def check_object_classes_for_update(self, *kwargs_list):
        """Check that object classes respect integrity constraints for an update operation."""
        # NOTE: To check for an update we basically 'remove' the current instance
        # and then check for an insert of the updated instance
        checked_kwargs_list = list()
        object_class_dict = {x.id: {"name": x.name} for x in self.object_class_list()}
        for kwargs in kwargs_list:
            try:
                id = kwargs["id"]
            except KeyError:
                raise SpineIntegrityError("Missing object class identifier.")
            try:
                # 'Remove' current instance
                updated_kwargs = object_class_dict.pop(id)
            except KeyError:
                raise SpineIntegrityError("Object class not found.")
            # Check for an insert of the updated instance
            updated_kwargs.update(kwargs)
            self.check_object_class(updated_kwargs, list(object_class_dict.values()))
            checked_kwargs_list.append(kwargs)
            # If the check passes, reinject the updated instance to `object_class_dict` for next iteration.
            object_class_dict[id] = updated_kwargs
        return checked_kwargs_list

    def check_object_class(self, kwargs, object_class_list):
        """Raise a SpineIntegrityError if the object class given by `kwargs` violates any
        integrity constraints.
        """
        try:
            name = kwargs["name"]
        except KeyError:
            raise SpineIntegrityError("Missing object class name.")
        if name in [x["name"] for x in object_class_list]:
            raise SpineIntegrityError("There can't be more than one object class called '{}'.".format(name))

    def check_objects_for_insert(self, *kwargs_list):
        """Check that objects respect integrity constraints for an insert operation."""
        checked_kwargs_list = list()
        object_list = [{"name": x.name} for x in self.object_list()]
        object_class_id_list = [x.id for x in self.object_class_list()]
        for kwargs in kwargs_list:
            self.check_object(kwargs, object_list, object_class_id_list)
            checked_kwargs_list.append(kwargs)
            object_list.append({"name": kwargs["name"]})
        return checked_kwargs_list

    def check_objects_for_update(self, *kwargs_list):
        """Check that objects respect integrity constraints for an update operation."""
        checked_kwargs_list = list()
        object_dict = {x.id: {"name": x.name, "class_id": x.class_id} for x in self.object_list()}
        object_class_id_list = [x.id for x in self.object_class_list()]
        for kwargs in kwargs_list:
            try:
                id = kwargs["id"]
            except KeyError:
                raise SpineIntegrityError("Missing object identifier.")
            try:
                updated_kwargs = object_dict.pop(id)
            except KeyError:
                raise SpineIntegrityError("Object not found.")
            updated_kwargs.update(kwargs)
            self.check_object(updated_kwargs, list(object_dict.values()), object_class_id_list)
            checked_kwargs_list.append(kwargs)
            object_dict[id] = updated_kwargs
        return checked_kwargs_list

    def check_object(self, kwargs, object_list, object_class_id_list):
        """Raise a SpineIntegrityError if the object given by `kwargs` violates any
        integrity constraints."""
        try:
            class_id = kwargs["class_id"]
        except KeyError:
            raise SpineIntegrityError("Missing object class identifier.")
        if class_id not in object_class_id_list:
            raise SpineIntegrityError("Object class not found.")
        try:
            name = kwargs["name"]
        except KeyError:
            raise SpineIntegrityError("Missing object name.")
        if name in [x["name"] for x in object_list]:
            raise SpineIntegrityError("There can't be more than one object called '{}'.".format(name))

    def check_wide_relationship_classes_for_insert(self, *wide_kwargs_list):
        """Check that relationship classes respect integrity constraints for an insert operation."""
        checked_wide_kwargs_list = list()
        relationship_class_list = [{"name": x.name} for x in self.wide_relationship_class_list()]
        object_class_id_list = [x.id for x in self.object_class_list()]
        for wide_kwargs in wide_kwargs_list:
            self.check_wide_relationship_class(wide_kwargs, relationship_class_list, object_class_id_list)
            checked_wide_kwargs_list.append(wide_kwargs)
            relationship_class_list.append({"name": wide_kwargs["name"]})
        return checked_wide_kwargs_list

    def check_wide_relationship_classes_for_update(self, *wide_kwargs_list):
        """Check that relationship classes respect integrity constraints for an update operation."""
        checked_wide_kwargs_list = list()
        relationship_class_dict = {
            x.id: {
                "name": x.name,
                "object_class_id_list": [int(y) for y in x.object_class_id_list.split(',')]
            } for x in self.wide_relationship_class_list()}
        object_class_id_list = [x.id for x in self.object_class_list()]
        for wide_kwargs in wide_kwargs_list:
            try:
                id = wide_kwargs["id"]
            except KeyError:
                raise SpineIntegrityError("Missing relationship class identifier.")
            try:
                updated_wide_kwargs = relationship_class_dict.pop(id)
            except KeyError:
                raise SpineIntegrityError("Relationship class not found.")
            updated_wide_kwargs.update(wide_kwargs)
            self.check_wide_relationship_class(
                updated_wide_kwargs, list(relationship_class_dict.values()), object_class_id_list)
            checked_wide_kwargs_list.append(wide_kwargs)
            relationship_class_dict[id] = updated_wide_kwargs
        return checked_wide_kwargs_list

    def check_wide_relationship_class(self, wide_kwargs, relationship_class_list, object_class_id_list):
        """Raise a SpineIntegrityError if the relationship class given by `kwargs` violates any
        integrity constraints."""
        try:
            given_object_class_id_list = wide_kwargs["object_class_id_list"]
        except KeyError:
            raise SpineIntegrityError("Missing object class identifier.")
        if len(given_object_class_id_list) < 2:
            raise SpineIntegrityError("At least two object classes are needed.")
        if not all([id in object_class_id_list for id in given_object_class_id_list]):
            raise SpineIntegrityError("Object class not found.")
        try:
            name = wide_kwargs["name"]
        except KeyError:
            raise SpineIntegrityError("Missing relationship class name.")
        if name in [x["name"] for x in relationship_class_list]:
            raise SpineIntegrityError("There can't be more than one relationship class called '{}'.".format(name))

    def check_wide_relationships_for_insert(self, *wide_kwargs_list):
        """Check that relationships respect integrity constraints for an insert operation."""
        checked_wide_kwargs_list = list()
        relationship_list = [
            {
                "class_id": x.class_id,
                "name": x.name,
                "object_id_list": [int(y) for y in x.object_id_list.split(',')]
            } for x in self.wide_relationship_list()
        ]
        relationship_class_dict = {
            x.id: [int(y) for y in x.object_class_id_list.split(',')] for x in self.wide_relationship_class_list()}
        object_dict = {x.id: x.class_id for x in self.object_list()}
        for wide_kwargs in wide_kwargs_list:
            self.check_wide_relationship(wide_kwargs, relationship_list, relationship_class_dict, object_dict)
            checked_wide_kwargs_list.append(wide_kwargs)
            relationship_list.append(wide_kwargs)
        return checked_wide_kwargs_list

    def check_wide_relationships_for_update(self, *wide_kwargs_list):
        """Check that relationships respect integrity constraints for an update operation."""
        checked_wide_kwargs_list = list()
        relationship_dict = {
            x.id: {
                "class_id": x.class_id,
                "name": x.name,
                "object_id_list": [int(y) for y in x.object_id_list.split(',')]
            } for x in self.wide_relationship_list()
        }
        relationship_class_dict = {
            x.id: [int(y) for y in x.object_class_id_list.split(',')] for x in self.wide_relationship_class_list()}
        object_dict = {x.id: x.class_id for x in self.object_list()}
        for wide_kwargs in wide_kwargs_list:
            try:
                id = wide_kwargs["id"]
            except KeyError:
                raise SpineIntegrityError("Missing relationship identifier.")
            try:
                updated_wide_kwargs = relationship_dict.pop(id)
            except KeyError:
                raise SpineIntegrityError("Relationship not found.")
            updated_wide_kwargs.update(wide_kwargs)
            self.check_wide_relationship(
                updated_wide_kwargs, list(relationship_dict.values()),
                relationship_class_dict, object_dict)
            checked_wide_kwargs_list.append(wide_kwargs)
            relationship_dict[id] = updated_wide_kwargs
        return checked_wide_kwargs_list

    def check_wide_relationship(self, wide_kwargs, relationship_list, relationship_class_dict, object_dict):
        """Raise a SpineIntegrityError if the relationship given by `kwargs` violates any integrity constraints."""
        try:
            class_id = wide_kwargs['class_id']
        except KeyError:
            raise SpineIntegrityError("Missing relationship class identifier.")
        try:
            object_class_id_list = relationship_class_dict[class_id]
        except KeyError:
            raise SpineIntegrityError("Relationship class not found.")
        try:
            object_id_list = wide_kwargs['object_id_list']
        except KeyError:
            raise SpineIntegrityError("Missing object identifier.")
        try:
            given_object_class_id_list = [object_dict[id] for id in object_id_list]
        except KeyError:
            raise SpineIntegrityError("Object not found.")
        if given_object_class_id_list != object_class_id_list:
            raise SpineIntegrityError("Incorrect objects for this relationship class.")
        if len(object_id_list) != len(set(object_id_list)):
            raise SpineIntegrityError("The same object can't appear twice in one relationship.")
        if (class_id, object_id_list) in [(x["class_id"], x["object_id_list"]) for x in relationship_list]:
            raise SpineIntegrityError("There can't be more than one relationship between the same objects "
                                      "in one class.")
        try:
            name = wide_kwargs["name"]
        except KeyError:
            raise SpineIntegrityError("Missing relationship name.")
        if name in [x["name"] for x in relationship_list]:
            raise SpineIntegrityError("There can't be more than one relationship called '{}'.".format(name))

    def check_parameters_for_insert(self, *kwargs_list):
        """Check that parameters respect integrity constraints for an insert operation."""
        checked_kwargs_list = list()
        parameter_list = [{"name": x.name} for x in self.parameter_list()]
        object_class_id_list = [x.id for x in self.object_class_list()]
        relationship_class_id_list = [x.id for x in self.wide_relationship_class_list()]
        for kwargs in kwargs_list:
            self.check_parameter(kwargs, parameter_list, object_class_id_list, relationship_class_id_list)
            checked_kwargs_list.append(kwargs)
            parameter_list.append({"name": kwargs["name"]})
        return checked_kwargs_list

    def check_parameters_for_update(self, *kwargs_list):
        """Check that parameters respect integrity constraints for an update operation."""
        checked_kwargs_list = list()
        parameter_dict = {
            x.id: {
                "name": x.name,
                "object_class_id": x.object_class_id,
                "relationship_class_id": x.relationship_class_id
            } for x in self.parameter_list()}
        object_class_id_list = [x.id for x in self.object_class_list()]
        relationship_class_id_list = [x.id for x in self.wide_relationship_class_list()]
        for kwargs in kwargs_list:
            try:
                id = kwargs["id"]
            except KeyError:
                raise SpineIntegrityError("Missing parameter identifier.")
            try:
                updated_kwargs = parameter_dict.pop(id)
            except KeyError:
                raise SpineIntegrityError("Parameter not found.")
            # Allow turning an object class parameter into a relationship class parameter, and viceversa
            if "object_class_id" in kwargs:
                kwargs.setdefault("relationship_class_id", None)
            if "relationship_class_id" in kwargs:
                kwargs.setdefault("object_class_id", None)
            updated_kwargs.update(kwargs)
            self.check_parameter(
                updated_kwargs, list(parameter_dict.values()),
                object_class_id_list, relationship_class_id_list)
            checked_kwargs_list.append(kwargs)
            parameter_dict[id] = updated_kwargs
        return checked_kwargs_list

    def check_parameter(self, kwargs, parameter_list, object_class_id_list, relationship_class_id_list):
        """Raise a SpineIntegrityError if the parameter given by `kwargs` violates any integrity constraints."""
        object_class_id = kwargs.get("object_class_id", None)
        relationship_class_id = kwargs.get("relationship_class_id", None)
        if object_class_id and relationship_class_id:
            raise SpineIntegrityError("Can't associate a parameter to both an object class and a relationship class.")
        if object_class_id:
            if object_class_id not in object_class_id_list:
                raise SpineIntegrityError("Object class not found.")
            try:
                name = kwargs["name"]
            except KeyError:
                raise SpineIntegrityError("Missing parameter name.")
            if name in [x["name"] for x in parameter_list]:
                raise SpineIntegrityError("There can't be more than one parameter called '{}'.".format(name))
        elif relationship_class_id:
            if relationship_class_id not in relationship_class_id_list:
                raise SpineIntegrityError("Relationship class not found.")
            try:
                name = kwargs["name"]
            except KeyError:
                raise SpineIntegrityError("Missing parameter name.")
            if name in [x["name"] for x in parameter_list]:
                raise SpineIntegrityError("There can't be more than one parameter called '{}'.".format(name))
        else:
            raise SpineIntegrityError("Missing object class or relationship class identifier.")

    def check_parameter_values_for_insert(self, *kwargs_list):
        """Check that parameter values respect integrity constraints for an insert operation."""
        checked_kwargs_list = list()
        parameter_value_list = [
            {
                "parameter_id": x.parameter_id,
                "object_id": x.object_id,
                "relationship_id": x.relationship_id
            } for x in self.parameter_value_list()]
        parameter_dict = {
            x.id: {
                "object_class_id": x.object_class_id,
                "relationship_class_id": x.relationship_class_id
            } for x in self.parameter_list()}
        object_dict = {x.id: x.class_id for x in self.object_list()}
        relationship_dict = {x.id: x.class_id for x in self.wide_relationship_list()}
        for kwargs in kwargs_list:
            self.check_parameter_value(kwargs, parameter_value_list, parameter_dict, object_dict, relationship_dict)
            checked_kwargs_list.append(kwargs)
            parameter_value_list.append(kwargs)
        return checked_kwargs_list

    def check_parameter_values_for_update(self, *kwargs_list):
        """Check that parameter values respect integrity constraints for an insert operation."""
        checked_kwargs_list = list()
        parameter_value_dict = {
            x.id: {
                "parameter_id": x.parameter_id,
                "object_id": x.object_id,
                "relationship_id": x.relationship_id
            } for x in self.parameter_value_list()}
        parameter_dict = {
            x.id: {
                "object_class_id": x.object_class_id,
                "relationship_class_id": x.relationship_class_id
            } for x in self.parameter_list()}
        object_dict = {x.id: x.class_id for x in self.object_list()}
        relationship_dict = {x.id: x.class_id for x in self.wide_relationship_list()}
        for kwargs in kwargs_list:
            try:
                id = kwargs["id"]
            except KeyError:
                raise SpineIntegrityError("Missing parameter value identifier.")
            try:
                updated_kwargs = parameter_value_dict.pop(id)
            except KeyError:
                raise SpineIntegrityError("Parameter value not found.")
            # Allow turning an object parameter value into a relationship parameter value, and viceversa
            if "object_id" in kwargs:
                kwargs.setdefault("relationship_id", None)
            if "relationship_id" in kwargs:
                kwargs.setdefault("object_id", None)
            updated_kwargs.update(kwargs)
            self.check_parameter_value(
                updated_kwargs, list(parameter_value_dict.values()),
                parameter_dict, object_dict, relationship_dict)
            checked_kwargs_list.append(kwargs)
            parameter_value_dict[id] = updated_kwargs
        return checked_kwargs_list

    def check_parameter_value(self, kwargs, parameter_value_list, parameter_dict, object_dict, relationship_dict):
        """Raise a SpineIntegrityError if the parameter value given by `kwargs` violates any integrity constraints."""
        try:
            parameter_id = kwargs["parameter_id"]
        except KeyError:
            raise SpineIntegrityError("Missing parameter identifier.")
        try:
            parameter = parameter_dict[parameter_id]
        except KeyError:
            raise SpineIntegrityError("Parameter not found.")
        object_id = kwargs.get("object_id", None)
        relationship_id = kwargs.get("relationship_id", None)
        if object_id and relationship_id:
            raise SpineIntegrityError("Can't associate a parameter value to both an object and a relationship.")
        if object_id:
            try:
                object_class_id = object_dict[object_id]
            except KeyError:
                raise SpineIntegrityError("Object not found")
            if object_class_id != parameter["object_class_id"]:
                raise SpineIntegrityError("Incorrect object for this parameter.")
            if (object_id, parameter_id) in [(x["object_id"], x["parameter_id"]) for x in parameter_value_list]:
                raise SpineIntegrityError("The value of this parameter is already specified for this object.")
        elif relationship_id:
            try:
                relationship_class_id = relationship_dict[relationship_id]
            except KeyError:
                raise SpineIntegrityError("Relationship not found")
            if relationship_class_id != parameter["relationship_class_id"]:
                raise SpineIntegrityError("Incorrect relationship for this parameter.")
            relationship_parameter_list = [(x["relationship_id"], x["parameter_id"]) for x in parameter_value_list]
            if (relationship_id, parameter_id) in relationship_parameter_list:
                raise SpineIntegrityError("The value of this parameter is already specified "
                                          "for this relationship.")
        else:
            raise SpineIntegrityError("Missing object or relationship identifier.")

    def next_id_with_lock(self):
        """A 'next_id' item to use for adding new items."""
        next_id = self.session.query(self.NextId).one_or_none()
        if next_id:
            next_id.user = self.username
            next_id.date = datetime.now(timezone.utc)
        else:
            next_id = self.NextId(
                user = self.username,
                date = datetime.now(timezone.utc)
            )
            self.session.add(next_id)
        try:
            # TODO: This flush is supposed to lock the database, so no one can steal our ids.... does it work?
            self.session.flush()
            return next_id
        except DBAPIError as e:
            # TODO: Find a way to try this again, or wait till the database is unlocked
            # Maybe listen for an event?
            self.session.rollback()
            raise SpineDBAPIError("Unable to get next id: {}".format(e.orig.args))

    def add_object_class(self, **kwargs):
        return self.add_object_classes(kwargs).one_or_none()

    def add_object(self, **kwargs):
        return self.add_objects(kwargs).one_or_none()

    def add_wide_relationship_class(self, **kwargs):
        return self.add_wide_relationship_classes(kwargs).one_or_none()

    def add_wide_relationship(self, **kwargs):
        return self.add_wide_relationships(kwargs).one_or_none()

    def add_parameter(self, **kwargs):
        return self.add_parameters(kwargs).one_or_none()

    def add_parameter_value(self, **kwargs):
        return self.add_parameter_values(kwargs).one_or_none()

    def add_object_classes(self, *kwargs_list):
        """Add object classes to database.

        Returns:
            object_classes (lists)
        """
        checked_kwargs_list = self.check_object_classes_for_insert(*kwargs_list)
        next_id = self.next_id_with_lock()
        if next_id.object_class_id:
            id = next_id.object_class_id
        else:
            max_id = self.session.query(func.max(self.ObjectClass.id)).scalar()
            id = max_id + 1 if max_id else 1
        try:
            item_list = list()
            id_list = set(range(id, id + len(checked_kwargs_list)))
            for kwargs in checked_kwargs_list:
                kwargs["id"] = id
                item_list.append(kwargs)
                id += 1
            self.session.bulk_insert_mappings(self.DiffObjectClass, item_list)
            next_id.object_class_id = id
            self.session.commit()
            self.new_item_id["object_class"].update(id_list)
            return self.object_class_list(id_list=id_list)
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while inserting object class: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def add_objects(self, *kwargs_list):
        """Add objects to database.

        Returns:
            objects (list)
        """
        checked_kwargs_list = self.check_objects_for_insert(*kwargs_list)
        next_id = self.next_id_with_lock()
        if next_id.object_id:
            id = next_id.object_id
        else:
            max_id = self.session.query(func.max(self.Object.id)).scalar()
            id = max_id + 1 if max_id else 1
        try:
            item_list = list()
            id_list = set(range(id, id + len(checked_kwargs_list)))
            for kwargs in checked_kwargs_list:
                kwargs["id"] = id
                item_list.append(kwargs)
                id += 1
            self.session.bulk_insert_mappings(self.DiffObject, item_list)
            next_id.object_id = id
            self.session.commit()
            self.new_item_id["object"].update(id_list)
            return self.object_list(id_list=id_list)
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while inserting object: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def add_wide_relationship_classes(self, *wide_kwargs_list):
        """Add relationship classes to database.

        Returns:
            wide_relationship_classes (list)
        """
        checked_wide_kwargs_list = self.check_wide_relationship_classes_for_insert(*wide_kwargs_list)
        next_id = self.next_id_with_lock()
        if next_id.relationship_class_id:
            id = next_id.relationship_class_id
        else:
            max_id = self.session.query(func.max(self.RelationshipClass.id)).scalar()
            id = max_id + 1 if max_id else 1
        try:
            item_list = list()
            id_list = set(range(id, id + len(checked_wide_kwargs_list)))
            for wide_kwargs in checked_wide_kwargs_list:
                for dimension, object_class_id in enumerate(wide_kwargs['object_class_id_list']):
                    narrow_kwargs = {
                        'id': id,
                        'dimension': dimension,
                        'object_class_id': object_class_id,
                        'name': wide_kwargs['name']
                    }
                    item_list.append(narrow_kwargs)
                id += 1
            self.session.bulk_insert_mappings(self.DiffRelationshipClass, item_list)
            next_id.relationship_class_id = id
            self.session.commit()
            self.new_item_id["relationship_class"].update(id_list)
            return self.wide_relationship_class_list(id_list=id_list)
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while inserting relationship class: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def add_wide_relationships(self, *wide_kwargs_list):
        """Add relationships to database.

        Returns:
            wide_relationships (list)
        """
        checked_wide_kwargs_list = self.check_wide_relationships_for_insert(*wide_kwargs_list)
        next_id = self.next_id_with_lock()
        if next_id.relationship_id:
            id = next_id.relationship_id
        else:
            max_id = self.session.query(func.max(self.Relationship.id)).scalar()
            id = max_id + 1 if max_id else 1
        try:
            item_list = list()
            id_list = set(range(id, id + len(checked_wide_kwargs_list)))
            for wide_kwargs in checked_wide_kwargs_list:
                for dimension, object_id in enumerate(wide_kwargs['object_id_list']):
                    narrow_kwargs = {
                        'id': id,
                        'class_id': wide_kwargs['class_id'],
                        'dimension': dimension,
                        'object_id': object_id,
                        'name': wide_kwargs['name']
                    }
                    item_list.append(narrow_kwargs)
                id += 1
            self.session.bulk_insert_mappings(self.DiffRelationship, item_list)
            next_id.relationship_id = id
            self.session.commit()
            self.new_item_id["relationship"].update(id_list)
            return self.wide_relationship_list(id_list=id_list)
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while inserting relationship: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def add_parameters(self, *kwargs_list):
        """Add parameter to database.

        Returns:
            An instance of self.Parameter if successful, None otherwise
        """
        checked_kwargs_list = self.check_parameters_for_insert(*kwargs_list)
        next_id = self.next_id_with_lock()
        if next_id.parameter_id:
            id = next_id.parameter_id
        else:
            max_id = self.session.query(func.max(self.Parameter.id)).scalar()
            id = max_id + 1 if max_id else 1
        try:
            item_list = list()
            id_list = set(range(id, id + len(checked_kwargs_list)))
            for kwargs in checked_kwargs_list:
                kwargs["id"] = id
                item_list.append(kwargs)
                id += 1
            self.session.bulk_insert_mappings(self.DiffParameter, item_list)
            next_id.parameter_id = id
            self.session.commit()
            self.new_item_id["parameter"].update(id_list)
            return self.parameter_list(id_list=id_list)
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while inserting parameters: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def add_parameter_values(self, *kwargs_list):
        """Add parameter value to database.

        Returns:
            An instance of self.ParameterValue if successful, None otherwise
        """
        checked_kwargs_list = self.check_parameter_values_for_insert(*kwargs_list)
        next_id = self.next_id_with_lock()
        if next_id.parameter_value_id:
            id = next_id.parameter_value_id
        else:
            max_id = self.session.query(func.max(self.ParameterValue.id)).scalar()
            id = max_id + 1 if max_id else 1
        try:
            item_list = list()
            id_list = set(range(id, id + len(checked_kwargs_list)))
            for kwargs in checked_kwargs_list:
                kwargs["id"] = id
                item_list.append(kwargs)
                id += 1
            self.session.bulk_insert_mappings(self.DiffParameterValue, item_list)
            next_id.parameter_value_id = id
            self.session.commit()
            self.new_item_id["parameter_value"].update(id_list)
            return self.parameter_value_list(id_list=id_list)
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while inserting parameter values: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def get_or_add_object_class(self, **kwargs):
        """Add object class to database if not exists.

        Returns:
            A dict if successful, None otherwise
        """
        if "name" not in kwargs:
            return None
        object_class = self.single_object_class(name=kwargs["name"]).one_or_none()
        if object_class:
            return object_class
        return self.add_object_classes(kwargs).one_or_none()

    def get_or_add_wide_relationship_class(self, **wide_kwargs):
        """Add relationship class to database if not exists.

        Returns:
            A dict if successful, None otherwise
        """
        if "name" not in wide_kwargs or "object_class_id_list" not in wide_kwargs:
            return None
        wide_relationship_class = self.single_wide_relationship_class(name=wide_kwargs["name"]).one_or_none()
        if not wide_relationship_class:
            return self.add_wide_relationship_classes(wide_kwargs).one_or_none()
        given_object_class_id_list = [int(x) for x in wide_kwargs["object_class_id_list"]]
        found_object_class_id_list = [int(x) for x in wide_relationship_class.object_class_id_list.split(",")]
        if given_object_class_id_list != found_object_class_id_list:
            return None  # TODO: should we raise an error here?
        return wide_relationship_class

    def get_or_add_parameter(self, **kwargs):
        """Add parameter to database if not exists.

        Returns:
            A dict if successful, None otherwise
        """
        if "name" not in kwargs:
            return None
        parameter = self.single_parameter(name=kwargs["name"]).one_or_none()
        if parameter:
            return parameter
        return self.add_parameters(kwargs).one_or_none()

    def update_object_classes(self, *kwargs_list):
        """Update object classes."""
        checked_kwargs_list = self.check_object_classes_for_update(*kwargs_list)
        try:
            items_for_update = list()
            items_for_insert = list()
            new_dirty_ids = set()
            updated_ids = set()
            for kwargs in checked_kwargs_list:
                try:
                    id = kwargs['id']
                except KeyError:
                    continue
                diff_item = self.session.query(self.DiffObjectClass).filter_by(id=id).one_or_none()
                if diff_item:
                    updated_kwargs = attr_dict(diff_item)
                    updated_kwargs.update(kwargs)
                    items_for_update.append(updated_kwargs)
                    updated_ids.add(id)
                else:
                    item = self.session.query(self.ObjectClass).filter_by(id=id).one_or_none()
                    if item:
                        updated_kwargs = attr_dict(item)
                        updated_kwargs.update(kwargs)
                        items_for_insert.append(updated_kwargs)
                        new_dirty_ids.add(id)
                        updated_ids.add(id)
            self.session.bulk_update_mappings(self.DiffObjectClass, items_for_update)
            self.session.bulk_insert_mappings(self.DiffObjectClass, items_for_insert)
            self.session.commit()
            self.touched_item_id["object_class"].update(new_dirty_ids)
            self.dirty_item_id["object_class"].update(new_dirty_ids)
            return self.object_class_list(id_list=updated_ids)
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while updating object classes: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def update_objects(self, *kwargs_list):
        """Update objects."""
        checked_kwargs_list = self.check_objects_for_update(*kwargs_list)
        try:
            items_for_update = list()
            items_for_insert = list()
            new_dirty_ids = set()
            updated_ids = set()
            for kwargs in checked_kwargs_list:
                if "class_id" in kwargs:
                    continue
                try:
                    id = kwargs['id']
                except KeyError:
                    continue
                diff_item = self.session.query(self.DiffObject).filter_by(id=id).one_or_none()
                if diff_item:
                    updated_kwargs = attr_dict(diff_item)
                    updated_kwargs.update(kwargs)
                    items_for_update.append(updated_kwargs)
                    updated_ids.add(id)
                else:
                    item = self.session.query(self.Object).filter_by(id=id).one_or_none()
                    if item:
                        updated_kwargs = attr_dict(item)
                        updated_kwargs.update(kwargs)
                        items_for_insert.append(updated_kwargs)
                        new_dirty_ids.add(id)
                        updated_ids.add(id)
            self.session.bulk_update_mappings(self.DiffObject, items_for_update)
            self.session.bulk_insert_mappings(self.DiffObject, items_for_insert)
            self.session.commit()
            self.touched_item_id["object"].update(new_dirty_ids)
            self.dirty_item_id["object"].update(new_dirty_ids)
            return self.object_list(id_list=updated_ids)
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while updating objects: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def update_wide_relationship_classes(self, *wide_kwargs_list):
        """Update relationship classes."""
        checked_wide_kwargs_list = self.check_wide_relationship_classes_for_update(*wide_kwargs_list)
        try:
            items_for_update = list()
            items_for_insert = list()
            new_dirty_ids = set()
            updated_ids = set()
            for wide_kwargs in checked_wide_kwargs_list:
                # Don't update object_class_id for now (even though below we handle it)
                if "object_class_id_list" in wide_kwargs:
                    continue
                try:
                    id = wide_kwargs['id']
                except KeyError:
                    continue
                object_class_id_list = wide_kwargs.pop('object_class_id_list', list())
                diff_item_list = self.session.query(self.DiffRelationshipClass).filter_by(id=id)
                if diff_item_list.count():
                    for dimension, diff_item in enumerate(diff_item_list):
                        narrow_kwargs = wide_kwargs
                        try:
                            narrow_kwargs.update({'object_class_id': object_class_id_list[dimension]})
                        except IndexError:
                            pass
                        updated_kwargs = attr_dict(diff_item)
                        updated_kwargs.update(narrow_kwargs)
                        items_for_update.append(updated_kwargs)
                    updated_ids.add(id)
                else:
                    item_list = self.session.query(self.RelationshipClass).filter_by(id=id)
                    if item_list.count():
                        for dimension, item in enumerate(item_list):
                            narrow_kwargs = wide_kwargs
                            try:
                                narrow_kwargs.update({'object_class_id': object_class_id_list[dimension]})
                            except IndexError:
                                pass
                            updated_kwargs = attr_dict(item)
                            updated_kwargs.update(narrow_kwargs)
                            items_for_insert.append(updated_kwargs)
                        new_dirty_ids.add(id)
                        updated_ids.add(id)
            self.session.bulk_update_mappings(self.DiffRelationshipClass, items_for_update)
            self.session.bulk_insert_mappings(self.DiffRelationshipClass, items_for_insert)
            self.session.commit()
            self.touched_item_id["relationship_class"].update(new_dirty_ids)
            self.dirty_item_id["relationship_class"].update(new_dirty_ids)
            return self.wide_relationship_class_list(id_list=updated_ids)
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while updating relationship classes: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def update_wide_relationships(self, *wide_kwargs_list):
        """Update relationships."""
        checked_wide_kwargs_list = self.check_wide_relationships_for_update(*wide_kwargs_list)
        try:
            items_for_update = list()
            items_for_insert = list()
            new_dirty_ids = set()
            updated_ids = set()
            for wide_kwargs in checked_wide_kwargs_list:
                if "class_id" in wide_kwargs:
                    continue
                try:
                    id = wide_kwargs['id']
                except KeyError:
                    continue
                object_id_list = wide_kwargs.pop('object_id_list', list())
                diff_item_list = self.session.query(self.DiffRelationship).filter_by(id=id).\
                    order_by(self.DiffRelationship.dimension)
                if diff_item_list.count():
                    for dimension, diff_item in enumerate(diff_item_list):
                        narrow_kwargs = wide_kwargs
                        try:
                            narrow_kwargs.update({'object_id': object_id_list[dimension]})
                        except IndexError:
                            pass
                        updated_kwargs = attr_dict(diff_item)
                        updated_kwargs.update(narrow_kwargs)
                        items_for_update.append(updated_kwargs)
                    updated_ids.add(id)
                else:
                    item_list = self.session.query(self.Relationship).filter_by(id=id)
                    if item_list.count():
                        for dimension, item in enumerate(item_list):
                            narrow_kwargs = wide_kwargs
                            try:
                                narrow_kwargs.update({'object_id': object_id_list[dimension]})
                            except IndexError:
                                pass
                            updated_kwargs = attr_dict(item)
                            updated_kwargs.update(narrow_kwargs)
                            items_for_insert.append(updated_kwargs)
                        new_dirty_ids.add(id)
                        updated_ids.add(id)
            self.session.bulk_update_mappings(self.DiffRelationship, items_for_update)
            self.session.bulk_insert_mappings(self.DiffRelationship, items_for_insert)
            self.session.commit()
            self.touched_item_id["relationship"].update(new_dirty_ids)
            self.dirty_item_id["relationship"].update(new_dirty_ids)
            return self.wide_relationship_list(id_list=updated_ids)
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while updating relationships: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def update_parameters(self, *kwargs_list):
        """Update parameters."""
        checked_kwargs_list = self.check_parameters_for_update(*kwargs_list)
        try:
            items_for_update = list()
            items_for_insert = list()
            new_dirty_ids = set()
            updated_ids = set()
            for kwargs in checked_kwargs_list:
                if "object_class_id" in kwargs or "relationship_class_id" in kwargs:
                    continue
                try:
                    id = kwargs['id']
                except KeyError:
                    continue
                diff_item = self.session.query(self.DiffParameter).filter_by(id=id).one_or_none()
                if diff_item:
                    updated_kwargs = attr_dict(diff_item)
                    updated_kwargs.update(kwargs)
                    items_for_update.append(updated_kwargs)
                    updated_ids.add(id)
                else:
                    item = self.session.query(self.Parameter).filter_by(id=id).one_or_none()
                    if item:
                        updated_kwargs = attr_dict(item)
                        updated_kwargs.update(kwargs)
                        items_for_insert.append(updated_kwargs)
                        new_dirty_ids.add(id)
                        updated_ids.add(id)
            self.session.bulk_update_mappings(self.DiffParameter, items_for_update)
            self.session.bulk_insert_mappings(self.DiffParameter, items_for_insert)
            self.session.commit()
            self.touched_item_id["parameter"].update(new_dirty_ids)
            self.dirty_item_id["parameter"].update(new_dirty_ids)
            return self.parameter_list(id_list=updated_ids)
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while updating parameters: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def update_parameter_values(self, *kwargs_list):
        """Update parameter values."""
        checked_kwargs_list = self.check_parameter_values_for_update(*kwargs_list)
        try:
            items_for_update = list()
            items_for_insert = list()
            new_dirty_ids = set()
            updated_ids = set()
            for kwargs in checked_kwargs_list:
                if "object_id" in kwargs or "relationship_id" in kwargs or "parameter_id" in kwargs:
                    continue
                try:
                    id = kwargs['id']
                except KeyError:
                    continue
                diff_item = self.session.query(self.DiffParameterValue).filter_by(id=id).one_or_none()
                if diff_item:
                    updated_kwargs = attr_dict(diff_item)
                    updated_kwargs.update(kwargs)
                    items_for_update.append(updated_kwargs)
                    updated_ids.add(id)
                else:
                    item = self.session.query(self.ParameterValue).filter_by(id=id).one_or_none()
                    if item:
                        updated_kwargs = attr_dict(item)
                        updated_kwargs.update(kwargs)
                        items_for_insert.append(updated_kwargs)
                        new_dirty_ids.add(id)
                        updated_ids.add(id)
            self.session.bulk_update_mappings(self.DiffParameterValue, items_for_update)
            self.session.bulk_insert_mappings(self.DiffParameterValue, items_for_insert)
            self.session.commit()
            self.touched_item_id["parameter_value"].update(new_dirty_ids)
            self.dirty_item_id["parameter_value"].update(new_dirty_ids)
            return self.parameter_value_list(id_list=updated_ids)
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while updating parameter values: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    # NOTE: OBSOLETE
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
                kwargs['name'] = new_name  # NOTE: we want to preserve the id here
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

    # NOTE: OBSOLETE
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

    # NOTE: OBSOLETE
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

    # NOTE: OBSOLETE
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

    # NOTE: OBSOLETE
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

    # NOTE: OBSOLETE
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
            return self.single_parameter_value(id=id).one_or_none()
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while updating parameter value: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def remove_items(
            self,
            object_class_ids=set(),
            object_ids=set(),
            relationship_class_ids=set(),
            relationship_ids=set(),
            parameter_ids=set(),
            parameter_value_ids=set()
        ):
        """Remove items."""
        removed_item_id, removed_diff_item_id = self.removed_items(
            object_class_ids=object_class_ids,
            object_ids=object_ids,
            relationship_class_ids=relationship_class_ids,
            relationship_ids=relationship_ids,
            parameter_ids=parameter_ids,
            parameter_value_ids=parameter_value_ids)
        diff_ids = removed_diff_item_id.get('object_class', set())
        self.session.query(self.DiffObjectClass).filter(self.DiffObjectClass.id.in_(diff_ids)).\
            delete(synchronize_session=False)
        diff_ids = removed_diff_item_id.get('object', set())
        self.session.query(self.DiffObject).filter(self.DiffObject.id.in_(diff_ids)).\
            delete(synchronize_session=False)
        diff_ids = removed_diff_item_id.get('relationship_class', set())
        self.session.query(self.DiffRelationshipClass).filter(self.DiffRelationshipClass.id.in_(diff_ids)).\
            delete(synchronize_session=False)
        diff_ids = removed_diff_item_id.get('relationship', set())
        self.session.query(self.DiffRelationship).filter(self.DiffRelationship.id.in_(diff_ids)).\
            delete(synchronize_session=False)
        diff_ids = removed_diff_item_id.get('parameter', set())
        self.session.query(self.DiffParameter).filter(self.DiffParameter.id.in_(diff_ids)).\
            delete(synchronize_session=False)
        diff_ids = removed_diff_item_id.get('parameter_value', set())
        self.session.query(self.DiffParameterValue).filter(self.DiffParameterValue.id.in_(diff_ids)).\
            delete(synchronize_session=False)
        try:
            self.session.commit()
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while removing items: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)
        for key, value in removed_item_id.items():
            self.removed_item_id[key].update(value)
            self.touched_item_id[key].update(value)

    def removed_items(
            self,
            object_class_ids=set(),
            object_ids=set(),
            relationship_class_ids=set(),
            relationship_ids=set(),
            parameter_ids=set(),
            parameter_value_ids=set()
        ):
        """Return items to be removed due to the removal of items given as arguments.

        Returns:
            removed_item_id (dict): removed items in the original tables
            removed_diff_item_id (dict): removed items in the difference tables
        """
        removed_item_id = {}
        removed_diff_item_id = {}
        # object_class
        item_list = self.session.query(self.ObjectClass.id).filter(self.ObjectClass.id.in_(object_class_ids))
        diff_item_list = self.session.query(self.DiffObjectClass.id).\
            filter(self.DiffObjectClass.id.in_(object_class_ids))
        self.remove_cascade_object_classes(
            [x.id for x in item_list],
            [x.id for x in diff_item_list],
            removed_item_id,
            removed_diff_item_id)
        # object
        item_list = self.session.query(self.Object.id).filter(self.Object.id.in_(object_ids))
        diff_item_list = self.session.query(self.DiffObject.id).filter(self.DiffObject.id.in_(object_ids))
        self.remove_cascade_objects(
            [x.id for x in item_list],
            [x.id for x in diff_item_list],
            removed_item_id,
            removed_diff_item_id)
        # relationship_class
        item_list = self.session.query(self.RelationshipClass.id).\
            filter(self.RelationshipClass.id.in_(relationship_class_ids))
        diff_item_list = self.session.query(self.DiffRelationshipClass.id).\
            filter(self.DiffRelationshipClass.id.in_(relationship_class_ids))
        self.remove_cascade_relationship_classes(
            [x.id for x in item_list],
            [x.id for x in diff_item_list],
            removed_item_id,
            removed_diff_item_id)
        # relationship
        item_list = self.session.query(self.Relationship.id).filter(self.Relationship.id.in_(relationship_ids))
        diff_item_list = self.session.query(self.DiffRelationship.id).\
            filter(self.DiffRelationship.id.in_(relationship_ids))
        self.remove_cascade_relationships(
            [x.id for x in item_list],
            [x.id for x in diff_item_list],
            removed_item_id,
            removed_diff_item_id)
        # parameter
        item_list = self.session.query(self.Parameter.id).filter(self.Parameter.id.in_(parameter_ids))
        diff_item_list = self.session.query(self.DiffParameter.id).filter(self.DiffParameter.id.in_(parameter_ids))
        self.remove_cascade_parameters(
            [x.id for x in item_list],
            [x.id for x in diff_item_list],
            removed_item_id,
            removed_diff_item_id)
        # parameter_value
        item_list = self.session.query(self.ParameterValue.id).filter(self.ParameterValue.id.in_(parameter_value_ids))
        diff_item_list = self.session.query(self.DiffParameterValue.id).\
            filter(self.DiffParameterValue.id.in_(parameter_value_ids))
        self.remove_cascade_parameter_values(
            [x.id for x in item_list],
            [x.id for x in diff_item_list],
            removed_item_id,
            removed_diff_item_id)
        return removed_item_id, removed_diff_item_id

    def remove_cascade_object_classes(self, ids, diff_ids, removed_item_id, removed_diff_item_id):
        """Remove object classes and all related items."""
        # Touch
        removed_item_id.setdefault("object_class", set()).update(ids)
        removed_diff_item_id.setdefault("object_class", set()).update(diff_ids)
        # object
        item_list = self.session.query(self.Object.id).filter(self.Object.class_id.in_(ids))
        diff_item_list = self.session.query(self.DiffObject.id).filter(self.DiffObject.class_id.in_(ids + diff_ids))
        self.remove_cascade_objects(
            [x.id for x in item_list],
            [x.id for x in diff_item_list],
            removed_item_id,
            removed_diff_item_id)
        # relationship_class
        item_list = self.session.query(self.RelationshipClass.id).\
            filter(self.RelationshipClass.object_class_id.in_(ids))
        diff_item_list = self.session.query(self.DiffRelationshipClass.id).\
            filter(self.DiffRelationshipClass.object_class_id.in_(ids + diff_ids))
        self.remove_cascade_relationship_classes(
            [x.id for x in item_list],
            [x.id for x in diff_item_list],
            removed_item_id,
            removed_diff_item_id)
        # parameter
        item_list = self.session.query(self.Parameter.id).filter(self.Parameter.object_class_id.in_(ids))
        diff_item_list = self.session.query(self.DiffParameter.id).\
            filter(self.DiffParameter.object_class_id.in_(ids + diff_ids))
        self.remove_cascade_parameters(
            [x.id for x in item_list],
            [x.id for x in diff_item_list],
            removed_item_id,
            removed_diff_item_id)

    def remove_cascade_objects(self, ids, diff_ids, removed_item_id, removed_diff_item_id):
        """Remove objects and all related items."""
        # Touch
        removed_item_id.setdefault("object", set()).update(ids)
        removed_diff_item_id.setdefault("object", set()).update(diff_ids)
        # relationship
        item_list = self.session.query(self.Relationship.id).filter(self.Relationship.object_id.in_(ids))
        diff_item_list = self.session.query(self.DiffRelationship.id).\
            filter(self.DiffRelationship.object_id.in_(ids + diff_ids))
        self.remove_cascade_relationships(
            [x.id for x in item_list],
            [x.id for x in diff_item_list],
            removed_item_id,
            removed_diff_item_id)
        # parameter_value
        item_list = self.session.query(self.ParameterValue.id).filter(self.ParameterValue.object_id.in_(ids))
        diff_item_list = self.session.query(self.DiffParameterValue.id).\
            filter(self.DiffParameterValue.object_id.in_(ids + diff_ids))
        self.remove_cascade_parameter_values(
            [x.id for x in item_list],
            [x.id for x in diff_item_list],
            removed_item_id,
            removed_diff_item_id)

    def remove_cascade_relationship_classes(self, ids, diff_ids, removed_item_id, removed_diff_item_id):
        """Remove relationship classes and all related items."""
        # Touch
        removed_item_id.setdefault("relationship_class", set()).update(ids)
        removed_diff_item_id.setdefault("relationship_class", set()).update(diff_ids)
        # relationship
        item_list = self.session.query(self.Relationship.id).filter(self.Relationship.class_id.in_(ids))
        diff_item_list = self.session.query(self.DiffRelationship.id).\
            filter(self.DiffRelationship.class_id.in_(ids + diff_ids))
        self.remove_cascade_relationships(
            [x.id for x in item_list],
            [x.id for x in diff_item_list],
            removed_item_id,
            removed_diff_item_id)
        # parameter
        item_list = self.session.query(self.Parameter.id).filter(self.Parameter.relationship_class_id.in_(ids))
        diff_item_list = self.session.query(self.DiffParameter.id).\
            filter(self.DiffParameter.relationship_class_id.in_(ids + diff_ids))
        self.remove_cascade_parameters(
            [x.id for x in item_list],
            [x.id for x in diff_item_list],
            removed_item_id,
            removed_diff_item_id)

    def remove_cascade_relationships(self, ids, diff_ids, removed_item_id, removed_diff_item_id):
        """Remove relationships and all related items."""
        # Touch
        removed_item_id.setdefault("relationship", set()).update(ids)
        removed_diff_item_id.setdefault("relationship", set()).update(diff_ids)
        # parameter_value
        item_list = self.session.query(self.ParameterValue.id).\
            filter(self.ParameterValue.relationship_id.in_(ids))
        diff_item_list = self.session.query(self.DiffParameterValue.id).\
            filter(self.DiffParameterValue.relationship_id.in_(ids + diff_ids))
        self.remove_cascade_parameter_values(
            [x.id for x in item_list],
            [x.id for x in diff_item_list],
            removed_item_id,
            removed_diff_item_id)

    def remove_cascade_parameters(self, ids, diff_ids, removed_item_id, removed_diff_item_id):
        """Remove parameters and all related items."""
        # Touch
        removed_item_id.setdefault("parameter", set()).update(ids)
        removed_diff_item_id.setdefault("parameter", set()).update(diff_ids)
        # parameter_value
        item_list = self.session.query(self.ParameterValue.id).\
            filter(self.ParameterValue.parameter_id.in_(ids))
        diff_item_list = self.session.query(self.DiffParameterValue.id).\
            filter(self.DiffParameterValue.parameter_id.in_(ids + diff_ids))
        self.remove_cascade_parameter_values(
            [x.id for x in item_list],
            [x.id for x in diff_item_list],
            removed_item_id,
            removed_diff_item_id)

    def remove_cascade_parameter_values(self, ids, diff_ids, removed_item_id, removed_diff_item_id):
        """Remove parameter values and all related items."""
        removed_item_id.setdefault("parameter_value", set()).update(ids)
        removed_diff_item_id.setdefault("parameter_value", set()).update(diff_ids)

    def reset_diff_mapping(self):
        """Delete all records from diff tables (but don't drop the tables)."""
        self.session.query(self.DiffObjectClass).delete()
        self.session.query(self.DiffObject).delete()
        self.session.query(self.DiffRelationshipClass).delete()
        self.session.query(self.DiffRelationship).delete()
        self.session.query(self.DiffParameter).delete()
        self.session.query(self.DiffParameterValue).delete()

    def commit_session(self, comment):
        """Make differences into original tables and commit."""
        try:
            user = self.username
            date = datetime.now(timezone.utc)
            commit = self.Commit(comment=comment, date=date, user=user)
            self.session.add(commit)
            self.session.flush()
            # Remove removed
            self.session.query(self.ObjectClass).filter(
                self.ObjectClass.id.in_(self.removed_item_id["object_class"])
            ).delete(synchronize_session=False)
            self.session.query(self.Object).filter(
                self.Object.id.in_(self.removed_item_id["object"])
            ).delete(synchronize_session=False)
            self.session.query(self.RelationshipClass).filter(
                self.RelationshipClass.id.in_(self.removed_item_id["relationship_class"])
            ).delete(synchronize_session=False)
            self.session.query(self.Relationship).filter(
                self.Relationship.id.in_(self.removed_item_id["relationship"])
            ).delete(synchronize_session=False)
            self.session.query(self.Parameter).filter(
                self.Parameter.id.in_(self.removed_item_id["parameter"])
            ).delete(synchronize_session=False)
            self.session.query(self.ParameterValue).filter(
                self.ParameterValue.id.in_(self.removed_item_id["parameter_value"])
            ).delete(synchronize_session=False)
            # Merge dirty
            dirty_items = {om: [] for om in [self.ObjectClass, self.Object,
                                             self.RelationshipClass, self.Relationship,
                                             self.Parameter, self.ParameterValue]}
            for item in self.session.query(self.DiffObjectClass).\
                    filter(self.DiffObjectClass.id.in_(self.dirty_item_id["object_class"])):
                kwargs = attr_dict(item)
                kwargs['commit_id'] = commit.id
                dirty_items[self.ObjectClass].append(kwargs)
            for item in self.session.query(self.DiffObject).\
                    filter(self.DiffObject.id.in_(self.dirty_item_id["object"])):
                kwargs = attr_dict(item)
                kwargs['commit_id'] = commit.id
                dirty_items[self.Object].append(kwargs)
            for item in self.session.query(self.DiffRelationshipClass).\
                    filter(self.DiffRelationshipClass.id.in_(self.dirty_item_id["relationship_class"])):
                kwargs = attr_dict(item)
                kwargs['commit_id'] = commit.id
                dirty_items[self.RelationshipClass].append(kwargs)
            for item in self.session.query(self.DiffRelationship).\
                    filter(self.DiffRelationship.id.in_(self.dirty_item_id["relationship"])):
                kwargs = attr_dict(item)
                kwargs['commit_id'] = commit.id
                dirty_items[self.Relationship].append(kwargs)
            for item in self.session.query(self.DiffParameter).\
                    filter(self.DiffParameter.id.in_(self.dirty_item_id["parameter"])):
                kwargs = attr_dict(item)
                kwargs['commit_id'] = commit.id
                dirty_items[self.Parameter].append(kwargs)
            for item in self.session.query(self.DiffParameterValue).\
                    filter(self.DiffParameterValue.id.in_(self.dirty_item_id["parameter_value"])):
                kwargs = attr_dict(item)
                kwargs['commit_id'] = commit.id
                dirty_items[self.ParameterValue].append(kwargs)
            self.session.flush()  # TODO: Check if this is needed
            # Bulk update
            for k, v in dirty_items.items():
                self.session.bulk_update_mappings(k, v)
            # Add new
            new_items = {om: [] for om in [self.ObjectClass, self.Object,
                                           self.RelationshipClass, self.Relationship,
                                           self.Parameter, self.ParameterValue]}
            for item in self.session.query(self.DiffObjectClass).\
                    filter(self.DiffObjectClass.id.in_(self.new_item_id["object_class"])):
                kwargs = attr_dict(item)
                kwargs['commit_id'] = commit.id
                new_items[self.ObjectClass].append(kwargs)
            for item in self.session.query(self.DiffObject).\
                    filter(self.DiffObject.id.in_(self.new_item_id["object"])):
                kwargs = attr_dict(item)
                kwargs['commit_id'] = commit.id
                new_items[self.Object].append(kwargs)
            for item in self.session.query(self.DiffRelationshipClass).\
                    filter(self.DiffRelationshipClass.id.in_(self.new_item_id["relationship_class"])):
                kwargs = attr_dict(item)
                kwargs['commit_id'] = commit.id
                new_items[self.RelationshipClass].append(kwargs)
            for item in self.session.query(self.DiffRelationship).\
                    filter(self.DiffRelationship.id.in_(self.new_item_id["relationship"])):
                kwargs = attr_dict(item)
                kwargs['commit_id'] = commit.id
                new_items[self.Relationship].append(kwargs)
            for item in self.session.query(self.DiffParameter).\
                    filter(self.DiffParameter.id.in_(self.new_item_id["parameter"])):
                kwargs = attr_dict(item)
                kwargs['commit_id'] = commit.id
                new_items[self.Parameter].append(kwargs)
            for item in self.session.query(self.DiffParameterValue).\
                    filter(self.DiffParameterValue.id.in_(self.new_item_id["parameter_value"])):
                kwargs = attr_dict(item)
                kwargs['commit_id'] = commit.id
                new_items[self.ParameterValue].append(kwargs)
            # Bulk insert
            for k, v in new_items.items():
                self.session.bulk_insert_mappings(k, v)
            self.reset_diff_mapping()
            self.session.commit()
            self.init_diff_dicts()
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while commiting changes: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def rollback_session(self):
        """Clear all differences."""
        try:
            self.reset_diff_mapping()
            self.session.commit()
            self.init_diff_dicts()
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while rolling back changes: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def close(self):
        """Drop differences tables and close."""
        if self.session:
            self.session.rollback()
            self.session.close()
        if self.diff_metadata and self.engine:
            self.diff_metadata.drop_all(self.engine)
        if self.engine:
            self.engine.dispose()
