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
    def __init__(self, db_url, username=None, create_all=True):
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
        self.DiffParameterDefinition = None
        self.DiffParameterValue = None
        self.DiffParameterTag = None
        self.DiffParameterDefinitionTag = None
        self.DiffParameterEnum = None
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
            "parameter_definition": set(),
            "parameter_value": set(),
            "parameter_tag": set(),
            "parameter_definition_tag": set(),
            "parameter_enum": set(),
        }
        self.dirty_item_id = {
            "object_class": set(),
            "object": set(),
            "relationship_class": set(),
            "relationship": set(),
            "parameter_definition": set(),
            "parameter_value": set(),
            "parameter_tag": set(),
            "parameter_definition_tag": set(),
            "parameter_enum": set(),
        }
        self.removed_item_id = {
            "object_class": set(),
            "object": set(),
            "relationship_class": set(),
            "relationship": set(),
            "parameter_definition": set(),
            "parameter_value": set(),
            "parameter_tag": set(),
            "parameter_definition_tag": set(),
            "parameter_enum": set(),
        }
        # Items that we don't want to read from the original tables (either dirty, or removed)
        self.touched_item_id = {
            "object_class": set(),
            "object": set(),
            "relationship_class": set(),
            "relationship": set(),
            "parameter_definition": set(),
            "parameter_value": set(),
            "parameter_tag": set(),
            "parameter_definition_tag": set(),
            "parameter_enum": set(),
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
            self.DiffParameterDefinition = getattr(self.DiffBase.classes, self.diff_prefix + "parameter_definition")
            self.DiffParameter = self.DiffParameterDefinition  # FIXME
            self.DiffParameterValue = getattr(self.DiffBase.classes, self.diff_prefix + "parameter_value")
            self.DiffParameterTag = getattr(self.DiffBase.classes, self.diff_prefix + "parameter_tag")
            self.DiffParameterDefinitionTag = getattr(
                self.DiffBase.classes, self.diff_prefix + "parameter_definition_tag")
            self.DiffParameterEnum = getattr(self.DiffBase.classes, self.diff_prefix + "parameter_enum")
        except NoSuchTableError as table:
            self.close()
            raise SpineTableNotFoundError(table)
        except AttributeError as table:
            self.close()
            raise SpineTableNotFoundError(table)

    def init_next_id(self):
        """Create next_id table if not exists and map it."""
        # TODO: Does this work? What happens if there's already a next_id table with a different definition?
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
            Column('parameter_definition_id', Integer),
            Column('parameter_value_id', Integer),
            Column('parameter_tag_id', Integer),
            Column('parameter_enum_id', Integer),
            Column('parameter_definition_tag_id', Integer)
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
                filter(or_(self.ParameterDefinition.id == id, self.DiffParameterDefinition.id == id))
        if name:
            return self.parameter_list().\
                filter(or_(self.ParameterDefinition.name == name, self.DiffParameterDefinition.name == name))
        return self.empty_list()

    def single_object_parameter(self, id):
        """Return object class and the parameter corresponding to id."""
        return self.object_parameter_list().\
            filter(or_(self.ParameterDefinition.id == id, self.DiffParameterDefinition.id == id))

    def single_relationship_parameter(self, id):
        """Return relationship class and the parameter corresponding to id."""
        return self.relationship_parameter_list().\
            filter(or_(self.ParameterDefinition.id == id, self.DiffParameterDefinition.id == id))

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
                    self.ParameterValue.parameter_definition_id == parameter_id,
                    self.ParameterValue.object_id == object_id),
                and_(
                    self.DiffParameterValue.parameter_definition_id == parameter_id,
                    self.DiffParameterValue.object_id == object_id)))
        return self.empty_list()

    def single_relationship_parameter_value(self, id):
        """Return relationship and the parameter value corresponding to id."""
        return self.relationship_parameter_value_list().\
            filter(or_(self.ParameterValue.id == id, self.DiffParameterValue.id == id))

    def object_class_list(self, id_list=None, ordered=True):
        """Return object classes ordered by display order."""
        qry = super().object_class_list(id_list=id_list, ordered=False).\
            filter(~self.ObjectClass.id.in_(self.touched_item_id["object_class"]))
        diff_qry = self.session.query(
            self.DiffObjectClass.id.label("id"),
            self.DiffObjectClass.name.label("name"),
            self.DiffObjectClass.display_order.label("display_order"),
            self.DiffObjectClass.description.label("description"))
        if id_list is not None:
            diff_qry = diff_qry.filter(self.DiffObjectClass.id.in_(id_list))
        qry = qry.union_all(diff_qry)
        if ordered:
            qry = qry.order_by(self.ObjectClass.display_order, self.DiffObjectClass.display_order)
        return qry

    def object_list(self, id_list=None, class_id=None):
        """Return objects, optionally filtered by class id."""
        qry = super().object_list(id_list=id_list, class_id=class_id).\
            filter(~self.Object.id.in_(self.touched_item_id["object"]))
        diff_qry = self.session.query(
            self.DiffObject.id.label('id'),
            self.DiffObject.class_id.label('class_id'),
            self.DiffObject.name.label('name'),
            self.DiffObject.description.label("description"))
        if id_list is not None:
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

    def wide_relationship_class_list(self, id_list=None, object_class_id=None):
        """Return list of relationship classes in wide format involving a given object class."""
        object_class_list = self.object_class_list(ordered=False).subquery()
        qry = self.session.query(
            self.RelationshipClass.id.label('id'),
            self.RelationshipClass.dimension.label('dimension'),
            self.RelationshipClass.object_class_id.label('object_class_id'),
            object_class_list.c.name.label('object_class_name'),
            self.RelationshipClass.name.label('name')
        ).filter(self.RelationshipClass.object_class_id == object_class_list.c.id).\
        filter(~self.RelationshipClass.id.in_(self.touched_item_id["relationship_class"]))
        diff_qry = self.session.query(
            self.DiffRelationshipClass.id.label('id'),
            self.DiffRelationshipClass.dimension.label('dimension'),
            self.DiffRelationshipClass.object_class_id.label('object_class_id'),
            object_class_list.c.name.label('object_class_name'),
            self.DiffRelationshipClass.name.label('name')
        ).filter(self.DiffRelationshipClass.object_class_id == object_class_list.c.id)
        if id_list is not None:
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
        ).order_by(subqry.c.id, subqry.c.dimension).group_by(subqry.c.id)

    def wide_relationship_list(self, id_list=None, class_id=None, object_id=None):
        """Return list of relationships in wide format involving a given relationship class and object."""
        object_list = self.object_list().subquery()
        qry = self.session.query(
            self.Relationship.id.label('id'),
            self.Relationship.dimension.label('dimension'),
            self.Relationship.class_id.label('class_id'),
            self.Relationship.object_id.label('object_id'),
            object_list.c.name.label('object_name'),
            self.Relationship.name.label('name')
        ).filter(self.Relationship.object_id == object_list.c.id).\
        filter(~self.Relationship.id.in_(self.touched_item_id["relationship"]))
        diff_qry = self.session.query(
            self.DiffRelationship.id.label('id'),
            self.DiffRelationship.dimension.label('dimension'),
            self.DiffRelationship.class_id.label('class_id'),
            self.DiffRelationship.object_id.label('object_id'),
            object_list.c.name.label('object_name'),
            self.DiffRelationship.name.label('name')
        ).filter(self.DiffRelationship.object_id == object_list.c.id)
        if id_list is not None:
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
        ).order_by(subqry.c.id, subqry.c.dimension).group_by(subqry.c.id)

    def parameter_list(self, id_list=None, object_class_id=None, relationship_class_id=None):
        """Return parameters."""
        qry = super().parameter_list(
            id_list=id_list,
            object_class_id=object_class_id,
            relationship_class_id=relationship_class_id
        ).filter(~self.ParameterDefinition.id.in_(self.touched_item_id["parameter_definition"]))
        diff_qry = self.session.query(
            self.DiffParameterDefinition.id.label('id'),
            self.DiffParameterDefinition.name.label('name'),
            self.DiffParameterDefinition.relationship_class_id.label('relationship_class_id'),
            self.DiffParameterDefinition.object_class_id.label('object_class_id'),
            self.DiffParameterDefinition.can_have_time_series.label('can_have_time_series'),
            self.DiffParameterDefinition.can_have_time_pattern.label('can_have_time_pattern'),
            self.DiffParameterDefinition.can_be_stochastic.label('can_be_stochastic'),
            self.DiffParameterDefinition.default_value.label('default_value'),
            self.DiffParameterDefinition.is_mandatory.label('is_mandatory'),
            self.DiffParameterDefinition.precision.label('precision'),
            self.DiffParameterDefinition.minimum_value.label('minimum_value'),
            self.DiffParameterDefinition.maximum_value.label('maximum_value'))
        if id_list is not None:
            diff_qry = diff_qry.filter(self.DiffParameterDefinition.id.in_(id_list))
        if object_class_id:
            diff_qry = diff_qry.filter_by(object_class_id=object_class_id)
        if relationship_class_id:
            diff_qry = diff_qry.filter_by(relationship_class_id=relationship_class_id)
        return qry.union_all(diff_qry)

    def object_parameter_list(self, object_class_id=None, parameter_id=None):
        """Return object classes and their parameters."""
        object_class_list = self.object_class_list().subquery()
        wide_parameter_definition_tag_list = self.wide_parameter_definition_tag_list().subquery()
        wide_parameter_enum_list = self.wide_parameter_enum_list().subquery()
        qry = self.session.query(
            self.ParameterDefinition.id.label('id'),
            object_class_list.c.id.label('object_class_id'),
            object_class_list.c.name.label('object_class_name'),
            self.ParameterDefinition.name.label('parameter_name'),
            self.ParameterDefinition.enum_id,
            wide_parameter_enum_list.c.name.label('enum_name'),
            wide_parameter_definition_tag_list.c.parameter_tag_id_list,
            wide_parameter_definition_tag_list.c.parameter_tag_list,
            self.ParameterDefinition.can_have_time_series,
            self.ParameterDefinition.can_have_time_pattern,
            self.ParameterDefinition.can_be_stochastic,
            self.ParameterDefinition.default_value,
            self.ParameterDefinition.is_mandatory,
            self.ParameterDefinition.precision,
            self.ParameterDefinition.minimum_value,
            self.ParameterDefinition.maximum_value
        ).filter(object_class_list.c.id == self.ParameterDefinition.object_class_id).\
        outerjoin(
            wide_parameter_definition_tag_list,
            wide_parameter_definition_tag_list.c.parameter_definition_id == self.ParameterDefinition.id).\
        outerjoin(
            wide_parameter_enum_list,
            wide_parameter_enum_list.c.id == self.ParameterDefinition.enum_id).\
        filter(~self.ParameterDefinition.id.in_(self.touched_item_id["parameter_definition"]))
        diff_qry = self.session.query(
            self.DiffParameterDefinition.id.label('id'),
            object_class_list.c.id.label('object_class_id'),
            object_class_list.c.name.label('object_class_name'),
            self.DiffParameterDefinition.name.label('parameter_name'),
            self.DiffParameterDefinition.enum_id,
            wide_parameter_enum_list.c.name.label('enum_name'),
            wide_parameter_definition_tag_list.c.parameter_tag_id_list,
            wide_parameter_definition_tag_list.c.parameter_tag_list,
            self.DiffParameterDefinition.can_have_time_series,
            self.DiffParameterDefinition.can_have_time_pattern,
            self.DiffParameterDefinition.can_be_stochastic,
            self.DiffParameterDefinition.default_value,
            self.DiffParameterDefinition.is_mandatory,
            self.DiffParameterDefinition.precision,
            self.DiffParameterDefinition.minimum_value,
            self.DiffParameterDefinition.maximum_value
        ).filter(object_class_list.c.id == self.DiffParameterDefinition.object_class_id).\
        outerjoin(
            wide_parameter_definition_tag_list,
            wide_parameter_definition_tag_list.c.parameter_definition_id == self.DiffParameterDefinition.id).\
        outerjoin(
            wide_parameter_enum_list,
            wide_parameter_enum_list.c.id == self.DiffParameterDefinition.enum_id)
        if object_class_id:
            qry = qry.filter(self.ParameterDefinition.object_class_id == object_class_id)
            diff_qry = diff_qry.filter(self.DiffParameterDefinition.object_class_id == object_class_id)
        if parameter_id:
            qry = qry.filter(self.ParameterDefinition.id == parameter_id)
            diff_qry = diff_qry.filter(self.DiffParameterDefinition.id == parameter_id)
        return qry.union_all(diff_qry).order_by(self.ParameterDefinition.id, self.DiffParameterDefinition.id)

    def relationship_parameter_list(self, relationship_class_id=None, parameter_id=None):
        """Return relationship classes and their parameters."""
        wide_relationship_class_list = self.wide_relationship_class_list().subquery()
        wide_parameter_definition_tag_list = self.wide_parameter_definition_tag_list().subquery()
        wide_parameter_enum_list = self.wide_parameter_enum_list().subquery()
        qry = self.session.query(
            self.ParameterDefinition.id.label('id'),
            wide_relationship_class_list.c.id.label('relationship_class_id'),
            wide_relationship_class_list.c.name.label('relationship_class_name'),
            wide_relationship_class_list.c.object_class_id_list,
            wide_relationship_class_list.c.object_class_name_list,
            self.ParameterDefinition.name.label('parameter_name'),
            self.ParameterDefinition.enum_id,
            wide_parameter_enum_list.c.name.label('enum_name'),
            wide_parameter_definition_tag_list.c.parameter_tag_id_list,
            wide_parameter_definition_tag_list.c.parameter_tag_list,
            self.ParameterDefinition.can_have_time_series,
            self.ParameterDefinition.can_have_time_pattern,
            self.ParameterDefinition.can_be_stochastic,
            self.ParameterDefinition.default_value,
            self.ParameterDefinition.is_mandatory,
            self.ParameterDefinition.precision,
            self.ParameterDefinition.minimum_value,
            self.ParameterDefinition.maximum_value
        ).filter(self.ParameterDefinition.relationship_class_id == wide_relationship_class_list.c.id).\
        outerjoin(
            wide_parameter_definition_tag_list,
            wide_parameter_definition_tag_list.c.parameter_definition_id == self.ParameterDefinition.id).\
        outerjoin(
            wide_parameter_enum_list,
            wide_parameter_enum_list.c.id == self.ParameterDefinition.enum_id).\
        filter(~self.ParameterDefinition.id.in_(self.touched_item_id["parameter_definition"]))
        diff_qry = self.session.query(
            self.DiffParameterDefinition.id.label('id'),
            wide_relationship_class_list.c.id.label('relationship_class_id'),
            wide_relationship_class_list.c.name.label('relationship_class_name'),
            wide_relationship_class_list.c.object_class_id_list,
            wide_relationship_class_list.c.object_class_name_list,
            self.DiffParameterDefinition.name.label('parameter_name'),
            self.DiffParameterDefinition.enum_id,
            wide_parameter_enum_list.c.name.label('enum_name'),
            wide_parameter_definition_tag_list.c.parameter_tag_id_list,
            wide_parameter_definition_tag_list.c.parameter_tag_list,
            self.DiffParameterDefinition.can_have_time_series,
            self.DiffParameterDefinition.can_have_time_pattern,
            self.DiffParameterDefinition.can_be_stochastic,
            self.DiffParameterDefinition.default_value,
            self.DiffParameterDefinition.is_mandatory,
            self.DiffParameterDefinition.precision,
            self.DiffParameterDefinition.minimum_value,
            self.DiffParameterDefinition.maximum_value
        ).filter(self.DiffParameterDefinition.relationship_class_id == wide_relationship_class_list.c.id).\
        outerjoin(
            wide_parameter_definition_tag_list,
            wide_parameter_definition_tag_list.c.parameter_definition_id == self.DiffParameterDefinition.id).\
        outerjoin(
            wide_parameter_enum_list,
            wide_parameter_enum_list.c.id == self.DiffParameterDefinition.enum_id)
        if relationship_class_id:
            qry = qry.filter(self.ParameterDefinition.relationship_class_id == relationship_class_id)
            diff_qry = diff_qry.filter(self.DiffParameterDefinition.relationship_class_id == relationship_class_id)
        if parameter_id:
            qry = qry.filter(self.ParameterDefinition.id == parameter_id)
            diff_qry = diff_qry.filter(self.DiffParameterDefinition.id == parameter_id)
        return qry.union_all(diff_qry).order_by(self.ParameterDefinition.id, self.DiffParameterDefinition.id)

    def wide_object_parameter_definition_list(self, object_class_id_list=None, parameter_definition_id_list=None):
        """Return object classes and their parameter definitions in wide format."""
        parameter_definition_list = self.parameter_list().subquery()
        qry = self.session.query(
            self.ObjectClass.id.label('object_class_id'),
            self.ObjectClass.name.label('object_class_name'),
            parameter_definition_list.c.id.label('parameter_definition_id'),
            parameter_definition_list.c.name.label('parameter_name')
        ).filter(self.ObjectClass.id == parameter_definition_list.c.object_class_id).\
        filter(~self.ObjectClass.id.in_(self.touched_item_id["object_class"]))
        diff_qry = self.session.query(
            self.DiffObjectClass.id.label('object_class_id'),
            self.DiffObjectClass.name.label('object_class_name'),
            parameter_definition_list.c.id.label('parameter_definition_id'),
            parameter_definition_list.c.name.label('parameter_name')
        ).filter(self.DiffObjectClass.id == parameter_definition_list.c.object_class_id)
        if object_class_id_list is not None:
            qry = qry.filter(self.ObjectClass.id.in_(object_class_id_list))
            diff_qry = diff_qry.filter(self.ObjectClass.id.in_(object_class_id_list))
        if parameter_definition_id_list is not None:
            qry = qry.filter(parameter_definition_list.c.id.in_(parameter_definition_id_list))
            diff_qry = diff_qry.filter(parameter_definition_list.c.id.in_(parameter_definition_id_list))
        subqry = qry.union_all(diff_qry).subquery()
        return self.session.query(
            subqry.c.object_class_id,
            subqry.c.object_class_name,
            func.group_concat(subqry.c.parameter_definition_id).label('parameter_definition_id_list'),
            func.group_concat(subqry.c.parameter_name).label('parameter_name_list')
        ).group_by(subqry.c.object_class_id)

    def wide_relationship_parameter_definition_list(
            self, relationship_class_id_list=None, parameter_definition_id_list=None):
        """Return relationship classes and their parameter definitions in wide format."""
        parameter_definition_list = self.parameter_list().subquery()
        qry = self.session.query(
            self.RelationshipClass.id.label('relationship_class_id'),
            self.RelationshipClass.name.label('relationship_class_name'),
            parameter_definition_list.c.id.label('parameter_definition_id'),
            parameter_definition_list.c.name.label('parameter_name')
        ).filter(self.RelationshipClass.id == parameter_definition_list.c.relationship_class_id).\
        filter(~self.RelationshipClass.id.in_(self.touched_item_id["relationship_class"]))
        diff_qry = self.session.query(
            self.DiffRelationshipClass.id.label('relationship_class_id'),
            self.DiffRelationshipClass.name.label('relationship_class_name'),
            parameter_definition_list.c.id.label('parameter_definition_id'),
            parameter_definition_list.c.name.label('parameter_name')
        ).filter(self.DiffRelationshipClass.id == parameter_definition_list.c.relationship_class_id)
        if relationship_class_id_list is not None:
            qry = qry.filter(self.RelationshipClass.id.in_(relationship_class_id_list))
            diff_qry = diff_qry.filter(self.DiffRelationshipClass.id.in_(relationship_class_id_list))
        if parameter_definition_id_list is not None:
            qry = qry.filter(parameter_definition_list.c.id.in_(parameter_definition_id_list))
            diff_qry = diff_qry.filter(parameter_definition_list.c.id.in_(parameter_definition_id_list))
        subqry = qry.union_all(diff_qry).subquery()
        return self.session.query(
            subqry.c.relationship_class_id,
            subqry.c.relationship_class_name,
            func.group_concat(subqry.c.parameter_definition_id).label('parameter_definition_id_list'),
            func.group_concat(subqry.c.parameter_name).label('parameter_name_list')
        ).group_by(subqry.c.relationship_class_id)

    def parameter_value_list(self, id_list=None, object_id=None, relationship_id=None):
        """Return parameter values."""
        qry = super().parameter_value_list(
            id_list=id_list,
            object_id=object_id,
            relationship_id=relationship_id
        ).filter(~self.ParameterValue.id.in_(self.touched_item_id["parameter_value"]))
        diff_qry = self.session.query(
            self.DiffParameterValue.id,
            self.DiffParameterValue.parameter_definition_id,
            self.DiffParameterValue.object_id,
            self.DiffParameterValue.relationship_id,
            self.DiffParameterValue.index,
            self.DiffParameterValue.value,
            self.DiffParameterValue.json,
            self.DiffParameterValue.expression,
            self.DiffParameterValue.time_pattern,
            self.DiffParameterValue.time_series_id,
            self.DiffParameterValue.stochastic_model_id)
        if id_list is not None:
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
        ).filter(parameter_list.c.id == self.ParameterValue.parameter_definition_id).\
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
        ).filter(parameter_list.c.id == self.DiffParameterValue.parameter_definition_id).\
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
        ).filter(parameter_list.c.id == self.ParameterValue.parameter_definition_id).\
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
        ).filter(parameter_list.c.id == self.DiffParameterValue.parameter_definition_id).\
        filter(self.DiffParameterValue.relationship_id == wide_relationship_list.c.id).\
        filter(parameter_list.c.relationship_class_id == wide_relationship_class_list.c.id)
        if relationship_class_id:
            qry = qry.filter(wide_relationship_class_list.c.id == relationship_class_id)
            diff_qry = diff_qry.filter(wide_relationship_class_list.c.id == relationship_class_id)
        if parameter_name:
            qry = qry.filter(parameter_list.c.name == parameter_name)
            diff_qry = diff_qry.filter(parameter_list.c.name == parameter_name)
        return qry.union_all(diff_qry)

    # TODO: Find out why we don't need to say, e.g., ~self.DiffParameterDefinition.id.in_(valued_parameter_ids)
    # NOTE: maybe these unvalued... are obsolete
    def unvalued_object_parameter_list(self, object_id):
        """Return parameters that do not have a value for given object."""
        object_ = self.single_object(id=object_id).one_or_none()
        if not object_:
            return self.empty_list()
        valued_parameters = self.parameter_value_list(object_id=object_id)
        return self.parameter_list(object_class_id=object_.class_id).\
            filter(~self.ParameterDefinition.id.in_([x.parameter_definition_id for x in valued_parameters]))

    def unvalued_object_list(self, parameter_id):
        """Return objects for which given parameter does not have a value."""
        parameter = self.single_parameter(parameter_id).one_or_none()
        if not parameter:
            return self.empty_list()
        valued_object_ids = self.session.query(self.ParameterValue.object_id).\
            filter_by(parameter_definition_id=parameter_id)
        return self.object_list().filter_by(class_id=parameter.object_class_id).\
            filter(~self.Object.id.in_(valued_object_ids))

    def unvalued_relationship_parameter_list(self, relationship_id):
        """Return parameters that do not have a value for given relationship."""
        relationship = self.single_wide_relationship(id=relationship_id).one_or_none()
        if not relationship:
            return self.empty_list()
        valued_parameters = self.parameter_value_list(relationship_id=relationship_id)
        return self.parameter_list(relationship_class_id=relationship.class_id).\
            filter(~self.ParameterDefinition.id.in_([x.parameter_definition_id for x in valued_parameters]))

    def unvalued_relationship_list(self, parameter_id):
        """Return relationships for which given parameter does not have a value."""
        parameter = self.single_parameter(parameter_id).one_or_none()
        if not parameter:
            return self.empty_list()
        valued_relationship_ids = self.session.query(self.ParameterValue.relationship_id).\
            filter_by(parameter_definition_id=parameter_id)
        return self.wide_relationship_list().filter_by(class_id=parameter.relationship_class_id).\
            filter(~self.Relationship.id.in_(valued_relationship_ids))

    def parameter_tag_list(self, id_list=None):
        """Return list of parameter tags."""
        qry = super().parameter_tag_list(id_list=id_list).\
            filter(~self.ParameterTag.id.in_(self.touched_item_id["parameter_tag"]))
        diff_qry = self.session.query(
            self.DiffParameterTag.id.label("id"),
            self.DiffParameterTag.tag.label("tag"),
            self.DiffParameterTag.description.label("description"))
        if id_list is not None:
            diff_qry = diff_qry.filter(self.DiffParameterTag.id.in_(id_list))
        return qry.union_all(diff_qry).order_by(self.ParameterTag.id, self.DiffParameterTag.id)

    def parameter_definition_tag_list(self, id_list=None):
        """Return list of parameter definition tags."""
        qry = super().parameter_definition_tag_list(id_list=id_list).\
            filter(~self.ParameterDefinitionTag.id.in_(self.touched_item_id["parameter_definition_tag"]))
        diff_qry = self.session.query(
            self.DiffParameterDefinitionTag.id.label('id'),
            self.DiffParameterDefinitionTag.parameter_definition_id.label('parameter_definition_id'),
            self.DiffParameterDefinitionTag.parameter_tag_id.label('parameter_tag_id'))
        if id_list is not None:
            diff_qry = diff_qry.filter(self.DiffParameterDefinitionTag.id.in_(id_list))
        return qry.union_all(diff_qry)

    def wide_parameter_definition_tag_list(self, parameter_definition_id=None):
        """Return list of parameter definitions and their tags in wide format.
        """
        parameter_tag_list = self.parameter_tag_list().subquery()
        qry = self.session.query(
            self.ParameterDefinitionTag.parameter_definition_id.label('parameter_definition_id'),
            self.ParameterDefinitionTag.parameter_tag_id.label('parameter_tag_id'),
            parameter_tag_list.c.tag.label('parameter_tag')
        ).filter(self.ParameterDefinitionTag.parameter_tag_id == parameter_tag_list.c.id).\
        filter(~self.ParameterDefinitionTag.id.in_(self.touched_item_id["parameter_definition_tag"]))
        diff_qry = self.session.query(
            self.DiffParameterDefinitionTag.parameter_definition_id.label('parameter_definition_id'),
            self.DiffParameterDefinitionTag.parameter_tag_id.label('parameter_tag_id'),
            parameter_tag_list.c.tag.label('parameter_tag')
        ).filter(self.DiffParameterDefinitionTag.parameter_tag_id == parameter_tag_list.c.id)
        if parameter_definition_id:
            qry = qry.filter(self.ParameterDefinitionTag.parameter_definition_id == parameter_definition_id)
            diff_qry = diff_qry.filter(
                self.DiffParameterDefinitionTag.parameter_definition_id == parameter_definition_id)
        subqry = qry.union_all(diff_qry).subquery()
        return self.session.query(
            subqry.c.parameter_definition_id,
            func.group_concat(subqry.c.parameter_tag_id).label('parameter_tag_id_list'),
            func.group_concat(subqry.c.parameter_tag).label('parameter_tag_list')
        ).group_by(subqry.c.parameter_definition_id)

    def wide_parameter_tag_definition_list(self, parameter_tag_id=None):
        """Return list of parameter tags (including the NULL tag) and their definitions in wide format.
        """
        parameter_definition_tag_list = self.parameter_definition_tag_list().subquery()
        qry = self.session.query(
            self.ParameterDefinition.id.label('parameter_definition_id'),
            parameter_definition_tag_list.c.parameter_tag_id.label('parameter_tag_id')
        ).outerjoin(
            parameter_definition_tag_list,
            self.ParameterDefinition.id == parameter_definition_tag_list.c.parameter_definition_id).\
        filter(~self.ParameterDefinition.id.in_(self.touched_item_id["parameter_definition"]))
        diff_qry = self.session.query(
            self.DiffParameterDefinition.id.label('parameter_definition_id'),
            parameter_definition_tag_list.c.parameter_tag_id.label('parameter_tag_id')
        ).outerjoin(
            parameter_definition_tag_list,
            self.DiffParameterDefinition.id == parameter_definition_tag_list.c.parameter_definition_id)
        if parameter_tag_id:
            qry = qry.filter(parameter_definition_tag_list.c.parameter_tag_id == parameter_tag_id)
            diff_qry = diff_qry.filter(parameter_definition_tag_list.c.parameter_tag_id == parameter_tag_id)
        subqry = qry.union_all(diff_qry).subquery()
        return self.session.query(
            subqry.c.parameter_tag_id,
            func.group_concat(subqry.c.parameter_definition_id).label('parameter_definition_id_list')
        ).group_by(subqry.c.parameter_tag_id)

    def parameter_enum_list(self, id_list=None):
        """Return list of parameter enums."""
        qry = super().parameter_enum_list(id_list=id_list).\
            filter(~self.ParameterEnum.id.in_(self.touched_item_id["parameter_enum"]))
        diff_qry = self.session.query(
            self.DiffParameterEnum.id.label("id"),
            self.DiffParameterEnum.name.label("name"),
            self.DiffParameterEnum.value_index.label("value_index"),
            self.DiffParameterEnum.value.label("value"))
        if id_list is not None:
            diff_qry = diff_qry.filter(self.DiffParameterEnum.id.in_(id_list))
        return qry.union_all(diff_qry)

    def check_object_classes_for_insert(self, *kwargs_list, raise_intgr_error=True):
        """Check that object classes respect integrity constraints for an insert operation."""
        intgr_error_log = []
        checked_kwargs_list = list()
        object_class_names = {x.name for x in self.object_class_list()}
        for kwargs in kwargs_list:
            try:
                self.check_object_class(kwargs, object_class_names)
                checked_kwargs_list.append(kwargs)
                # If the check passes, append kwargs to `object_class_names` for next iteration.
                object_class_names.add(kwargs["name"])
            except SpineIntegrityError as e:
                if raise_intgr_error:
                    raise e
                intgr_error_log.append(e.msg)
        return checked_kwargs_list, intgr_error_log

    def check_object_classes_for_update(self, *kwargs_list, raise_intgr_error=True):
        """Check that object classes respect integrity constraints for an update operation.
        NOTE: To check for an update we basically 'remove' the current instance
        and then check for an insert of the updated instance.
        """
        intgr_error_log = []
        checked_kwargs_list = list()
        object_class_dict = {x.id: {"name": x.name} for x in self.object_class_list()}
        object_class_names = {x.name for x in self.object_class_list()}
        for kwargs in kwargs_list:
            try:
                id = kwargs["id"]
            except KeyError:
                msg = "Missing object class identifier."
                if raise_intgr_error:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(msg)
                continue
            try:
                # 'Remove' current instance
                updated_kwargs = object_class_dict.pop(id)
                object_class_names.remove(updated_kwargs["name"])
            except KeyError:
                msg = "Object class not found."
                if raise_intgr_error:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(msg)
                continue
            # Check for an insert of the updated instance
            try:
                updated_kwargs.update(kwargs)
                self.check_object_class(updated_kwargs, object_class_names)
                checked_kwargs_list.append(kwargs)
                # If the check passes, reinject the updated instance for next iteration.
                object_class_dict[id] = updated_kwargs
                object_class_names.add(updated_kwargs["name"])
            except SpineIntegrityError as e:
                if raise_intgr_error:
                    raise e
                intgr_error_log.append(e.msg)
        return checked_kwargs_list, intgr_error_log

    def check_object_class(self, kwargs, object_class_names):
        """Raise a SpineIntegrityError if the object class given by `kwargs` violates any
        integrity constraints.
        """
        try:
            name = kwargs["name"]
        except KeyError:
            raise SpineIntegrityError("Missing object class name.")
        if name in object_class_names:
            raise SpineIntegrityError("There can't be more than one object class called '{}'.".format(name))

    def check_objects_for_insert(self, *kwargs_list, raise_intgr_error=True):
        """Check that objects respect integrity constraints for an insert operation."""
        intgr_error_log = []
        checked_kwargs_list = list()
        object_names = {x.name for x in self.object_list()}
        object_class_id_list = [x.id for x in self.object_class_list()]
        for kwargs in kwargs_list:
            try:
                self.check_object(kwargs, object_names, object_class_id_list)
                checked_kwargs_list.append(kwargs)
                object_names.add(kwargs["name"])
            except SpineIntegrityError as e:
                if raise_intgr_error:
                    raise e
                intgr_error_log.append(e.msg)
        return checked_kwargs_list, intgr_error_log

    def check_objects_for_update(self, *kwargs_list, raise_intgr_error=True):
        """Check that objects respect integrity constraints for an update operation."""
        intgr_error_log = []
        checked_kwargs_list = list()
        object_names = {x.name for x in self.object_list()}
        object_dict = {x.id: {"name": x.name, "class_id": x.class_id} for x in self.object_list()}
        object_class_id_list = [x.id for x in self.object_class_list()]
        for kwargs in kwargs_list:
            try:
                id = kwargs["id"]
            except KeyError:
                msg = "Missing object identifier."
                if raise_intgr_error:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(msg)
                continue
            try:
                updated_kwargs = object_dict.pop(id)
                object_names.remove(updated_kwargs["name"])
            except KeyError:
                msg = "Object not found."
                if raise_intgr_error:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(msg)
                continue
            try:
                updated_kwargs.update(kwargs)
                self.check_object(updated_kwargs, object_names, object_class_id_list)
                checked_kwargs_list.append(kwargs)
                object_dict[id] = updated_kwargs
                object_names.add(updated_kwargs["name"])
            except SpineIntegrityError as e:
                if raise_intgr_error:
                    raise e
                intgr_error_log.append(e.msg)
        return checked_kwargs_list, intgr_error_log

    def check_object(self, kwargs, object_names, object_class_id_list):
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
        if name in object_names:
            raise SpineIntegrityError("There can't be more than one object called '{}'.".format(name))

    def check_wide_relationship_classes_for_insert(self, *wide_kwargs_list, raise_intgr_error=True):
        """Check that relationship classes respect integrity constraints for an insert operation."""
        intgr_error_log = []
        checked_wide_kwargs_list = list()
        relationship_class_names = {x.name for x in self.wide_relationship_class_list()}
        object_class_id_list = [x.id for x in self.object_class_list()]
        for wide_kwargs in wide_kwargs_list:
            try:
                self.check_wide_relationship_class(wide_kwargs, relationship_class_names, object_class_id_list)
                checked_wide_kwargs_list.append(wide_kwargs)
                relationship_class_names.add(wide_kwargs["name"])
            except SpineIntegrityError as e:
                if raise_intgr_error:
                    raise e
                intgr_error_log.append(e.msg)
        return checked_wide_kwargs_list, intgr_error_log

    def check_wide_relationship_classes_for_update(self, *wide_kwargs_list, raise_intgr_error=True):
        """Check that relationship classes respect integrity constraints for an update operation."""
        intgr_error_log = []
        checked_wide_kwargs_list = list()
        relationship_class_names = {x.name for x in self.wide_relationship_class_list()}
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
                msg = "Missing relationship class identifier."
                if raise_intgr_error:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(msg)
                continue
            try:
                updated_wide_kwargs = relationship_class_dict.pop(id)
                relationship_class_names.remove(updated_wide_kwargs["name"])
            except KeyError:
                msg = "Relationship class not found."
                if raise_intgr_error:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(msg)
                continue
            try:
                updated_wide_kwargs.update(wide_kwargs)
                self.check_wide_relationship_class(
                    updated_wide_kwargs, list(relationship_class_dict.values()), object_class_id_list)
                checked_wide_kwargs_list.append(wide_kwargs)
                relationship_class_dict[id] = updated_wide_kwargs
                relationship_class_names.add(updated_wide_kwargs["name"])
            except SpineIntegrityError as e:
                if raise_intgr_error:
                    raise e
                intgr_error_log.append(e.msg)
        return checked_wide_kwargs_list, intgr_error_log

    def check_wide_relationship_class(self, wide_kwargs, relationship_class_names, object_class_id_list):
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
        if name in relationship_class_names:
            raise SpineIntegrityError("There can't be more than one relationship class called '{}'.".format(name))

    def check_wide_relationships_for_insert(self, *wide_kwargs_list, raise_intgr_error=True):
        """Check that relationships respect integrity constraints for an insert operation."""
        intgr_error_log = []
        checked_wide_kwargs_list = list()
        relationship_names = {x.name for x in self.wide_relationship_list()}
        relationship_class_objects_tuples = {
            (x.class_id, x.object_id_list) for x in self.wide_relationship_list()}
        relationship_class_dict = {
            x.id: {
                "object_class_id_list": [int(y) for y in x.object_class_id_list.split(',')],
                "name": x.name
            } for x in self.wide_relationship_class_list()}
        object_dict = {
            x.id: {
                'class_id': x.class_id,
                'name': x.name
            } for x in self.object_list()}
        for wide_kwargs in wide_kwargs_list:
            try:
                self.check_wide_relationship(
                    wide_kwargs, relationship_names, relationship_class_objects_tuples,
                    relationship_class_dict, object_dict)
                checked_wide_kwargs_list.append(wide_kwargs)
                relationship_names.add(wide_kwargs['name'])
                join_object_id_list = ",".join([str(x) for x in wide_kwargs['object_id_list']])
                relationship_class_objects_tuples.add((wide_kwargs['class_id'], join_object_id_list))
            except SpineIntegrityError as e:
                if raise_intgr_error:
                    raise e
                intgr_error_log.append(e.msg)
        return checked_wide_kwargs_list, intgr_error_log

    def check_wide_relationships_for_update(self, *wide_kwargs_list, raise_intgr_error=True):
        """Check that relationships respect integrity constraints for an update operation."""
        intgr_error_log = []
        checked_wide_kwargs_list = list()
        relationship_names = {x.name for x in self.wide_relationship_list()}
        relationship_class_objects_tuples = {
            (x.class_id, x.object_id_list) for x in self.wide_relationship_list()}
        relationship_dict = {
            x.id: {
                "class_id": x.class_id,
                "name": x.name,
                "object_id_list": [int(y) for y in x.object_id_list.split(',')]
            } for x in self.wide_relationship_list()
        }
        relationship_class_dict = {
            x.id: {
                "object_class_id_list": [int(y) for y in x.object_class_id_list.split(',')],
                "name": x.name
            } for x in self.wide_relationship_class_list()}
        object_dict = {
            x.id: {
                'class_id': x.class_id,
                'name': x.name
            } for x in self.object_list()}
        for wide_kwargs in wide_kwargs_list:
            try:
                id = wide_kwargs["id"]
            except KeyError:
                msg = "Missing relationship identifier."
                if raise_intgr_error:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(msg)
                continue
            try:
                updated_wide_kwargs = relationship_dict.pop(id)
                relationship_names.remove(updated_wide_kwargs['name'])
                join_object_id_list = ",".join([str(x) for x in updated_wide_kwargs['object_id_list']])
                relationship_class_objects_tuples.remove((updated_wide_kwargs['class_id'], join_object_id_list))
            except KeyError:
                msg = "Relationship not found."
                if raise_intgr_error:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(msg)
                continue
            try:
                updated_wide_kwargs.update(wide_kwargs)
                self.check_wide_relationship(
                    updated_wide_kwargs, relationship_names, relationship_class_objects_tuples,
                    relationship_class_dict, object_dict)
                checked_wide_kwargs_list.append(wide_kwargs)
                relationship_dict[id] = updated_wide_kwargs
                relationship_names.add(updated_wide_kwargs['name'])
                join_object_id_list = ",".join([str(x) for x in updated_wide_kwargs['object_id_list']])
                relationship_class_objects_tuples.add((updated_wide_kwargs['class_id'], join_object_id_list))
            except SpineIntegrityError as e:
                if raise_intgr_error:
                    raise e
                intgr_error_log.append(e.msg)
        return checked_wide_kwargs_list, intgr_error_log

    def check_wide_relationship(
            self, wide_kwargs, relationship_names, relationship_class_objects_tuples,
            relationship_class_dict, object_dict):
        """Raise a SpineIntegrityError if the relationship given by `kwargs` violates any integrity constraints."""
        try:
            class_id = wide_kwargs['class_id']
        except KeyError:
            raise SpineIntegrityError("Missing relationship class identifier.")
        try:
            object_class_id_list = relationship_class_dict[class_id]['object_class_id_list']
        except KeyError:
            raise SpineIntegrityError("Relationship class not found.")
        try:
            object_id_list = wide_kwargs['object_id_list']
        except KeyError:
            raise SpineIntegrityError("Missing object identifier.")
        try:
            given_object_class_id_list = [object_dict[id]['class_id'] for id in object_id_list]
        except KeyError as e:
            raise SpineIntegrityError("Object id '{}' not found.".format(e))
        if given_object_class_id_list != object_class_id_list:
            object_name_list = [object_dict[id]['name'] for id in object_id_list]
            relationship_class_name = relationship_class_dict[class_id]['name']
            raise SpineIntegrityError("Incorrect objects '{}' for "
                                      "relationship class '{}'.".format(object_name_list, relationship_class_name))
        # if len(object_id_list) != len(set(object_id_list)):
            # object_name_list = [object_dict[id]['name'] for id in object_id_list]
            # raise SpineIntegrityError("Incorrect object name list '{}'. "
                                      # "The same object can't appear twice "
                                      # "in one relationship.".format(object_name_list))
        join_object_id_list = ",".join([str(x) for x in object_id_list])
        if (class_id, join_object_id_list) in relationship_class_objects_tuples:
            object_name_list = [object_dict[id]['name'] for id in object_id_list]
            relationship_class_name = relationship_class_dict[class_id]['name']
            raise SpineIntegrityError("There's already a relationship between objects {} "
                                      "in class {}.".format(object_name_list, relationship_class_name))
        try:
            name = wide_kwargs["name"]
        except KeyError:
            raise SpineIntegrityError("Missing relationship name.")
        if name in relationship_names:
            raise SpineIntegrityError("There can't be more than one relationship called '{}'.".format(name))

    def check_parameter_definitions_for_insert(self, *kwargs_list, raise_intgr_error=True):
        """Check that parameters respect integrity constraints for an insert operation."""
        intgr_error_log = []
        checked_kwargs_list = list()
        parameter_definition_names = {x.name for x in self.parameter_list()}
        object_class_dict = {x.id: x.name for x in self.object_class_list()}
        relationship_class_dict = {x.id: x.name for x in self.wide_relationship_class_list()}
        for kwargs in kwargs_list:
            try:
                self.check_parameter_definition(
                    kwargs, parameter_definition_names, object_class_dict, relationship_class_dict)
                checked_kwargs_list.append(kwargs)
                parameter_definition_names.add(kwargs["name"])
            except SpineIntegrityError as e:
                if raise_intgr_error:
                    raise e
                intgr_error_log.append(e.msg)
        return checked_kwargs_list, intgr_error_log

    def check_parameter_definitions_for_update(self, *kwargs_list, raise_intgr_error=True):
        """Check that parameters respect integrity constraints for an update operation."""
        intgr_error_log = []
        checked_kwargs_list = list()
        parameter_definition_names = {x.name for x in self.parameter_list()}
        parameter_definition_dict = {
            x.id: {
                "name": x.name,
                "object_class_id": x.object_class_id,
                "relationship_class_id": x.relationship_class_id
            } for x in self.parameter_list()}
        object_class_dict = {x.id: x.name for x in self.object_class_list()}
        relationship_class_dict = {x.id: x.name for x in self.wide_relationship_class_list()}
        for kwargs in kwargs_list:
            try:
                id = kwargs["id"]
            except KeyError:
                msg = "Missing parameter definition identifier."
                if raise_intgr_error:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(msg)
                continue
            try:
                updated_kwargs = parameter_definition_dict.pop(id)
                parameter_definition_names.remove(updated_kwargs["name"])
            except KeyError:
                msg = "Parameter not found."
                if raise_intgr_error:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(msg)
                continue
            try:
                # Allow turning an object class parameter into a relationship class parameter, and viceversa
                if "object_class_id" in kwargs:
                    kwargs.setdefault("relationship_class_id", None)
                if "relationship_class_id" in kwargs:
                    kwargs.setdefault("object_class_id", None)
                updated_kwargs.update(kwargs)
                self.check_parameter_definition(
                    updated_kwargs, parameter_definition_names,
                    object_class_dict, relationship_class_dict)
                checked_kwargs_list.append(kwargs)
                parameter_definition_dict[id] = updated_kwargs
                parameter_definition_names.add(updated_kwargs["name"])
            except SpineIntegrityError as e:
                if raise_intgr_error:
                    raise e
                intgr_error_log.append(e.msg)
        return checked_kwargs_list, intgr_error_log

    def check_parameter_definition(
            self, kwargs, parameter_definition_names, object_class_dict, relationship_class_dict):
        """Raise a SpineIntegrityError if the parameter definition given by `kwargs` violates any
        integrity constraints."""
        object_class_id = kwargs.get("object_class_id", None)
        relationship_class_id = kwargs.get("relationship_class_id", None)
        if object_class_id and relationship_class_id:
            try:
                object_class_name = object_class_dict[object_class_id]
            except KeyError:
                object_class_name = 'object class id ' + object_class_id
            try:
                relationship_class_name = relationship_class_dict[relationship_class_id]
            except KeyError:
                relationship_class_name = 'relationship class id ' + relationship_class_id
            raise SpineIntegrityError("Can't associate a parameter to both object class '{}' and "
                                      "relationship class '{}'.".format(object_class_name, relationship_class_name))
        if object_class_id:
            if object_class_id not in object_class_dict:
                raise SpineIntegrityError("Object class not found.")
            try:
                name = kwargs["name"]
            except KeyError:
                raise SpineIntegrityError("Missing parameter name.")
            if name in parameter_definition_names:
                raise SpineIntegrityError("There can't be more than one parameter called '{}'.".format(name))
        elif relationship_class_id:
            if relationship_class_id not in relationship_class_dict:
                raise SpineIntegrityError("Relationship class not found.")
            try:
                name = kwargs["name"]
            except KeyError:
                raise SpineIntegrityError("Missing parameter name.")
            if name in parameter_definition_names:
                raise SpineIntegrityError("There can't be more than one parameter called '{}'.".format(name))
        else:
            raise SpineIntegrityError("Missing object class or relationship class identifier.")

    def check_parameter_values_for_insert(self, *kwargs_list, raise_intgr_error=True):
        """Check that parameter values respect integrity constraints for an insert operation."""
        intgr_error_log = []
        checked_kwargs_list = list()
        # Per's suggestions
        object_parameter_values = {
            (x.object_id, x.parameter_definition_id) for x in self.parameter_value_list() if x.object_id
        }
        relationship_parameter_values = {
            (x.relationship_id, x.parameter_definition_id) for x in self.parameter_value_list() if x.relationship_id
        }
        parameter_definition_dict = {
            x.id: {
                "name": x.name,
                "object_class_id": x.object_class_id,
                "relationship_class_id": x.relationship_class_id
            } for x in self.parameter_list()}
        object_dict = {
            x.id: {
                'class_id': x.class_id,
                'name': x.name
            } for x in self.object_list()}
        relationship_dict = {
            x.id: {
                'class_id': x.class_id,
                'name': x.name
            } for x in self.wide_relationship_list()}
        for kwargs in kwargs_list:
            try:
                self.check_parameter_value(
                    kwargs, object_parameter_values, relationship_parameter_values,
                    parameter_definition_dict, object_dict, relationship_dict)
                checked_kwargs_list.append(kwargs)
                # Update sets of tuples (object_id, parameter_definition_id)
                # and (relationship_id, parameter_definition_id)
                object_id = kwargs.get("object_id", None)
                relationship_id = kwargs.get("relationship_id", None)
                if object_id:
                    object_parameter_values.add((object_id, kwargs['parameter_id']))
                elif relationship_id:
                    relationship_parameter_values.add((relationship_id, kwargs['parameter_id']))
            except SpineIntegrityError as e:
                if raise_intgr_error:
                    raise e
                intgr_error_log.append(e.msg)
        return checked_kwargs_list, intgr_error_log

    def check_parameter_values_for_update(self, *kwargs_list, raise_intgr_error=True):
        """Check that parameter values respect integrity constraints for an update operation."""
        intgr_error_log = []
        checked_kwargs_list = list()
        parameter_value_dict = {
            x.id: {
                "parameter_definition_id": x.parameter_definition_id,
                "object_id": x.object_id,
                "relationship_id": x.relationship_id
            } for x in self.parameter_value_list()}
        # Per's suggestions
        object_parameter_values = {
            (x.object_id, x.parameter_definition_id) for x in self.parameter_value_list() if x.object_id
        }
        relationship_parameter_values = {
            (x.relationship_id, x.parameter_definition_id) for x in self.parameter_value_list() if x.relationship_id
        }
        parameter_definition_dict = {
            x.id: {
                "name": x.name,
                "object_class_id": x.object_class_id,
                "relationship_class_id": x.relationship_class_id
            } for x in self.parameter_list()}
        object_dict = {
            x.id: {
                'class_id': x.class_id,
                'name': x.name
            } for x in self.object_list()}
        relationship_dict = {
            x.id: {
                'class_id': x.class_id,
                'name': x.name
            } for x in self.wide_relationship_list()}
        for kwargs in kwargs_list:
            try:
                id = kwargs["id"]
            except KeyError:
                msg = "Missing parameter value identifier."
                if raise_intgr_error:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(msg)
                continue
            try:
                updated_kwargs = parameter_value_dict.pop(id)
                # Remove current tuples (object_id, parameter_definition_id)
                # and (relationship_id, parameter_definition_id)
                object_id = updated_kwargs.get("object_id", None)
                relationship_id = updated_kwargs.get("relationship_id", None)
                if object_id:
                    object_parameter_values.remove((object_id, updated_kwargs['parameter_definition_id']))
                elif relationship_id:
                    relationship_parameter_values.remove((relationship_id, updated_kwargs['parameter_definition_id']))
            except KeyError:
                msg = "Parameter value not found."
                if raise_intgr_error:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(msg)
                continue
            try:
                # Allow turning an object parameter value into a relationship parameter value, and viceversa
                if "object_id" in kwargs:
                    kwargs.setdefault("relationship_id", None)
                if "relationship_id" in kwargs:
                    kwargs.setdefault("object_id", None)
                updated_kwargs.update(kwargs)
                self.check_parameter_value(
                    updated_kwargs, object_parameter_values, relationship_parameter_values,
                    parameter_definition_dict, object_dict, relationship_dict)
                checked_kwargs_list.append(kwargs)
                parameter_value_dict[id] = updated_kwargs
                # Add updated tuples (object_id, parameter_definition_id)
                # and (relationship_id, parameter_definition_id)
                object_id = updated_kwargs.get("object_id", None)
                relationship_id = updated_kwargs.get("relationship_id", None)
                if object_id:
                    object_parameter_values.add((object_id, updated_kwargs['parameter_definition_id']))
                elif relationship_id:
                    relationship_parameter_values.add((relationship_id, updated_kwargs['parameter_definition_id']))
            except SpineIntegrityError as e:
                if raise_intgr_error:
                    raise e
                intgr_error_log.append(e.msg)
        return checked_kwargs_list, intgr_error_log

    def check_parameter_value(
            self, kwargs, object_parameter_values, relationship_parameter_values,
            parameter_definition_dict, object_dict, relationship_dict):
        """Raise a SpineIntegrityError if the parameter value given by `kwargs` violates any integrity constraints."""
        try:
            parameter_definition_id = kwargs["parameter_definition_id"]
        except KeyError:
            raise SpineIntegrityError("Missing parameter identifier.")
        try:
            parameter_definition = parameter_definition_dict[parameter_definition_id]
        except KeyError:
            raise SpineIntegrityError("Parameter not found.")
        object_id = kwargs.get("object_id", None)
        relationship_id = kwargs.get("relationship_id", None)
        if object_id and relationship_id:
            try:
                object_name = object_dict[object_id]['name']
            except KeyError:
                object_name = 'object id ' + object_id
            try:
                relationship_name = relationship_dict[relationship_id]['name']
            except KeyError:
                relationship_name = 'relationship id ' + relationship_id
            raise SpineIntegrityError("Can't associate a parameter value to both "
                                      "object '{}' and relationship '{}'.".format(object_name, relationship_name))
        if object_id:
            try:
                object_class_id = object_dict[object_id]['class_id']
            except KeyError:
                raise SpineIntegrityError("Object not found")
            if object_class_id != parameter_definition["object_class_id"]:
                object_name = object_dict[object_id]['name']
                parameter_name = parameter_definition['name']
                raise SpineIntegrityError("Incorrect object '{}' for "
                                          "parameter '{}'.".format(object_name, parameter_name))
            if (object_id, parameter_definition_id) in object_parameter_values:
                object_name = object_dict[object_id]['name']
                parameter_name = parameter_definition['name']
                raise SpineIntegrityError("The value of parameter '{}' for object '{}' is "
                                          "already specified.".format(parameter_name, object_name))
        elif relationship_id:
            try:
                relationship_class_id = relationship_dict[relationship_id]['class_id']
            except KeyError:
                raise SpineIntegrityError("Relationship not found")
            if relationship_class_id != parameter_definition["relationship_class_id"]:
                relationship_name = relationship_dict[relationship_id]['name']
                parameter_name = parameter_definition['name']
                raise SpineIntegrityError("Incorrect relationship '{}' for "
                                          "parameter '{}'.".format(relationship_name, parameter_name))
            if (relationship_id, parameter_definition_id) in relationship_parameter_values:
                relationship_name = relationship_dict[relationship_id]['name']
                parameter_name = parameter_definition['name']
                raise SpineIntegrityError("The value of parameter '{}' for relationship '{}' is "
                                          "already specified.".format(parameter_name, relationship_name))
        else:
            raise SpineIntegrityError("Missing object or relationship identifier.")

    def check_parameter_tags_for_insert(self, *kwargs_list, raise_intgr_error=True):
        """Check that parameter tags respect integrity constraints for an insert operation."""
        intgr_error_log = []
        checked_kwargs_list = list()
        parameter_tags = {x.tag for x in self.parameter_tag_list()}
        for kwargs in kwargs_list:
            try:
                self.check_parameter_tag(kwargs, parameter_tags)
                checked_kwargs_list.append(kwargs)
                parameter_tags.add(kwargs["tag"])
            except SpineIntegrityError as e:
                if raise_intgr_error:
                    raise e
                intgr_error_log.append(e.msg)
        return checked_kwargs_list, intgr_error_log

    def check_parameter_tags_for_update(self, *kwargs_list, raise_intgr_error=True):
        """Check that parameter tags respect integrity constraints for an update operation.
        NOTE: To check for an update we basically 'remove' the current instance
        and then check for an insert of the updated instance.
        """
        intgr_error_log = []
        checked_kwargs_list = list()
        parameter_tag_dict = {x.id: {"tag": x.tag} for x in self.parameter_tag_list()}
        parameter_tags = {x.tag for x in self.parameter_tag_list()}
        for kwargs in kwargs_list:
            try:
                id = kwargs["id"]
            except KeyError:
                msg = "Missing parameter tag identifier."
                if raise_intgr_error:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(msg)
                continue
            try:
                # 'Remove' current instance
                updated_kwargs = parameter_tag_dict.pop(id)
                parameter_tags.remove(updated_kwargs["tag"])
            except KeyError:
                msg = "Parameter tag not found."
                if raise_intgr_error:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(msg)
                continue
            # Check for an insert of the updated instance
            try:
                updated_kwargs.update(kwargs)
                self.check_parameter_tag(updated_kwargs, parameter_tags)
                checked_kwargs_list.append(kwargs)
                parameter_tag_dict[id] = updated_kwargs
                parameter_tags.add(updated_kwargs["tag"])
            except SpineIntegrityError as e:
                if raise_intgr_error:
                    raise e
                intgr_error_log.append(e.msg)
        return checked_kwargs_list, intgr_error_log

    def check_parameter_tag(self, kwargs, parameter_tags):
        """Raise a SpineIntegrityError if the parameter tag given by `kwargs` violates any
        integrity constraints.
        """
        try:
            tag = kwargs["tag"]
        except KeyError:
            raise SpineIntegrityError("Missing parameter tag.")
        if tag in parameter_tags:
            raise SpineIntegrityError("There can't be more than one '{}' tag.".format(tag))

    def check_parameter_definition_tags_for_insert(self, *kwargs_list, raise_intgr_error=True):
        """Check that parameter definition tags respect integrity constraints for an insert operation."""
        intgr_error_log = []
        checked_kwargs_list = list()
        parameter_definition_tags = {
            (x.parameter_definition_id, x.parameter_tag_id) for x in self.parameter_definition_tag_list()}
        parameter_name_dict = {x.id: x.name for x in self.parameter_list()}
        parameter_tag_dict = {x.id: x.tag for x in self.parameter_tag_list()}
        for kwargs in kwargs_list:
            try:
                self.check_parameter_definition_tag(
                    kwargs, parameter_definition_tags, parameter_name_dict, parameter_tag_dict)
                checked_kwargs_list.append(kwargs)
                parameter_definition_tags.add((kwargs["parameter_definition_id"], kwargs["parameter_tag_id"]))
            except SpineIntegrityError as e:
                if raise_intgr_error:
                    raise e
                intgr_error_log.append(e.msg)
        return checked_kwargs_list, intgr_error_log

    def check_parameter_definition_tag(self, kwargs, parameter_definition_tags,
            parameter_name_dict, parameter_tag_dict):
        """Raise a SpineIntegrityError if the parameter definition tag given by `kwargs` violates any
        integrity constraints.
        """
        try:
            parameter_definition_id = kwargs["parameter_definition_id"]
        except KeyError:
            raise SpineIntegrityError("Missing parameter definition identifier.")
        try:
            parameter_tag_id = kwargs["parameter_tag_id"]
        except KeyError:
            raise SpineIntegrityError("Missing parameter tag identifier.")
        try:
            parameter_name = parameter_name_dict[parameter_definition_id]
        except KeyError:
            raise SpineIntegrityError("Parameter definition not found.")
        try:
            tag = parameter_tag_dict[parameter_tag_id]
        except KeyError:
            raise SpineIntegrityError("Parameter tag not found.")
        if (parameter_definition_id, parameter_tag_id) in parameter_definition_tags:
            raise SpineIntegrityError("Parameter '{0}' already has the tag '{1}'.".format(parameter_name, tag))

    def check_wide_parameter_enums_for_insert(self, *wide_kwargs_list, raise_intgr_error=True):
        """Check that parameter enums respect integrity constraints for an insert operation."""
        intgr_error_log = []
        checked_wide_kwargs_list = list()
        parameter_enum_names = {x.name for x in self.wide_parameter_enum_list()}
        for wide_kwargs in wide_kwargs_list:
            try:
                self.check_wide_parameter_enum(wide_kwargs, parameter_enum_names)
                checked_wide_kwargs_list.append(wide_kwargs)
                parameter_enum_names.add((wide_kwargs["name"]))
            except SpineIntegrityError as e:
                if raise_intgr_error:
                    raise e
                intgr_error_log.append(e.msg)
        return checked_wide_kwargs_list, intgr_error_log

    def check_wide_parameter_enums_for_update(self, *wide_kwargs_list, raise_intgr_error=True):
        """Check that parameter enums respect integrity constraints for an update operation.
        NOTE: To check for an update we basically 'remove' the current instance
        and then check for an insert of the updated instance.
        """
        intgr_error_log = []
        checked_wide_kwargs_list = list()
        parameter_enum_dict = {
            x.id: {
                "name": x.name,
                "element_list": x.element_list.split(",")
            } for x in self.wide_parameter_enum_list()
        }
        parameter_enum_names = {x.name for x in self.wide_parameter_enum_list()}
        for wide_kwargs in wide_kwargs_list:
            try:
                id = wide_kwargs["id"]
            except KeyError:
                msg = "Missing parameter enum identifier."
                if raise_intgr_error:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(msg)
                continue
            try:
                # 'Remove' current instance
                updated_wide_kwargs = parameter_enum_dict.pop(id)
                parameter_enum_names.remove(updated_wide_kwargs['name'])
            except KeyError:
                msg = "Parameter enum not found."
                if raise_intgr_error:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(msg)
                continue
            # Check for an insert of the updated instance
            try:
                updated_wide_kwargs.update(wide_kwargs)
                self.check_wide_parameter_enum(updated_wide_kwargs, parameter_enum_names)
                checked_wide_kwargs_list.append(wide_kwargs)
                parameter_enum_dict[id] = updated_wide_kwargs
                parameter_enum_names.add(updated_wide_kwargs["name"])
            except SpineIntegrityError as e:
                if raise_intgr_error:
                    raise e
                intgr_error_log.append(e.msg)
        return checked_wide_kwargs_list, intgr_error_log

    def check_wide_parameter_enum(self, wide_kwargs, parameter_enum_names):
        """Raise a SpineIntegrityError if the parameter enum given by `wide_kwargs` violates any
        integrity constraints.
        """
        try:
            name = wide_kwargs["name"]
        except KeyError:
            raise SpineIntegrityError("Missing parameter enum name.")
        if name in parameter_enum_names:
            raise SpineIntegrityError("There can't be more than one parameter enum called '{}'.".format(name))
        try:
            value_list = wide_kwargs["value_list"]
        except KeyError:
            raise SpineIntegrityError("Missing list of values.")
        if len(value_list) != len(set(value_list)):
            raise SpineIntegrityError("Values must be unique.")

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

    def add_object_classes(self, *kwargs_list, raise_intgr_error=True):
        """Add object classes to database.

        Args:
            kwargs_list (iter): list of dictionaries which correspond to the instances to add
            raise_intgr_error (bool): if True (the default) SpineIntegrityError are raised. Otherwise
                they are catched and returned as a log

        Returns:
            object_classes (list): added instances
            intgr_error_log (list): list of integrity error messages
        """
        checked_kwargs_list, intgr_error_log = self.check_object_classes_for_insert(
            *kwargs_list, raise_intgr_error=raise_intgr_error)
        new_item_list = self._add_object_classes(*checked_kwargs_list)
        if not raise_intgr_error:
            return new_item_list, intgr_error_log
        return new_item_list

    def _add_object_classes(self, *kwargs_list):
        """Add object classes to database without testing classes for integrity

        Args:
            kwargs_list (iter): list of dictionaries which correspond to the instances to add
            raise_intgr_error (bool): if True (the default) SpineIntegrityError are raised. Otherwise
                they are catched and returned as a log

        Returns:
            object_classes (list): added instances
        """
        next_id = self.next_id_with_lock()
        if next_id.object_class_id:
            id = next_id.object_class_id
        else:
            max_id = self.session.query(func.max(self.ObjectClass.id)).scalar()
            id = max_id + 1 if max_id else 1
        try:
            item_list = list()
            id_list = set(range(id, id + len(kwargs_list)))
            for kwargs in kwargs_list:
                kwargs["id"] = id
                item_list.append(kwargs)
                id += 1
            self.session.bulk_insert_mappings(self.DiffObjectClass, item_list)
            next_id.object_class_id = id
            self.session.commit()
            self.new_item_id["object_class"].update(id_list)
            new_item_list = self.object_class_list(id_list=id_list)
            return new_item_list
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while inserting object classes: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def add_objects(self, *kwargs_list, raise_intgr_error=True):
        """Add objects to database.

        Args:
            kwargs_list (iter): list of dictionaries which correspond to the instances to add
            raise_intgr_error (bool): if True (the default) SpineIntegrityError are raised. Otherwise
                they are catched and returned as a log

        Returns:
            objects (list): added instances
            intgr_error_log (list): list of integrity error messages
        """
        checked_kwargs_list, intgr_error_log = self.check_objects_for_insert(
            *kwargs_list, raise_intgr_error=raise_intgr_error)
        new_item_list = self._add_objects(*checked_kwargs_list)
        if not raise_intgr_error:
            return new_item_list, intgr_error_log
        return new_item_list

    def _add_objects(self, *kwargs_list):
        """Add objects to database without checking integrity

        Args:
            kwargs_list (iter): list of dictionaries which correspond to the instances to add

        Returns:
            objects (list): added instances
        """
        next_id = self.next_id_with_lock()
        if next_id.object_id:
            id = next_id.object_id
        else:
            max_id = self.session.query(func.max(self.Object.id)).scalar()
            id = max_id + 1 if max_id else 1
        try:
            item_list = list()
            id_list = set(range(id, id + len(kwargs_list)))
            for kwargs in kwargs_list:
                kwargs["id"] = id
                item_list.append(kwargs)
                id += 1
            self.session.bulk_insert_mappings(self.DiffObject, item_list)
            next_id.object_id = id
            self.session.commit()
            self.new_item_id["object"].update(id_list)
            new_item_list = self.object_list(id_list=id_list)
            return new_item_list
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while inserting objects: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def add_wide_relationship_classes(self, *wide_kwargs_list, raise_intgr_error=True):
        """Add relationship classes to database.

        Args:
            wide_kwargs_list (iter): list of dictionaries which correspond to the instances to add
            raise_intgr_error (bool): if True (the default) SpineIntegrityError are raised. Otherwise
                they are catched and returned as a log

        Returns:
            wide_relationship_classes (list): added instances
            intgr_error_log (list): list of integrity error messages
        """
        checked_wide_kwargs_list, intgr_error_log = self.check_wide_relationship_classes_for_insert(
            *wide_kwargs_list, raise_intgr_error=raise_intgr_error)
        new_item_list = self._add_wide_relationship_classes(*checked_wide_kwargs_list)
        if not raise_intgr_error:
            return new_item_list, intgr_error_log
        return new_item_list

    def _add_wide_relationship_classes(self, *wide_kwargs_list):
        """Add relationship classes to database without integrity check

        Args:
            wide_kwargs_list (iter): list of dictionaries which correspond to the instances to add
            raise_intgr_error (bool): if True (the default) SpineIntegrityError are raised. Otherwise
                they are catched and returned as a log

        Returns:
            wide_relationship_classes (list): added instances
        """
        next_id = self.next_id_with_lock()
        if next_id.relationship_class_id:
            id = next_id.relationship_class_id
        else:
            max_id = self.session.query(func.max(self.RelationshipClass.id)).scalar()
            id = max_id + 1 if max_id else 1
        try:
            item_list = list()
            id_list = set(range(id, id + len(wide_kwargs_list)))
            for wide_kwargs in wide_kwargs_list:
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
            new_item_list = self.wide_relationship_class_list(id_list=id_list)
            return new_item_list
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while inserting relationship classes: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def add_wide_relationships(self, *wide_kwargs_list, raise_intgr_error=True):
        """Add relationships to database.

        Args:
            wide_kwargs_list (iter): list of dictionaries which correspond to the instances to add
            raise_intgr_error (bool): if True (the default) SpineIntegrityError are raised. Otherwise
                they are catched and returned as a log

        Returns:
            wide_relationships (list): added instances
            intgr_error_log (list): list of integrity error messages
        """
        checked_wide_kwargs_list, intgr_error_log = self.check_wide_relationships_for_insert(
            *wide_kwargs_list, raise_intgr_error=raise_intgr_error)
        new_item_list = self._add_wide_relationships(*checked_wide_kwargs_list)
        if not raise_intgr_error:
            return new_item_list, intgr_error_log
        return new_item_list

    def _add_wide_relationships(self, *wide_kwargs_list):
        """Add relationships to database without integrity

        Args:
            wide_kwargs_list (iter): list of dictionaries which correspond to the instances to add

        Returns:
            wide_relationships (list): added instances
        """
        next_id = self.next_id_with_lock()
        if next_id.relationship_id:
            id = next_id.relationship_id
        else:
            max_id = self.session.query(func.max(self.Relationship.id)).scalar()
            id = max_id + 1 if max_id else 1
        try:
            item_list = list()
            id_list = set(range(id, id + len(wide_kwargs_list)))
            for wide_kwargs in wide_kwargs_list:
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
            new_item_list = self.wide_relationship_list(id_list=id_list)
            return new_item_list
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while inserting relationships: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def add_parameters(self, *kwargs_list, raise_intgr_error=True):
        """Add parameter to database.

        Args:
            kwargs_list (iter): list of dictionaries which correspond to the instances to add
            raise_intgr_error (bool): if True (the default) SpineIntegrityError are raised. Otherwise
                they are catched and returned as a log

        Returns:
            parameters (list): added instances
            intgr_error_log (list): list of integrity error messages
        """
        checked_kwargs_list, intgr_error_log = self.check_parameter_definitions_for_insert(
            *kwargs_list, raise_intgr_error=raise_intgr_error)
        new_item_list = self._add_parameters(*checked_kwargs_list)
        if not raise_intgr_error:
            return new_item_list, intgr_error_log
        return new_item_list

    def _add_parameters(self, *kwargs_list):
        """Add parameter to database without integrity check

        Args:
            kwargs_list (iter): list of dictionaries which correspond to the instances to add

        Returns:
            parameters (list): added instances
        """
        next_id = self.next_id_with_lock()
        if next_id.parameter_definition_id:
            id = next_id.parameter_definition_id
        else:
            max_id = self.session.query(func.max(self.ParameterDefinition.id)).scalar()
            id = max_id + 1 if max_id else 1
        try:
            item_list = list()
            id_list = set(range(id, id + len(kwargs_list)))
            for kwargs in kwargs_list:
                kwargs["id"] = id
                item_list.append(kwargs)
                id += 1
            self.session.bulk_insert_mappings(self.DiffParameterDefinition, item_list)
            next_id.parameter_definition_id = id
            self.session.commit()
            self.new_item_id["parameter_definition"].update(id_list)
            new_item_list = self.parameter_list(id_list=id_list)
            return new_item_list
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while inserting parameters: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def add_parameter_values(self, *kwargs_list, raise_intgr_error=True):
        """Add parameter value to database.

        Returns:
            parameter_values (list): added instances
            intgr_error_log (list): list of integrity error messages
        """
        # FIXME: this should be removed once the 'parameter_definition_id' comes in the kwargs
        for kwargs in kwargs_list:
            kwargs["parameter_definition_id"] = kwargs["parameter_id"]
        checked_kwargs_list, intgr_error_log = self.check_parameter_values_for_insert(
            *kwargs_list, raise_intgr_error=raise_intgr_error)
        new_item_list = self._add_parameter_values(*checked_kwargs_list)
        if not raise_intgr_error:
            return new_item_list, intgr_error_log
        return new_item_list

    def _add_parameter_values(self, *kwargs_list):
        """Add parameter value to database.

        Returns:
            parameter_values (list): added instances
        """
        next_id = self.next_id_with_lock()
        if next_id.parameter_value_id:
            id = next_id.parameter_value_id
        else:
            max_id = self.session.query(func.max(self.ParameterValue.id)).scalar()
            id = max_id + 1 if max_id else 1
        try:
            item_list = list()
            id_list = set(range(id, id + len(kwargs_list)))
            for kwargs in kwargs_list:
                kwargs["id"] = id
                item_list.append(kwargs)
                id += 1
            self.session.bulk_insert_mappings(self.DiffParameterValue, item_list)
            next_id.parameter_value_id = id
            self.session.commit()
            self.new_item_id["parameter_value"].update(id_list)
            new_item_list = self.parameter_value_list(id_list=id_list)
            return new_item_list
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while inserting parameter values: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def add_parameter_tags(self, *kwargs_list, raise_intgr_error=True):
        """Add parameter tags to database.

        Args:
            kwargs_list (iter): list of dictionaries which correspond to the instances to add
            raise_intgr_error (bool): if True (the default) SpineIntegrityError are raised. Otherwise
                they are catched and returned as a log

        Returns:
            parameter_tags (list): added instances
            intgr_error_log (list): list of integrity error messages
        """
        checked_kwargs_list, intgr_error_log = self.check_parameter_tags_for_insert(
            *kwargs_list, raise_intgr_error=raise_intgr_error)
        new_item_list = self._add_parameter_tags(*checked_kwargs_list)
        if not raise_intgr_error:
            return new_item_list, intgr_error_log
        return new_item_list

    def _add_parameter_tags(self, *kwargs_list):
        """Add parameter tags to database.

        Returns:
            parameter_tags (list): added instances
        """
        next_id = self.next_id_with_lock()
        if next_id.parameter_tag_id:
            id = next_id.parameter_tag_id
        else:
            max_id = self.session.query(func.max(self.ParameterTag.id)).scalar()
            id = max_id + 1 if max_id else 1
        try:
            item_list = list()
            id_list = set(range(id, id + len(kwargs_list)))
            for kwargs in kwargs_list:
                kwargs["id"] = id
                item_list.append(kwargs)
                id += 1
            self.session.bulk_insert_mappings(self.DiffParameterTag, item_list)
            next_id.parameter_tag_id = id
            self.session.commit()
            self.new_item_id["parameter_tag"].update(id_list)
            new_item_list = self.parameter_tag_list(id_list=id_list)
            return new_item_list
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while inserting parameter tags: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def add_parameter_definition_tags(self, *kwargs_list, raise_intgr_error=True):
        """Add parameter definition tags to database.

        Args:
            kwargs_list (iter): list of dictionaries which correspond to the instances to add
            raise_intgr_error (bool): if True (the default) SpineIntegrityError are raised. Otherwise
                they are catched and returned as a log

        Returns:
            parameter_definition_tags (list): added instances
            intgr_error_log (list): list of integrity error messages
        """
        checked_kwargs_list, intgr_error_log = self.check_parameter_definition_tags_for_insert(
            *kwargs_list, raise_intgr_error=raise_intgr_error)
        new_item_list = self._add_parameter_definition_tags(*checked_kwargs_list)
        if not raise_intgr_error:
            return new_item_list, intgr_error_log
        return new_item_list

    def _add_parameter_definition_tags(self, *kwargs_list):
        """Add parameter definition tags to database.

        Returns:
            parameter_definition_tags (list): added instances
        """
        next_id = self.next_id_with_lock()
        if next_id.parameter_definition_tag_id:
            id = next_id.parameter_definition_tag_id
        else:
            max_id = self.session.query(func.max(self.ParameterDefinitionTag.id)).scalar()
            id = max_id + 1 if max_id else 1
        try:
            item_list = list()
            id_list = set(range(id, id + len(kwargs_list)))
            for kwargs in kwargs_list:
                kwargs["id"] = id
                item_list.append(kwargs)
                id += 1
            self.session.bulk_insert_mappings(self.DiffParameterDefinitionTag, item_list)
            next_id.parameter_definition_tag_id = id
            self.session.commit()
            self.new_item_id["parameter_definition_tag"].update(id_list)
            new_item_list = self.parameter_definition_tag_list(id_list=id_list)
            return new_item_list
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while inserting parameter definition tags: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def add_wide_parameter_enums(self, *wide_kwargs_list, raise_intgr_error=True):
        """Add wide parameter enums to database.

        Args:
            wide_kwargs_list (iter): list of dictionaries which correspond to the instances to add
            raise_intgr_error (bool): if True (the default) SpineIntegrityError are raised. Otherwise
                they are catched and returned as a log

        Returns:
            parameter_enums (list): added instances
            intgr_error_log (list): list of integrity error messages
        """
        checked_wide_kwargs_list, intgr_error_log = self.check_wide_parameter_enums_for_insert(
            *wide_kwargs_list, raise_intgr_error=raise_intgr_error)
        new_item_list = self._add_wide_parameter_enums(*checked_wide_kwargs_list)
        if not raise_intgr_error:
            return new_item_list, intgr_error_log
        return new_item_list

    def _add_wide_parameter_enums(self, *wide_kwargs_list):
        """Add wide parameter enums to database.

        Returns:
            parameter_enums (list): added instances
        """
        next_id = self.next_id_with_lock()
        if next_id.parameter_enum_id:
            id = next_id.parameter_enum_id
        else:
            max_id = self.session.query(func.max(self.ParameterEnum.id)).scalar()
            id = max_id + 1 if max_id else 1
        try:
            item_list = list()
            id_list = set(range(id, id + len(wide_kwargs_list)))
            for wide_kwargs in wide_kwargs_list:
                for k, value in enumerate(wide_kwargs['value_list']):
                    narrow_kwargs = {
                        'id': id,
                        'name': wide_kwargs['name'],
                        'value_index': k,
                        'value': value
                    }
                    item_list.append(narrow_kwargs)
                id += 1
            self.session.bulk_insert_mappings(self.DiffParameterEnum, item_list)
            next_id.parameter_enum_id = id
            self.session.commit()
            self.new_item_id["parameter_enum"].update(id_list)
            new_item_list = self.wide_parameter_enum_list(id_list=id_list)
            return new_item_list
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while inserting parameter enums: {}".format(e.orig.args)
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

    def update_object_classes(self, *kwargs_list, raise_intgr_error=True):
        """Update object classes."""
        checked_kwargs_list, intgr_error_log = self.check_object_classes_for_update(
            *kwargs_list, raise_intgr_error=raise_intgr_error)
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
            updated_item_list = self.object_class_list(id_list=updated_ids)
            if not raise_intgr_error:
                return updated_item_list, intgr_error_log
            return updated_item_list
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while updating object classes: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def update_objects(self, *kwargs_list, raise_intgr_error=True):
        """Update objects."""
        checked_kwargs_list, intgr_error_log = self.check_objects_for_update(
            *kwargs_list, raise_intgr_error=raise_intgr_error)
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
            updated_item_list = self.object_list(id_list=updated_ids)
            if not raise_intgr_error:
                return updated_item_list, intgr_error_log
            return updated_item_list
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while updating objects: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def update_wide_relationship_classes(self, *wide_kwargs_list, raise_intgr_error=True):
        """Update relationship classes."""
        checked_wide_kwargs_list, intgr_error_log = self.check_wide_relationship_classes_for_update(
            *wide_kwargs_list, raise_intgr_error=raise_intgr_error)
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
            updated_item_list = self.wide_relationship_class_list(id_list=updated_ids)
            if not raise_intgr_error:
                return updated_item_list, intgr_error_log
            return updated_item_list
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while updating relationship classes: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def update_wide_relationships(self, *wide_kwargs_list, raise_intgr_error=True):
        """Update relationships."""
        checked_wide_kwargs_list, intgr_error_log = self.check_wide_relationships_for_update(
            *wide_kwargs_list, raise_intgr_error=raise_intgr_error)
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
            updated_item_list = self.wide_relationship_list(id_list=updated_ids)
            if not raise_intgr_error:
                return updated_item_list, intgr_error_log
            return updated_item_list
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while updating relationships: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def update_parameters(self, *kwargs_list, raise_intgr_error=True):
        """Update parameters."""
        checked_kwargs_list, intgr_error_log = self.check_parameter_definitions_for_update(
            *kwargs_list, raise_intgr_error=raise_intgr_error)
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
                diff_item = self.session.query(self.DiffParameterDefinition).filter_by(id=id).one_or_none()
                if diff_item:
                    updated_kwargs = attr_dict(diff_item)
                    updated_kwargs.update(kwargs)
                    items_for_update.append(updated_kwargs)
                    updated_ids.add(id)
                else:
                    item = self.session.query(self.ParameterDefinition).filter_by(id=id).one_or_none()
                    if item:
                        updated_kwargs = attr_dict(item)
                        updated_kwargs.update(kwargs)
                        items_for_insert.append(updated_kwargs)
                        new_dirty_ids.add(id)
                        updated_ids.add(id)
            self.session.bulk_update_mappings(self.DiffParameterDefinition, items_for_update)
            self.session.bulk_insert_mappings(self.DiffParameterDefinition, items_for_insert)
            self.session.commit()
            self.touched_item_id["parameter_definition"].update(new_dirty_ids)
            self.dirty_item_id["parameter_definition"].update(new_dirty_ids)
            updated_item_list = self.parameter_list(id_list=updated_ids)
            if not raise_intgr_error:
                return updated_item_list, intgr_error_log
            return updated_item_list
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while updating parameters: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def update_parameter_values(self, *kwargs_list, raise_intgr_error=True):
        """Update parameter values."""
        checked_kwargs_list, intgr_error_log = self.check_parameter_values_for_update(
            *kwargs_list, raise_intgr_error=raise_intgr_error)
        updated_item_list = self._update_parameter_values(*checked_kwargs_list)
        if not raise_intgr_error:
            return updated_item_list, intgr_error_log
        return updated_item_list

    def _update_parameter_values(self, *kwargs_list):
        """Update parameter values."""
        try:
            items_for_update = list()
            items_for_insert = list()
            new_dirty_ids = set()
            updated_ids = set()
            for kwargs in kwargs_list:
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
            updated_item_list = self.parameter_value_list(id_list=updated_ids)
            return updated_item_list
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while updating parameter values: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def update_parameter_tags(self, *kwargs_list, raise_intgr_error=True):
        """Update parameter tags."""
        checked_kwargs_list, intgr_error_log = self.check_parameter_tags_for_update(
            *kwargs_list, raise_intgr_error=raise_intgr_error)
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
                diff_item = self.session.query(self.DiffParameterTag).filter_by(id=id).one_or_none()
                if diff_item:
                    updated_kwargs = attr_dict(diff_item)
                    updated_kwargs.update(kwargs)
                    items_for_update.append(updated_kwargs)
                    updated_ids.add(id)
                else:
                    item = self.session.query(self.ParameterTag).filter_by(id=id).one_or_none()
                    if item:
                        updated_kwargs = attr_dict(item)
                        updated_kwargs.update(kwargs)
                        items_for_insert.append(updated_kwargs)
                        new_dirty_ids.add(id)
                        updated_ids.add(id)
            self.session.bulk_update_mappings(self.DiffParameterTag, items_for_update)
            self.session.bulk_insert_mappings(self.DiffParameterTag, items_for_insert)
            self.session.commit()
            self.touched_item_id["parameter_tag"].update(new_dirty_ids)
            self.dirty_item_id["parameter_tag"].update(new_dirty_ids)
            updated_item_list = self.parameter_tag_list(id_list=updated_ids)
            if not raise_intgr_error:
                return updated_item_list, intgr_error_log
            return updated_item_list
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while updating parameter tags: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def set_parameter_definition_tags(self, tag_dict, raise_intgr_error=True):
        """Set tags for parameter definitions."""
        if not tag_dict:
            return None
        tag_id_lists = {
            x.parameter_definition_id: [int(y) for y in x.parameter_tag_id_list.split(",")]
            for x in self.wide_parameter_definition_tag_list()
        }
        definition_tag_id_dict = {
            (x.parameter_definition_id, x.parameter_tag_id): x.id for x in self.parameter_definition_tag_list()
        }
        items_to_insert = list()
        ids_to_delete = set()
        for definition_id, tag_id_list in tag_dict.items():
            target_tag_id_list = [int(x) for x in tag_id_list.split(",")] if tag_id_list else []
            current_tag_id_list = tag_id_lists.get(definition_id, [])
            for tag_id in target_tag_id_list:
                if tag_id not in current_tag_id_list:
                    item = {
                        "parameter_definition_id": definition_id,
                        "parameter_tag_id": tag_id
                    }
                    items_to_insert.append(item)
            for tag_id in current_tag_id_list:
                if tag_id not in target_tag_id_list:
                    ids_to_delete.add(definition_tag_id_dict[definition_id, tag_id])
        self.remove_items(parameter_definition_tag_ids=ids_to_delete)
        ret = self.add_parameter_definition_tags(*items_to_insert, raise_intgr_error=raise_intgr_error)
        if not raise_intgr_error:
            return ret[1]

    def update_wide_parameter_enums(self, *wide_kwargs_list, raise_intgr_error=True):
        """Update parameter enums.
        NOTE: It's too difficult to do it cleanly, so we just remove and then add.
        """
        try:
            wide_parameter_enum_dict = {x.id: x._asdict() for x in self.wide_parameter_enum_list()}
            updated_ids = set()
            item_list = list()
            for wide_kwargs in wide_kwargs_list:
                try:
                    id = wide_kwargs['id']
                    updated_wide_kwargs = wide_parameter_enum_dict[id]
                except KeyError:
                    continue
                updated_ids.add(id)
                updated_wide_kwargs.update(wide_kwargs)
                for k, value in enumerate(updated_wide_kwargs['value_list']):
                    updated_narrow_kwargs = {
                        'id': id,
                        'name': updated_wide_kwargs['name'],
                        'value_index': k,
                        'value': value
                    }
                    item_list.append(updated_narrow_kwargs)
            self.remove_items(parameter_enum_ids=updated_ids)
            self.session.bulk_insert_mappings(self.DiffParameterEnum, item_list)
            self.session.commit()
            self.new_item_id["parameter_enum"].update(updated_ids)
            updated_item_list = self.wide_parameter_enum_list(id_list=updated_ids)
            if not raise_intgr_error:
                return updated_item_list, intgr_error_log
            return updated_item_list
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while updating parameter enums: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def remove_items(
            self,
            object_class_ids=set(),
            object_ids=set(),
            relationship_class_ids=set(),
            relationship_ids=set(),
            parameter_ids=set(),
            parameter_value_ids=set(),
            parameter_tag_ids=set(),
            parameter_definition_tag_ids=set(),
            parameter_enum_ids=set()
        ):
        """Remove items."""
        removed_item_id, removed_diff_item_id = self._removed_items(
            object_class_ids=object_class_ids,
            object_ids=object_ids,
            relationship_class_ids=relationship_class_ids,
            relationship_ids=relationship_ids,
            parameter_definition_ids=parameter_ids,
            parameter_value_ids=parameter_value_ids,
            parameter_tag_ids=parameter_tag_ids,
            parameter_definition_tag_ids=parameter_definition_tag_ids,
            parameter_enum_ids=parameter_enum_ids)
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
        self.session.query(self.DiffParameterDefinition).filter(self.DiffParameterDefinition.id.in_(diff_ids)).\
            delete(synchronize_session=False)
        diff_ids = removed_diff_item_id.get('parameter_value', set())
        self.session.query(self.DiffParameterValue).filter(self.DiffParameterValue.id.in_(diff_ids)).\
            delete(synchronize_session=False)
        diff_ids = removed_diff_item_id.get('parameter_tag', set())
        self.session.query(self.DiffParameterTag).filter(self.DiffParameterTag.id.in_(diff_ids)).\
            delete(synchronize_session=False)
        diff_ids = removed_diff_item_id.get('parameter_definition_tag', set())
        self.session.query(self.DiffParameterDefinitionTag).filter(self.DiffParameterDefinitionTag.id.in_(diff_ids)).\
            delete(synchronize_session=False)
        diff_ids = removed_diff_item_id.get('parameter_enum', set())
        self.session.query(self.DiffParameterEnum).filter(self.DiffParameterEnum.id.in_(diff_ids)).\
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

    def _removed_items(
            self,
            object_class_ids=set(),
            object_ids=set(),
            relationship_class_ids=set(),
            relationship_ids=set(),
            parameter_definition_ids=set(),
            parameter_value_ids=set(),
            parameter_tag_ids=set(),
            parameter_definition_tag_ids=set(),
            parameter_enum_ids=set()
        ):
        """Return all items that should be removed when removing items given as arguments.

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
        self._remove_cascade_object_classes(
            [x.id for x in item_list],
            [x.id for x in diff_item_list],
            removed_item_id,
            removed_diff_item_id)
        # object
        item_list = self.session.query(self.Object.id).filter(self.Object.id.in_(object_ids))
        diff_item_list = self.session.query(self.DiffObject.id).filter(self.DiffObject.id.in_(object_ids))
        self._remove_cascade_objects(
            [x.id for x in item_list],
            [x.id for x in diff_item_list],
            removed_item_id,
            removed_diff_item_id)
        # relationship_class
        item_list = self.session.query(self.RelationshipClass.id).\
            filter(self.RelationshipClass.id.in_(relationship_class_ids))
        diff_item_list = self.session.query(self.DiffRelationshipClass.id).\
            filter(self.DiffRelationshipClass.id.in_(relationship_class_ids))
        self._remove_cascade_relationship_classes(
            [x.id for x in item_list],
            [x.id for x in diff_item_list],
            removed_item_id,
            removed_diff_item_id)
        # relationship
        item_list = self.session.query(self.Relationship.id).filter(self.Relationship.id.in_(relationship_ids))
        diff_item_list = self.session.query(self.DiffRelationship.id).\
            filter(self.DiffRelationship.id.in_(relationship_ids))
        self._remove_cascade_relationships(
            [x.id for x in item_list],
            [x.id for x in diff_item_list],
            removed_item_id,
            removed_diff_item_id)
        # parameter
        item_list = self.session.query(self.ParameterDefinition.id).\
            filter(self.ParameterDefinition.id.in_(parameter_definition_ids))
        diff_item_list = self.session.query(self.DiffParameterDefinition.id).\
            filter(self.DiffParameterDefinition.id.in_(parameter_definition_ids))
        self._remove_cascade_parameter_definitions(
            [x.id for x in item_list],
            [x.id for x in diff_item_list],
            removed_item_id,
            removed_diff_item_id)
        # parameter_value
        item_list = self.session.query(self.ParameterValue.id).filter(self.ParameterValue.id.in_(parameter_value_ids))
        diff_item_list = self.session.query(self.DiffParameterValue.id).\
            filter(self.DiffParameterValue.id.in_(parameter_value_ids))
        self._remove_cascade_parameter_values(
            [x.id for x in item_list],
            [x.id for x in diff_item_list],
            removed_item_id,
            removed_diff_item_id)
        # parameter_tag
        item_list = self.session.query(self.ParameterTag.id).filter(self.ParameterTag.id.in_(parameter_tag_ids))
        diff_item_list = self.session.query(self.DiffParameterTag.id).\
            filter(self.DiffParameterTag.id.in_(parameter_tag_ids))
        self._remove_cascade_parameter_tags(
            [x.id for x in item_list],
            [x.id for x in diff_item_list],
            removed_item_id,
            removed_diff_item_id)
        # parameter_definition_tag
        item_list = self.session.query(self.ParameterDefinitionTag.id).\
            filter(self.ParameterDefinitionTag.id.in_(parameter_definition_tag_ids))
        diff_item_list = self.session.query(self.DiffParameterDefinitionTag.id).\
            filter(self.DiffParameterDefinitionTag.id.in_(parameter_definition_tag_ids))
        self._remove_cascade_parameter_definition_tags(
            [x.id for x in item_list],
            [x.id for x in diff_item_list],
            removed_item_id,
            removed_diff_item_id)
        # parameter_enum
        item_list = self.session.query(self.ParameterEnum.id).filter(self.ParameterEnum.id.in_(parameter_enum_ids))
        diff_item_list = self.session.query(self.DiffParameterEnum.id).\
            filter(self.DiffParameterEnum.id.in_(parameter_enum_ids))
        self._remove_cascade_parameter_enums(
            [x.id for x in item_list],
            [x.id for x in diff_item_list],
            removed_item_id,
            removed_diff_item_id)
        return removed_item_id, removed_diff_item_id

    def _remove_cascade_object_classes(self, ids, diff_ids, removed_item_id, removed_diff_item_id):
        """Remove object classes and all related items."""
        # Touch
        removed_item_id.setdefault("object_class", set()).update(ids)
        removed_diff_item_id.setdefault("object_class", set()).update(diff_ids)
        # object
        item_list = self.session.query(self.Object.id).filter(self.Object.class_id.in_(ids))
        diff_item_list = self.session.query(self.DiffObject.id).filter(self.DiffObject.class_id.in_(ids + diff_ids))
        self._remove_cascade_objects(
            [x.id for x in item_list],
            [x.id for x in diff_item_list],
            removed_item_id,
            removed_diff_item_id)
        # relationship_class
        item_list = self.session.query(self.RelationshipClass.id).\
            filter(self.RelationshipClass.object_class_id.in_(ids))
        diff_item_list = self.session.query(self.DiffRelationshipClass.id).\
            filter(self.DiffRelationshipClass.object_class_id.in_(ids + diff_ids))
        self._remove_cascade_relationship_classes(
            [x.id for x in item_list],
            [x.id for x in diff_item_list],
            removed_item_id,
            removed_diff_item_id)
        # parameter
        item_list = self.session.query(self.ParameterDefinition.id).\
            filter(self.ParameterDefinition.object_class_id.in_(ids))
        diff_item_list = self.session.query(self.DiffParameterDefinition.id).\
            filter(self.DiffParameterDefinition.object_class_id.in_(ids + diff_ids))
        self._remove_cascade_parameter_definitions(
            [x.id for x in item_list],
            [x.id for x in diff_item_list],
            removed_item_id,
            removed_diff_item_id)

    def _remove_cascade_objects(self, ids, diff_ids, removed_item_id, removed_diff_item_id):
        """Remove objects and all related items."""
        # Touch
        removed_item_id.setdefault("object", set()).update(ids)
        removed_diff_item_id.setdefault("object", set()).update(diff_ids)
        # relationship
        item_list = self.session.query(self.Relationship.id).filter(self.Relationship.object_id.in_(ids))
        diff_item_list = self.session.query(self.DiffRelationship.id).\
            filter(self.DiffRelationship.object_id.in_(ids + diff_ids))
        self._remove_cascade_relationships(
            [x.id for x in item_list],
            [x.id for x in diff_item_list],
            removed_item_id,
            removed_diff_item_id)
        # parameter_value
        item_list = self.session.query(self.ParameterValue.id).filter(self.ParameterValue.object_id.in_(ids))
        diff_item_list = self.session.query(self.DiffParameterValue.id).\
            filter(self.DiffParameterValue.object_id.in_(ids + diff_ids))
        self._remove_cascade_parameter_values(
            [x.id for x in item_list],
            [x.id for x in diff_item_list],
            removed_item_id,
            removed_diff_item_id)

    def _remove_cascade_relationship_classes(self, ids, diff_ids, removed_item_id, removed_diff_item_id):
        """Remove relationship classes and all related items."""
        # Touch
        removed_item_id.setdefault("relationship_class", set()).update(ids)
        removed_diff_item_id.setdefault("relationship_class", set()).update(diff_ids)
        # relationship
        item_list = self.session.query(self.Relationship.id).filter(self.Relationship.class_id.in_(ids))
        diff_item_list = self.session.query(self.DiffRelationship.id).\
            filter(self.DiffRelationship.class_id.in_(ids + diff_ids))
        self._remove_cascade_relationships(
            [x.id for x in item_list],
            [x.id for x in diff_item_list],
            removed_item_id,
            removed_diff_item_id)
        # parameter
        item_list = self.session.query(self.ParameterDefinition.id).\
            filter(self.ParameterDefinition.relationship_class_id.in_(ids))
        diff_item_list = self.session.query(self.DiffParameterDefinition.id).\
            filter(self.DiffParameterDefinition.relationship_class_id.in_(ids + diff_ids))
        self._remove_cascade_parameter_definitions(
            [x.id for x in item_list],
            [x.id for x in diff_item_list],
            removed_item_id,
            removed_diff_item_id)

    def _remove_cascade_relationships(self, ids, diff_ids, removed_item_id, removed_diff_item_id):
        """Remove relationships and all related items."""
        # Touch
        removed_item_id.setdefault("relationship", set()).update(ids)
        removed_diff_item_id.setdefault("relationship", set()).update(diff_ids)
        # parameter_value
        item_list = self.session.query(self.ParameterValue.id).\
            filter(self.ParameterValue.relationship_id.in_(ids))
        diff_item_list = self.session.query(self.DiffParameterValue.id).\
            filter(self.DiffParameterValue.relationship_id.in_(ids + diff_ids))
        self._remove_cascade_parameter_values(
            [x.id for x in item_list],
            [x.id for x in diff_item_list],
            removed_item_id,
            removed_diff_item_id)

    def _remove_cascade_parameter_definitions(self, ids, diff_ids, removed_item_id, removed_diff_item_id):
        """Remove parameter definitons and all related items."""
        # Touch
        removed_item_id.setdefault("parameter_definition", set()).update(ids)
        removed_diff_item_id.setdefault("parameter_definition", set()).update(diff_ids)
        # parameter_value
        item_list = self.session.query(self.ParameterValue.id).\
            filter(self.ParameterValue.parameter_definition_id.in_(ids))
        diff_item_list = self.session.query(self.DiffParameterValue.id).\
            filter(self.DiffParameterValue.parameter_definition_id.in_(ids + diff_ids))
        self._remove_cascade_parameter_values(
            [x.id for x in item_list],
            [x.id for x in diff_item_list],
            removed_item_id,
            removed_diff_item_id)
        # parameter_definition_tag
        item_list = self.session.query(self.ParameterDefinitionTag.id).\
            filter(self.ParameterDefinitionTag.parameter_definition_id.in_(ids))
        diff_item_list = self.session.query(self.DiffParameterDefinitionTag.id).\
            filter(self.DiffParameterDefinitionTag.parameter_definition_id.in_(ids + diff_ids))
        self._remove_cascade_parameter_definition_tags(
            [x.id for x in item_list],
            [x.id for x in diff_item_list],
            removed_item_id,
            removed_diff_item_id)

    def _remove_cascade_parameter_values(self, ids, diff_ids, removed_item_id, removed_diff_item_id):
        """Remove parameter values and all related items."""
        removed_item_id.setdefault("parameter_value", set()).update(ids)
        removed_diff_item_id.setdefault("parameter_value", set()).update(diff_ids)

    def _remove_cascade_parameter_tags(self, ids, diff_ids, removed_item_id, removed_diff_item_id):
        """Remove parameter tags and all related items."""
        # Touch
        removed_item_id.setdefault("parameter_tag", set()).update(ids)
        removed_diff_item_id.setdefault("parameter_tag", set()).update(diff_ids)
        # parameter_definition_tag
        item_list = self.session.query(self.ParameterDefinitionTag.id).\
            filter(self.ParameterDefinitionTag.parameter_tag_id.in_(ids))
        diff_item_list = self.session.query(self.DiffParameterDefinitionTag.id).\
            filter(self.DiffParameterDefinitionTag.parameter_tag_id.in_(ids + diff_ids))
        self._remove_cascade_parameter_definition_tags(
            [x.id for x in item_list],
            [x.id for x in diff_item_list],
            removed_item_id,
            removed_diff_item_id)

    def _remove_cascade_parameter_definition_tags(self, ids, diff_ids, removed_item_id, removed_diff_item_id):
        """Remove parameter definition tag pairs and all related items."""
        removed_item_id.setdefault("parameter_definition_tag", set()).update(ids)
        removed_diff_item_id.setdefault("parameter_definition_tag", set()).update(diff_ids)

    def _remove_cascade_parameter_enums(self, ids, diff_ids, removed_item_id, removed_diff_item_id):
        """Remove parameter enums and all related items.
        TODO: Should we remove parameter definitions here? Do we care if they have invalid enum?
        If we *do* remove parameter definitions then we need to fix `update_wide_parameter_enum`
        """
        removed_item_id.setdefault("parameter_enum", set()).update(ids)
        removed_diff_item_id.setdefault("parameter_enum", set()).update(diff_ids)

    def reset_diff_mapping(self):
        """Delete all records from diff tables (but don't drop the tables)."""
        self.session.query(self.DiffObjectClass).delete()
        self.session.query(self.DiffObject).delete()
        self.session.query(self.DiffRelationshipClass).delete()
        self.session.query(self.DiffRelationship).delete()
        self.session.query(self.DiffParameterDefinition).delete()
        self.session.query(self.DiffParameterValue).delete()
        self.session.query(self.DiffParameterTag).delete()
        self.session.query(self.DiffParameterDefinitionTag).delete()
        self.session.query(self.DiffParameterEnum).delete()

    def commit_session(self, comment):
        """Make differences into original tables and commit."""
        try:
            user = self.username
            date = datetime.now(timezone.utc)
            commit = self.Commit(comment=comment, date=date, user=user)
            self.session.add(commit)
            self.session.flush()
            n = 999  # Maximum number of sql variables
            # Remove removed
            removed_object_class_id = list(self.removed_item_id["object_class"])
            removed_object_id = list(self.removed_item_id["object"])
            removed_relationship_class_id = list(self.removed_item_id["relationship_class"])
            removed_relationship_id = list(self.removed_item_id["relationship"])
            removed_parameter_definition_id = list(self.removed_item_id["parameter_definition"])
            removed_parameter_value_id = list(self.removed_item_id["parameter_value"])
            removed_parameter_tag_id = list(self.removed_item_id["parameter_tag"])
            removed_parameter_definition_tag_id = list(self.removed_item_id["parameter_definition_tag"])
            removed_parameter_enum_id = list(self.removed_item_id["parameter_enum"])
            for i in range(0, len(removed_object_class_id), n):
                self.session.query(self.ObjectClass).filter(
                    self.ObjectClass.id.in_(removed_object_class_id[i:i + n])
                ).delete(synchronize_session=False)
            for i in range(0, len(removed_object_id), n):
                self.session.query(self.Object).filter(
                    self.Object.id.in_(removed_object_id[i:i + n])
                ).delete(synchronize_session=False)
            for i in range(0, len(removed_relationship_class_id), n):
                self.session.query(self.RelationshipClass).filter(
                    self.RelationshipClass.id.in_(removed_relationship_class_id[i:i + n])
                ).delete(synchronize_session=False)
            for i in range(0, len(removed_relationship_id), n):
                self.session.query(self.Relationship).filter(
                    self.Relationship.id.in_(removed_relationship_id[i:i + n])
                ).delete(synchronize_session=False)
            for i in range(0, len(removed_parameter_definition_id), n):
                self.session.query(self.ParameterDefinition).filter(
                    self.ParameterDefinition.id.in_(removed_parameter_definition_id[i:i + n])
                ).delete(synchronize_session=False)
            for i in range(0, len(removed_parameter_value_id), n):
                self.session.query(self.ParameterValue).filter(
                    self.ParameterValue.id.in_(removed_parameter_value_id[i:i + n])
                ).delete(synchronize_session=False)
            for i in range(0, len(removed_parameter_tag_id), n):
                self.session.query(self.ParameterTag).filter(
                    self.ParameterTag.id.in_(removed_parameter_tag_id[i:i + n])
                ).delete(synchronize_session=False)
            for i in range(0, len(removed_parameter_definition_tag_id), n):
                self.session.query(self.ParameterDefinitionTag).filter(
                    self.ParameterDefinitionTag.id.in_(removed_parameter_definition_tag_id[i:i + n])
                ).delete(synchronize_session=False)
            for i in range(0, len(removed_parameter_enum_id), n):
                self.session.query(self.ParameterEnum).filter(
                    self.ParameterEnum.id.in_(removed_parameter_enum_id[i:i + n])
                ).delete(synchronize_session=False)
            # Merge dirty
            dirty_object_class_id = list(self.dirty_item_id["object_class"])
            dirty_object_id = list(self.dirty_item_id["object"])
            dirty_relationship_class_id = list(self.dirty_item_id["relationship_class"])
            dirty_relationship_id = list(self.dirty_item_id["relationship"])
            dirty_parameter_id = list(self.dirty_item_id["parameter_definition"])
            dirty_parameter_value_id = list(self.dirty_item_id["parameter_value"])
            dirty_parameter_tag_id = list(self.dirty_item_id["parameter_tag"])
            dirty_parameter_definition_tag_id = list(self.dirty_item_id["parameter_definition_tag"])
            dirty_parameter_enum_id = list(self.dirty_item_id["parameter_enum"])
            dirty_items = {}
            for i in range(0, len(dirty_object_class_id), n):
                for item in self.session.query(self.DiffObjectClass).\
                        filter(self.DiffObjectClass.id.in_(dirty_object_class_id[i:i + n])):
                    kwargs = attr_dict(item)
                    kwargs['commit_id'] = commit.id
                    dirty_items.setdefault(self.ObjectClass, []).append(kwargs)
            for i in range(0, len(dirty_object_id), n):
                for item in self.session.query(self.DiffObject).\
                        filter(self.DiffObject.id.in_(dirty_object_id[i:i + n])):
                    kwargs = attr_dict(item)
                    kwargs['commit_id'] = commit.id
                    dirty_items.setdefault(self.Object, []).append(kwargs)
            for i in range(0, len(dirty_relationship_class_id), n):
                for item in self.session.query(self.DiffRelationshipClass).\
                        filter(self.DiffRelationshipClass.id.in_(dirty_relationship_class_id[i:i + n])):
                    kwargs = attr_dict(item)
                    kwargs['commit_id'] = commit.id
                    dirty_items.setdefault(self.RelationshipClass, []).append(kwargs)
            for i in range(0, len(dirty_relationship_id), n):
                for item in self.session.query(self.DiffRelationship).\
                        filter(self.DiffRelationship.id.in_(dirty_relationship_id[i:i + n])):
                    kwargs = attr_dict(item)
                    kwargs['commit_id'] = commit.id
                    dirty_items.setdefault(self.Relationship, []).append(kwargs)
            for i in range(0, len(dirty_parameter_id), n):
                for item in self.session.query(self.DiffParameterDefinition).\
                        filter(self.DiffParameterDefinition.id.in_(dirty_parameter_id[i:i + n])):
                    kwargs = attr_dict(item)
                    kwargs['commit_id'] = commit.id
                    dirty_items.setdefault(self.ParameterDefinition, []).append(kwargs)
            for i in range(0, len(dirty_parameter_value_id), n):
                for item in self.session.query(self.DiffParameterValue).\
                        filter(self.DiffParameterValue.id.in_(dirty_parameter_value_id[i:i + n])):
                    kwargs = attr_dict(item)
                    kwargs['commit_id'] = commit.id
                    dirty_items.setdefault(self.ParameterValue, []).append(kwargs)
            for i in range(0, len(dirty_parameter_tag_id), n):
                for item in self.session.query(self.DiffParameterTag).\
                        filter(self.DiffParameterTag.id.in_(dirty_parameter_tag_id[i:i + n])):
                    kwargs = attr_dict(item)
                    kwargs['commit_id'] = commit.id
                    dirty_items.setdefault(self.ParameterTag, []).append(kwargs)
            for i in range(0, len(dirty_parameter_definition_tag_id), n):
                for item in self.session.query(self.DiffParameterDefinitionTag).\
                        filter(self.DiffParameterDefinitionTag.id.in_(dirty_parameter_definition_tag_id[i:i + n])):
                    kwargs = attr_dict(item)
                    kwargs['commit_id'] = commit.id
                    dirty_items.setdefault(self.ParameterDefinitionTag, []).append(kwargs)
            for i in range(0, len(dirty_parameter_enum_id), n):
                for item in self.session.query(self.DiffParameterEnum).\
                        filter(self.DiffParameterEnum.id.in_(dirty_parameter_enum_id[i:i + n])):
                    kwargs = attr_dict(item)
                    kwargs['commit_id'] = commit.id
                    dirty_items.setdefault(self.ParameterEnum, []).append(kwargs)
            self.session.flush()  # TODO: Check if this is needed
            # Bulk update
            for k, v in dirty_items.items():
                self.session.bulk_update_mappings(k, v)
            # Add new
            new_object_class_id = list(self.new_item_id["object_class"])
            new_object_id = list(self.new_item_id["object"])
            new_relationship_class_id = list(self.new_item_id["relationship_class"])
            new_relationship_id = list(self.new_item_id["relationship"])
            new_parameter_id = list(self.new_item_id["parameter_definition"])
            new_parameter_value_id = list(self.new_item_id["parameter_value"])
            new_parameter_tag_id = list(self.new_item_id["parameter_tag"])
            new_parameter_definition_tag_id = list(self.new_item_id["parameter_definition_tag"])
            new_parameter_enum_id = list(self.new_item_id["parameter_enum"])
            new_items = {}
            for i in range(0, len(new_object_class_id), n):
                for item in self.session.query(self.DiffObjectClass).\
                        filter(self.DiffObjectClass.id.in_(new_object_class_id[i:i + n])):
                    kwargs = attr_dict(item)
                    kwargs['commit_id'] = commit.id
                    new_items.setdefault(self.ObjectClass, []).append(kwargs)
            for i in range(0, len(new_object_id), n):
                for item in self.session.query(self.DiffObject).\
                        filter(self.DiffObject.id.in_(new_object_id[i:i + n])):
                    kwargs = attr_dict(item)
                    kwargs['commit_id'] = commit.id
                    new_items.setdefault(self.Object, []).append(kwargs)
            for i in range(0, len(new_relationship_class_id), n):
                for item in self.session.query(self.DiffRelationshipClass).\
                        filter(self.DiffRelationshipClass.id.in_(new_relationship_class_id[i:i + n])):
                    kwargs = attr_dict(item)
                    kwargs['commit_id'] = commit.id
                    new_items.setdefault(self.RelationshipClass, []).append(kwargs)
            for i in range(0, len(new_relationship_id), n):
                for item in self.session.query(self.DiffRelationship).\
                        filter(self.DiffRelationship.id.in_(new_relationship_id[i:i + n])):
                    kwargs = attr_dict(item)
                    kwargs['commit_id'] = commit.id
                    new_items.setdefault(self.Relationship, []).append(kwargs)
            for i in range(0, len(new_parameter_id), n):
                for item in self.session.query(self.DiffParameterDefinition).\
                        filter(self.DiffParameterDefinition.id.in_(new_parameter_id[i:i + n])):
                    kwargs = attr_dict(item)
                    kwargs['commit_id'] = commit.id
                    new_items.setdefault(self.ParameterDefinition, []).append(kwargs)
            for i in range(0, len(new_parameter_value_id), n):
                for item in self.session.query(self.DiffParameterValue).\
                        filter(self.DiffParameterValue.id.in_(new_parameter_value_id[i:i + n])):
                    kwargs = attr_dict(item)
                    kwargs['commit_id'] = commit.id
                    new_items.setdefault(self.ParameterValue, []).append(kwargs)
            for i in range(0, len(new_parameter_tag_id), n):
                for item in self.session.query(self.DiffParameterTag).\
                        filter(self.DiffParameterTag.id.in_(new_parameter_tag_id[i:i + n])):
                    kwargs = attr_dict(item)
                    kwargs['commit_id'] = commit.id
                    new_items.setdefault(self.ParameterTag, []).append(kwargs)
            for i in range(0, len(new_parameter_definition_tag_id), n):
                for item in self.session.query(self.DiffParameterDefinitionTag).\
                        filter(self.DiffParameterDefinitionTag.id.in_(new_parameter_definition_tag_id[i:i + n])):
                    kwargs = attr_dict(item)
                    kwargs['commit_id'] = commit.id
                    new_items.setdefault(self.ParameterDefinitionTag, []).append(kwargs)
            for i in range(0, len(new_parameter_enum_id), n):
                for item in self.session.query(self.DiffParameterEnum).\
                        filter(self.DiffParameterEnum.id.in_(new_parameter_enum_id[i:i + n])):
                    kwargs = attr_dict(item)
                    kwargs['commit_id'] = commit.id
                    new_items.setdefault(self.ParameterEnum, []).append(kwargs)
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

    def reset_mapping(self):
        """Delete all records from all tables (but don't drop the tables)."""
        super().reset_mapping()
        self.session.query(self.DiffObjectClass).delete(synchronize_session=False)
        self.session.query(self.DiffObject).delete(synchronize_session=False)
        self.session.query(self.DiffRelationshipClass).delete(synchronize_session=False)
        self.session.query(self.DiffRelationship).delete(synchronize_session=False)
        self.session.query(self.DiffParameterDefinition).delete(synchronize_session=False)
        self.session.query(self.DiffParameterValue).delete(synchronize_session=False)
        self.session.query(self.DiffParameterTag).delete(synchronize_session=False)
        self.session.query(self.DiffParameterDefinitionTag).delete(synchronize_session=False)
        self.session.query(self.DiffParameterEnum).delete(synchronize_session=False)
        self.session.query(self.DiffCommit).delete(synchronize_session=False)

    def close(self):
        """Drop differences tables and close."""
        if self.session:
            self.session.rollback()
            self.session.close()
        if self.diff_metadata and self.engine:
            self.diff_metadata.drop_all(self.engine)
        if self.engine:
            self.engine.dispose()
