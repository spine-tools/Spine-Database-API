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

:author: Manuel Marin <manuelma@kth.se>
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
        # List of items that are either dirty, removed, or pulled
        self.touched_item_id = {
            "object_class": set(),
            "object": set(),
            "relationship_class": set(),
            "relationship": set(),
            "parameter": set(),
            "parameter_value": set(),
        }
        self.DiffBase = None
        self.DiffObjectClass = None
        self.DiffObject = None
        self.DiffRelationshipClass = None
        self.DiffRelationship = None
        self.DiffParameter = None
        self.DiffParameterValue = None
        self.diff_engine = None
        self.create_diff_engine_and_session()
        self.create_diff_tables()
        id = self.session.query(func.max(self.ObjectClass.id)).first()[0]
        self.next_object_class_id = id + 1 if id else 1
        id = self.session.query(func.max(self.Object.id)).first()[0]
        self.next_object_id = id + 1 if id else 1
        id = self.session.query(func.max(self.RelationshipClass.id)).first()[0]
        self.next_relationship_class_id = id + 1 if id else 1
        id = self.session.query(func.max(self.Relationship.id)).first()[0]
        self.next_relationship_id = id + 1 if id else 1
        id = self.session.query(func.max(self.Parameter.id)).first()[0]
        self.next_parameter_id = id + 1 if id else 1
        id = self.session.query(func.max(self.ParameterValue.id)).first()[0]
        self.next_parameter_value_id = id + 1 if id else 1
        toc = time.clock()
        logging.debug("Temp mapping created in {} seconds".format(toc - tic))

    def create_diff_engine_and_session(self):
        """Create engine and session."""
        self.diff_engine = create_engine(
            # 'sqlite:///tmp.sqlite',
            'sqlite://', # TODO: Uncomment when it's all done, some day
            connect_args={'check_same_thread':False},
            poolclass=StaticPool,
            echo=False)
        create_session = sessionmaker(class_=ShardedSession)
        create_session.configure(shards={
            'diff': self.diff_engine,
            'orig': self.engine
        })
        # Shard chooser
        def shard_chooser(mapper, instance, clause=None):
            """Looks at the given instance and returns a shard id.
            Always return diff so new instances go to the diff db."""
            return 'diff'
        # Id chooser
        def id_chooser(query, ident):
            """Return both"""
            return ['orig', 'diff']
        # Query chooser
        def query_chooser(query):
            """Return both. NOTE: Original first allows using 'first()' to get the next ids.
            But we could do better.
            """
            return ['orig', 'diff']
        # Create session
        create_session.configure(
            shard_chooser=shard_chooser,
            id_chooser=id_chooser,
            query_chooser=query_chooser
        )
        self.session = create_session()

    def create_diff_tables(self):
        """Create difference tables in both orig and diff db.
        """
        self.Base.metadata.drop_all(self.diff_engine)
        self.Base.metadata.create_all(self.diff_engine)
        meta = MetaData(bind=self.engine)
        diff_meta = MetaData(bind=self.diff_engine)
        diff_tables = list()
        for t in self.Base.metadata.sorted_tables:
            if t.name.startswith('diff_'):
                continue
            diff_columns = list()
            for column in t.columns:
                diff_columns.append(column.copy())
            diff_table = Table(
                "diff_" + t.name, diff_meta,
                *diff_columns)
            diff_tables.append(diff_table)
        meta.drop_all(tables=diff_tables)
        diff_meta.drop_all(tables=diff_tables)
        for diff_table in diff_tables:
            diff_table.create(self.engine)
            diff_table.create(self.diff_engine)
        # Reflect diff tables
        self.DiffBase = automap_base()
        self.DiffBase.prepare(self.diff_engine, reflect=True, generate_relationship=custom_generate_relationship)
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

    def pull_object_class(self, id, pulled_item_dict):
        """Append object class to list of pulled items."""
        if id in [x.id for x in pulled_item_dict.get("object_class", set())]:
            return
        item = self.session.query(self.ObjectClass).set_shard('orig').filter_by(id=id).one_or_none()
        if not item:
            return
        kwargs = attr_dict(item)
        pulled_item = self.DiffObjectClass(**kwargs)
        pulled_item_dict.setdefault("object_class", set()).add(pulled_item)

    def pull_object(self, id, pulled_item_dict):
        """Append object to and its references (the class) list of pulled items."""
        if id in [x.id for x in pulled_item_dict.get("object", set())]:
            return
        item = self.session.query(self.Object).set_shard('orig').filter_by(id=id).one_or_none()
        if not item:
            return
        kwargs = attr_dict(item)
        pulled_item = self.DiffObject(**kwargs)
        pulled_item_dict.setdefault("object", set()).add(pulled_item)
        self.pull_object_class(item.class_id, pulled_item_dict)

    def pull_relationship_class(self, id, pulled_item_dict):
        """Append relationship class and its references to list of pulled items."""
        if id in [x.id for x in pulled_item_dict.get("relationship_class", set())]:
            return
        items = self.session.query(self.RelationshipClass).set_shard('orig').filter_by(id=id)
        if not items.count():
            return
        for item in items:
            kwargs = attr_dict(item)
            pulled_item = self.DiffRelationshipClass(**kwargs)
            pulled_item_dict.setdefault("relationship_class", set()).add(pulled_item)
            self.pull_object_class(item.object_class_id, pulled_item_dict)

    def pull_relationship(self, id, pulled_item_dict):
        """Append relationship and its references to list of pulled items."""
        if id in [x.id for x in pulled_item_dict.get("relationship", set())]:
            return
        items = self.session.query(self.Relationship).set_shard('orig').filter_by(id=id)
        if not items.count():
            return
        for item in items:
            kwargs = attr_dict(item)
            pulled_item = self.DiffRelationship(**kwargs)
            pulled_item_dict.setdefault("relationship", set()).add(pulled_item)
            self.pull_object(item.object_id, pulled_item_dict)
            self.pull_relationship_class(item.class_id, pulled_item_dict)

    def pull_parameter(self, id, pulled_item_dict):
        """Append parameter and its references to list of pulled items."""
        if id in [x.id for x in pulled_item_dict.get("parameter", set())]:
            return
        item = self.session.query(self.Parameter).set_shard('orig').filter_by(id=id).one_or_none()
        if not item:
            return
        kwargs = attr_dict(item)
        pulled_item = self.DiffParameter(**kwargs)
        pulled_item_dict.setdefault("parameter", set()).add(pulled_item)
        if item.object_class_id:
            self.pull_object_class(item.object_class_id, pulled_item_dict)
        if item.relationship_class_id:
            self.pull_relationship_class(item.relationship_class_id, pulled_item_dict)

    def pull_parameter_value(self, id, pulled_item_dict):
        """Append parameter value and its references to list of pulled items."""
        if id in [x.id for x in pulled_item_dict.get("parameter_value", set())]:
            return
        item = self.session.query(self.ParameterValue).set_shard('orig').filter_by(id=id).one_or_none()
        if not item:
            return
        kwargs = attr_dict(item)
        pulled_item = self.DiffParameterValue(**kwargs)
        pulled_item_dict.setdefault("parameter_value", set()).add(pulled_item)
        self.pull_parameter(item.parameter_id, pulled_item_dict)
        if item.object_id:
            self.pull_object(item.object_id, pulled_item_dict)
        if item.relationship_id:
            self.pull_relationship(item.relationship_id, pulled_item_dict)

    def prepare_object_query(self):
        """Make sure everything is there for queries to work.
        Pull all objects that have been directly touched,
        or indirectly by touching its class.
        """
        touched_object_list = self.session.query(self.Object.id).set_shard('orig').\
            filter(or_(
                self.Object.class_id.in_(self.touched_item_id["object_class"]),
                self.Object.id.in_(self.touched_item_id["object"])))
        pulled_item_dict = dict()
        for object_ in touched_object_list:
            self.pull_object(object_.id, pulled_item_dict)
        try:
            for key, value in pulled_item_dict.items():
                pulled_item_list = [x for x in value if x.id not in self.touched_item_id[key]]
                self.session.add_all(pulled_item_list)
            self.session.commit()
            for key, value in pulled_item_dict.items():
                self.touched_item_id[key].update({x.id for x in value})
            return True
        except DBAPIError as e:
            self.session.rollback()
            raise e

    def prepare_relationship_class_query(self):
        """Make sure everything is there for queries to work.
        Pull all relationship classes that have been directly touched,
        or indirectly by touching one of its object classes.
        """
        touched_relationship_class_list = self.session.query(self.RelationshipClass.id).set_shard('orig').\
            filter(or_(
                self.RelationshipClass.object_class_id.in_(self.touched_item_id["object_class"]),
                self.RelationshipClass.id.in_(self.touched_item_id["relationship_class"])))
        pulled_item_dict = dict()
        for relationship_class in touched_relationship_class_list:
            self.pull_relationship_class(relationship_class.id, pulled_item_dict)
        try:
            for key, value in pulled_item_dict.items():
                pulled_item_list = [x for x in value if x.id not in self.touched_item_id[key]]
                self.session.add_all(pulled_item_list)
            self.session.commit()
            for key, value in pulled_item_dict.items():
                self.touched_item_id[key].update({x.id for x in value})
            return True
        except DBAPIError as e:
            self.session.rollback()
            raise e

    def prepare_relationship_query(self):
        """Make sure everything is there for queries to work.
        Pull all relationships that have been directly touched,
        or indirectly by touching either one of its objects, or its class.
        """
        touched_relationship_list = self.session.query(self.Relationship.id).set_shard('orig').\
            filter(or_(
                self.Relationship.object_id.in_(self.touched_item_id["object"]),
                self.Relationship.class_id.in_(self.touched_item_id["relationship_class"]),
                self.Relationship.id.in_(self.touched_item_id["relationship"])))
        pulled_item_dict = dict()
        for relationship in touched_relationship_list:
            self.pull_relationship(relationship.id, pulled_item_dict)
        try:
            for key, value in pulled_item_dict.items():
                pulled_item_list = [x for x in value if x.id not in self.touched_item_id[key]]
                self.session.add_all(pulled_item_list)
            self.session.commit()
            for key, value in pulled_item_dict.items():
                self.touched_item_id[key].update({x.id for x in value})
            return True
        except DBAPIError as e:
            self.session.rollback()
            raise e

    def prepare_parameter_query(self):
        """Make sure everything is there for queries to work.
        Pull all parameters that have been directly touched,
        or indirectly by touching either its object class, or relationship class.
        """
        touched_parameter_list = self.session.query(
            self.Parameter.id
        ).set_shard('orig').filter(or_(
            self.Parameter.object_class_id.in_(self.touched_item_id["object_class"]),
            self.Parameter.relationship_class_id.in_(self.touched_item_id["relationship_class"]),
            self.Parameter.id.in_(self.touched_item_id["parameter"])))
        pulled_item_dict = dict()
        for parameter in touched_parameter_list:
            self.pull_parameter(parameter.id, pulled_item_dict)
        try:
            for key, value in pulled_item_dict.items():
                pulled_item_list = [x for x in value if x.id not in self.touched_item_id[key]]
                self.session.add_all(pulled_item_list)
            self.session.commit()
            for key, value in pulled_item_dict.items():
                self.touched_item_id[key].update({x.id for x in value})
            return True
        except DBAPIError as e:
            self.session.rollback()
            raise e

    def prepare_parameter_value_query(self):
        """Make sure everything is there for queries to work.
        Pull all parameter values that have been directly touched,
        or indirectly by touching either its parameter, object, or relationship.
        """
        touched_parameter_value_list = self.session.query(
            self.ParameterValue.id
        ).set_shard('orig').filter(or_(
            self.ParameterValue.object_id.in_(self.touched_item_id["object"]),
            self.ParameterValue.relationship_id.in_(self.touched_item_id["relationship"]),
            self.ParameterValue.parameter_id.in_(self.touched_item_id["parameter"]),
            self.ParameterValue.id.in_(self.touched_item_id["parameter_value"])))
        pulled_item_dict = dict()
        for parameter_value in touched_parameter_value_list:
            self.pull_parameter_value(parameter_value.id, pulled_item_dict)
        try:
            for key, value in pulled_item_dict.items():
                pulled_item_list = [x for x in value if x.id not in self.touched_item_id[key]]
                self.session.add_all(pulled_item_list)
            self.session.commit()
            for key, value in pulled_item_dict.items():
                self.touched_item_id[key].update({x.id for x in value})
            return True
        except DBAPIError as e:
            self.session.rollback()
            raise e

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
            self.ObjectClass.id,
            self.ObjectClass.name,
            self.ObjectClass.display_order,
        ).filter(~self.ObjectClass.id.in_(self.touched_item_id["object_class"]))
        diff_qry = self.session.query(
            self.DiffObjectClass.id,
            self.DiffObjectClass.name,
            self.DiffObjectClass.display_order,
        )
        return qry.union_all(diff_qry)

    def object_list(self, class_id=None):
        """Return objects, optionally filtered by class id."""
        self.prepare_object_query()
        qry = self.session.query(
            self.Object.id,
            self.Object.class_id,
            self.Object.name,
        ).filter(~self.Object.id.in_(self.touched_item_id["object"]))
        diff_qry = self.session.query(
            self.DiffObject.id,
            self.DiffObject.class_id,
            self.DiffObject.name,
        )
        if class_id:
            qry = qry.filter_by(class_id=class_id)
            diff_qry = diff_qry.filter_by(class_id=class_id)
        return qry.union_all(diff_qry)

    def wide_relationship_class_list(self, object_class_id=None):
        """Return list of relationship classes in wide format involving a given object class."""
        self.prepare_relationship_class_query()
        qry = self.session.query(
            self.RelationshipClass.id.label('id'),
            self.RelationshipClass.object_class_id.label('object_class_id'),
            self.ObjectClass.name.label('object_class_name'),
            self.RelationshipClass.name.label('name')
        ).filter(self.RelationshipClass.object_class_id == self.ObjectClass.id).\
        filter(~self.RelationshipClass.id.in_(self.touched_item_id["relationship_class"]))
        diff_qry = self.session.query(
            self.DiffRelationshipClass.id.label('id'),
            self.DiffRelationshipClass.object_class_id.label('object_class_id'),
            self.DiffObjectClass.name.label('object_class_name'),
            self.DiffRelationshipClass.name.label('name')
        ).filter(self.DiffRelationshipClass.object_class_id == self.DiffObjectClass.id)
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
        self.prepare_relationship_query()
        qry = self.session.query(
            self.Relationship.id.label('id'),
            self.Relationship.class_id.label('class_id'),
            self.Relationship.object_id.label('object_id'),
            self.Object.name.label('object_name'),
            self.Relationship.name.label('name')
        ).filter(self.Relationship.object_id == self.Object.id).\
        filter(~self.Relationship.id.in_(self.touched_item_id["relationship"]))
        diff_qry = self.session.query(
            self.DiffRelationship.id.label('id'),
            self.DiffRelationship.class_id.label('class_id'),
            self.DiffRelationship.object_id.label('object_id'),
            self.DiffObject.name.label('object_name'),
            self.DiffRelationship.name.label('name')
        ).filter(self.DiffRelationship.object_id == self.DiffObject.id)
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
        self.prepare_parameter_query()
        qry = self.session.query(
            self.Parameter.id,
            self.Parameter.name,
            self.Parameter.relationship_class_id,
            self.Parameter.object_class_id,
            self.Parameter.can_have_time_series,
            self.Parameter.can_have_time_pattern,
            self.Parameter.can_be_stochastic,
            self.Parameter.default_value,
            self.Parameter.is_mandatory,
            self.Parameter.precision,
            self.Parameter.minimum_value,
            self.Parameter.maximum_value
        ).filter(~self.Parameter.id.in_(self.touched_item_id["parameter"]))
        diff_qry = self.session.query(
            self.DiffParameter.id,
            self.DiffParameter.name,
            self.DiffParameter.relationship_class_id,
            self.DiffParameter.object_class_id,
            self.DiffParameter.can_have_time_series,
            self.DiffParameter.can_have_time_pattern,
            self.DiffParameter.can_be_stochastic,
            self.DiffParameter.default_value,
            self.DiffParameter.is_mandatory,
            self.DiffParameter.precision,
            self.DiffParameter.minimum_value,
            self.DiffParameter.maximum_value)
        if object_class_id:
            qry = qry.filter_by(object_class_id=object_class_id)
            diff_qry = diff_qry.filter_by(object_class_id=object_class_id)
        if relationship_class_id:
            qry = qry.filter_by(object_class_id=object_class_id)
            diff_qry = diff_qry.filter_by(object_class_id=object_class_id)
        return qry.union_all(diff_qry)

    def object_parameter_list(self, object_class_id=None, parameter_id=None):
        """Return object classes and their parameters."""
        self.prepare_parameter_query()
        qry = self.session.query(
            self.Parameter.id.label('parameter_id'),
            self.ObjectClass.name.label('object_class_name'),
            self.Parameter.name.label('parameter_name'),
            self.Parameter.can_have_time_series,
            self.Parameter.can_have_time_pattern,
            self.Parameter.can_be_stochastic,
            self.Parameter.default_value,
            self.Parameter.is_mandatory,
            self.Parameter.precision,
            self.Parameter.minimum_value,
            self.Parameter.maximum_value
        ).filter(~self.Parameter.id.in_(self.touched_item_id["parameter"])).\
        filter(self.ObjectClass.id == self.Parameter.object_class_id)
        diff_qry = self.session.query(
            self.DiffParameter.id.label('parameter_id'),
            self.DiffObjectClass.name.label('object_class_name'),
            self.DiffParameter.name.label('parameter_name'),
            self.DiffParameter.can_have_time_series,
            self.DiffParameter.can_have_time_pattern,
            self.DiffParameter.can_be_stochastic,
            self.DiffParameter.default_value,
            self.DiffParameter.is_mandatory,
            self.DiffParameter.precision,
            self.DiffParameter.minimum_value,
            self.DiffParameter.maximum_value
        ).filter(self.DiffObjectClass.id == self.DiffParameter.object_class_id)
        if object_class_id:
            qry = qry.filter(self.Parameter.object_class_id == object_class_id)
            diff_qry = diff_qry.filter(self.DiffParameter.object_class_id == object_class_id)
        if parameter_id:
            qry = qry.filter(self.Parameter.id == parameter_id)
            diff_qry = diff_qry.filter(self.DiffParameter.id == parameter_id)
        return qry.union_all(diff_qry).order_by(self.Parameter.id, self.DiffParameter.id)

    def relationship_parameter_list(self, relationship_class_id=None, parameter_id=None):
        """Return relationship classes and their parameters."""
        self.prepare_parameter_query()
        wide_relationship_class_subqry = self.wide_relationship_class_list().subquery()
        qry = self.session.query(
            self.Parameter.id.label('parameter_id'),
            wide_relationship_class_subqry.c.name.label('relationship_class_name'),
            wide_relationship_class_subqry.c.object_class_name_list,
            self.Parameter.name.label('parameter_name'),
            self.Parameter.can_have_time_series,
            self.Parameter.can_have_time_pattern,
            self.Parameter.can_be_stochastic,
            self.Parameter.default_value,
            self.Parameter.is_mandatory,
            self.Parameter.precision,
            self.Parameter.minimum_value,
            self.Parameter.maximum_value
        ).filter(~self.Parameter.id.in_(self.touched_item_id["parameter"])).\
        filter(self.Parameter.relationship_class_id == wide_relationship_class_subqry.c.id)
        diff_qry = self.session.query(
            self.DiffParameter.id.label('parameter_id'),
            wide_relationship_class_subqry.c.name.label('relationship_class_name'),
            wide_relationship_class_subqry.c.object_class_name_list,
            self.DiffParameter.name.label('parameter_name'),
            self.DiffParameter.can_have_time_series,
            self.DiffParameter.can_have_time_pattern,
            self.DiffParameter.can_be_stochastic,
            self.DiffParameter.default_value,
            self.DiffParameter.is_mandatory,
            self.DiffParameter.precision,
            self.DiffParameter.minimum_value,
            self.DiffParameter.maximum_value
        ).filter(self.DiffParameter.relationship_class_id == wide_relationship_class_subqry.c.id)
        if relationship_class_id:
            qry = qry.filter(self.Parameter.relationship_class_id == relationship_class_id)
            diff_qry = diff_qry.filter(self.DiffParameter.relationship_class_id == relationship_class_id)
        if parameter_id:
            qry = qry.filter(self.Parameter.id == parameter_id)
            diff_qry = diff_qry.filter(self.DiffParameter.id == parameter_id)
        return qry.union_all(diff_qry).order_by(self.Parameter.id, self.DiffParameter.id)

    def parameter_value_list(self, object_id=None, relationship_id=None):
        """Return parameter values."""
        self.prepare_parameter_value_query()
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
        self.prepare_parameter_query()
        self.prepare_parameter_value_query()
        qry = self.session.query(
            self.ParameterValue.id.label('parameter_value_id'),
            self.ObjectClass.name.label('object_class_name'),
            self.Object.name.label('object_name'),
            self.Parameter.name.label('parameter_name'),
            self.ParameterValue.index,
            self.ParameterValue.value,
            self.ParameterValue.json,
            self.ParameterValue.expression,
            self.ParameterValue.time_pattern,
            self.ParameterValue.time_series_id,
            self.ParameterValue.stochastic_model_id
        ).filter(~self.ParameterValue.id.in_(self.touched_item_id["parameter_value"])).\
        filter(self.Parameter.id == self.ParameterValue.parameter_id).\
        filter(self.ParameterValue.object_id == self.Object.id).\
        filter(self.Parameter.object_class_id == self.ObjectClass.id)
        diff_qry = self.session.query(
            self.DiffParameterValue.id.label('parameter_value_id'),
            self.DiffObjectClass.name.label('object_class_name'),
            self.DiffObject.name.label('object_name'),
            self.DiffParameter.name.label('parameter_name'),
            self.DiffParameterValue.index,
            self.DiffParameterValue.value,
            self.DiffParameterValue.json,
            self.DiffParameterValue.expression,
            self.DiffParameterValue.time_pattern,
            self.DiffParameterValue.time_series_id,
            self.DiffParameterValue.stochastic_model_id
        ).filter(self.DiffParameter.id == self.DiffParameterValue.parameter_id).\
        filter(self.DiffParameterValue.object_id == self.DiffObject.id).\
        filter(self.DiffParameter.object_class_id == self.DiffObjectClass.id)
        if parameter_name:
            qry = qry.filter(self.Parameter.name == parameter_name)
            diff_qry = diff_qry.filter(self.DiffParameter.name == parameter_name)
        return qry.union_all(diff_qry)

    def relationship_parameter_value_list(self, parameter_name=None):
        """Return relationships and their parameter values."""
        self.prepare_parameter_query()
        self.prepare_parameter_value_query()
        wide_relationship_subqry = self.wide_relationship_list().subquery()
        qry = self.session.query(
            self.ParameterValue.id.label('parameter_value_id'),
            self.RelationshipClass.name.label('relationship_class_name'),
            wide_relationship_subqry.c.object_name_list,
            self.Parameter.name.label('parameter_name'),
            self.ParameterValue.index,
            self.ParameterValue.value,
            self.ParameterValue.json,
            self.ParameterValue.expression,
            self.ParameterValue.time_pattern,
            self.ParameterValue.time_series_id,
            self.ParameterValue.stochastic_model_id
        ).filter(~self.ParameterValue.id.in_(self.touched_item_id["parameter_value"])).\
        filter(self.Parameter.id == self.ParameterValue.parameter_id).\
        filter(self.ParameterValue.relationship_id == wide_relationship_subqry.c.id).\
        filter(self.Parameter.relationship_class_id == self.RelationshipClass.id)
        diff_qry = self.session.query(
            self.DiffParameterValue.id.label('parameter_value_id'),
            self.DiffRelationshipClass.name.label('relationship_class_name'),
            wide_relationship_subqry.c.object_name_list,
            self.DiffParameter.name.label('parameter_name'),
            self.DiffParameterValue.index,
            self.DiffParameterValue.value,
            self.DiffParameterValue.json,
            self.DiffParameterValue.expression,
            self.DiffParameterValue.time_pattern,
            self.DiffParameterValue.time_series_id,
            self.DiffParameterValue.stochastic_model_id
        ).filter(self.DiffParameter.id == self.DiffParameterValue.parameter_id).\
        filter(self.DiffParameterValue.relationship_id == wide_relationship_subqry.c.id).\
        filter(self.DiffParameter.relationship_class_id == self.DiffRelationshipClass.id)
        if parameter_name:
            qry = qry.filter(self.Parameter.name == parameter_name)
            diff_qry = diff_qry.filter(self.DiffParameter.name == parameter_name)
        return qry.union_all(diff_qry)

    def add_object_class(self, **kwargs):
        """Add object class to database.

        Returns:
            object_class (KeyedTuple): the object class now with the id
        """
        try:
            # NOTE: This one will go to the diff db, because of shard_chooser
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
            diff_item = self.session.query(self.DiffObjectClass).set_shard('diff').filter_by(id=id).one_or_none()
            if diff_item:
                diff_item.name = new_name
            else:
                item = self.session.query(self.ObjectClass).set_shard('orig').filter_by(id=id).one_or_none()
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
            diff_item = self.session.query(self.DiffObject).set_shard('diff').filter_by(id=id).one_or_none()
            if diff_item:
                diff_item.name = new_name
            else:
                item = self.session.query(self.Object).set_shard('orig').filter_by(id=id).one_or_none()
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
            diff_item_list = self.session.query(self.DiffRelationshipClass).set_shard('diff').filter_by(id=id)
            if diff_item_list.count():
                for diff_item in diff_item_list:
                    diff_item.name = new_name
            else:
                item_list = self.session.query(self.RelationshipClass).set_shard('orig').filter_by(id=id)
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
            diff_item_list = self.session.query(self.DiffRelationship).set_shard('diff').filter_by(id=id)
            if diff_item_list.count():
                for diff_item in diff_item_list:
                    diff_item.name = new_name
            else:
                item_list = self.session.query(self.Relationship).set_shard('orig').filter_by(id=id)
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
            diff_item = self.session.query(self.DiffParameter).set_shard('diff').filter_by(id=id).one_or_none()
            if diff_item:
                setattr(diff_item, field_name, new_value)
            else:
                item = self.session.query(self.Parameter).set_shard('orig').filter_by(id=id).one_or_none()
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
            diff_item = self.session.query(self.DiffParameterValue).set_shard('diff').filter_by(id=id).one_or_none()
            if diff_item:
                setattr(diff_item, field_name, new_value)
            else:
                item = self.session.query(self.ParameterValue).set_shard('orig').filter_by(id=id).one_or_none()
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
        # By touching the item we make sure it's not brought by any query anymore
        self.touched_item_id["object_class"].add(id)
        self.removed_item_id["object_class"].add(id)

    def remove_object(self, id):
        """Remove object."""
        # By touching the item we make sure it's not brought by any query anymore
        self.touched_item_id["object"].add(id)
        self.removed_item_id["object"].add(id)

    def remove_relationship_class(self, id):
        """Remove relationship class."""
        # By touching the item we make sure it's not brought by any query anymore
        self.touched_item_id["relationship_class"].add(id)
        self.removed_item_id["relationship_class"].add(id)

    def remove_relationship(self, id):
        """Remove relationship."""
        # By touching the item we make sure it's not brought by any query anymore
        self.touched_item_id["relationship"].add(id)
        self.removed_item_id["relationship"].add(id)

    def remove_parameter(self, id):
        """Remove parameter."""
        # By touching the item we make sure it's not brought by any query anymore
        self.touched_item_id["parameter"].add(id)
        self.removed_item_id["parameter"].add(id)

    def remove_parameter_value(self, id):
        """Remove parameter value."""
        # By touching the item we make sure it's not brought by any query anymore
        self.touched_item_id["parameter_value"].add(id)
        self.removed_item_id["parameter_value"].add(id)

    def commit_session(self, comment):
        """Commit changes to source database."""
        try:
            session = Session(self.engine)
            user = self.username
            date = datetime.now(timezone.utc)
            commit = self.Commit(comment=comment, date=date, user=user)
            session.add(commit)
            session.flush()
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
            session.add_all(new_items)
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
                session.merge(dirty_item)
            # Remove removed
            removed_items = list()
            for id in self.removed_item_id["object_class"]:
                removed_item = session.query(self.ObjectClass).filter_by(id=id).one_or_none()
                removed_items.append(removed_item)
            for id in self.removed_item_id["object"]:
                item = session.query(self.Object).filter_by(id=id).one_or_none()
                removed_items.append(removed_item)
            for id in self.removed_item_id["relationship_class"]:
                for removed_item in session.query(self.RelationshipClass).filter_by(id=id):
                    removed_items.append(removed_item)
            for id in self.removed_item_id["relationship"]:
                for removed_item in session.query(self.Relationship).filter_by(id=id):
                    removed_items.append(removed_item)
            for id in self.removed_item_id["parameter"]:
                removed_item = session.query(self.Parameter).filter_by(id=id).one_or_none()
                removed_items.append(removed_item)
            for id in self.removed_item_id["parameter_value"]:
                removed_item = session.query(self.ParameterValue).filter_by(id=id).one_or_none()
                removed_items.append(removed_item)
            for removed_item in removed_items:
                session.delete(removed_item)
            session.commit()
        except DBAPIError as e:
            session.rollback()
            msg = "DBAPIError while commiting changes: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def rollback_session(self):
        # TODO: just reload the diff database, and clear all dicts
        pass
