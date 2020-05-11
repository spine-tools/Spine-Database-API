######################################################################################################################
# Copyright (C) 2017 - 2019 Spine project consortium
# This file is part of Spine Database API.
# Spine Database API is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser
# General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

"""Provides :class:`.DatabaseMappingBase`.

:author: Manuel Marin (KTH)
:date:   11.8.2018
"""
# TODO: Finish docstrings

import logging
from sqlalchemy import create_engine, inspect, func, case, MetaData, Table, Column, Integer, false, true
from sqlalchemy.sql.expression import label
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.engine.url import make_url
from sqlalchemy.orm import Session
from sqlalchemy.exc import NoSuchTableError
from alembic.migration import MigrationContext
from alembic.environment import EnvironmentContext
from alembic.script import ScriptDirectory
from alembic.config import Config
from .exception import SpineDBAPIError, SpineDBVersionError, SpineTableNotFoundError
from .helpers import compare_schemas, model_meta, custom_generate_relationship, _create_first_spine_database


logging.getLogger("alembic").setLevel(logging.CRITICAL)


class DatabaseMappingBase:
    """Base class for all database mappings.

    It provides the :meth:`query` method for custom db querying.

    :param str db_url: A URL in RFC-1738 format pointing to the database to be mapped.
    :param str username: A user name. If ``None``, it gets replaced by the string ``"anon"``.
    :param bool upgrade: Whether or not the db at the given URL should be upgraded to the most recent version.
    """

    def __init__(self, db_url, username=None, upgrade=False, codename=None, _create_engine=None):
        """Initialize class."""
        self.db_url = db_url
        self.username = username if username else "anon"
        self.codename = str(codename) if codename else str(db_url)
        self.engine = _create_engine(db_url) if _create_engine is not None else self._create_engine(db_url)
        self._check_db_version(upgrade=upgrade)
        self.connection = self.engine.connect()
        self.session = Session(self.connection, autoflush=False)
        self.sa_url = make_url(self.db_url)
        self.Alternative = None
        self.Scenario = None
        self.ScenarioAlternatives = None
        self.Commit = None
        self.EntityClassType = None
        self.EntityClass = None
        self.EntityType = None
        self.Entity = None
        self.ObjectClass = None
        self.Object = None
        self.RelationshipClass = None
        self.Relationship = None
        self.RelationshipEntity = None
        self.RelationshipEntityClass = None
        self.ParameterDefinition = None
        self.ParameterValue = None
        self.ParameterTag = None
        self.ParameterDefinitionTag = None
        self.ParameterValueList = None
        self.IdsForIn = None
        self._ids_for_in_clause_id = 0
        # class and entity type id
        self._object_class_type = None
        self._relationship_class_type = None
        self._object_entity_type = None
        self._relationship_entity_type = None
        # Subqueries that select everything from each table
        self._alternative_sq = None
        self._scenario_sq = None
        self._scenario_alternatives_sq = None
        self._entity_class_sq = None
        self._entity_sq = None
        self._entity_class_type_sq = None
        self._entity_type_sq = None
        self._object_sq = None
        self._object_class_sq = None
        self._object_sq = None
        self._relationship_class_sq = None
        self._relationship_sq = None
        self._parameter_definition_sq = None
        self._parameter_value_sq = None
        self._parameter_tag_sq = None
        self._parameter_definition_tag_sq = None
        self._parameter_value_list_sq = None
        # Special convenience subqueries that join two or more tables
        self._ext_object_sq = None
        self._ext_relationship_class_sq = None
        self._wide_relationship_class_sq = None
        self._ext_relationship_sq = None
        self._wide_relationship_sq = None
        self._object_parameter_definition_sq = None
        self._relationship_parameter_definition_sq = None
        self._object_parameter_value_sq = None
        self._relationship_parameter_value_sq = None
        self._ext_parameter_definition_tag_sq = None
        self._wide_parameter_definition_tag_sq = None
        self._ext_parameter_tag_definition_sq = None
        self._wide_parameter_tag_definition_sq = None
        self._ord_parameter_value_list_sq = None
        self._wide_parameter_value_list_sq = None
        # Table to class map for convenience
        self.table_to_class = {
            "alternative": "Alternative",
            "scenario": "Scenario",
            "scenario_alternatives": "ScenarioAlternatives",
            "commit": "Commit",
            "entity_class": "EntityClass",
            "entity_class_type": "EntityClassType",
            "entity": "Entity",
            "entity_type": "EntityType",
            "object": "Object",
            "object_class": "ObjectClass",
            "relationship_class": "RelationshipClass",
            "relationship": "Relationship",
            "relationship_entity": "RelationshipEntity",
            "relationship_entity_class": "RelationshipEntityClass",
            "parameter_definition": "ParameterDefinition",
            "parameter_value": "ParameterValue",
            "parameter_tag": "ParameterTag",
            "parameter_definition_tag": "ParameterDefinitionTag",
            "parameter_value_list": "ParameterValueList",
        }
        # Table primary ids map:
        self.table_ids = {
            "relationship_entity_class": "entity_class_id",
            "object_class": "entity_class_id",
            "relationship_class": "entity_class_id",
            "object": "entity_id",
            "relationship": "entity_id",
            "relationship_entity": "entity_id",
        }
        self._create_mapping()
        self._create_ids_for_in()

    @staticmethod
    def _create_engine(db_url):
        """Create engine."""
        try:
            engine = create_engine(db_url)
            with engine.connect():
                pass
        except Exception as e:
            raise SpineDBAPIError(
                "Could not connect to '{0}': {1}. Please make sure that '{0}' is the URL "
                "of a Spine database and try again.".format(db_url, str(e))
            )
        return engine

    def _check_db_version(self, upgrade=False):
        """Check if database is the latest version and raise a `SpineDBVersionError` if not.
        If upgrade is `True`, then don't raise the error and upgrade the database instead.
        """
        config = Config()
        config.set_main_option("script_location", "spinedb_api:alembic")
        script = ScriptDirectory.from_config(config)
        head = script.get_current_head()
        with self.engine.connect() as connection:
            migration_context = MigrationContext.configure(connection)
            current = migration_context.get_current_revision()
            if current is None:
                # No revision information. Check if the schema of the given url corresponds to
                # a 'first' Spine db --otherwise we can't go on.
                ref_engine = _create_first_spine_database("sqlite://")
                if not compare_schemas(self.engine, ref_engine):
                    raise SpineDBAPIError("The db at '{0}' doesn't seem like a valid Spine db.".format(self.db_url))
            if current == head:
                return
            if not upgrade:
                raise SpineDBVersionError(url=self.db_url, current=current, expected=head)

            # Upgrade function
            def upgrade_to_head(rev, context):
                return script._upgrade_revs("head", rev)

            with EnvironmentContext(
                config, script, fn=upgrade_to_head, as_sql=False, starting_rev=None, destination_rev="head", tag=None
            ) as environment_context:
                environment_context.configure(connection=connection, target_metadata=model_meta)
                with environment_context.begin_transaction():
                    environment_context.run_migrations()

    def _create_mapping(self):
        """Create ORM."""
        Base = automap_base()
        Base.prepare(self.engine, reflect=True, generate_relationship=custom_generate_relationship)
        not_found = []
        for tablename, classname in self.table_to_class.items():
            try:
                setattr(self, classname, getattr(Base.classes, tablename))
            except (NoSuchTableError, AttributeError):
                not_found.append(tablename)
        if not_found:
            raise SpineTableNotFoundError(not_found, self.db_url)

    def reconnect(self):
        self.connection = self.engine.connect()

    def _create_ids_for_in(self):
        """Create `ids_for_in` table if not exists and map it."""
        metadata = MetaData()
        ids_for_in_table = Table(
            "ids_for_in",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("clause_id", Integer),
            Column("id_for_in", Integer),
            prefixes=["TEMPORARY"],
        )
        ids_for_in_table.create(self.engine, checkfirst=True)
        metadata.create_all(self.connection)
        Base = automap_base(metadata=metadata)
        Base.prepare()
        self.IdsForIn = Base.classes.ids_for_in

    def in_(self, column, ids):
        """Returns an expression equivalent to ``column.in_(ids)`` that shouldn't trigger ``too many sql variables`` in sqlite.
        The strategy is to insert the ids in the temp table ``ids_for_in`` and then query them.
        """
        if ids is None:
            return true()
        if not ids:
            return false()
        # NOTE: We need to isolate ids by clause, since there might be multiple clauses using this function in the same query.
        # TODO: Try to find something better
        self._ids_for_in_clause_id += 1
        clause_id = self._ids_for_in_clause_id
        self.session.bulk_insert_mappings(self.IdsForIn, ({"id_for_in": id_, "clause_id": clause_id} for id_ in ids))
        return column.in_(self.query(self.IdsForIn.id_for_in).filter_by(clause_id=clause_id))

    def query(self, *args, **kwargs):
        """Return a sqlalchemy :class:`~sqlalchemy.orm.query.Query` object applied
        to this :class:`.DatabaseMappingBase`.

        To perform custom ``SELECT`` statements, call this method with one or more of the class documented
        :class:`~sqlalchemy.sql.expression.Alias` properties. For example, to select the object class with
        ``id`` equal to 1::

            from spinedb_api import DatabaseMapping
            url = 'sqlite:///spine.db'
            ...
            db_map = DatabaseMapping(url)
            db_map.query(db_map.object_class_sq).filter_by(id=1).one_or_none()

        To perform more complex queries, just use this method in combination with the SQLAlchemy API.
        For example, to select all object class names and the names of their objects concatenated in a string::

            from sqlalchemy import func

            db_map.query(
                db_map.object_class_sq.c.name, func.group_concat(db_map.object_sq.c.name)
            ).filter(
                db_map.object_sq.c.class_id == db_map.object_class_sq.c.id
            ).group_by(db_map.object_class_sq.c.name).all()
        """
        return self.session.query(*args, **kwargs)

    def _subquery(self, tablename):
        """A subquery of the form:

        .. code-block:: sql

            SELECT * FROM {tablename}

        :param str tablename: A string indicating the table to be queried.
        :type: :class:`~sqlalchemy.sql.expression.Alias`
        """
        classname = self.table_to_class[tablename]
        class_ = getattr(self, classname)
        return self.query(*[c.label(c.name) for c in inspect(class_).mapper.columns]).subquery()

    @property
    def alternative_sq(self):
        if self._alternative_sq is None:
            self._alternative_sq = self._subquery("alternative")
        return self._alternative_sq

    @property
    def scenario_sq(self):
        if self._scenario_sq is None:
            self._scenario_sq = self._subquery("scenario")
        return self._scenario_sq

    @property
    def scenario_alternatives_sq(self):
        if self._scenario_alternatives_sq is None:
            self._scenario_alternatives_sq = self._subquery("scenario_alternatives")
        return self._scenario_alternatives_sq

    @property
    def object_class_type(self):
        if self._object_class_type is None:
            result = self.query(self.entity_class_type_sq).filter(self.entity_class_type_sq.c.name == "object").first()
            self._object_class_type = result.id
        return self._object_class_type

    @property
    def relationship_class_type(self):
        if self._relationship_class_type is None:
            result = (
                self.query(self.entity_class_type_sq).filter(self.entity_class_type_sq.c.name == "relationship").first()
            )
            self._relationship_class_type = result.id
        return self._relationship_class_type

    @property
    def object_entity_type(self):
        if self._object_entity_type is None:
            result = self.query(self.entity_type_sq).filter(self.entity_type_sq.c.name == "object").first()
            self._object_entity_type = result.id
        return self._object_entity_type

    @property
    def relationship_entity_type(self):
        if self._relationship_entity_type is None:
            result = self.query(self.entity_type_sq).filter(self.entity_type_sq.c.name == "relationship").first()
            self._relationship_entity_type = result.id
        return self._relationship_entity_type

    @property
    def entity_class_type_sq(self):
        """A subquery of the form:

        .. code-block:: sql

            SELECT * FROM class_type

        :type: :class:`~sqlalchemy.sql.expression.Alias`
        """
        if self._entity_class_type_sq is None:
            self._entity_class_type_sq = self._subquery("entity_class_type")
        return self._entity_class_type_sq

    @property
    def entity_type_sq(self):
        """A subquery of the form:

        .. code-block:: sql

            SELECT * FROM class_type

        :type: :class:`~sqlalchemy.sql.expression.Alias`
        """
        if self._entity_type_sq is None:
            self._entity_type_sq = self._subquery("entity_type")
        return self._entity_type_sq

    @property
    def entity_class_sq(self):
        """A subquery of the form:

        .. code-block:: sql

            SELECT * FROM class

        :type: :class:`~sqlalchemy.sql.expression.Alias`
        """
        if self._entity_class_sq is None:
            self._entity_class_sq = self._subquery("entity_class")
        return self._entity_class_sq

    @property
    def entity_sq(self):
        """A subquery of the form:

        .. code-block:: sql

            SELECT * FROM entity

        :type: :class:`~sqlalchemy.sql.expression.Alias`
        """
        if self._entity_sq is None:
            self._entity_sq = self._subquery("entity")
        return self._entity_sq

    @property
    def object_class_sq(self):
        """A subquery of the form:

        .. code-block:: sql

            SELECT * FROM object_class

        :type: :class:`~sqlalchemy.sql.expression.Alias`
        """
        if self._object_class_sq is None:
            object_class_sq = self._subquery("object_class")
            self._object_class_sq = (
                self.query(
                    self.entity_class_sq.c.id.label("id"),
                    self.entity_class_sq.c.name.label("name"),
                    self.entity_class_sq.c.description.label("description"),
                    self.entity_class_sq.c.display_order.label("display_order"),
                    self.entity_class_sq.c.display_icon.label("display_icon"),
                    self.entity_class_sq.c.hidden.label("hidden"),
                    self.entity_class_sq.c.commit_id.label("commit_id"),
                )
                .filter(self.entity_class_sq.c.id == object_class_sq.c.entity_class_id)
                .subquery()
            )
        return self._object_class_sq

    @property
    def object_sq(self):
        """A subquery of the form:

        .. code-block:: sql

            SELECT * FROM object

        :type: :class:`~sqlalchemy.sql.expression.Alias`
        """
        if self._object_sq is None:
            object_sq = self._subquery("object")
            self._object_sq = (
                self.query(
                    self.entity_sq.c.id.label("id"),
                    self.entity_sq.c.class_id.label("class_id"),
                    self.entity_sq.c.name.label("name"),
                    self.entity_sq.c.description.label("description"),
                    self.entity_sq.c.commit_id.label("commit_id"),
                )
                .filter(self.entity_sq.c.id == object_sq.c.entity_id)
                .subquery()
            )
        return self._object_sq

    @property
    def relationship_class_sq(self):
        """A subquery of the form:

        .. code-block:: sql

            SELECT * FROM relationship_class

        :type: :class:`~sqlalchemy.sql.expression.Alias`
        """
        if self._relationship_class_sq is None:
            rel_ent_cls_sq = self._subquery("relationship_entity_class")
            self._relationship_class_sq = (
                self.query(
                    rel_ent_cls_sq.c.entity_class_id.label("id"),
                    rel_ent_cls_sq.c.dimension.label("dimension"),
                    rel_ent_cls_sq.c.member_class_id.label("object_class_id"),
                    self.entity_class_sq.c.name.label("name"),
                    self.entity_class_sq.c.description.label("description"),
                    self.entity_class_sq.c.hidden.label("hidden"),
                    self.entity_class_sq.c.commit_id.label("commit_id"),
                )
                .filter(self.entity_class_sq.c.id == rel_ent_cls_sq.c.entity_class_id)
                .subquery()
            )
        return self._relationship_class_sq

    @property
    def relationship_sq(self):
        """A subquery of the form:

        .. code-block:: sql

            SELECT * FROM relationship

        :type: :class:`~sqlalchemy.sql.expression.Alias`
        """
        if self._relationship_sq is None:
            rel_ent_sq = self._subquery("relationship_entity")
            self._relationship_sq = (
                self.query(
                    rel_ent_sq.c.entity_id.label("id"),
                    rel_ent_sq.c.dimension.label("dimension"),
                    rel_ent_sq.c.member_id.label("object_id"),
                    rel_ent_sq.c.entity_class_id.label("class_id"),
                    self.entity_sq.c.name.label("name"),
                    self.entity_sq.c.commit_id.label("commit_id"),
                )
                .filter(self.entity_sq.c.id == rel_ent_sq.c.entity_id)
                .subquery()
            )
        return self._relationship_sq

    @property
    def parameter_definition_sq(self):
        """A subquery of the form:

        .. code-block:: sql

            SELECT * FROM parameter_definition

        :type: :class:`~sqlalchemy.sql.expression.Alias`
        """

        if self._parameter_definition_sq is None:
            par_def_sq = self._subquery("parameter_definition")

            object_class_case = case(
                [(self.entity_class_sq.c.type_id == self.object_class_type, par_def_sq.c.entity_class_id)], else_=None
            )
            rel_class_case = case(
                [(self.entity_class_sq.c.type_id == self.relationship_class_type, par_def_sq.c.entity_class_id)],
                else_=None,
            )

            self._parameter_definition_sq = (
                self.query(
                    par_def_sq.c.id.label("id"),
                    par_def_sq.c.name.label("name"),
                    par_def_sq.c.description.label("description"),
                    par_def_sq.c.data_type.label("data_type"),
                    par_def_sq.c.entity_class_id,
                    label("object_class_id", object_class_case),
                    label("relationship_class_id", rel_class_case),
                    par_def_sq.c.default_value.label("default_value"),
                    par_def_sq.c.commit_id.label("commit_id"),
                    par_def_sq.c.parameter_value_list_id.label("parameter_value_list_id"),
                )
                .join(self.entity_class_sq, self.entity_class_sq.c.id == par_def_sq.c.entity_class_id)
                .subquery()
            )
        return self._parameter_definition_sq

    @property
    def parameter_value_sq(self):
        """A subquery of the form:

        .. code-block:: sql

            SELECT * FROM parameter_value

        :type: :class:`~sqlalchemy.sql.expression.Alias`
        """
        if self._parameter_value_sq is None:
            par_val_sq = self._subquery("parameter_value")

            object_class_case = case(
                [(self.entity_class_sq.c.type_id == self.object_class_type, par_val_sq.c.entity_class_id)], else_=None
            )
            rel_class_case = case(
                [(self.entity_class_sq.c.type_id == self.relationship_class_type, par_val_sq.c.entity_class_id)],
                else_=None,
            )
            object_entity_case = case(
                [(self.entity_sq.c.type_id == self.object_entity_type, par_val_sq.c.entity_id)], else_=None
            )
            rel_entity_case = case(
                [(self.entity_sq.c.type_id == self.relationship_entity_type, par_val_sq.c.entity_id)], else_=None
            )

            self._parameter_value_sq = (
                self.query(
                    par_val_sq.c.id.label("id"),
                    par_val_sq.c.parameter_definition_id,
                    par_val_sq.c.entity_class_id,
                    par_val_sq.c.entity_id,
                    label("object_class_id", object_class_case),
                    label("relationship_class_id", rel_class_case),
                    label("object_id", object_entity_case),
                    label("relationship_id", rel_entity_case),
                    par_val_sq.c.value.label("value"),
                    par_val_sq.c.commit_id.label("commit_id"),
                    par_val_sq.c.alternative_id
                )
                .join(self.entity_sq, self.entity_sq.c.id == par_val_sq.c.entity_id)
                .join(self.entity_class_sq, self.entity_class_sq.c.id == par_val_sq.c.entity_class_id)
                .subquery()
            )

        return self._parameter_value_sq

    @property
    def parameter_tag_sq(self):
        """A subquery of the form:

        .. code-block:: sql

            SELECT * FROM parameter_tag

        :type: :class:`~sqlalchemy.sql.expression.Alias`
        """
        if self._parameter_tag_sq is None:
            self._parameter_tag_sq = self._subquery("parameter_tag")
        return self._parameter_tag_sq

    @property
    def parameter_definition_tag_sq(self):
        """A subquery of the form:

        .. code-block:: sql

            SELECT * FROM parameter_definition_tag

        :type: :class:`~sqlalchemy.sql.expression.Alias`
        """
        if self._parameter_definition_tag_sq is None:
            self._parameter_definition_tag_sq = self._subquery("parameter_definition_tag")
        return self._parameter_definition_tag_sq

    @property
    def parameter_value_list_sq(self):
        """A subquery of the form:

        .. code-block:: sql

            SELECT * FROM parameter_value_list

        :type: :class:`~sqlalchemy.sql.expression.Alias`
        """
        if self._parameter_value_list_sq is None:
            self._parameter_value_list_sq = self._subquery("parameter_value_list")
        return self._parameter_value_list_sq

    @property
    def ext_object_sq(self):
        """A subquery of the form:

        .. code-block:: sql

            SELECT
                o.id,
                o.class_id,
                oc.name AS class_name,
                o.name,
                o.description,
            FROM object AS o, object_class AS oc
            WHERE o.class_id = oc.id

        :type: :class:`~sqlalchemy.sql.expression.Alias`
        """
        if self._ext_object_sq is None:
            self._ext_object_sq = (
                self.query(
                    self.object_sq.c.id.label("id"),
                    self.object_sq.c.class_id.label("class_id"),
                    self.object_class_sq.c.name.label("class_name"),
                    self.object_sq.c.name.label("name"),
                    self.object_sq.c.description.label("description"),
                )
                .filter(self.object_sq.c.class_id == self.object_class_sq.c.id)
                .subquery()
            )
        return self._ext_object_sq

    @property
    def ext_relationship_class_sq(self):
        """A subquery of the form:

        .. code-block:: sql

            SELECT
                rc.id,
                rc.name,
                oc.id AS object_class_id,
                oc.name AS object_class_name
            FROM relationship_class AS rc, object_class AS oc
            WHERE rc.object_class_id = oc.id
            ORDER BY rc.id, rc.dimension

        :type: :class:`~sqlalchemy.sql.expression.Alias`
        """
        if self._ext_relationship_class_sq is None:
            self._ext_relationship_class_sq = (
                self.query(
                    self.relationship_class_sq.c.id.label("id"),
                    self.relationship_class_sq.c.name.label("name"),
                    self.relationship_class_sq.c.description.label("description"),
                    self.object_class_sq.c.id.label("object_class_id"),
                    self.object_class_sq.c.name.label("object_class_name"),
                )
                .filter(self.relationship_class_sq.c.object_class_id == self.object_class_sq.c.id)
                .order_by(self.relationship_class_sq.c.id, self.relationship_class_sq.c.dimension)
                .subquery()
            )
        return self._ext_relationship_class_sq

    @property
    def wide_relationship_class_sq(self):
        """A subquery of the form:

        .. code-block:: sql

            SELECT
                id,
                name,
                GROUP_CONCAT(object_class_id) AS object_class_id_list,
                GROUP_CONCAT(object_class_name) AS object_class_name_list
            FROM (
                SELECT
                    rc.id,
                    rc.name,
                    oc.id AS object_class_id,
                    oc.name AS object_class_name
                FROM relationship_class AS rc, object_class AS oc
                WHERE rc.object_class_id = oc.id
                ORDER BY rc.id, rc.dimension
            )
            GROUP BY id, name

        :type: :class:`~sqlalchemy.sql.expression.Alias`
        """
        if self._wide_relationship_class_sq is None:
            self._wide_relationship_class_sq = (
                self.query(
                    self.ext_relationship_class_sq.c.id,
                    self.ext_relationship_class_sq.c.name,
                    self.ext_relationship_class_sq.c.description,
                    func.group_concat(self.ext_relationship_class_sq.c.object_class_id).label("object_class_id_list"),
                    func.group_concat(self.ext_relationship_class_sq.c.object_class_name).label(
                        "object_class_name_list"
                    ),
                )
                .group_by(self.ext_relationship_class_sq.c.id, self.ext_relationship_class_sq.c.name)
                .subquery()
            )
        return self._wide_relationship_class_sq

    @property
    def ext_relationship_sq(self):
        """A subquery of the form:

        .. code-block:: sql

            SELECT
                r.id,
                r.class_id,
                r.name,
                o.id AS object_id,
                o.name AS object_name,
                o.class_id AS object_class_id,
            FROM relationship as r, object AS o
            WHERE r.object_id = o.id
            ORDER BY r.id, r.dimension

        :type: :class:`~sqlalchemy.sql.expression.Alias`
        """
        if self._ext_relationship_sq is None:
            self._ext_relationship_sq = (
                self.query(
                    self.relationship_sq.c.id.label("id"),
                    self.relationship_sq.c.name.label("name"),
                    self.relationship_sq.c.class_id.label("class_id"),
                    self.wide_relationship_class_sq.c.name.label("class_name"),
                    self.object_sq.c.id.label("object_id"),
                    self.object_sq.c.name.label("object_name"),
                    self.object_sq.c.class_id.label("object_class_id"),
                )
                .filter(self.relationship_sq.c.object_id == self.object_sq.c.id)
                .filter(self.relationship_sq.c.class_id == self.wide_relationship_class_sq.c.id)
                .order_by(self.relationship_sq.c.id, self.relationship_sq.c.dimension)
                .subquery()
            )
        return self._ext_relationship_sq

    @property
    def wide_relationship_sq(self):
        """A subquery of the form:

        .. code-block:: sql

            SELECT
                id,
                class_id,
                class_name,
                name,
                GROUP_CONCAT(object_id) AS object_id_list,
                GROUP_CONCAT(object_name) AS object_name_list
            FROM (
                SELECT
                    r.id,
                    r.class_id,
                    r.name,
                    o.id AS object_id,
                    o.name AS object_name
                FROM relationship as r, object AS o
                WHERE r.object_id = o.id
                ORDER BY r.id, r.dimension
            )
            GROUP BY id, class_id, name

        :type: :class:`~sqlalchemy.sql.expression.Alias`
        """
        if self._wide_relationship_sq is None:
            self._wide_relationship_sq = (
                self.query(
                    self.ext_relationship_sq.c.id,
                    self.ext_relationship_sq.c.name,
                    self.ext_relationship_sq.c.class_id,
                    self.ext_relationship_sq.c.class_name,
                    func.group_concat(self.ext_relationship_sq.c.object_id).label("object_id_list"),
                    func.group_concat(self.ext_relationship_sq.c.object_name).label("object_name_list"),
                    func.group_concat(self.ext_relationship_sq.c.object_class_id).label("object_class_id_list"),
                )
                .group_by(
                    self.ext_relationship_sq.c.id, self.ext_relationship_sq.c.class_id, self.ext_relationship_sq.c.name
                )
                .subquery()
            )
        return self._wide_relationship_sq

    @property
    def object_parameter_definition_sq(self):
        """A subquery of the form:

        .. code-block:: sql

            SELECT
                pd.id,
                oc.id AS object_class_id,
                oc.name AS object_class_name,
                pd.name AS parameter_name,
                wpvl.id AS value_list_id,
                wpvl.name AS value_list_name,
                wpdt.parameter_tag_id_list,
                wpdt.parameter_tag_list,
                pd.default_value
            FROM parameter_definition AS pd, object_class AS oc
            LEFT JOIN (
                SELECT
                    parameter_definition_id,
                    GROUP_CONCAT(parameter_tag_id) AS parameter_tag_id_list,
                    GROUP_CONCAT(parameter_tag) AS parameter_tag_list
                FROM (
                    SELECT
                        pdt.parameter_definition_id,
                        pt.id AS parameter_tag_id,
                        pt.tag AS parameter_tag
                    FROM parameter_definition_tag as pdt, parameter_tag AS pt
                    WHERE pdt.parameter_tag_id = pt.id
                )
                GROUP BY parameter_definition_id
            ) AS wpdt
            ON wpdt.parameter_definition_id = pd.id
            LEFT JOIN (
                SELECT
                    id,
                    name,
                    GROUP_CONCAT(value) AS value_list
                FROM (
                    SELECT id, name, value
                    FROM parameter_value_list
                    ORDER BY id, value_index
                )
                GROUP BY id, name
            ) AS wpvl
            ON wpvl.id = pd.parameter_value_list_id
            WHERE pd.object_class_id = oc.id

        :type: :class:`~sqlalchemy.sql.expression.Alias`
        """
        if self._object_parameter_definition_sq is None:
            self._object_parameter_definition_sq = (
                self.query(
                    self.parameter_definition_sq.c.id.label("id"),
                    self.parameter_definition_sq.c.entity_class_id,
                    self.object_class_sq.c.id.label("object_class_id"),
                    self.object_class_sq.c.name.label("object_class_name"),
                    self.parameter_definition_sq.c.name.label("parameter_name"),
                    self.parameter_definition_sq.c.parameter_value_list_id.label("value_list_id"),
                    self.wide_parameter_value_list_sq.c.name.label("value_list_name"),
                    self.wide_parameter_definition_tag_sq.c.parameter_tag_id_list,
                    self.wide_parameter_definition_tag_sq.c.parameter_tag_list,
                    self.parameter_definition_sq.c.default_value,
                    self.parameter_definition_sq.c.description,
                )
                .filter(self.object_class_sq.c.id == self.parameter_definition_sq.c.object_class_id)
                .filter(self.wide_parameter_definition_tag_sq.c.id == self.parameter_definition_sq.c.id)
                .outerjoin(
                    self.wide_parameter_value_list_sq,
                    self.wide_parameter_value_list_sq.c.id == self.parameter_definition_sq.c.parameter_value_list_id,
                )
                .subquery()
            )
        return self._object_parameter_definition_sq

    @property
    def relationship_parameter_definition_sq(self):
        """A subquery of the form:

        .. code-block:: sql

            SELECT
                pd.id,
                wrc.id AS relationship_class_id,
                wrc.name AS relationship_class_name,
                wrc.object_class_id_list,
                wrc.object_class_name_list,
                pd.name AS parameter_name,
                wpvl.id AS value_list_id,
                wpvl.name AS value_list_name,
                wpdt.parameter_tag_id_list,
                wpdt.parameter_tag_list,
                pd.default_value
            FROM
                parameter_definition AS pd,
                (
                    SELECT
                        id,
                        name,
                        GROUP_CONCAT(object_class_id) AS object_class_id_list,
                        GROUP_CONCAT(object_class_name) AS object_class_name_list
                    FROM (
                        SELECT
                            rc.id,
                            rc.name,
                            oc.id AS object_class_id,
                            oc.name AS object_class_name
                        FROM relationship_class AS rc, object_class AS oc
                        WHERE rc.object_class_id = oc.id
                        ORDER BY rc.id, rc.dimension
                    )
                    GROUP BY id, name
                ) AS wrc
            LEFT JOIN (
                SELECT
                    parameter_definition_id,
                    GROUP_CONCAT(parameter_tag_id) AS parameter_tag_id_list,
                    GROUP_CONCAT(parameter_tag) AS parameter_tag_list
                FROM (
                    SELECT
                        pdt.parameter_definition_id,
                        pt.id AS parameter_tag_id,
                        pt.tag AS parameter_tag
                    FROM parameter_definition_tag as pdt, parameter_tag AS pt
                    WHERE pdt.parameter_tag_id = pt.id
                )
                GROUP BY parameter_definition_id
            ) AS wpdt
            ON wpdt.parameter_definition_id = pd.id
            LEFT JOIN (
                SELECT
                    id,
                    name,
                    GROUP_CONCAT(value) AS value_list
                FROM (
                    SELECT id, name, value
                    FROM parameter_value_list
                    ORDER BY id, value_index
                )
                GROUP BY id, name
            ) AS wpvl
            ON wpvl.id = pd.parameter_value_list_id
            WHERE pd.relationship_class_id = wrc.id

        :type: :class:`~sqlalchemy.sql.expression.Alias`
        """
        if self._relationship_parameter_definition_sq is None:
            self._relationship_parameter_definition_sq = (
                self.query(
                    self.parameter_definition_sq.c.id.label("id"),
                    self.parameter_definition_sq.c.entity_class_id,
                    self.wide_relationship_class_sq.c.id.label("relationship_class_id"),
                    self.wide_relationship_class_sq.c.name.label("relationship_class_name"),
                    self.wide_relationship_class_sq.c.object_class_id_list,
                    self.wide_relationship_class_sq.c.object_class_name_list,
                    self.parameter_definition_sq.c.name.label("parameter_name"),
                    self.parameter_definition_sq.c.parameter_value_list_id.label("value_list_id"),
                    self.wide_parameter_value_list_sq.c.name.label("value_list_name"),
                    self.wide_parameter_definition_tag_sq.c.parameter_tag_id_list,
                    self.wide_parameter_definition_tag_sq.c.parameter_tag_list,
                    self.parameter_definition_sq.c.default_value,
                    self.parameter_definition_sq.c.description,
                )
                .filter(self.parameter_definition_sq.c.relationship_class_id == self.wide_relationship_class_sq.c.id)
                .filter(self.wide_parameter_definition_tag_sq.c.id == self.parameter_definition_sq.c.id)
                .outerjoin(
                    self.wide_parameter_value_list_sq,
                    self.wide_parameter_value_list_sq.c.id == self.parameter_definition_sq.c.parameter_value_list_id,
                )
                .subquery()
            )
        return self._relationship_parameter_definition_sq

    @property
    def object_parameter_value_sq(self):
        """A subquery of the form:

        :type: :class:`~sqlalchemy.sql.expression.Alias`
        """
        # TODO: Should this also bring `value_list` and `tag_list`?
        if self._object_parameter_value_sq is None:
            self._object_parameter_value_sq = (
                self.query(
                    self.parameter_value_sq.c.id.label("id"),
                    self.parameter_definition_sq.c.entity_class_id,
                    self.object_class_sq.c.id.label("object_class_id"),
                    self.object_class_sq.c.name.label("object_class_name"),
                    self.parameter_value_sq.c.entity_id,
                    self.object_sq.c.id.label("object_id"),
                    self.object_sq.c.name.label("object_name"),
                    self.parameter_definition_sq.c.id.label("parameter_id"),
                    self.parameter_definition_sq.c.name.label("parameter_name"),
                    self.parameter_value_sq.c.value,
                    self.parameter_value_sq.c.alternative_id
                )
                .filter(self.parameter_definition_sq.c.id == self.parameter_value_sq.c.parameter_definition_id)
                .filter(self.parameter_value_sq.c.object_id == self.object_sq.c.id)
                .filter(self.parameter_definition_sq.c.object_class_id == self.object_class_sq.c.id)
                .subquery()
            )
        return self._object_parameter_value_sq

    @property
    def relationship_parameter_value_sq(self):
        """A subquery of the form:


        :type: :class:`~sqlalchemy.sql.expression.Alias`
        """
        # TODO: Should this also bring `value_list` and `tag_list`?
        if self._relationship_parameter_value_sq is None:
            self._relationship_parameter_value_sq = (
                self.query(
                    self.parameter_value_sq.c.id.label("id"),
                    self.parameter_definition_sq.c.entity_class_id,
                    self.wide_relationship_class_sq.c.id.label("relationship_class_id"),
                    self.wide_relationship_class_sq.c.name.label("relationship_class_name"),
                    self.wide_relationship_class_sq.c.object_class_id_list,
                    self.wide_relationship_class_sq.c.object_class_name_list,
                    self.parameter_value_sq.c.entity_id,
                    self.wide_relationship_sq.c.id.label("relationship_id"),
                    self.wide_relationship_sq.c.object_id_list,
                    self.wide_relationship_sq.c.object_name_list,
                    self.parameter_definition_sq.c.id.label("parameter_id"),
                    self.parameter_definition_sq.c.name.label("parameter_name"),
                    self.parameter_value_sq.c.value,
                    self.parameter_value_sq.c.alternative_id
                )
                .filter(self.parameter_definition_sq.c.id == self.parameter_value_sq.c.parameter_definition_id)
                .filter(self.parameter_value_sq.c.relationship_id == self.wide_relationship_sq.c.id)
                .filter(self.parameter_definition_sq.c.relationship_class_id == self.wide_relationship_class_sq.c.id)
                .subquery()
            )
        return self._relationship_parameter_value_sq

    @property
    def ext_parameter_definition_tag_sq(self):
        """A subquery of the form:

        .. code-block:: sql

            SELECT
                pdt.parameter_definition_id,
                pt.id AS parameter_tag_id,
                pt.tag AS parameter_tag
            FROM parameter_definition_tag as pdt, parameter_tag AS pt
            WHERE pdt.parameter_tag_id = pt.id

        :type: :class:`~sqlalchemy.sql.expression.Alias`
        """
        if self._ext_parameter_definition_tag_sq is None:
            self._ext_parameter_definition_tag_sq = (
                self.query(
                    self.parameter_definition_sq.c.id.label("parameter_definition_id"),
                    self.parameter_definition_tag_sq.c.parameter_tag_id.label("parameter_tag_id"),
                    self.parameter_tag_sq.c.tag.label("parameter_tag"),
                )
                .outerjoin(
                    self.parameter_definition_tag_sq,
                    self.parameter_definition_tag_sq.c.parameter_definition_id == self.parameter_definition_sq.c.id,
                )
                .outerjoin(
                    self.parameter_tag_sq,
                    self.parameter_tag_sq.c.id == self.parameter_definition_tag_sq.c.parameter_tag_id,
                )
                .subquery()
            )
        return self._ext_parameter_definition_tag_sq

    @property
    def wide_parameter_definition_tag_sq(self):
        """A subquery of the form:

        .. code-block:: sql

            SELECT
                parameter_definition_id,
                GROUP_CONCAT(parameter_tag_id) AS parameter_tag_id_list,
                GROUP_CONCAT(parameter_tag) AS parameter_tag_list
            FROM (
                SELECT
                    pdt.parameter_definition_id,
                    pt.id AS parameter_tag_id,
                    pt.tag AS parameter_tag
                FROM parameter_definition_tag as pdt, parameter_tag AS pt
                WHERE pdt.parameter_tag_id = pt.id
            )
            GROUP BY parameter_definition_id

        :type: :class:`~sqlalchemy.sql.expression.Alias`
        """
        if self._wide_parameter_definition_tag_sq is None:
            self._wide_parameter_definition_tag_sq = (
                self.query(
                    self.ext_parameter_definition_tag_sq.c.parameter_definition_id.label("id"),
                    func.group_concat(self.ext_parameter_definition_tag_sq.c.parameter_tag_id).label(
                        "parameter_tag_id_list"
                    ),
                    func.group_concat(self.ext_parameter_definition_tag_sq.c.parameter_tag).label("parameter_tag_list"),
                )
                .group_by(self.ext_parameter_definition_tag_sq.c.parameter_definition_id)
                .subquery()
            )
        return self._wide_parameter_definition_tag_sq

    @property
    def ord_parameter_value_list_sq(self):
        """A subquery of the form:

        .. code-block:: sql

            SELECT *
            FROM parameter_value_list
            ORDER BY id, value_index

        :type: :class:`~sqlalchemy.sql.expression.Alias`
        """
        # NOTE: Not sure what the purpose of this was
        if self._ord_parameter_value_list_sq is None:
            self._ord_parameter_value_list_sq = (
                self.query(self.parameter_value_list_sq)
                .order_by(self.parameter_value_list_sq.c.id, self.parameter_value_list_sq.c.value_index)
                .subquery()
            )
        return self._ord_parameter_value_list_sq

    @property
    def wide_parameter_value_list_sq(self):
        """A subquery of the form:

        .. code-block:: sql

            SELECT
                id,
                name,
                GROUP_CONCAT(value) AS value_list
            FROM (
                SELECT id, name, value
                FROM parameter_value_list
                ORDER BY id, value_index
            )
            GROUP BY id

        :type: :class:`~sqlalchemy.sql.expression.Alias`
        """
        if self._wide_parameter_value_list_sq is None:
            self._wide_parameter_value_list_sq = (
                self.query(
                    self.parameter_value_list_sq.c.id,
                    self.parameter_value_list_sq.c.name,
                    func.group_concat(self.parameter_value_list_sq.c.value).label("value_list"),
                ).group_by(self.parameter_value_list_sq.c.id, self.parameter_value_list_sq.c.name)
            ).subquery()
        return self._wide_parameter_value_list_sq

    def _reset_mapping(self):
        """Delete all records from all tables but don't drop the tables.
        Useful for writing tests
        """
        self.query(self.Alternative).delete(synchronize_session=False)
        self.connection.execute("INSERT INTO alternative VALUES (1, 'Base', 'Base alternative')")
        self.query(self.Scenario).delete(synchronize_session=False)
        self.query(self.ScenarioAlternatives).delete(synchronize_session=False)
        self.query(self.EntityClass).delete(synchronize_session=False)
        self.query(self.Entity).delete(synchronize_session=False)
        self.query(self.Object).delete(synchronize_session=False)
        self.query(self.ObjectClass).delete(synchronize_session=False)
        self.query(self.RelationshipEntityClass).delete(synchronize_session=False)
        self.query(self.RelationshipClass).delete(synchronize_session=False)
        self.query(self.Relationship).delete(synchronize_session=False)
        self.query(self.RelationshipEntity).delete(synchronize_session=False)
        self.query(self.ParameterDefinition).delete(synchronize_session=False)
        self.query(self.ParameterValue).delete(synchronize_session=False)
        self.query(self.ParameterTag).delete(synchronize_session=False)
        self.query(self.ParameterDefinitionTag).delete(synchronize_session=False)
        self.query(self.ParameterValueList).delete(synchronize_session=False)
        self.query(self.Commit).delete(synchronize_session=False)
