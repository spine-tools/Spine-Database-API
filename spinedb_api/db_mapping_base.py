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

"""Provides :class:`.DatabaseMappingBase`.

"""
# TODO: Finish docstrings
import uuid
import hashlib
import os
import logging
import time
from collections import Counter
from types import MethodType
from contextlib import contextmanager
from sqlalchemy import create_engine, case, MetaData, Table, Column, false, and_, func, inspect, cast, Integer, or_
from sqlalchemy.sql.expression import label, Alias
from sqlalchemy.engine.url import make_url, URL
from sqlalchemy.orm import Session, aliased
from sqlalchemy.exc import DatabaseError
from sqlalchemy.event import listen
from sqlalchemy.pool import NullPool
from alembic.migration import MigrationContext
from alembic.environment import EnvironmentContext
from alembic.script import ScriptDirectory
from alembic.config import Config
from alembic.util.exc import CommandError
from .exception import SpineDBAPIError, SpineDBVersionError
from .helpers import (
    _create_first_spine_database,
    create_new_spine_database,
    compare_schemas,
    forward_sweep,
    group_concat,
    model_meta,
    copy_database_bind,
)
from .filters.tools import pop_filter_configs
from .spine_db_client import get_db_url_from_server
from .db_cache import DBCache

logging.getLogger("alembic").setLevel(logging.CRITICAL)


class DatabaseMappingBase:
    """Base class for all database mappings.

    It provides the :meth:`query` method for custom db querying.
    """

    _session_kwargs = {}
    ITEM_TYPES = (
        "object_class",
        "relationship_class",
        "parameter_value_list",
        "list_value",
        "parameter_definition",
        "object",
        "relationship",
        "entity_group",
        "parameter_value",
        "alternative",
        "scenario",
        "scenario_alternative",
        "feature",
        "tool",
        "tool_feature",
        "tool_feature_method",
        "metadata",
        "entity_metadata",
        "parameter_value_metadata",
    )

    def __init__(
        self,
        db_url,
        username=None,
        upgrade=False,
        codename=None,
        create=False,
        apply_filters=True,
        memory=False,
        sqlite_timeout=1800,
        advance_cache_query=None,
    ):
        """
        Args:
            db_url (str or URL): A URL in RFC-1738 format pointing to the database to be mapped, or to a DB server.
            username (str, optional): A user name. If ``None``, it gets replaced by the string ``"anon"``.
            upgrade (bool): Whether or not the db at the given URL should be upgraded to the most recent version.
            codename (str, optional): A name that uniquely identifies the class instance within a client application.
            create (bool): Whether or not to create a Spine db at the given URL if it's not already.
            apply_filters (bool): Whether or not filters in the URL's query part are applied to the database map.
            memory (bool): Whether or not to use a sqlite memory db as replacement for this DB map.
        """
        if advance_cache_query is None:
            advance_cache_query = self._advance_cache_query
        # FIXME: We should also check the server memory property and use it here
        db_url = get_db_url_from_server(db_url)
        self.db_url = str(db_url)
        if isinstance(db_url, str):
            filter_configs, db_url = pop_filter_configs(db_url)
        elif isinstance(db_url, URL):
            filter_configs = db_url.query.pop("spinedbfilter", [])
        else:
            filter_configs = []
        self._filter_configs = filter_configs if apply_filters else None
        self.sa_url = make_url(db_url)
        self.username = username if username else "anon"
        self.codename = self._make_codename(codename)
        self._memory = memory
        self.committing = True
        self._memory_dirty = False
        self._original_engine = self.create_engine(
            self.sa_url, upgrade=upgrade, create=create, sqlite_timeout=sqlite_timeout
        )
        # NOTE: The NullPool is needed to receive the close event (or any events), for some reason
        self.engine = create_engine("sqlite://", poolclass=NullPool) if self._memory else self._original_engine
        self.connection = self.engine.connect()
        if self._memory:
            copy_database_bind(self.connection, self._original_engine)
        listen(self.engine, 'close', self._receive_engine_close)
        self._metadata = MetaData(self.connection)
        self._metadata.reflect()
        self._tablenames = [t.name for t in self._metadata.sorted_tables]
        self.session = Session(self.connection, **self._session_kwargs)
        self.cache = DBCache(advance_cache_query)
        # class and entity type id
        self._object_class_type = None
        self._relationship_class_type = None
        self._object_entity_type = None
        self._relationship_entity_type = None
        # Subqueries that select everything from each table
        self._commit_sq = None
        self._alternative_sq = None
        self._scenario_sq = None
        self._scenario_alternative_sq = None
        self._entity_class_sq = None
        self._entity_sq = None
        self._entity_class_type_sq = None
        self._entity_type_sq = None
        self._object_class_sq = None
        self._object_sq = None
        self._relationship_class_sq = None
        self._relationship_sq = None
        self._entity_group_sq = None
        self._parameter_definition_sq = None
        self._parameter_value_sq = None
        self._parameter_value_list_sq = None
        self._list_value_sq = None
        self._feature_sq = None
        self._tool_sq = None
        self._tool_feature_sq = None
        self._tool_feature_method_sq = None
        self._metadata_sq = None
        self._parameter_value_metadata_sq = None
        self._entity_metadata_sq = None
        self._clean_parameter_value_sq = None
        # Special convenience subqueries that join two or more tables
        self._ext_parameter_value_list_sq = None
        self._wide_parameter_value_list_sq = None
        self._ord_list_value_sq = None
        self._ext_scenario_sq = None
        self._wide_scenario_sq = None
        self._linked_scenario_alternative_sq = None
        self._ext_linked_scenario_alternative_sq = None
        self._ext_object_sq = None
        self._ext_relationship_class_sq = None
        self._wide_relationship_class_sq = None
        self._ext_relationship_class_object_parameter_definition_sq = None
        self._wide_relationship_class_object_parameter_definition_sq = None
        self._ext_relationship_sq = None
        self._wide_relationship_sq = None
        self._ext_entity_group_sq = None
        self._entity_parameter_definition_sq = None
        self._object_parameter_definition_sq = None
        self._relationship_parameter_definition_sq = None
        self._entity_parameter_value_sq = None
        self._object_parameter_value_sq = None
        self._relationship_parameter_value_sq = None
        self._ext_feature_sq = None
        self._ext_tool_feature_sq = None
        self._ext_tool_feature_method_sq = None
        self._ext_parameter_value_metadata_sq = None
        self._ext_entity_metadata_sq = None
        # Import alternative suff
        self._import_alternative_id = None
        self._import_alternative_name = None
        self._table_to_sq_attr = {}
        # Table primary ids map:
        self.table_ids = {
            "relationship_entity_class": "entity_class_id",
            "object_class": "entity_class_id",
            "relationship_class": "entity_class_id",
            "object": "entity_id",
            "relationship": "entity_id",
            "relationship_entity": "entity_id",
        }
        self.composite_pks = {
            "relationship_entity": ("entity_id", "dimension"),
            "relationship_entity_class": ("entity_class_id", "dimension"),
        }
        # Subqueries used to populate cache
        self.cache_sqs = {
            "entity": "entity_sq",
            "feature": "feature_sq",
            "tool": "tool_sq",
            "tool_feature": "tool_feature_sq",
            "tool_feature_method": "tool_feature_method_sq",
            "parameter_value_list": "parameter_value_list_sq",
            "list_value": "list_value_sq",
            "alternative": "alternative_sq",
            "scenario": "scenario_sq",
            "scenario_alternative": "scenario_alternative_sq",
            "object_class": "object_class_sq",
            "object": "object_sq",
            "relationship_class": "wide_relationship_class_sq",
            "relationship": "wide_relationship_sq",
            "entity_group": "entity_group_sq",
            "parameter_definition": "parameter_definition_sq",
            "parameter_value": "clean_parameter_value_sq",
            "metadata": "metadata_sq",
            "entity_metadata": "ext_entity_metadata_sq",
            "parameter_value_metadata": "ext_parameter_value_metadata_sq",
            "commit": "commit_sq",
        }
        self.ancestor_tablenames = {
            "feature": ("parameter_definition",),
            "tool_feature": ("tool", "feature"),
            "tool_feature_method": ("tool_feature", "parameter_value_list", "list_value"),
            "scenario_alternative": ("scenario", "alternative"),
            "relationship_class": ("object_class",),
            "object": ("object_class",),
            "entity_group": ("object_class", "relationship_class", "object", "relationship"),
            "relationship": ("relationship_class", "object"),
            "parameter_definition": ("object_class", "relationship_class", "parameter_value_list", "list_value"),
            "parameter_value": (
                "alternative",
                "object_class",
                "relationship_class",
                "object",
                "relationship",
                "parameter_definition",
                "parameter_value_list",
                "list_value",
            ),
            "entity_metadata": ("metadata", "object", "object_class", "relationship", "relationship_class"),
            "parameter_value_metadata": (
                "metadata",
                "parameter_value",
                "parameter_definition",
                "object",
                "object_class",
                "relationship",
                "relationship_class",
                "alternative",
            ),
            "list_value": ("parameter_value_list",),
        }
        self.descendant_tablenames = {
            tablename: set(self._descendant_tablenames(tablename)) for tablename in self.cache_sqs
        }
        self.object_class_type = None
        self.relationship_class_type = None
        self.object_entity_type = None
        self.relationship_entity_type = None

    def _init_type_attributes(self):
        self.object_class_type = (
            self.query(self.entity_class_type_sq).filter(self.entity_class_type_sq.c.name == "object").first().id
        )
        self.relationship_class_type = (
            self.query(self.entity_class_type_sq).filter(self.entity_class_type_sq.c.name == "relationship").first().id
        )
        self.object_entity_type = (
            self.query(self.entity_type_sq).filter(self.entity_type_sq.c.name == "object").first().id
        )
        self.relationship_entity_type = (
            self.query(self.entity_type_sq).filter(self.entity_type_sq.c.name == "relationship").first().id
        )

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc_val, _exc_tb):
        self.connection.close()

    @contextmanager
    def override_committing(self, new_committing):
        committing = self.committing
        self.committing = new_committing
        try:
            yield None
        finally:
            self.committing = committing

    def _descendant_tablenames(self, tablename):
        child_tablenames = {
            "alternative": ("parameter_value", "scenario_alternative"),
            "scenario": ("scenario_alternative",),
            "object_class": ("object", "relationship_class", "parameter_definition"),
            "object": ("relationship", "parameter_value", "entity_group", "entity_metadata"),
            "relationship_class": ("relationship", "parameter_definition"),
            "relationship": ("parameter_value", "entity_group", "entity_metadata"),
            "parameter_definition": ("parameter_value", "feature"),
            "parameter_value_list": ("feature",),
            "parameter_value": ("parameter_value_metadata", "entity_metadata"),
            "feature": ("tool_feature",),
            "tool": ("tool_feature",),
            "tool_feature": ("tool_feature_method",),
            "entity_metadata": ("metadata",),
            "parameter_value_metadata": ("metadata",),
        }
        for parent, children in child_tablenames.items():
            if tablename == parent:
                for child in children:
                    yield child
                    yield from self._descendant_tablenames(child)

    def sorted_tablenames(self):
        tablenames = list(self.ITEM_TYPES)
        sorted_tablenames = []
        while tablenames:
            tablename = tablenames.pop(0)
            ancestors = self.ancestor_tablenames.get(tablename)
            if ancestors is None or all(x in sorted_tablenames for x in ancestors):
                sorted_tablenames.append(tablename)
            else:
                tablenames.append(tablename)
        return sorted_tablenames

    def commit_id(self):
        return self._commit_id

    def _make_commit_id(self):
        return None

    def _check_commit(self, comment):
        """Raises if commit not possible.

        Args:
            comment (str): commit message
        """
        if not self.has_pending_changes():
            raise SpineDBAPIError("Nothing to commit.")
        if not comment:
            raise SpineDBAPIError("Commit message cannot be empty.")

    def _make_codename(self, codename):
        if codename:
            return str(codename)
        if not self.sa_url.drivername.startswith("sqlite"):
            return self.sa_url.database
        if self.sa_url.database is not None:
            return os.path.basename(self.sa_url.database)
        hashing = hashlib.sha1()
        hashing.update(bytes(str(time.time()), "utf-8"))
        return hashing.hexdigest()

    @staticmethod
    def create_engine(sa_url, upgrade=False, create=False, sqlite_timeout=1800):
        """Create engine.

        Args
            sa_url (URL)
            upgrade (bool, optional): If True, upgrade the db to the latest version.
            create (bool, optional): If True, create a new Spine db at the given url if none found.

        Returns
            Engine
        """
        if sa_url.drivername == "sqlite":
            connect_args = {'timeout': sqlite_timeout}
        else:
            connect_args = {}
        try:
            engine = create_engine(sa_url, connect_args=connect_args)
            with engine.connect():
                pass
        except Exception as e:
            raise SpineDBAPIError(
                f"Could not connect to '{sa_url}': {str(e)}. "
                f"Please make sure that '{sa_url}' is a valid sqlalchemy URL."
            ) from None
        config = Config()
        config.set_main_option("script_location", "spinedb_api:alembic")
        script = ScriptDirectory.from_config(config)
        head = script.get_current_head()
        with engine.connect() as connection:
            migration_context = MigrationContext.configure(connection)
            try:
                current = migration_context.get_current_revision()
            except DatabaseError as error:
                raise SpineDBAPIError(str(error)) from None
            if current is None:
                # No revision information. Check that the schema of the given url corresponds to a 'first' Spine db
                # Otherwise we either raise or create a new Spine db at the url.
                ref_engine = _create_first_spine_database("sqlite://")
                if not compare_schemas(engine, ref_engine):
                    if not create or inspect(engine).get_table_names():
                        raise SpineDBAPIError(
                            "Unable to determine db revision. "
                            f"Please check that\n\n\t{sa_url}\n\nis the URL of a valid Spine db."
                        )
                    return create_new_spine_database(sa_url)
            if current != head:
                if not upgrade:
                    try:
                        script.get_revision(current)  # Check if current revision is part of alembic rev. history
                    except CommandError:
                        # Can't find 'current' revision
                        raise SpineDBVersionError(
                            url=sa_url, current=current, expected=head, upgrade_available=False
                        ) from None
                    raise SpineDBVersionError(url=sa_url, current=current, expected=head)

                # Upgrade function
                def upgrade_to_head(rev, context):
                    return script._upgrade_revs("head", rev)

                with EnvironmentContext(
                    config,
                    script,
                    fn=upgrade_to_head,
                    as_sql=False,
                    starting_rev=None,
                    destination_rev="head",
                    tag=None,
                ) as environment_context:
                    environment_context.configure(connection=connection, target_metadata=model_meta)
                    with environment_context.begin_transaction():
                        environment_context.run_migrations()
        return engine

    def _receive_engine_close(self, dbapi_con, _connection_record):
        if dbapi_con == self.connection.connection.connection and self._memory_dirty:
            copy_database_bind(self._original_engine, self.connection)

    def reconnect(self):
        self.connection = self.engine.connect()

    def in_(self, column, values):
        """Returns an expression equivalent to column.in_(values), that circumvents the
        'too many sql variables' problem in sqlite."""
        if not values:
            return false()
        if not self.sa_url.drivername.startswith("sqlite"):
            return column.in_(values)
        in_value = Table(
            "in_value_" + str(uuid.uuid4()),
            MetaData(),
            Column("value", column.type, primary_key=True),
            prefixes=['TEMPORARY'],
        )
        in_value.create(self.connection, checkfirst=True)
        python_type = column.type.python_type
        self._checked_execute(in_value.insert(), [{"value": python_type(val)} for val in set(values)])
        return column.in_(self.query(in_value.c.value))

    def _get_table_to_sq_attr(self):
        if not self._table_to_sq_attr:
            self._table_to_sq_attr = self._make_table_to_sq_attr()
        return self._table_to_sq_attr

    def _make_table_to_sq_attr(self):
        """Returns a dict mapping table names to subquery attribute names, involving that table."""
        # This 'loads' our subquery attributes
        for attr in dir(self):
            getattr(self, attr)
        table_to_sq_attr = {}
        for attr, val in vars(self).items():
            if not isinstance(val, Alias):
                continue
            tables = set()

            def _func(x):
                if isinstance(x, Table):
                    tables.add(x.name)  # pylint: disable=cell-var-from-loop

            forward_sweep(val, _func)
            # Now `tables` contains all tables related to `val`
            for table in tables:
                table_to_sq_attr.setdefault(table, set()).add(attr)
        return table_to_sq_attr

    def _clear_subqueries(self, *tablenames):
        """Set to `None` subquery attributes involving the affected tables.
        This forces the subqueries to be refreshed when the corresponding property is accessed.
        """
        tablenames = list(tablenames)
        for tablename in tablenames:
            if self.cache.pop(tablename, None):
                self._do_advance_cache_query(tablename)
        attr_names = set(attr for tablename in tablenames for attr in self._get_table_to_sq_attr().get(tablename, []))
        for attr_name in attr_names:
            setattr(self, attr_name, None)

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

            SELECT * FROM tablename

        Args:
            tablename (str): the table to be queried.

        Returns:
            sqlalchemy.sql.expression.Alias
        """
        table = self._metadata.tables[tablename]
        return self.query(table).subquery()

    @property
    def alternative_sq(self):
        if self._alternative_sq is None:
            self._alternative_sq = self._make_alternative_sq()
        return self._alternative_sq

    @property
    def scenario_sq(self):
        if self._scenario_sq is None:
            self._scenario_sq = self._make_scenario_sq()
        return self._scenario_sq

    @property
    def scenario_alternative_sq(self):
        if self._scenario_alternative_sq is None:
            self._scenario_alternative_sq = self._make_scenario_alternative_sq()
        return self._scenario_alternative_sq

    @property
    def entity_class_type_sq(self):
        """A subquery of the form:

        .. code-block:: sql

            SELECT * FROM class_type

        Returns:
            sqlalchemy.sql.expression.Alias
        """
        if self._entity_class_type_sq is None:
            self._entity_class_type_sq = self._subquery("entity_class_type")
        return self._entity_class_type_sq

    @property
    def entity_type_sq(self):
        """A subquery of the form:

        .. code-block:: sql

            SELECT * FROM class_type

        Returns:
            sqlalchemy.sql.expression.Alias
        """
        if self._entity_type_sq is None:
            self._entity_type_sq = self._subquery("entity_type")
        return self._entity_type_sq

    @property
    def entity_class_sq(self):
        """A subquery of the form:

        .. code-block:: sql

            SELECT * FROM class

        Returns:
            sqlalchemy.sql.expression.Alias
        """
        if self._entity_class_sq is None:
            self._entity_class_sq = self._make_entity_class_sq()
        return self._entity_class_sq

    @property
    def entity_sq(self):
        """A subquery of the form:

        .. code-block:: sql

            SELECT * FROM entity

        Returns:
            sqlalchemy.sql.expression.Alias
        """
        if self._entity_sq is None:
            self._entity_sq = self._make_entity_sq()
        return self._entity_sq

    @property
    def object_class_sq(self):
        """A subquery of the form:

        .. code-block:: sql

            SELECT * FROM object_class

        Returns:
            sqlalchemy.sql.expression.Alias
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

        Returns:
            sqlalchemy.sql.expression.Alias
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

        Returns:
            sqlalchemy.sql.expression.Alias
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
                    self.entity_class_sq.c.display_icon.label("display_icon"),
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

        Returns:
            sqlalchemy.sql.expression.Alias
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
    def entity_group_sq(self):
        """A subquery of the form:

        .. code-block:: sql

            SELECT * FROM entity_group

        Returns:
            sqlalchemy.sql.expression.Alias
        """
        if self._entity_group_sq is None:
            group_entity = aliased(self.entity_sq)
            member_entity = aliased(self.entity_sq)
            entity_group_sq = self._subquery("entity_group")
            self._entity_group_sq = (
                self.query(
                    entity_group_sq.c.id,
                    entity_group_sq.c.entity_class_id,
                    group_entity.c.id.label("entity_id"),
                    member_entity.c.id.label("member_id"),
                )
                .join(group_entity, group_entity.c.id == entity_group_sq.c.entity_id)
                .join(member_entity, member_entity.c.id == entity_group_sq.c.member_id)
                .subquery()
            )
        return self._entity_group_sq

    @property
    def parameter_definition_sq(self):
        """A subquery of the form:

        .. code-block:: sql

            SELECT * FROM parameter_definition

        Returns:
            sqlalchemy.sql.expression.Alias
        """

        if self._parameter_definition_sq is None:
            self._parameter_definition_sq = self._make_parameter_definition_sq()
        return self._parameter_definition_sq

    @property
    def parameter_value_sq(self):
        """A subquery of the form:

        .. code-block:: sql

            SELECT * FROM parameter_value

        Returns:
            sqlalchemy.sql.expression.Alias
        """
        if self._parameter_value_sq is None:
            self._parameter_value_sq = self._make_parameter_value_sq()
        return self._parameter_value_sq

    @property
    def clean_parameter_value_sq(self):
        """A subquery of the parameter_value table that excludes rows with filtered entities.
        This yields the correct results whenever there are both a scenario filter that filters some parameter values,
        and a tool filter that then filters some entities based on the value of some their parameters
        after the scenario filtering. Mildly insane.
        """
        if self._clean_parameter_value_sq is None:
            self._clean_parameter_value_sq = (
                self.query(self.parameter_value_sq)
                .join(self.entity_sq, self.entity_sq.c.id == self.parameter_value_sq.c.entity_id)
                .subquery()
            )
        return self._clean_parameter_value_sq

    @property
    def parameter_value_list_sq(self):
        """A subquery of the form:

        .. code-block:: sql

            SELECT * FROM parameter_value_list

        Returns:
            sqlalchemy.sql.expression.Alias
        """
        if self._parameter_value_list_sq is None:
            self._parameter_value_list_sq = self._subquery("parameter_value_list")
        return self._parameter_value_list_sq

    @property
    def list_value_sq(self):
        if self._list_value_sq is None:
            self._list_value_sq = self._subquery("list_value")
        return self._list_value_sq

    @property
    def feature_sq(self):
        if self._feature_sq is None:
            self._feature_sq = self._subquery("feature")
        return self._feature_sq

    @property
    def tool_sq(self):
        if self._tool_sq is None:
            self._tool_sq = self._subquery("tool")
        return self._tool_sq

    @property
    def tool_feature_sq(self):
        if self._tool_feature_sq is None:
            self._tool_feature_sq = self._subquery("tool_feature")
        return self._tool_feature_sq

    @property
    def tool_feature_method_sq(self):
        if self._tool_feature_method_sq is None:
            self._tool_feature_method_sq = self._subquery("tool_feature_method")
        return self._tool_feature_method_sq

    @property
    def metadata_sq(self):
        if self._metadata_sq is None:
            self._metadata_sq = self._subquery("metadata")
        return self._metadata_sq

    @property
    def parameter_value_metadata_sq(self):
        if self._parameter_value_metadata_sq is None:
            self._parameter_value_metadata_sq = self._subquery("parameter_value_metadata")
        return self._parameter_value_metadata_sq

    @property
    def entity_metadata_sq(self):
        if self._entity_metadata_sq is None:
            self._entity_metadata_sq = self._subquery("entity_metadata")
        return self._entity_metadata_sq

    @property
    def commit_sq(self):
        if self._commit_sq is None:
            commit_sq = self._subquery("commit")
            self._commit_sq = self.query(commit_sq).filter(commit_sq.c.comment != "").subquery()
        return self._commit_sq

    @property
    def ext_parameter_value_list_sq(self):
        if self._ext_parameter_value_list_sq is None:
            self._ext_parameter_value_list_sq = (
                self.query(
                    self.parameter_value_list_sq.c.id,
                    self.parameter_value_list_sq.c.name,
                    self.parameter_value_list_sq.c.commit_id,
                    self.list_value_sq.c.id.label("value_id"),
                    self.list_value_sq.c.index.label("value_index"),
                ).outerjoin(
                    self.list_value_sq,
                    self.list_value_sq.c.parameter_value_list_id == self.parameter_value_list_sq.c.id,
                )
            ).subquery()
        return self._ext_parameter_value_list_sq

    @property
    def wide_parameter_value_list_sq(self):
        if self._wide_parameter_value_list_sq is None:
            self._wide_parameter_value_list_sq = (
                self.query(
                    self.ext_parameter_value_list_sq.c.id,
                    self.ext_parameter_value_list_sq.c.name,
                    self.ext_parameter_value_list_sq.c.commit_id,
                    group_concat(
                        self.ext_parameter_value_list_sq.c.value_id, self.ext_parameter_value_list_sq.c.value_index
                    ).label("value_id_list"),
                    group_concat(
                        self.ext_parameter_value_list_sq.c.value_index, self.ext_parameter_value_list_sq.c.value_index
                    ).label("value_index_list"),
                ).group_by(
                    self.ext_parameter_value_list_sq.c.id,
                    self.ext_parameter_value_list_sq.c.name,
                    self.ext_parameter_value_list_sq.c.commit_id,
                )
            ).subquery()
        return self._wide_parameter_value_list_sq

    @property
    def ord_list_value_sq(self):
        if self._ord_list_value_sq is None:
            self._ord_list_value_sq = (
                self.query(
                    self.list_value_sq.c.id,
                    self.list_value_sq.c.parameter_value_list_id,
                    self.list_value_sq.c.index,
                    self.list_value_sq.c.value,
                    self.list_value_sq.c.type,
                    self.list_value_sq.c.commit_id,
                )
                .order_by(self.list_value_sq.c.parameter_value_list_id, self.list_value_sq.c.index)
                .subquery()
            )
        return self._ord_list_value_sq

    @property
    def ext_scenario_sq(self):
        if self._ext_scenario_sq is None:
            self._ext_scenario_sq = (
                self.query(
                    self.scenario_sq.c.id.label("id"),
                    self.scenario_sq.c.name.label("name"),
                    self.scenario_sq.c.description.label("description"),
                    self.scenario_sq.c.active.label("active"),
                    self.scenario_alternative_sq.c.alternative_id.label("alternative_id"),
                    self.scenario_alternative_sq.c.rank.label("rank"),
                    self.alternative_sq.c.name.label("alternative_name"),
                    self.scenario_sq.c.commit_id.label("commit_id"),
                )
                .outerjoin(
                    self.scenario_alternative_sq, self.scenario_alternative_sq.c.scenario_id == self.scenario_sq.c.id
                )
                .outerjoin(
                    self.alternative_sq, self.alternative_sq.c.id == self.scenario_alternative_sq.c.alternative_id
                )
                .order_by(self.scenario_sq.c.id, self.scenario_alternative_sq.c.rank)
                .subquery()
            )
        return self._ext_scenario_sq

    @property
    def wide_scenario_sq(self):
        if self._wide_scenario_sq is None:
            self._wide_scenario_sq = (
                self.query(
                    self.ext_scenario_sq.c.id.label("id"),
                    self.ext_scenario_sq.c.name.label("name"),
                    self.ext_scenario_sq.c.description.label("description"),
                    self.ext_scenario_sq.c.active.label("active"),
                    self.ext_scenario_sq.c.commit_id.label("commit_id"),
                    group_concat(self.ext_scenario_sq.c.alternative_id, self.ext_scenario_sq.c.rank).label(
                        "alternative_id_list"
                    ),
                    group_concat(self.ext_scenario_sq.c.alternative_name, self.ext_scenario_sq.c.rank).label(
                        "alternative_name_list"
                    ),
                )
                .group_by(
                    self.ext_scenario_sq.c.id,
                    self.ext_scenario_sq.c.name,
                    self.ext_scenario_sq.c.description,
                    self.ext_scenario_sq.c.active,
                    self.ext_scenario_sq.c.commit_id,
                )
                .subquery()
            )
        return self._wide_scenario_sq

    @property
    def linked_scenario_alternative_sq(self):
        if self._linked_scenario_alternative_sq is None:
            scenario_next_alternative = aliased(self.scenario_alternative_sq)
            self._linked_scenario_alternative_sq = (
                self.query(
                    self.scenario_alternative_sq.c.id.label("id"),
                    self.scenario_alternative_sq.c.scenario_id.label("scenario_id"),
                    self.scenario_alternative_sq.c.alternative_id.label("alternative_id"),
                    self.scenario_alternative_sq.c.rank.label("rank"),
                    scenario_next_alternative.c.alternative_id.label("before_alternative_id"),
                    scenario_next_alternative.c.rank.label("before_rank"),
                    self.scenario_alternative_sq.c.commit_id.label("commit_id"),
                )
                .outerjoin(
                    scenario_next_alternative,
                    and_(
                        scenario_next_alternative.c.scenario_id == self.scenario_alternative_sq.c.scenario_id,
                        scenario_next_alternative.c.rank == self.scenario_alternative_sq.c.rank + 1,
                    ),
                )
                .order_by(self.scenario_alternative_sq.c.scenario_id, self.scenario_alternative_sq.c.rank)
                .subquery()
            )
        return self._linked_scenario_alternative_sq

    @property
    def ext_linked_scenario_alternative_sq(self):
        if self._ext_linked_scenario_alternative_sq is None:
            next_alternative = aliased(self.alternative_sq)
            self._ext_linked_scenario_alternative_sq = (
                self.query(
                    self.linked_scenario_alternative_sq.c.id.label("id"),
                    self.linked_scenario_alternative_sq.c.scenario_id.label("scenario_id"),
                    self.scenario_sq.c.name.label("scenario_name"),
                    self.linked_scenario_alternative_sq.c.alternative_id.label("alternative_id"),
                    self.alternative_sq.c.name.label("alternative_name"),
                    self.linked_scenario_alternative_sq.c.rank.label("rank"),
                    self.linked_scenario_alternative_sq.c.before_alternative_id.label("before_alternative_id"),
                    self.linked_scenario_alternative_sq.c.before_rank.label("before_rank"),
                    next_alternative.c.name.label("before_alternative_name"),
                    self.linked_scenario_alternative_sq.c.commit_id.label("commit_id"),
                )
                .filter(self.linked_scenario_alternative_sq.c.scenario_id == self.scenario_sq.c.id)
                .filter(self.alternative_sq.c.id == self.linked_scenario_alternative_sq.c.alternative_id)
                .outerjoin(
                    next_alternative,
                    next_alternative.c.id == self.linked_scenario_alternative_sq.c.before_alternative_id,
                )
                .subquery()
            )
        return self._ext_linked_scenario_alternative_sq

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

        Returns:
            sqlalchemy.sql.expression.Alias
        """
        if self._ext_object_sq is None:
            self._ext_object_sq = (
                self.query(
                    self.object_sq.c.id.label("id"),
                    self.object_sq.c.class_id.label("class_id"),
                    self.object_class_sq.c.name.label("class_name"),
                    self.object_sq.c.name.label("name"),
                    self.object_sq.c.description.label("description"),
                    self.entity_group_sq.c.entity_id.label("group_id"),
                    self.object_sq.c.commit_id.label("commit_id"),
                )
                .filter(self.object_sq.c.class_id == self.object_class_sq.c.id)
                .outerjoin(self.entity_group_sq, self.entity_group_sq.c.entity_id == self.object_sq.c.id)
                .distinct(self.entity_group_sq.c.entity_id)
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

        Returns:
            sqlalchemy.sql.expression.Alias
        """
        if self._ext_relationship_class_sq is None:
            self._ext_relationship_class_sq = (
                self.query(
                    self.relationship_class_sq.c.id.label("id"),
                    self.relationship_class_sq.c.name.label("name"),
                    self.relationship_class_sq.c.description.label("description"),
                    self.relationship_class_sq.c.dimension.label("dimension"),
                    self.relationship_class_sq.c.display_icon.label("display_icon"),
                    self.object_class_sq.c.id.label("object_class_id"),
                    self.object_class_sq.c.name.label("object_class_name"),
                    self.relationship_class_sq.c.commit_id.label("commit_id"),
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

        Returns:
            sqlalchemy.sql.expression.Alias
        """
        if self._wide_relationship_class_sq is None:
            self._wide_relationship_class_sq = (
                self.query(
                    self.ext_relationship_class_sq.c.id,
                    self.ext_relationship_class_sq.c.name,
                    self.ext_relationship_class_sq.c.description,
                    self.ext_relationship_class_sq.c.display_icon,
                    self.ext_relationship_class_sq.c.commit_id,
                    group_concat(
                        self.ext_relationship_class_sq.c.object_class_id, self.ext_relationship_class_sq.c.dimension
                    ).label("object_class_id_list"),
                    group_concat(
                        self.ext_relationship_class_sq.c.object_class_name, self.ext_relationship_class_sq.c.dimension
                    ).label("object_class_name_list"),
                )
                .group_by(
                    self.ext_relationship_class_sq.c.id,
                    self.ext_relationship_class_sq.c.name,
                    self.ext_relationship_class_sq.c.description,
                    self.ext_relationship_class_sq.c.display_icon,
                    self.ext_relationship_class_sq.c.commit_id,
                )
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

        Returns:
            sqlalchemy.sql.expression.Alias
        """
        if self._ext_relationship_sq is None:
            self._ext_relationship_sq = (
                self.query(
                    self.relationship_sq.c.id.label("id"),
                    self.relationship_sq.c.name.label("name"),
                    self.relationship_sq.c.class_id.label("class_id"),
                    self.relationship_sq.c.dimension.label("dimension"),
                    self.wide_relationship_class_sq.c.name.label("class_name"),
                    self.ext_object_sq.c.id.label("object_id"),
                    self.ext_object_sq.c.name.label("object_name"),
                    self.ext_object_sq.c.class_id.label("object_class_id"),
                    self.ext_object_sq.c.class_name.label("object_class_name"),
                    self.relationship_sq.c.commit_id.label("commit_id"),
                )
                .filter(self.relationship_sq.c.class_id == self.wide_relationship_class_sq.c.id)
                .outerjoin(self.ext_object_sq, self.relationship_sq.c.object_id == self.ext_object_sq.c.id)
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

        Returns:
            sqlalchemy.sql.expression.Alias
        """
        if self._wide_relationship_sq is None:
            self._wide_relationship_sq = (
                self.query(
                    self.ext_relationship_sq.c.id,
                    self.ext_relationship_sq.c.name,
                    self.ext_relationship_sq.c.class_id,
                    self.ext_relationship_sq.c.class_name,
                    self.ext_relationship_sq.c.commit_id,
                    group_concat(self.ext_relationship_sq.c.object_id, self.ext_relationship_sq.c.dimension).label(
                        "object_id_list"
                    ),
                    group_concat(self.ext_relationship_sq.c.object_name, self.ext_relationship_sq.c.dimension).label(
                        "object_name_list"
                    ),
                    group_concat(
                        self.ext_relationship_sq.c.object_class_id, self.ext_relationship_sq.c.dimension
                    ).label("object_class_id_list"),
                    group_concat(
                        self.ext_relationship_sq.c.object_class_name, self.ext_relationship_sq.c.dimension
                    ).label("object_class_name_list"),
                )
                .group_by(
                    self.ext_relationship_sq.c.id,
                    self.ext_relationship_sq.c.name,
                    self.ext_relationship_sq.c.class_id,
                    self.ext_relationship_sq.c.class_name,
                    self.ext_relationship_sq.c.commit_id,
                )
                # dimension count might be higher than object count when objects have been filtered out
                .having(
                    func.count(self.ext_relationship_sq.c.dimension) == func.count(self.ext_relationship_sq.c.object_id)
                )
                .subquery()
            )
        return self._wide_relationship_sq

    @property
    def ext_entity_group_sq(self):
        """A subquery of the form:

        Returns:
            sqlalchemy.sql.expression.Alias
        """
        if self._ext_entity_group_sq is None:
            group_entity = aliased(self.entity_sq)
            member_entity = aliased(self.entity_sq)
            self._ext_entity_group_sq = (
                self.query(
                    self.entity_group_sq.c.id.label("id"),
                    self.entity_group_sq.c.entity_class_id.label("class_id"),
                    self.entity_group_sq.c.entity_id.label("group_id"),
                    self.entity_group_sq.c.member_id.label("member_id"),
                    self.entity_class_sq.c.name.label("class_name"),
                    group_entity.c.name.label("group_name"),
                    member_entity.c.name.label("member_name"),
                    label("object_class_id", self._object_class_id()),
                    label("relationship_class_id", self._relationship_class_id()),
                )
                .filter(self.entity_group_sq.c.entity_class_id == self.entity_class_sq.c.id)
                .join(group_entity, self.entity_group_sq.c.entity_id == group_entity.c.id)
                .join(member_entity, self.entity_group_sq.c.member_id == member_entity.c.id)
                .subquery()
            )
        return self._ext_entity_group_sq

    @property
    def entity_parameter_definition_sq(self):
        """
        Returns:
            sqlalchemy.sql.expression.Alias
        """
        if self._entity_parameter_definition_sq is None:
            self._entity_parameter_definition_sq = (
                self.query(
                    self.parameter_definition_sq.c.id.label("id"),
                    self.parameter_definition_sq.c.entity_class_id,
                    self.parameter_definition_sq.c.object_class_id,
                    self.parameter_definition_sq.c.relationship_class_id,
                    self.entity_class_sq.c.name.label("entity_class_name"),
                    label("object_class_name", self._object_class_name()),
                    label("relationship_class_name", self._relationship_class_name()),
                    label("object_class_id_list", self._object_class_id_list()),
                    label("object_class_name_list", self._object_class_name_list()),
                    self.parameter_definition_sq.c.name.label("parameter_name"),
                    self.parameter_definition_sq.c.parameter_value_list_id.label("value_list_id"),
                    self.parameter_value_list_sq.c.name.label("value_list_name"),
                    self.parameter_definition_sq.c.default_value,
                    self.parameter_definition_sq.c.default_type,
                    self.parameter_definition_sq.c.list_value_id,
                    self.parameter_definition_sq.c.description,
                    self.parameter_definition_sq.c.commit_id,
                )
                .join(self.entity_class_sq, self.entity_class_sq.c.id == self.parameter_definition_sq.c.entity_class_id)
                .outerjoin(
                    self.parameter_value_list_sq,
                    self.parameter_value_list_sq.c.id == self.parameter_definition_sq.c.parameter_value_list_id,
                )
                .outerjoin(
                    self.wide_relationship_class_sq, self.wide_relationship_class_sq.c.id == self.entity_class_sq.c.id
                )
                .subquery()
            )
        return self._entity_parameter_definition_sq

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
                pd.default_value
            FROM parameter_definition AS pd, object_class AS oc
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

        Returns:
            sqlalchemy.sql.expression.Alias
        """
        if self._object_parameter_definition_sq is None:
            self._object_parameter_definition_sq = (
                self.query(
                    self.parameter_definition_sq.c.id.label("id"),
                    self.parameter_definition_sq.c.entity_class_id,
                    self.object_class_sq.c.name.label("entity_class_name"),
                    self.object_class_sq.c.id.label("object_class_id"),
                    self.object_class_sq.c.name.label("object_class_name"),
                    self.parameter_definition_sq.c.name.label("parameter_name"),
                    self.parameter_definition_sq.c.parameter_value_list_id.label("value_list_id"),
                    self.parameter_value_list_sq.c.name.label("value_list_name"),
                    self.parameter_definition_sq.c.default_value,
                    self.parameter_definition_sq.c.default_type,
                    self.parameter_definition_sq.c.description,
                )
                .filter(self.object_class_sq.c.id == self.parameter_definition_sq.c.object_class_id)
                .outerjoin(
                    self.parameter_value_list_sq,
                    self.parameter_value_list_sq.c.id == self.parameter_definition_sq.c.parameter_value_list_id,
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

        Returns:
            sqlalchemy.sql.expression.Alias
        """
        if self._relationship_parameter_definition_sq is None:
            self._relationship_parameter_definition_sq = (
                self.query(
                    self.parameter_definition_sq.c.id.label("id"),
                    self.parameter_definition_sq.c.entity_class_id,
                    self.wide_relationship_class_sq.c.name.label("entity_class_name"),
                    self.wide_relationship_class_sq.c.id.label("relationship_class_id"),
                    self.wide_relationship_class_sq.c.name.label("relationship_class_name"),
                    self.wide_relationship_class_sq.c.object_class_id_list,
                    self.wide_relationship_class_sq.c.object_class_name_list,
                    self.parameter_definition_sq.c.name.label("parameter_name"),
                    self.parameter_definition_sq.c.parameter_value_list_id.label("value_list_id"),
                    self.parameter_value_list_sq.c.name.label("value_list_name"),
                    self.parameter_definition_sq.c.default_value,
                    self.parameter_definition_sq.c.default_type,
                    self.parameter_definition_sq.c.description,
                )
                .filter(self.parameter_definition_sq.c.relationship_class_id == self.wide_relationship_class_sq.c.id)
                .outerjoin(
                    self.parameter_value_list_sq,
                    self.parameter_value_list_sq.c.id == self.parameter_definition_sq.c.parameter_value_list_id,
                )
                .subquery()
            )
        return self._relationship_parameter_definition_sq

    @property
    def entity_parameter_value_sq(self):
        """
        Returns:
            sqlalchemy.sql.expression.Alias
        """
        if self._entity_parameter_value_sq is None:
            self._entity_parameter_value_sq = (
                self.query(
                    self.parameter_value_sq.c.id.label("id"),
                    self.parameter_definition_sq.c.entity_class_id,
                    self.parameter_definition_sq.c.object_class_id,
                    self.parameter_definition_sq.c.relationship_class_id,
                    self.entity_class_sq.c.name.label("entity_class_name"),
                    label("object_class_name", self._object_class_name()),
                    label("relationship_class_name", self._relationship_class_name()),
                    label("object_class_id_list", self._object_class_id_list()),
                    label("object_class_name_list", self._object_class_name_list()),
                    self.parameter_value_sq.c.entity_id,
                    self.entity_sq.c.name.label("entity_name"),
                    self.parameter_value_sq.c.object_id,
                    self.parameter_value_sq.c.relationship_id,
                    label("object_name", self._object_name()),
                    label("object_id_list", self._object_id_list()),
                    label("object_name_list", self._object_name_list()),
                    self.parameter_definition_sq.c.id.label("parameter_id"),
                    self.parameter_definition_sq.c.name.label("parameter_name"),
                    self.parameter_value_sq.c.alternative_id,
                    self.alternative_sq.c.name.label("alternative_name"),
                    self.parameter_value_sq.c.value,
                    self.parameter_value_sq.c.type,
                    self.parameter_value_sq.c.list_value_id,
                    self.parameter_value_sq.c.commit_id,
                )
                .join(
                    self.parameter_definition_sq,
                    self.parameter_definition_sq.c.id == self.parameter_value_sq.c.parameter_definition_id,
                )
                .join(self.entity_sq, self.parameter_value_sq.c.entity_id == self.entity_sq.c.id)
                .join(self.entity_class_sq, self.parameter_definition_sq.c.entity_class_id == self.entity_class_sq.c.id)
                .join(self.alternative_sq, self.parameter_value_sq.c.alternative_id == self.alternative_sq.c.id)
                .outerjoin(
                    self.wide_relationship_class_sq, self.wide_relationship_class_sq.c.id == self.entity_class_sq.c.id
                )
                .outerjoin(self.wide_relationship_sq, self.wide_relationship_sq.c.id == self.entity_sq.c.id)
                # object_id_list might be None when objects have been filtered out
                .filter(
                    or_(
                        self.parameter_value_sq.c.relationship_id.is_(None),
                        self.wide_relationship_sq.c.object_id_list.isnot(None),
                    )
                )
                .subquery()
            )
        return self._entity_parameter_value_sq

    @property
    def object_parameter_value_sq(self):
        """A subquery of the form:

        Returns:
            sqlalchemy.sql.expression.Alias
        """
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
                    self.parameter_value_sq.c.alternative_id,
                    self.alternative_sq.c.name.label("alternative_name"),
                    self.parameter_value_sq.c.value,
                    self.parameter_value_sq.c.type,
                )
                .filter(self.parameter_definition_sq.c.id == self.parameter_value_sq.c.parameter_definition_id)
                .filter(self.parameter_value_sq.c.object_id == self.object_sq.c.id)
                .filter(self.parameter_definition_sq.c.object_class_id == self.object_class_sq.c.id)
                .filter(self.parameter_value_sq.c.alternative_id == self.alternative_sq.c.id)
                .subquery()
            )
        return self._object_parameter_value_sq

    @property
    def relationship_parameter_value_sq(self):
        """A subquery of the form:

        Returns:
            sqlalchemy.sql.expression.Alias
        """
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
                    self.parameter_value_sq.c.alternative_id,
                    self.alternative_sq.c.name.label("alternative_name"),
                    self.parameter_value_sq.c.value,
                    self.parameter_value_sq.c.type,
                )
                .filter(self.parameter_definition_sq.c.id == self.parameter_value_sq.c.parameter_definition_id)
                .filter(self.parameter_value_sq.c.relationship_id == self.wide_relationship_sq.c.id)
                .filter(self.parameter_definition_sq.c.relationship_class_id == self.wide_relationship_class_sq.c.id)
                .filter(self.parameter_value_sq.c.alternative_id == self.alternative_sq.c.id)
                .subquery()
            )
        return self._relationship_parameter_value_sq

    @property
    def ext_feature_sq(self):
        """
        Returns:
            sqlalchemy.sql.expression.Alias
        """
        if self._ext_feature_sq is None:
            self._ext_feature_sq = (
                self.query(
                    self.feature_sq.c.id.label("id"),
                    self.entity_class_sq.c.id.label("entity_class_id"),
                    self.entity_class_sq.c.name.label("entity_class_name"),
                    self.feature_sq.c.parameter_definition_id.label("parameter_definition_id"),
                    self.parameter_definition_sq.c.name.label("parameter_definition_name"),
                    self.parameter_value_list_sq.c.id.label("parameter_value_list_id"),
                    self.parameter_value_list_sq.c.name.label("parameter_value_list_name"),
                    self.feature_sq.c.description.label("description"),
                    self.feature_sq.c.commit_id.label("commit_id"),
                )
                .filter(self.feature_sq.c.parameter_definition_id == self.parameter_definition_sq.c.id)
                .filter(self.parameter_definition_sq.c.parameter_value_list_id == self.parameter_value_list_sq.c.id)
                .filter(self.parameter_definition_sq.c.entity_class_id == self.entity_class_sq.c.id)
                .subquery()
            )
        return self._ext_feature_sq

    @property
    def ext_tool_feature_sq(self):
        """
        Returns:
            sqlalchemy.sql.expression.Alias
        """
        if self._ext_tool_feature_sq is None:
            self._ext_tool_feature_sq = (
                self.query(
                    self.tool_feature_sq.c.id.label("id"),
                    self.tool_feature_sq.c.tool_id.label("tool_id"),
                    self.tool_sq.c.name.label("tool_name"),
                    self.tool_feature_sq.c.feature_id.label("feature_id"),
                    self.ext_feature_sq.c.entity_class_id.label("entity_class_id"),
                    self.ext_feature_sq.c.entity_class_name.label("entity_class_name"),
                    self.ext_feature_sq.c.parameter_definition_id.label("parameter_definition_id"),
                    self.ext_feature_sq.c.parameter_definition_name.label("parameter_definition_name"),
                    self.ext_feature_sq.c.parameter_value_list_id.label("parameter_value_list_id"),
                    self.ext_feature_sq.c.parameter_value_list_name.label("parameter_value_list_name"),
                    self.tool_feature_sq.c.required.label("required"),
                    self.tool_feature_sq.c.commit_id.label("commit_id"),
                )
                .filter(self.tool_feature_sq.c.tool_id == self.tool_sq.c.id)
                .filter(self.tool_feature_sq.c.feature_id == self.ext_feature_sq.c.id)
                .subquery()
            )
        return self._ext_tool_feature_sq

    @property
    def ext_tool_feature_method_sq(self):
        """
        Returns:
            sqlalchemy.sql.expression.Alias
        """
        if self._ext_tool_feature_method_sq is None:
            self._ext_tool_feature_method_sq = (
                self.query(
                    self.tool_feature_method_sq.c.id,
                    self.ext_tool_feature_sq.c.id.label("tool_feature_id"),
                    self.ext_tool_feature_sq.c.tool_id,
                    self.ext_tool_feature_sq.c.tool_name,
                    self.ext_tool_feature_sq.c.feature_id,
                    self.ext_tool_feature_sq.c.entity_class_id,
                    self.ext_tool_feature_sq.c.entity_class_name,
                    self.ext_tool_feature_sq.c.parameter_definition_id,
                    self.ext_tool_feature_sq.c.parameter_definition_name,
                    self.ext_tool_feature_sq.c.parameter_value_list_id,
                    self.ext_tool_feature_sq.c.parameter_value_list_name,
                    self.tool_feature_method_sq.c.method_index,
                    self.list_value_sq.c.value.label("method"),
                    self.tool_feature_method_sq.c.commit_id,
                )
                .filter(self.tool_feature_method_sq.c.tool_feature_id == self.ext_tool_feature_sq.c.id)
                .filter(self.ext_tool_feature_sq.c.parameter_value_list_id == self.parameter_value_list_sq.c.id)
                .filter(self.parameter_value_list_sq.c.id == self.list_value_sq.c.parameter_value_list_id)
                .filter(self.tool_feature_method_sq.c.method_index == self.list_value_sq.c.index)
                .subquery()
            )
        return self._ext_tool_feature_method_sq

    @property
    def ext_parameter_value_metadata_sq(self):
        """
        Returns:
            sqlalchemy.sql.expression.Alias
        """
        if self._ext_parameter_value_metadata_sq is None:
            self._ext_parameter_value_metadata_sq = (
                self.query(
                    self.parameter_value_metadata_sq.c.id,
                    self.parameter_value_metadata_sq.c.parameter_value_id,
                    self.metadata_sq.c.id.label("metadata_id"),
                    self.entity_sq.c.name.label("entity_name"),
                    self.parameter_definition_sq.c.name.label("parameter_name"),
                    self.alternative_sq.c.name.label("alternative_name"),
                    self.metadata_sq.c.name.label("metadata_name"),
                    self.metadata_sq.c.value.label("metadata_value"),
                    self.parameter_value_metadata_sq.c.commit_id,
                )
                .filter(self.parameter_value_metadata_sq.c.parameter_value_id == self.parameter_value_sq.c.id)
                .filter(self.parameter_value_sq.c.parameter_definition_id == self.parameter_definition_sq.c.id)
                .filter(self.parameter_value_sq.c.entity_id == self.entity_sq.c.id)
                .filter(self.parameter_value_sq.c.alternative_id == self.alternative_sq.c.id)
                .filter(self.parameter_value_metadata_sq.c.metadata_id == self.metadata_sq.c.id)
                .subquery()
            )
        return self._ext_parameter_value_metadata_sq

    @property
    def ext_entity_metadata_sq(self):
        """
        Returns:
            sqlalchemy.sql.expression.Alias
        """
        if self._ext_entity_metadata_sq is None:
            self._ext_entity_metadata_sq = (
                self.query(
                    self.entity_metadata_sq.c.id,
                    self.entity_metadata_sq.c.entity_id,
                    self.metadata_sq.c.id.label("metadata_id"),
                    self.entity_sq.c.name.label("entity_name"),
                    self.metadata_sq.c.name.label("metadata_name"),
                    self.metadata_sq.c.value.label("metadata_value"),
                    self.entity_metadata_sq.c.commit_id,
                )
                .filter(self.entity_metadata_sq.c.entity_id == self.entity_sq.c.id)
                .filter(self.entity_metadata_sq.c.metadata_id == self.metadata_sq.c.id)
                .subquery()
            )
        return self._ext_entity_metadata_sq

    def _make_entity_sq(self):
        """
        Creates a subquery for entities.

        Returns:
            Alias: an entity subquery
        """
        return self._subquery("entity")

    def _make_entity_class_sq(self):
        """
        Creates a subquery for entity classes.

        Returns:
            Alias: an entity class subquery
        """
        return self._subquery("entity_class")

    def _make_parameter_definition_sq(self):
        """
        Creates a subquery for parameter definitions.

        Returns:
            Alias: a parameter definition subquery
        """
        par_def_sq = self._subquery("parameter_definition")
        list_value_id = case(
            [(par_def_sq.c.default_type == "list_value_ref", cast(par_def_sq.c.default_value, Integer()))], else_=None
        )
        default_value = case(
            [(par_def_sq.c.default_type == "list_value_ref", self.list_value_sq.c.value)],
            else_=par_def_sq.c.default_value,
        )
        default_type = case(
            [(par_def_sq.c.default_type == "list_value_ref", self.list_value_sq.c.type)],
            else_=par_def_sq.c.default_type,
        )
        return (
            self.query(
                par_def_sq.c.id.label("id"),
                par_def_sq.c.name.label("name"),
                par_def_sq.c.description.label("description"),
                par_def_sq.c.entity_class_id,
                label("object_class_id", self._object_class_id()),
                label("relationship_class_id", self._relationship_class_id()),
                label("default_value", default_value),
                label("default_type", default_type),
                label("list_value_id", list_value_id),
                par_def_sq.c.commit_id.label("commit_id"),
                par_def_sq.c.parameter_value_list_id.label("parameter_value_list_id"),
            )
            .join(self.entity_class_sq, self.entity_class_sq.c.id == par_def_sq.c.entity_class_id)
            .outerjoin(self.list_value_sq, self.list_value_sq.c.id == list_value_id)
            .subquery()
        )

    def _make_parameter_value_sq(self):
        """
        Creates a subquery for parameter values.

        Returns:
            Alias: a parameter value subquery
        """
        par_val_sq = self._subquery("parameter_value")
        list_value_id = case([(par_val_sq.c.type == "list_value_ref", cast(par_val_sq.c.value, Integer()))], else_=None)
        value = case([(par_val_sq.c.type == "list_value_ref", self.list_value_sq.c.value)], else_=par_val_sq.c.value)
        type_ = case([(par_val_sq.c.type == "list_value_ref", self.list_value_sq.c.type)], else_=par_val_sq.c.type)
        return (
            self.query(
                par_val_sq.c.id.label("id"),
                par_val_sq.c.parameter_definition_id,
                par_val_sq.c.entity_class_id,
                par_val_sq.c.entity_id,
                label("object_class_id", self._object_class_id()),
                label("relationship_class_id", self._relationship_class_id()),
                label("object_id", self._object_id()),
                label("relationship_id", self._relationship_id()),
                label("value", value),
                label("type", type_),
                label("list_value_id", list_value_id),
                par_val_sq.c.commit_id.label("commit_id"),
                par_val_sq.c.alternative_id,
            )
            .join(self.entity_sq, self.entity_sq.c.id == par_val_sq.c.entity_id)
            .join(self.entity_class_sq, self.entity_class_sq.c.id == par_val_sq.c.entity_class_id)
            .outerjoin(self.list_value_sq, self.list_value_sq.c.id == list_value_id)
            .subquery()
        )

    def _make_alternative_sq(self):
        """
        Creates a subquery for alternatives.

        Returns:
            Alias: an alternative subquery
        """
        return self._subquery("alternative")

    def _make_scenario_sq(self):
        """
        Creates a subquery for scenarios.

        Returns:
            Alias: a scenario subquery
        """
        return self._subquery("scenario")

    def _make_scenario_alternative_sq(self):
        """
        Creates a subquery for scenario alternatives.

        Returns:
            Alias: a scenario alternative subquery
        """
        return self._subquery("scenario_alternative")

    def get_import_alternative(self, cache=None):
        """Returns the id of the alternative to use as default for all import operations.

        Returns:
            int, str
        """
        if self._import_alternative_id is None:
            self._create_import_alternative(cache=cache)
        return self._import_alternative_id, self._import_alternative_name

    def _create_import_alternative(self, cache=None):
        """Creates the alternative to be used as default for all import operations."""
        if "alternative" not in cache:
            cache = self.make_cache({"alternative"})
        self._import_alternative_name = "Base"
        self._import_alternative_id = next(
            (id_ for id_, alt in cache.get("alternative", {}).items() if alt.name == self._import_alternative_name),
            None,
        )
        if not self._import_alternative_id:
            ids = self._add_alternatives({"name": self._import_alternative_name})
            self._import_alternative_id = next(iter(ids))

    def override_entity_sq_maker(self, method):
        """
        Overrides the function that creates the ``entity_sq`` property.

        Args:
            method (Callable): a function that accepts a :class:`DatabaseMappingBase` as its argument and
                returns entity subquery as an :class:`Alias` object
        """
        self._make_entity_sq = MethodType(method, self)
        self._clear_subqueries("entity")

    def restore_entity_sq_maker(self):
        """Restores the original function that creates the ``entity_sq`` property."""
        self._make_entity_sq = MethodType(DatabaseMappingBase._make_entity_sq, self)
        self._clear_subqueries("entity")

    def override_entity_class_sq_maker(self, method):
        """
        Overrides the function that creates the ``entity_class_sq`` property.

        Args:
            method (Callable): a function that accepts a :class:`DatabaseMappingBase` as its argument and
                returns entity class subquery as an :class:`Alias` object
        """
        self._make_entity_class_sq = MethodType(method, self)
        self._clear_subqueries("entity_class")

    def restore_entity_class_sq_maker(self):
        """Restores the original function that creates the ``entity_class_sq`` property."""
        self._make_entity_class_sq = MethodType(DatabaseMappingBase._make_entity_class_sq, self)
        self._clear_subqueries("entity_class")

    def override_parameter_definition_sq_maker(self, method):
        """
        Overrides the function that creates the ``parameter_definition_sq`` property.

        Args:
            method (Callable): a function that accepts a :class:`DatabaseMappingBase` as its argument and
                returns parameter definition subquery as an :class:`Alias` object
        """
        self._make_parameter_definition_sq = MethodType(method, self)
        self._clear_subqueries("parameter_definition")

    def restore_parameter_definition_sq_maker(self):
        """Restores the original function that creates the ``parameter_definition_sq`` property."""
        self._make_parameter_definition_sq = MethodType(DatabaseMappingBase._make_parameter_definition_sq, self)
        self._clear_subqueries("parameter_definition")

    def override_parameter_value_sq_maker(self, method):
        """
        Overrides the function that creates the ``parameter_value_sq`` property.

        Args:
            method (Callable): a function that accepts a :class:`DatabaseMappingBase` as its argument and
                returns parameter value subquery as an :class:`Alias` object
        """
        self._make_parameter_value_sq = MethodType(method, self)
        self._clear_subqueries("parameter_value")

    def restore_parameter_value_sq_maker(self):
        """Restores the original function that creates the ``parameter_value_sq`` property."""
        self._make_parameter_value_sq = MethodType(DatabaseMappingBase._make_parameter_value_sq, self)
        self._clear_subqueries("parameter_value")

    def override_create_import_alternative(self, method):
        """
        Overrides the ``_create_import_alternative`` function.

        Args:
            method (Callable)
        """
        self._create_import_alternative = MethodType(method, self)
        self._import_alternative_id = None

    def override_alternative_sq_maker(self, method):
        """
        Overrides the function that creates the ``alternative_sq`` property.

        Args:
            method (Callable): a function that accepts a :class:`DatabaseMappingBase` as its argument and
                returns alternative subquery as an :class:`Alias` object
        """
        self._make_alternative_sq = MethodType(method, self)
        self._clear_subqueries("alternative")

    def restore_alternative_sq_maker(self):
        """Restores the original function that creates the ``alternative_sq`` property."""
        self._make_alternative_sq = MethodType(DatabaseMappingBase._make_alternative_sq, self)
        self._clear_subqueries("alternative")

    def override_scenario_sq_maker(self, method):
        """
        Overrides the function that creates the ``scenario_sq`` property.

        Args:
            method (Callable): a function that accepts a :class:`DatabaseMappingBase` as its argument and
                returns scenario subquery as an :class:`Alias` object
        """
        self._make_scenario_sq = MethodType(method, self)
        self._clear_subqueries("scenario")

    def restore_scenario_sq_maker(self):
        """Restores the original function that creates the ``scenario_sq`` property."""
        self._make_scenario_sq = MethodType(DatabaseMappingBase._make_scenario_sq, self)
        self._clear_subqueries("scenario")

    def override_scenario_alternative_sq_maker(self, method):
        """
        Overrides the function that creates the ``scenario_alternative_sq`` property.

        Args:
            method (Callable): a function that accepts a :class:`DatabaseMappingBase` as its argument and
                returns scenario alternative subquery as an :class:`Alias` object
        """
        self._make_scenario_alternative_sq = MethodType(method, self)
        self._clear_subqueries("scenario_alternative")

    def restore_scenario_alternative_sq_maker(self):
        """Restores the original function that creates the ``scenario_alternative_sq`` property."""
        self._make_scenario_alternative_sq = MethodType(DatabaseMappingBase._make_scenario_alternative_sq, self)
        self._clear_subqueries("scenario_alternative")

    def _checked_execute(self, stmt, items):
        if not items:
            return
        return self.connection.execute(stmt, items)

    def _get_primary_key(self, tablename):
        pk = self.composite_pks.get(tablename)
        if pk is None:
            table_id = self.table_ids.get(tablename, "id")
            pk = (table_id,)
        return pk

    def _reset_mapping(self):
        """Delete all records from all tables but don't drop the tables.
        Useful for writing tests
        """
        for tablename in self._tablenames:
            table = self._metadata.tables[tablename]
            self.connection.execute(table.delete())
        self.connection.execute("INSERT INTO alternative VALUES (1, 'Base', 'Base alternative', null)")

    def make_cache(
        self, tablenames, include_descendants=False, include_ancestors=False, force_tablenames=None, keep_existing=False
    ):
        if include_descendants:
            tablenames |= {
                descendant for tablename in tablenames for descendant in self.descendant_tablenames.get(tablename, ())
            }
        if include_ancestors:
            tablenames |= {
                ancestor for tablename in tablenames for ancestor in self.ancestor_tablenames.get(tablename, ())
            }
        if force_tablenames:
            tablenames |= force_tablenames
        for tablename in tablenames & self.cache_sqs.keys():
            self._do_advance_cache_query(tablename, keep_existing)
        return self.cache

    def _advance_cache_query(self, tablename, callback=None):
        advanced = False
        if tablename not in self.cache:
            advanced = True
            self._do_advance_cache_query(tablename)
        if callback is not None:
            callback()
        return advanced

    def _do_advance_cache_query(self, tablename, keep_existing=False):
        table_cache = self.cache.table_cache(tablename)
        for x in self.query(getattr(self, self.cache_sqs[tablename])).yield_per(1000).enable_eagerloads(False):
            table_cache.add_item(x._asdict(), keep_existing)

    def _items_with_type_id(self, tablename, *items):
        type_id = {
            "object_class": self.object_class_type,
            "relationship_class": self.relationship_class_type,
            "object": self.object_entity_type,
            "relationship": self.relationship_entity_type,
        }.get(tablename)
        if type_id is None:
            yield from items
            return
        for item in items:
            item["type_id"] = type_id
            yield item

    def _object_class_id(self):
        return case([(self.entity_class_sq.c.type_id == self.object_class_type, self.entity_class_sq.c.id)], else_=None)

    def _relationship_class_id(self):
        return case(
            [(self.entity_class_sq.c.type_id == self.relationship_class_type, self.entity_class_sq.c.id)], else_=None
        )

    def _object_id(self):
        return case([(self.entity_sq.c.type_id == self.object_entity_type, self.entity_sq.c.id)], else_=None)

    def _relationship_id(self):
        return case([(self.entity_sq.c.type_id == self.relationship_entity_type, self.entity_sq.c.id)], else_=None)

    def _object_class_name(self):
        return case(
            [(self.entity_class_sq.c.type_id == self.object_class_type, self.entity_class_sq.c.name)], else_=None
        )

    def _relationship_class_name(self):
        return case(
            [(self.entity_class_sq.c.type_id == self.relationship_class_type, self.entity_class_sq.c.name)], else_=None
        )

    def _object_class_id_list(self):
        return case(
            [
                (
                    self.entity_class_sq.c.type_id == self.relationship_class_type,
                    self.wide_relationship_class_sq.c.object_class_id_list,
                )
            ],
            else_=None,
        )

    def _object_class_name_list(self):
        return case(
            [
                (
                    self.entity_class_sq.c.type_id == self.relationship_class_type,
                    self.wide_relationship_class_sq.c.object_class_name_list,
                )
            ],
            else_=None,
        )

    def _object_name(self):
        return case([(self.entity_sq.c.type_id == self.object_entity_type, self.entity_sq.c.name)], else_=None)

    def _object_id_list(self):
        return case(
            [(self.entity_sq.c.type_id == self.relationship_entity_type, self.wide_relationship_sq.c.object_id_list)],
            else_=None,
        )

    def _object_name_list(self):
        return case(
            [(self.entity_sq.c.type_id == self.relationship_entity_type, self.wide_relationship_sq.c.object_name_list)],
            else_=None,
        )

    @staticmethod
    def _metadata_usage_counts(cache):
        """Counts references to metadata name, value pairs in entity_metadata and parameter_value_metadata tables.

        Args:
            cache (dict): database cache

        Returns:
            Counter: usage counts keyed by metadata id
        """
        usage_counts = Counter()
        for entry in cache.get("entity_metadata", {}).values():
            usage_counts[entry.metadata_id] += 1
        for entry in cache.get("parameter_value_metadata", {}).values():
            usage_counts[entry.metadata_id] += 1
        return usage_counts

    def __del__(self):
        try:
            self.connection.close()
        except AttributeError:
            pass

    def make_temporary_table(self, table_name, *columns):
        """Creates a temporary table.

        Args:
            table_name (str): table name
            *columns: table's columns

        Returns:
            Table: created table
        """
        table = Table(table_name, self._metadata, *columns, prefixes=["TEMPORARY"])
        table.drop(self.connection, checkfirst=True)
        table.create(self.connection)
        return table

    def get_filter_configs(self):
        return self._filter_configs
