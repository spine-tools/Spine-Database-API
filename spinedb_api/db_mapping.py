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

"""
This module defines the :class:`.DatabaseMapping` class.


DB mapping schema
=================
<db_mapping_schema>
"""

import hashlib
import os
import time
import logging
from datetime import datetime, timezone
from types import MethodType
from sqlalchemy import create_engine, MetaData, inspect
from sqlalchemy.pool import NullPool
from sqlalchemy.event import listen
from sqlalchemy.exc import DatabaseError, DBAPIError
from sqlalchemy.engine.url import make_url, URL
from alembic.migration import MigrationContext
from alembic.environment import EnvironmentContext
from alembic.script import ScriptDirectory
from alembic.config import Config
from alembic.util.exc import CommandError

from .filters.tools import pop_filter_configs, apply_filter_stack, load_filters
from .spine_db_client import get_db_url_from_server
from .mapped_items import item_factory
from .db_mapping_base import DatabaseMappingBase
from .db_mapping_commit_mixin import DatabaseMappingCommitMixin
from .db_mapping_query_mixin import DatabaseMappingQueryMixin
from .exception import SpineDBAPIError, SpineDBVersionError, SpineIntegrityError
from .query import Query
from .compatibility import compatibility_transformations
from .helpers import (
    _create_first_spine_database,
    create_new_spine_database,
    compare_schemas,
    model_meta,
    copy_database_bind,
    Asterisk,
)

logging.getLogger("alembic").setLevel(logging.CRITICAL)


class DatabaseMapping(DatabaseMappingQueryMixin, DatabaseMappingCommitMixin, DatabaseMappingBase):
    """Enables communication with a Spine DB.

    The DB is incrementally mapped into memory as data is requested/modified.

    Data is typically retrieved using :meth:`get_item` or :meth:`get_items`.
    If the requested data is already in memory, it is returned from there;
    otherwise it is fetched from the DB, stored in memory, and then returned.
    In other words, the data is fetched from the DB exactly once.

    Data is added via :meth:`add_item`;
    updated via :meth:`update_item`;
    removed via :meth:`remove_item`;
    and restored via :meth:`restore_item`.
    All the above methods modify the in-memory mapping (not the DB itself).
    These methods also fetch data from the DB into the in-memory mapping to perform the necessary integrity checks
    (unique and foreign key constraints).

    Modifications to the in-memory mapping are committed (written) to the DB via :meth:`commit_session`,
    or rolled back (discarded) via :meth:`rollback_session`.

    The DB fetch status is reset via :meth:`refresh_session`.
    This allows new items in the DB (added by other clients in the meantime) to be retrieved as well.

    You can also control the fetching process via :meth:`fetch_more` and/or :meth:`fetch_all`.
    For example, a UI application might want to fetch data in the background so the UI is not blocked in the process.
    In that case they can call e.g. :meth:`fetch_more` asynchronously as the user scrolls or expands the views.

    The :meth:`query` method is also provided as an alternative way to retrieve data from the DB
    while bypassing the in-memory mapping entirely.

    """

    _sq_name_by_item_type = {
        "entity_class": "wide_entity_class_sq",
        "entity": "wide_entity_sq",
        "entity_alternative": "entity_alternative_sq",
        "parameter_value_list": "parameter_value_list_sq",
        "list_value": "list_value_sq",
        "alternative": "alternative_sq",
        "scenario": "scenario_sq",
        "scenario_alternative": "scenario_alternative_sq",
        "entity_group": "entity_group_sq",
        "parameter_definition": "parameter_definition_sq",
        "parameter_value": "parameter_value_sq",
        "metadata": "metadata_sq",
        "entity_metadata": "entity_metadata_sq",
        "parameter_value_metadata": "parameter_value_metadata_sq",
        "commit": "commit_sq",
    }

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
    ):
        """
        Args:
            db_url (str or :class:`~sqlalchemy.engine.url.URL`): A URL in RFC-1738 format pointing to the database
                to be mapped, or to a DB server.
            username (str, optional): A user name. If not given, it gets replaced by the string `anon`.
            upgrade (bool, optional): Whether the DB at the given `url` should be upgraded to the most recent
                version.
            codename (str, optional): A name to identify this object in your application.
            create (bool, optional): Whether to create a new Spine DB at the given `url` if it's not already one.
            apply_filters (bool, optional): Whether to apply filters in the `url`'s query segment.
            memory (bool, optional): Whether to use a SQLite memory DB as replacement for the original one.
            sqlite_timeout (int, optional): The number of seconds to wait before raising SQLite connection errors.
        """
        super().__init__()
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
        self._memory_dirty = False
        self._original_engine = self.create_engine(
            self.sa_url, upgrade=upgrade, create=create, sqlite_timeout=sqlite_timeout
        )
        # NOTE: The NullPool is needed to receive the close event (or any events), for some reason
        self.engine = create_engine("sqlite://", poolclass=NullPool) if self._memory else self._original_engine
        listen(self.engine, 'close', self._receive_engine_close)
        if self._memory:
            copy_database_bind(self.engine, self._original_engine)
        self._metadata = MetaData(self.engine)
        self._metadata.reflect()
        self._tablenames = [t.name for t in self._metadata.sorted_tables]
        self.closed = False
        if self._filter_configs is not None:
            stack = load_filters(self._filter_configs)
            apply_filter_stack(self, stack)

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc_val, _exc_tb):
        self.close()

    def __del__(self):
        self.close()

    @staticmethod
    def item_types():
        return [x for x in DatabaseMapping._sq_name_by_item_type if item_factory(x).fields]

    @staticmethod
    def item_factory(item_type):
        return item_factory(item_type)

    def make_query(self, item_type):
        if self.closed:
            return None
        sq_name = self._sq_name_by_item_type[item_type]
        return self.query(getattr(self, sq_name))

    def close(self):
        """Closes this DB mapping."""
        self.closed = True

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
        """Creates engine.

        Args:
            sa_url (URL)
            upgrade (bool, optional): If True, upgrade the db to the latest version.
            create (bool, optional): If True, create a new Spine db at the given url if none found.

        Returns:
            :class:`~sqlalchemy.engine.Engine`
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
        if self._memory_dirty:
            copy_database_bind(self._original_engine, self.engine)

    @staticmethod
    def _real_tablename(tablename):
        return {
            "object_class": "entity_class",
            "relationship_class": "entity_class",
            "object": "entity",
            "relationship": "entity",
        }.get(tablename, tablename)

    @staticmethod
    def _convert_legacy(tablename, item):
        if tablename in ("entity_class", "entity"):
            object_class_id_list = tuple(item.pop("object_class_id_list", ()))
            if object_class_id_list:
                item["dimension_id_list"] = object_class_id_list
            object_class_name_list = tuple(item.pop("object_class_name_list", ()))
            if object_class_name_list:
                item["dimension_name_list"] = object_class_name_list
        if tablename == "entity":
            object_id_list = tuple(item.pop("object_id_list", ()))
            if object_id_list:
                item["element_id_list"] = object_id_list
            object_name_list = tuple(item.pop("object_name_list", ()))
            if object_name_list:
                item["element_name_list"] = object_name_list
        if tablename in ("parameter_definition", "parameter_value"):
            entity_class_id = item.pop("object_class_id", None) or item.pop("relationship_class_id", None)
            if entity_class_id:
                item["entity_class_id"] = entity_class_id
        if tablename == "parameter_value":
            entity_id = item.pop("object_id", None) or item.pop("relationship_id", None)
            if entity_id:
                item["entity_id"] = entity_id

    def get_import_alternative_name(self):
        if self._import_alternative_name is None:
            self._create_import_alternative()
        return self._import_alternative_name

    def _create_import_alternative(self):
        """Creates the alternative to be used as default for all import operations."""
        self._import_alternative_name = "Base"

    def override_create_import_alternative(self, method):
        self._create_import_alternative = MethodType(method, self)
        self._import_alternative_name = None

    def get_filter_configs(self):
        """Returns the filters used to build this DB mapping.

        Returns:
            list(dict):
        """
        return self._filter_configs

    def get_table(self, tablename):
        # For tests
        return self._metadata.tables[tablename]

    def get_item(self, item_type, fetch=True, skip_removed=True, **kwargs):
        """Finds and returns an item matching the arguments, or None if none found.

        Args:
            item_type (str): One of <spine_item_types>.
            fetch (bool, optional): Whether to fetch the DB in case the item is not found in memory.
            skip_removed (bool, optional): Whether to ignore removed items.
            **kwargs: Fields and values for one the unique keys as specified for the item type in `DB mapping schema`_.

        Returns:
            :class:`PublicItem` or None
        """
        item_type = self._real_tablename(item_type)
        cache_item = self.mapped_table(item_type).find_item(kwargs, fetch=fetch)
        if not cache_item:
            return None
        if skip_removed and not cache_item.is_valid():
            return None
        return PublicItem(self, cache_item)

    def get_items(self, item_type, fetch=True, skip_removed=True):
        """Finds and returns all the items of one type.

        Args:
            item_type (str): One of <spine_item_types>.
            fetch (bool, optional): Whether to fetch the DB before returning the items.
            skip_removed (bool, optional): Whether to ignore removed items.

        Returns:
            list(:class:`PublicItem`): The items.
        """
        item_type = self._real_tablename(item_type)
        if fetch and item_type not in self.fetched_item_types:
            self.fetch_all(item_type)
        mapped_table = self.mapped_table(item_type)
        get_items = mapped_table.valid_values if skip_removed else mapped_table.values
        return [PublicItem(self, x) for x in get_items()]

    def add_item(self, item_type, check=True, **kwargs):
        """Adds an item to the in-memory mapping.

        Args:
            item_type (str): One of <spine_item_types>.
            check (bool, optional): Whether to carry out integrity checks.
            **kwargs: Fields and values as specified for the item type in `DB mapping schema`_.

        Returns:
            tuple(:class:`PublicItem` or None, str): The added item and any errors.
        """
        item_type = self._real_tablename(item_type)
        mapped_table = self.mapped_table(item_type)
        self._convert_legacy(item_type, kwargs)
        if not check:
            return mapped_table.add_item(kwargs, new=True), None
        checked_item, error = mapped_table.check_item(kwargs)
        return (
            PublicItem(self, mapped_table.add_item(checked_item, new=True)) if checked_item and not error else None,
            error,
        )

    def add_items(self, item_type, *items, check=True, strict=False):
        """Add many items to the in-memory mapping.

        Args:
            item_type (str): One of <spine_item_types>.
            *items (Iterable(dict)): One or more :class:`dict` objects mapping fields to values,
                as specified for the item type in `DB mapping schema`_.
            check (bool): Whether or not to run integrity checks.
            strict (bool): Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
                if the insertion of one of the items violates an integrity constraint.

        Returns:
            tuple(list(:class:`PublicItem`),list(str)): items successfully added and found violations.
        """
        added, errors = [], []
        for item in items:
            item, error = self.add_item(item_type, check, **item)
            if error:
                if strict:
                    raise SpineIntegrityError(error)
                errors.append(error)
                continue
            added.append(item)
        return added, errors

    def update_item(self, item_type, check=True, **kwargs):
        """Updates an item in the in-memory mapping.

        Args:
            item_type (str): One of <spine_item_types>.
            check (bool, optional): Whether to carry out integrity checks.
            id (int): The id of the item to update.
            **kwargs: Fields to update and their new values as specified for the item type in `DB mapping schema`_.

        Returns:
            tuple(:class:`PublicItem` or None, str): The updated item and any errors.
        """
        item_type = self._real_tablename(item_type)
        mapped_table = self.mapped_table(item_type)
        self._convert_legacy(item_type, kwargs)
        if not check:
            return mapped_table.update_item(kwargs), None
        checked_item, error = mapped_table.check_item(kwargs, for_update=True)
        return (PublicItem(self, mapped_table.update_item(checked_item._asdict())) if checked_item else None, error)

    def update_items(self, item_type, *items, check=True, strict=False):
        """Updates many items in the in-memory mapping.

        Args:
            item_type (str): One of <spine_item_types>.
            *items (Iterable(dict)): One or more :class:`dict` objects mapping fields to values,
                as specified for the item type in `DB mapping schema`_ and including the `id`.
            check (bool): Whether or not to run integrity checks.
            strict (bool): Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
                if the update of one of the items violates an integrity constraint.

        Returns:
            tuple(list(:class:`PublicItem`),list(str)): items successfully updated and found violations.
        """
        updated, errors = [], []
        for item in items:
            item, error = self.update_item(item_type, check=check, **item)
            if error:
                if strict:
                    raise SpineIntegrityError(error)
                errors.append(error)
            if item:
                updated.append(item)
        return updated, errors

    def remove_item(self, item_type, id_):
        """Removes an item from the in-memory mapping.

        Example::

                with DatabaseMapping(url) as db_map:
                    my_dog = db_map.get_item("entity", class_name="dog", name="Pluto")
                    db_map.remove_item("entity", my_dog["id])


        Args:
            item_type (str): One of <spine_item_types>.
            id (int): The id of the item to remove.

        Returns:
            tuple(:class:`PublicItem` or None, str): The removed item if any.
        """
        item_type = self._real_tablename(item_type)
        mapped_table = self.mapped_table(item_type)
        return PublicItem(self, mapped_table.remove_item(id_))

    def remove_items(self, item_type, *ids):
        """Removes many items from the in-memory mapping.

        Args:
            item_type (str): One of <spine_item_types>.
            *ids (Iterable(int)): Ids of items to be removed.

        Returns:
            list(:class:`PublicItem`): the removed items.
        """
        if not ids:
            return []
        item_type = self._real_tablename(item_type)
        mapped_table = self.mapped_table(item_type)
        if Asterisk in ids:
            self.fetch_all(item_type)
            ids = mapped_table
        ids = set(ids)
        if item_type == "alternative":
            # Do not remove the Base alternative
            ids.discard(1)
        return [self.remove_item(item_type, id_) for id_ in ids]

    def restore_item(self, item_type, id_):
        """Restores a previously removed item into the in-memory mapping.

        Example::

                with DatabaseMapping(url) as db_map:
                    my_dog = db_map.get_item("entity", skip_removed=False, class_name="dog", name="Pluto")
                    db_map.restore_item("entity", my_dog["id])

        Args:
            item_type (str): One of <spine_item_types>.
            id (int): The id of the item to restore.

        Returns:
            tuple(:class:`PublicItem` or None, str): The restored item if any.
        """
        item_type = self._real_tablename(item_type)
        mapped_table = self.mapped_table(item_type)
        return PublicItem(self, mapped_table.restore_item(id_))

    def restore_items(self, item_type, *ids):
        """Restores many previously removed items into the in-memory mapping.

        Args:
            item_type (str): One of <spine_item_types>.
            *ids (Iterable(int)): Ids of items to be removed.

        Returns:
            list(:class:`PublicItem`): the restored items.
        """
        if not ids:
            return []
        return [self.restore_item(item_type, id_) for id_ in ids]

    def purge_items(self, item_type):
        """Removes all items of one type.

        Args:
            item_type (str): One of <spine_item_types>.

        Returns:
            bool: True if operation was successful, False otherwise
        """
        return self.remove_items(item_type, Asterisk)

    def can_fetch_more(self, item_type):
        """Whether or not more data can be fetched from the DB for the given item type.

        Args:
            item_type (str): One of <spine_item_types>.

        Returns:
            bool
        """
        return item_type not in self.fetched_item_types

    def fetch_more(self, item_type, limit=None):
        """Fetches items from the DB into the in-memory mapping, incrementally.

        Args:
            item_type (str): One of <spine_item_types>.
            limit (int): The maximum number of items to fetch. Successive calls to this function
                will start from the point where the last one left.
                In other words, each item is fetched from the DB exactly once.

        Returns:
            list(PublicItem): The items fetched.
        """
        item_type = self._real_tablename(item_type)
        return [PublicItem(self, x) for x in self.do_fetch_more(item_type, limit=limit)]

    def fetch_all(self, *item_types):
        """Fetches items from the DB into the in-memory mapping.
        Unlike :meth:`fetch_more`, this method fetches entire tables.

        Args:
            *item_types (Iterable(str)): One or more of <spine_item_types>.
                If none given, then the entire DB is fetched.
        """
        item_types = set(self.item_types()) if not item_types else set(item_types) & set(self.item_types())
        for item_type in item_types:
            item_type = self._real_tablename(item_type)
            self.do_fetch_all(item_type)

    def query(self, *args, **kwargs):
        """Returns a :class:`~spinedb_api.query.Query` object to execute against the mapped DB.

        To perform custom ``SELECT`` statements, call this method with one or more of the documented
        subquery properties of :class:`~spinedb_api.DatabaseMappingQueryMixin` returning
        :class:`~sqlalchemy.sql.expression.Alias` objetcs.
        For example, to select the entity class with ``id`` equal to 1::

            from spinedb_api import DatabaseMapping
            url = 'sqlite:///spine.db'
            ...
            db_map = DatabaseMapping(url)
            db_map.query(db_map.entity_class_sq).filter_by(id=1).one_or_none()

        To perform more complex queries, just use the :class:`~spinedb_api.query.Query` interface
        (which is a close clone of SQL Alchemy's :class:`~sqlalchemy.orm.query.Query`).
        For example, to select all entity class names and the names of their entities concatenated in a comma-separated
        string::

            from sqlalchemy import func

            db_map.query(
                db_map.entity_class_sq.c.name, func.group_concat(db_map.entity_sq.c.name)
            ).filter(
                db_map.entity_sq.c.class_id == db_map.entity_class_sq.c.id
            ).group_by(db_map.entity_class_sq.c.name).all()
        """
        return Query(self.engine, *args)

    def commit_session(self, comment):
        """Commits the changes from the in-memory mapping to the database.

        Args:
            comment (str): commit message
        """
        if not comment:
            raise SpineDBAPIError("Commit message cannot be empty.")
        dirty_items = self._dirty_items()
        if not dirty_items:
            raise SpineDBAPIError("Nothing to commit.")
        user = self.username
        date = datetime.now(timezone.utc)
        ins = self._metadata.tables["commit"].insert()
        with self.engine.begin() as connection:
            try:
                commit_id = connection.execute(ins, dict(user=user, date=date, comment=comment)).inserted_primary_key[0]
            except DBAPIError as e:
                raise SpineDBAPIError(f"Fail to commit: {e.orig.args}") from e
            for tablename, (to_add, to_update, to_remove) in dirty_items:
                for item in to_add + to_update + to_remove:
                    item.commit(commit_id)
                # Remove before add, to help with keeping integrity constraints
                self._do_remove_items(connection, tablename, *{x["id"] for x in to_remove})
                self._do_update_items(connection, tablename, *to_update)
                self._do_add_items(connection, tablename, *to_add)
            if self._memory:
                self._memory_dirty = True
            return compatibility_transformations(connection)

    def rollback_session(self):
        """Discards all the changes from the in-memory mapping."""
        if not self._rollback():
            raise SpineDBAPIError("Nothing to rollback.")
        if self._memory:
            self._memory_dirty = False

    def refresh_session(self):
        """Resets the fetch status so new items from the DB can be retrieved."""
        self._refresh()

    def add_ext_entity_metadata(self, *items, **kwargs):
        metadata_items = self.get_metadata_to_add_with_item_metadata_items(*items)
        self.add_items("metadata", *metadata_items, **kwargs)
        return self.add_items("entity_metadata", *items, **kwargs)

    def add_ext_parameter_value_metadata(self, *items, **kwargs):
        metadata_items = self.get_metadata_to_add_with_item_metadata_items(*items)
        self.add_items("metadata", *metadata_items, **kwargs)
        return self.add_items("parameter_value_metadata", *items, **kwargs)

    def get_metadata_to_add_with_item_metadata_items(self, *items):
        metadata_items = ({"name": item["metadata_name"], "value": item["metadata_value"]} for item in items)
        return [x for x in metadata_items if not self.mapped_table("metadata").find_item(x)]

    def _update_ext_item_metadata(self, tablename, *items, **kwargs):
        metadata_items = self.get_metadata_to_add_with_item_metadata_items(*items)
        added, errors = self.add_items("metadata", *metadata_items, **kwargs)
        updated, more_errors = self.update_items(tablename, *items, **kwargs)
        return added + updated, errors + more_errors

    def update_ext_entity_metadata(self, *items, **kwargs):
        return self._update_ext_item_metadata("entity_metadata", *items, **kwargs)

    def update_ext_parameter_value_metadata(self, *items, **kwargs):
        return self._update_ext_item_metadata("parameter_value_metadata", *items, **kwargs)

    def get_data_to_set_scenario_alternatives(self, *scenarios, strict=True):
        """Returns data to add and remove, in order to set wide scenario alternatives.

        Args:
            *scenarios: One or more wide scenario :class:`dict` objects to set.
                Each item must include the following keys:

                - "id": integer scenario id
                - "alternative_id_list": list of alternative ids for that scenario

        Returns
            list: scenario_alternative :class:`dict` objects to add.
            set: integer scenario_alternative ids to remove
        """

        def _is_equal(to_add, to_rm):
            return all(to_rm[k] == v for k, v in to_add.items())

        scen_alts_to_add = []
        scen_alt_ids_to_remove = {}
        errors = []
        for scen in scenarios:
            current_scen = self.mapped_table("scenario").find_item(scen)
            if current_scen is None:
                error = f"no scenario matching {scen} to set alternatives for"
                if strict:
                    raise SpineIntegrityError(error)
                errors.append(error)
                continue
            for k, alternative_id in enumerate(scen.get("alternative_id_list", ())):
                item_to_add = {"scenario_id": current_scen["id"], "alternative_id": alternative_id, "rank": k + 1}
                scen_alts_to_add.append(item_to_add)
            for k, alternative_name in enumerate(scen.get("alternative_name_list", ())):
                item_to_add = {"scenario_id": current_scen["id"], "alternative_name": alternative_name, "rank": k + 1}
                scen_alts_to_add.append(item_to_add)
            for alternative_id in current_scen["alternative_id_list"]:
                scen_alt = {"scenario_id": current_scen["id"], "alternative_id": alternative_id}
                current_scen_alt = self.mapped_table("scenario_alternative").find_item(scen_alt)
                scen_alt_ids_to_remove[current_scen_alt["id"]] = current_scen_alt
        # Remove items that are both to add and to remove
        for id_, to_rm in list(scen_alt_ids_to_remove.items()):
            i = next((i for i, to_add in enumerate(scen_alts_to_add) if _is_equal(to_add, to_rm)), None)
            if i is not None:
                del scen_alts_to_add[i]
                del scen_alt_ids_to_remove[id_]
        return scen_alts_to_add, set(scen_alt_ids_to_remove), errors

    def remove_unused_metadata(self):
        used_metadata_ids = set()
        for x in self.mapped_table("entity_metadata").valid_values():
            used_metadata_ids.add(x["metadata_id"])
        for x in self.mapped_table("parameter_value_metadata").valid_values():
            used_metadata_ids.add(x["metadata_id"])
        unused_metadata_ids = {x["id"] for x in self.mapped_table("metadata").valid_values()} - used_metadata_ids
        self.remove_items("metadata", *unused_metadata_ids)


class PublicItem:
    def __init__(self, db_map, cache_item):
        self._db_map = db_map
        self._cache_item = cache_item

    @property
    def item_type(self):
        return self._cache_item.item_type

    def __getitem__(self, key):
        return self._cache_item[key]

    def __eq__(self, other):
        if isinstance(other, dict):
            return self._cache_item == other
        return super().__eq__(other)

    def __repr__(self):
        return repr(self._cache_item)

    def __str__(self):
        return str(self._cache_item)

    def get(self, key, default=None):
        return self._cache_item.get(key, default)

    def is_valid(self):
        return self._cache_item.is_valid()

    def is_committed(self):
        return self._cache_item.is_committed()

    def _asdict(self):
        return self._cache_item._asdict()

    def update(self, **kwargs):
        self._db_map.update_item(self.item_type, id=self["id"], **kwargs)

    def remove(self):
        return self._db_map.remove_item(self.item_type, self["id"])

    def restore(self):
        return self._db_map.restore_item(self.item_type, self["id"])

    def add_update_callback(self, callback):
        self._cache_item.update_callbacks.add(callback)

    def add_remove_callback(self, callback):
        self._cache_item.remove_callbacks.add(callback)

    def add_restore_callback(self, callback):
        self._cache_item.restore_callbacks.add(callback)
