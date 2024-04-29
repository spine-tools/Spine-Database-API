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

"""
This module defines the :class:`.DatabaseMapping` class, the main mean to communicate with a Spine DB.
If you're planning to use this class, it is probably a good idea to first familiarize yourself a little bit with the
:ref:`db_mapping_schema`.
"""

import hashlib
import os
import time
import logging
from functools import partialmethod
from datetime import datetime, timezone
from types import MethodType
from sqlalchemy import create_engine, MetaData, inspect
from sqlalchemy.pool import NullPool
from sqlalchemy.event import listen
from sqlalchemy.exc import DatabaseError, DBAPIError, ArgumentError
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
    create_new_spine_database_from_bind,
    compare_schemas,
    model_meta,
    copy_database_bind,
    Asterisk,
)

logging.getLogger("alembic").setLevel(logging.CRITICAL)


class DatabaseMapping(DatabaseMappingQueryMixin, DatabaseMappingCommitMixin, DatabaseMappingBase):
    """Enables communication with a Spine DB.

    The DB is incrementally mapped into memory as data is requested/modified, following the :ref:`db_mapping_schema`.

    Data is typically retrieved using :meth:`get_item` or :meth:`get_items`.
    If the requested data is already in memory, it is returned from there;
    otherwise it is fetched from the DB, stored in memory, and then returned.
    In other words, the data is fetched from the DB exactly once.

    For convenience, we also provide specialized 'get' methods for each item type, e.g., :meth:`get_entity_item`
    and :meth:`get_entity_items`.

    Data is added via :meth:`add_item`;
    updated via :meth:`update_item`;
    removed via :meth:`remove_item`;
    and restored via :meth:`restore_item`.
    All the above methods modify the in-memory mapping (not the DB itself).
    These methods also fetch data from the DB into the in-memory mapping to perform the necessary integrity checks
    (unique and foreign key constraints).

    For convenience, we also provide specialized 'add', 'update', 'remove', and 'restore' methods
    for each item type, e.g.,
    :meth:`add_entity_item`, :meth:`update_entity_item`, :meth:`remove_entity_item`, :meth:`restore_entity_item`.

    Modifications to the in-memory mapping are committed (written) to the DB via :meth:`commit_session`,
    or rolled back (discarded) via :meth:`rollback_session`.

    The DB fetch status is reset via :meth:`refresh_session`.
    This allows new items in the DB (added by other clients in the meantime) to be retrieved as well.

    You can also control the fetching process via :meth:`fetch_more` and/or :meth:`fetch_all`.
    For example, you can call :meth:`fetch_more` in a dedicated thread while you do some work on the main thread.
    This will nicely place items in the in-memory mapping so you can access them later, without
    the overhead of fetching them from the DB.

    The :meth:`query` method is also provided as an alternative way to retrieve data from the DB
    while bypassing the in-memory mapping entirely.

    You can use this class as a context manager, e.g.::

        with DatabaseMapping(db_url) as db_map:
            # Do stuff with db_map
            ...

    """

    _sq_name_by_item_type = {
        "alternative": "alternative_sq",
        "scenario": "scenario_sq",
        "scenario_alternative": "scenario_alternative_sq",
        "entity_class": "wide_entity_class_sq",
        "superclass_subclass": "superclass_subclass_sq",
        "entity": "wide_entity_sq",
        "entity_group": "entity_group_sq",
        "entity_alternative": "entity_alternative_sq",
        "parameter_value_list": "parameter_value_list_sq",
        "list_value": "list_value_sq",
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
        backup_url="",
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
            backup_url (str, optional): A URL to backup the DB before upgrading.
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
        try:
            self.sa_url = make_url(db_url)
        except ArgumentError:
            raise SpineDBAPIError("Could not parse the given URL. Please check that it is valid.")
        self.username = username if username else "anon"
        self.codename = self._make_codename(codename)
        self._memory = memory
        self._memory_dirty = False
        self._original_engine = self.create_engine(
            self.sa_url, create=create, upgrade=upgrade, backup_url=backup_url, sqlite_timeout=sqlite_timeout
        )
        # NOTE: The NullPool is needed to receive the close event (or any events), for some reason
        self.engine = create_engine("sqlite://", poolclass=NullPool) if self._memory else self._original_engine
        listen(self.engine, "close", self._receive_engine_close)
        if self._memory:
            copy_database_bind(self.engine, self._original_engine)
        self._metadata = MetaData(self.engine)
        self._metadata.reflect()
        self._tablenames = [t.name for t in self._metadata.sorted_tables]
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
        return [x for x in DatabaseMapping._sq_name_by_item_type if not item_factory(x).is_protected]

    @staticmethod
    def all_item_types():
        return list(DatabaseMapping._sq_name_by_item_type)

    @staticmethod
    def item_factory(item_type):
        return item_factory(item_type)

    def _query_commit_count(self):
        return self.query(self.commit_sq).count()

    def _make_sq(self, item_type):
        sq_name = self._sq_name_by_item_type[item_type]
        return getattr(self, sq_name)

    def _make_codename(self, codename):
        if codename:
            return str(codename)
        if not self.sa_url.drivername.startswith("sqlite"):
            return self.sa_url.database
        if self.sa_url.database is not None:
            return os.path.splitext(os.path.basename(self.sa_url.database))[0]
        hashing = hashlib.sha1()
        hashing.update(bytes(str(time.time()), "utf-8"))
        return hashing.hexdigest()

    @staticmethod
    def get_upgrade_db_prompt_data(url, create=False):
        """Returns data to prompt the user what to do if the DB at the given url is not the latest version.
        If it is, then returns None.

        Args:
            url (str)
            create (bool,optional)

        Returns:
            str: The title of the prompt
            str: The text of the prompt
            dict: Mapping different options, to kwargs to pass to DatabaseMapping constructor in order to apply them
            dict or None: Mapping different options, to additional notes
            int or None: The preferred option if any
        """
        sa_url = make_url(url)
        try:
            DatabaseMapping.create_engine(sa_url, create=create)
            return None
        except SpineDBVersionError as v_err:
            if v_err.upgrade_available:
                title = "Incompatible database version"
                text = (
                    f"The database at <br><center>'{sa_url}'</center><br> is at revision <b>{v_err.current}</b> "
                    f"and needs to be upgraded to revision <b>{v_err.expected}</b> "
                    "in order to be used with the current version of Spine."
                    "<p><b>WARNING</b>: After the upgrade, the database may no longer be used with previous versions."
                )
                if sa_url.drivername == "sqlite":
                    folder_name, file_name = os.path.split(sa_url.database)
                    file_name, _ = os.path.splitext(file_name)
                else:
                    folder_name = os.path.expanduser("~")
                    file_name = sa_url.database
                database = os.path.join(folder_name, file_name + "." + v_err.current)
                backup_url = str(URL("sqlite", database=database))
                option_to_kwargs = {
                    "Backup and upgrade": dict(upgrade=True, backup_url=backup_url),
                    "Just upgrade": dict(upgrade=True),
                }
                notes = {"Backup and upgrade": f"The backup will be written at '{backup_url}'"}
                preferred = 0
            else:
                title = "Unsupported database version"
                text = (
                    f"The database at <br><center>'{sa_url}'</center><br> is at revision <b>{v_err.current}</b> "
                    f"while this version of Spine supports revisions up to <b>{v_err.expected}</b>."
                    "<p>Please upgrade Spine to use this database."
                )
                option_to_kwargs = {}
                notes = None
                preferred = None
            return title, text, option_to_kwargs, notes, preferred

    @staticmethod
    def create_engine(sa_url, create=False, upgrade=False, backup_url="", sqlite_timeout=1800):
        if sa_url.drivername == "sqlite":
            connect_args = {"timeout": sqlite_timeout}
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
        with engine.begin() as connection:
            if sa_url.drivername == "sqlite":
                connection.execute("BEGIN IMMEDIATE")
            # TODO: Do other dialects need to lock?
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
                    create_new_spine_database_from_bind(connection)
                    return engine
            config = Config()
            config.set_main_option("script_location", "spinedb_api:alembic")
            script = ScriptDirectory.from_config(config)
            head = script.get_current_head()
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
                if backup_url:
                    dst_engine = create_engine(backup_url)
                    copy_database_bind(dst_engine, engine)

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
    def real_item_type(tablename):
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

    def get_table(self, tablename):
        # For tests
        return self._metadata.tables[tablename]

    def get_item(self, item_type, fetch=True, skip_removed=True, **kwargs):
        """Finds and returns an item matching the arguments, or None if none found.

        Example::

            with DatabaseMapping(db_url) as db_map:
                prince = db_map.get_item("entity", entity_class_name="musician", name="Prince")

        Args:
            item_type (str): One of <spine_item_types>.
            fetch (bool, optional): Whether to fetch the DB in case the item is not found in memory.
            skip_removed (bool, optional): Whether to ignore removed items.
            **kwargs: Fields and values for one the unique keys as specified for the item type
                in :ref:`db_mapping_schema`.

        Returns:
            :class:`PublicItem` or None
        """
        item_type = self.real_item_type(item_type)
        mapped_table = self.mapped_table(item_type)
        mapped_table.check_fields(kwargs, valid_types=(type(None),))
        item = mapped_table.find_item(kwargs)
        if not item and fetch:
            self.do_fetch_more(item_type, offset=0, limit=None, **kwargs)
            item = mapped_table.find_item(kwargs)
        if not item or (skip_removed and not item.is_valid()):
            return {}
        return item.public_item

    def get_items(self, item_type, fetch=True, skip_removed=True, **kwargs):
        """Finds and returns all the items of one type.

        Args:
            item_type (str): One of <spine_item_types>.
            fetch (bool, optional): Whether to fetch the DB before returning the items.
            skip_removed (bool, optional): Whether to ignore removed items.
            **kwargs: Fields and values for one the unique keys as specified for the item type
                in :ref:`db_mapping_schema`.

        Returns:
            list(:class:`PublicItem`): The items.
        """
        item_type = self.real_item_type(item_type)
        mapped_table = self.mapped_table(item_type)
        mapped_table.check_fields(kwargs, valid_types=(type(None),))
        if fetch:
            self.do_fetch_more(item_type, offset=0, limit=None, **kwargs)
        get_items = mapped_table.valid_values if skip_removed else mapped_table.values
        return [x.public_item for x in get_items() if all(x.get(k) == v for k, v in kwargs.items())]

    @staticmethod
    def _modify_items(function, *items, strict=False):
        modified, errors = [], []
        for item in items:
            item, error = function(item)
            if error:
                if strict:
                    raise SpineIntegrityError(error)
                errors.append(error)
            if item:
                modified.append(item)
        return modified, errors

    def add_item(self, item_type, check=True, **kwargs):
        """Adds an item to the in-memory mapping.

        Example::

            with DatabaseMapping(db_url) as db_map:
                db_map.add_item("entity_class", name="musician")
                db_map.add_item("entity", entity_class_name="musician", name="Prince")

        Args:
            item_type (str): One of <spine_item_types>.
            **kwargs: Fields and values as specified for the item type in :ref:`db_mapping_schema`.

        Returns:
            tuple(:class:`PublicItem` or None, str): The added item and any errors.
        """
        item_type = self.real_item_type(item_type)
        mapped_table = self.mapped_table(item_type)
        self._convert_legacy(item_type, kwargs)
        if not check:
            return mapped_table.add_item(kwargs), None
        checked_item, error = mapped_table.checked_item_and_error(kwargs)
        return (mapped_table.add_item(checked_item).public_item if checked_item else None, error)

    def add_items(self, item_type, *items, check=True, strict=False):
        """Adds many items to the in-memory mapping.

        Args:
            item_type (str): One of <spine_item_types>.
            *items (Iterable(dict)): One or more :class:`dict` objects mapping fields to values of the item type,
                as specified in :ref:`db_mapping_schema`.
            strict (bool): Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
                if the insertion of one of the items violates an integrity constraint.

        Returns:
            tuple(list(:class:`PublicItem`),list(str)): items successfully added and found violations.
        """
        return self._modify_items(lambda x: self.add_item(item_type, check=check, **x), *items, strict=strict)

    def update_item(self, item_type, check=True, **kwargs):
        """Updates an item in the in-memory mapping.

        Example::

            with DatabaseMapping(db_url) as db_map:
                prince = db_map.get_item("entity", entity_class_name="musician", name="Prince")
                db_map.update_item(
                    "entity", id=prince["id"], name="the Artist", description="Formerly known as Prince."
                )

        Args:
            item_type (str): One of <spine_item_types>.
            id (int): The id of the item to update.
            **kwargs: Fields to update and their new values as specified for the item type in :ref:`db_mapping_schema`.

        Returns:
            tuple(:class:`PublicItem` or None, str): The updated item and any errors.
        """
        item_type = self.real_item_type(item_type)
        mapped_table = self.mapped_table(item_type)
        self._convert_legacy(item_type, kwargs)
        if not check:
            return mapped_table.update_item(kwargs), None
        checked_item, error = mapped_table.checked_item_and_error(kwargs, for_update=True)
        return (mapped_table.update_item(checked_item._asdict()).public_item if checked_item else None, error)

    def update_items(self, item_type, *items, check=True, strict=False):
        """Updates many items in the in-memory mapping.

        Args:
            item_type (str): One of <spine_item_types>.
            *items (Iterable(dict)): One or more :class:`dict` objects mapping fields to values of the item type,
                as specified in :ref:`db_mapping_schema` and including the `id`.
            strict (bool): Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
                if the update of one of the items violates an integrity constraint.

        Returns:
            tuple(list(:class:`PublicItem`),list(str)): items successfully updated and found violations.
        """
        return self._modify_items(lambda x: self.update_item(item_type, check=check, **x), *items, strict=strict)

    def add_update_item(self, item_type, check=True, **kwargs):
        """Adds an item to the in-memory mapping if it doesn't exist; otherwise updates the current one.

        Args:
            item_type (str): One of <spine_item_types>.
            **kwargs: Fields and values as specified for the item type in :ref:`db_mapping_schema`.

        Returns:
            tuple(:class:`PublicItem` or None, :class:`PublicItem` or None, str): The added item if any,
                the updated item if any, and any errors.
        """
        added, add_error = self.add_item(item_type, check=check, **kwargs)
        if not add_error:
            return added, None, add_error
        updated, update_error = self.update_item(item_type, check=check, **kwargs)
        if not update_error:
            return None, updated, update_error
        return None, None, add_error or update_error

    def add_update_items(self, item_type, *items, check=True, strict=False):
        """Adds or updates many items into the in-memory mapping.

        Args:
            item_type (str): One of <spine_item_types>.
            *items (Iterable(dict)): One or more :class:`dict` objects mapping fields to values of the item type,
                as specified in :ref:`db_mapping_schema`.
            strict (bool): Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
                if the insertion of one of the items violates an integrity constraint.

        Returns:
            tuple(list(:class:`PublicItem`),list(:class:`PublicItem`),list(str)): items successfully added,
                items successfully updated, and found violations.
        """

        def _function(item):
            added, updated, error = self.add_update_item(item_type, check=check, **item)
            return (added, updated), error

        added_updated, errors = self._modify_items(_function, *items, strict=strict)
        added, updated = zip(*added_updated) if added_updated else ([], [])
        added = [x for x in added if x]
        updated = [x for x in updated if x]
        return added, updated, errors

    def remove_item(self, item_type, id_, check=True):
        """Removes an item from the in-memory mapping.

        Example::

            with DatabaseMapping(db_url) as db_map:
                prince = db_map.get_item("entity", entity_class_name="musician", name="Prince")
                db_map.remove_item("entity", prince["id"])

        Args:
            item_type (str): One of <spine_item_types>.
            id_ (int): The id of the item to remove.

        Returns:
            tuple(:class:`PublicItem` or None, str): The removed item and any errors.
        """
        item_type = self.real_item_type(item_type)
        mapped_table = self.mapped_table(item_type)
        item, error = mapped_table.item_to_remove_and_error(id_)
        if check and error:
            return None, error
        return mapped_table.remove_item(item).public_item, None

    def remove_items(self, item_type, *ids, check=True, strict=False):
        """Removes many items from the in-memory mapping.

        Args:
            item_type (str): One of <spine_item_types>.
            *ids (Iterable(int)): Ids of items to be removed.
            strict (bool): Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
                if the update of one of the items violates an integrity constraint.

        Returns:
            tuple(list(:class:`PublicItem`),list(str)): items successfully removed and found violations.
        """
        item_type = self.real_item_type(item_type)
        ids = set(ids)
        if item_type == "alternative":
            # Do not remove the Base alternative
            ids.discard(1)
        if not ids:
            return [], []
        return self._modify_items(lambda x: self.remove_item(item_type, x, check=check), *ids, strict=strict)

    def cascade_remove_items(self, cache=None, **kwargs):
        # Legacy
        for item_type, ids in kwargs.items():
            self.remove_items(item_type, *ids)

    def restore_item(self, item_type, id_):
        """Restores a previously removed item into the in-memory mapping.

        Example::

            with DatabaseMapping(db_url) as db_map:
                prince = db_map.get_item("entity", skip_remove=False, entity_class_name="musician", name="Prince")
                db_map.restore_item("entity", prince["id"])

        Args:
            item_type (str): One of <spine_item_types>.
            id_ (int): The id of the item to restore.

        Returns:
            tuple(:class:`PublicItem` or None, str): The restored item if any.
        """
        item_type = self.real_item_type(item_type)
        mapped_table = self.mapped_table(item_type)
        return mapped_table.restore_item(id_).public_item

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
            bool: True if any data was removed, False otherwise.
        """
        return bool(self.remove_items(item_type, Asterisk))

    def fetch_more(self, item_type, offset=0, limit=None, **kwargs):
        """Fetches items from the DB into the in-memory mapping, incrementally.

        Args:
            item_type (str): One of <spine_item_types>.
            offset (int): The initial row.
            limit (int): The maximum number of rows to fetch.
            **kwargs: Fields and values for one the unique keys as specified for the item type
                in :ref:`db_mapping_schema`.

        Returns:
            list(:class:`PublicItem`): The items fetched.
        """
        item_type = self.real_item_type(item_type)
        return [x.public_item for x in self.do_fetch_more(item_type, offset=offset, limit=limit, **kwargs)]

    def fetch_all(self, *item_types):
        """Fetches items from the DB into the in-memory mapping.
        Unlike :meth:`fetch_more`, this method fetches entire tables.

        Args:
            *item_types (Iterable(str)): One or more of <spine_item_types>.
                If none given, then the entire DB is fetched.
        """
        item_types = set(self.item_types()) if not item_types else set(item_types) & set(self.item_types())
        for item_type in item_types:
            item_type = self.real_item_type(item_type)
            self.do_fetch_more(item_type)

    def query(self, *args, **kwargs):
        """Returns a :class:`~spinedb_api.query.Query` object to execute against the mapped DB.

        To perform custom ``SELECT`` statements, call this method with one or more of the documented
        subquery properties of :class:`~spinedb_api.db_mapping_query_mixin.DatabaseMappingQueryMixin` returning
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

        Returns:
            :class:`~spinedb_api.query.Query`: The resulting query.
        """
        return Query(self.engine, *args)

    def commit_session(self, comment, apply_compatibility_transforms=True):
        """Commits the changes from the in-memory mapping to the database.

        Args:
            comment (str): commit message
            apply_compatibility_transforms (bool): if True, apply compatibility transforms

        Returns:
            tuple(list, list): compatibility transformations
        """
        if not comment:
            raise SpineDBAPIError("Commit message cannot be empty.")
        with self.engine.begin() as connection:
            commit = self._metadata.tables["commit"]
            commit_item = dict(user=self.username, date=datetime.now(timezone.utc), comment=comment)
            try:
                # TODO: The below locks the DB in sqlite, how about other dialects?
                commit_id = connection.execute(commit.insert(), commit_item).inserted_primary_key[0]
            except DBAPIError as e:
                raise SpineDBAPIError(f"Fail to commit: {e.orig.args}") from e
            dirty_items = self._dirty_items()
            if not dirty_items:
                connection.execute(commit.delete().where(commit.c.id == commit_id))
                raise SpineDBAPIError("Nothing to commit.")
            for tablename, (to_add, to_update, to_remove) in dirty_items:
                for item in to_add + to_update + to_remove:
                    item.commit(commit_id)
                # Remove before add, to help with keeping integrity constraints
                self._do_remove_items(connection, tablename, *{x["id"] for x in to_remove})
                self._do_update_items(connection, tablename, *to_update)
                self._do_add_items(connection, tablename, *to_add)
            if self._memory:
                self._memory_dirty = True
            transformation_info = compatibility_transformations(connection, apply=apply_compatibility_transforms)
        self._commit_count = self._query_commit_count()
        return transformation_info

    def rollback_session(self):
        """Discards all the changes from the in-memory mapping."""
        if not self._rollback():
            raise SpineDBAPIError("Nothing to rollback.")
        if self._memory:
            self._memory_dirty = False

    def refresh_session(self):
        """Resets the fetch status so new items from the DB can be retrieved."""
        self._refresh()

    def has_external_commits(self):
        """Tests whether the database has had commits from other sources than this mapping.

        Returns:
            bool: True if database has external commits, False otherwise
        """
        return self._commit_count != self._query_commit_count()

    def close(self):
        """Closes this DB mapping. This is only needed if you're keeping a long-lived session.
        For instance::

            class MyDBMappingWrapper:
                def __init__(self, url):
                    self._db_map = DatabaseMapping(url)

                # More methods that do stuff with self._db_map

                def __del__(self):
                    self._db_map.close()

        Otherwise, the usage as context manager is recommended::

            with DatabaseMapping(url) as db_map:
                # Do stuff with db_map
                ...
                # db_map.close() is automatically called when leaving this block
        """
        self.closed = True

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

    def remove_unused_metadata(self):
        used_metadata_ids = set()
        for x in self.mapped_table("entity_metadata").valid_values():
            used_metadata_ids.add(x["metadata_id"])
        for x in self.mapped_table("parameter_value_metadata").valid_values():
            used_metadata_ids.add(x["metadata_id"])
        unused_metadata_ids = {x["id"] for x in self.mapped_table("metadata").valid_values()} - used_metadata_ids
        self.remove_items("metadata", *unused_metadata_ids)

    def get_filter_configs(self):
        """Returns the filters from this mapping's URL.

        Returns:
            list(dict):
        """
        return self._filter_configs


# Define convenience methods
for it in DatabaseMapping.item_types():
    setattr(DatabaseMapping, "get_" + it + "_item", partialmethod(DatabaseMapping.get_item, it))
    setattr(DatabaseMapping, "get_" + it + "_items", partialmethod(DatabaseMapping.get_items, it))
    setattr(DatabaseMapping, "add_" + it + "_item", partialmethod(DatabaseMapping.add_item, it))
    setattr(DatabaseMapping, "update_" + it + "_item", partialmethod(DatabaseMapping.update_item, it))
    setattr(DatabaseMapping, "add_update_" + it + "_item", partialmethod(DatabaseMapping.add_update_item, it))
    setattr(DatabaseMapping, "remove_" + it + "_item", partialmethod(DatabaseMapping.remove_item, it))
    setattr(DatabaseMapping, "restore_" + it + "_item", partialmethod(DatabaseMapping.restore_item, it))

# Astroid transform so DatabaseMapping looks like it has the convenience methods defined above
def _add_convenience_methods(node):
    if node.name != "DatabaseMapping":
        return node

    def _a(item_type):
        return "an" if any(item_type.lower().startswith(x) for x in "aeiou") else "a"

    def _uq_fields(factory):
        return {
            f_name: factory.fields[f_name]
            for f_names in factory._unique_keys
            for f_name in set(f_names) & set(factory.fields.keys())
        }

    def _kwargs(fields):
        def type_(f_dict):
            return f_dict["type"].__name__ + (", optional" if f_dict.get("optional", False) else "")

        return f"\n{padding}".join(
            [f"{f_name} ({type_(f_dict)}): {f_dict['value']}" for f_name, f_dict in fields.items()]
        )

    padding = 20 * " "
    for item_type in DatabaseMapping.item_types():
        factory = DatabaseMapping.item_factory(item_type)
        a = _a(item_type)
        get_kwargs = _kwargs(_uq_fields(factory))
        child = astroid.extract_node(
            f'''
            def get_{item_type}_item(self, fetch=True, skip_removed=True, **kwargs):
                """Finds and returns {a} `{item_type}` item matching the arguments, or None if none found.

                Args:
                    fetch (bool, optional): Whether to fetch the DB in case the item is not found in memory.
                    skip_removed (bool, optional): Whether to ignore removed items.
                    {get_kwargs}

                Returns:
                    :class:`PublicItem` or None
                """
            '''
        )
        child.parent = node
        node.body.append(child)
    for item_type in DatabaseMapping.item_types():
        factory = DatabaseMapping.item_factory(item_type)
        a = _a(item_type)
        get_kwargs = _kwargs(_uq_fields(factory))
        child = astroid.extract_node(
            f'''
            def get_{item_type}_items(self, fetch=True, skip_removed=True, **kwargs):
                """Finds and returns all {item_type} items.

                Args:
                    fetch (bool, optional): Whether to fetch the DB before returning the items.
                    skip_removed (bool, optional): Whether to ignore removed items.
                    {get_kwargs}

                Returns:
                    list(:class:`PublicItem`): The items.
                """
            '''
        )
        child.parent = node
        node.body.append(child)
    for item_type in DatabaseMapping.item_types():
        factory = DatabaseMapping.item_factory(item_type)
        a = _a(item_type)
        add_kwargs = _kwargs(factory.fields)
        child = astroid.extract_node(
            f'''
            def add_{item_type}_item(self, check=True, **kwargs):
                """Adds {a} `{item_type}` item to the in-memory mapping.

                Args:
                    {add_kwargs}

                Returns:
                    tuple(:class:`PublicItem` or None, str): The added item and any errors.
                """
            '''
        )
        child.parent = node
        node.body.append(child)
    for item_type in DatabaseMapping.item_types():
        factory = DatabaseMapping.item_factory(item_type)
        a = _a(item_type)
        update_kwargs = f"id (int): The id of the item to update.\n{padding}" + _kwargs(factory.fields)
        child = astroid.extract_node(
            f'''
            def update_{item_type}_item(self, check=True, **kwargs):
                """Updates {a} `{item_type}` item in the in-memory mapping.

                Args:
                    {update_kwargs}

                Returns:
                    tuple(:class:`PublicItem` or None, str): The updated item and any errors.
                """
            '''
        )
        child.parent = node
        node.body.append(child)
    for item_type in DatabaseMapping.item_types():
        factory = DatabaseMapping.item_factory(item_type)
        a = _a(item_type)
        add_kwargs = _kwargs(factory.fields)
        child = astroid.extract_node(
            f'''
            def add_update_{item_type}_item(self, check=True, **kwargs):
                """Adds {a} `{item_type}` item to the in-memory mapping if it doesn't exist;
                otherwise updates the current one.

                Args:
                    {add_kwargs}

                Returns:
                    tuple(:class:`PublicItem` or None, :class:`PublicItem` or None, str): The added item if any,
                        the updated item if any, and any errors.
                """
            '''
        )
        child.parent = node
        node.body.append(child)
    for item_type in DatabaseMapping.item_types():
        child = astroid.extract_node(
            f'''
            def remove_{item_type}_item(self, id):
                """Removes {a} `{item_type}` item from the in-memory mapping.

                Args:
                    id (int): the id of the item to remove.

                Returns:
                    tuple(:class:`PublicItem` or None, str): The removed item if any.
                """
            '''
        )
        child.parent = node
        node.body.append(child)
    for item_type in DatabaseMapping.item_types():
        child = astroid.extract_node(
            f'''
            def restore_{item_type}_item(self, id):
                """Restores a previously removed `{item_type}` item into the in-memory mapping.

                Args:
                    id (int): the id of the item to restore.

                Returns:
                    tuple(:class:`PublicItem` or None, str): The restored item if any.
                """
            '''
        )
        child.parent = node
        node.body.append(child)
    return node


try:
    import astroid

    astroid.MANAGER.register_transform(astroid.ClassDef, _add_convenience_methods)
except ModuleNotFoundError:
    pass
