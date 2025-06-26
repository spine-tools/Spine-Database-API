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
from collections.abc import Callable
from datetime import datetime, timezone
from functools import partialmethod
import logging
import os
from types import MethodType
from typing import Optional
from alembic.config import Config
from alembic.environment import EnvironmentContext
from alembic.migration import MigrationContext
from alembic.script import ScriptDirectory
from alembic.util.exc import CommandError
from sqlalchemy import MetaData, create_engine, inspect, text
from sqlalchemy.engine.url import URL, make_url
from sqlalchemy.event import listen
from sqlalchemy.exc import ArgumentError, DatabaseError, DBAPIError
from sqlalchemy.orm import Session
from sqlalchemy.pool import NullPool, StaticPool
from .compatibility import CompatibilityTransformations, compatibility_transformations
from .db_mapping_base import DatabaseMappingBase, MappedItemBase, MappedTable, PublicItem
from .db_mapping_commit_mixin import DatabaseMappingCommitMixin
from .db_mapping_query_mixin import DatabaseMappingQueryMixin
from .exception import NothingToCommit, NothingToRollback, SpineDBAPIError, SpineDBVersionError, SpineIntegrityError
from .filters.tools import apply_filter_stack, load_filters, pop_filter_configs
from .helpers import (
    Asterisk,
    _create_first_spine_database,
    compare_schemas,
    copy_database_bind,
    create_new_spine_database_from_engine,
    model_meta,
)
from .mapped_item_status import Status
from .mapped_items import ITEM_CLASS_BY_TYPE
from .spine_db_client import get_db_url_from_server
from .temp_id import TempId, resolve

logging.getLogger("alembic").setLevel(logging.CRITICAL)


class DatabaseMapping(DatabaseMappingQueryMixin, DatabaseMappingCommitMixin, DatabaseMappingBase):
    """Enables communication with a Spine DB.

    The DB is incrementally mapped into memory as data is requested/modified, following the :ref:`db_mapping_schema`.

    Data is typically retrieved using :meth:`item` or :meth:`find`.
    If the requested data is already in memory, it is returned from there;
    otherwise it is fetched from the DB, stored in memory, and then returned.
    In other words, the data is fetched from the DB exactly once.

    For convenience, we also provide specialized getter methods for each item type, e.g., :meth:`entity`
    and :meth:`find_entities`.

    Data is added via :meth:`add`;
    updated via :meth:`update`;
    removed via :meth:`remove`;
    and restored via :meth:`restore`.
    :meth:`add_or_update` adds an item or updates an existing one.
    All the above methods modify the in-memory mapping (not the DB itself).
    These methods also fetch data from the DB into the in-memory mapping to perform the necessary integrity checks
    (unique and foreign key constraints).

    For convenience, we also provide specialized 'add', 'update', 'remove', and 'restore' methods
    for each item type, e.g.,
    :meth:`add_entity`, :meth:`update_entity`, :meth:`remove_entity` and :meth:`restore_entity`.

    Modifications to the in-memory mapping are committed (written) to the DB via :meth:`commit_session`,
    or rolled back (discarded) via :meth:`rollback_session`.

    The DB fetch status is reset via :meth:`refresh_session`.
    This allows new items in the DB (added by other clients in the meantime) to be retrieved as well.

    You can also control the fetching process via :meth:`fetch_more` and/or :meth:`fetch_all`.
    For example, you can call :meth:`fetch_more` in a dedicated thread while you do some work on the main thread.
    This will nicely place items in the in-memory mapping, so you can access them later, without
    the overhead of fetching them from the DB.

    The :meth:`query` method is also provided as an alternative way to retrieve data from the DB
    while bypassing the in-memory mapping entirely.

    You usually use this class as a context manager, e.g.::

        with DatabaseMapping(db_url) as db_map:
            # Do stuff with db_map
            ...

    or::

        db_map = DatabaseMapping(db_url)
        ...
        with db_map:
            # Do stuff with db_map
            ...

    """

    _sq_name_by_item_type = {
        "alternative": "alternative_sq",
        "scenario": "scenario_sq",
        "scenario_alternative": "scenario_alternative_sq",
        "entity_class": "wide_entity_class_sq",
        "display_mode": "display_mode_sq",
        "entity_class_display_mode": "entity_class_display_mode_sq",
        "superclass_subclass": "superclass_subclass_sq",
        "entity": "wide_entity_sq",
        "entity_group": "entity_group_sq",
        "entity_alternative": "entity_alternative_sq",
        "parameter_value_list": "parameter_value_list_sq",
        "list_value": "list_value_sq",
        "parameter_definition": "parameter_definition_sq",
        "parameter_type": "parameter_type_sq",
        "parameter_value": "parameter_value_sq",
        "metadata": "metadata_sq",
        "entity_metadata": "entity_metadata_sq",
        "parameter_value_metadata": "parameter_value_metadata_sq",
        "entity_location": "entity_location_sq",
        "commit": "commit_sq",
    }

    def __init__(
        self,
        db_url,
        username=None,
        upgrade=False,
        backup_url="",
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
            filter_configs = db_url.query.get("spinedbfilter", [])
            db_url = db_url.difference_update_query("spinedbfilter")
        else:
            filter_configs = []
        self._filter_configs = filter_configs if apply_filters else None
        try:
            self.sa_url = make_url(db_url)
        except ArgumentError as error:
            raise SpineDBAPIError("Could not parse the given URL. Please check that it is valid.") from error
        self.username = username if username else "anon"
        self._memory = memory
        self._memory_dirty = False
        self._original_engine = self.create_engine(
            self.sa_url, create=create, upgrade=upgrade, backup_url=backup_url, sqlite_timeout=sqlite_timeout
        )
        # NOTE: The NullPool is needed to receive the close event (or any events), for some reason
        self.engine = (
            create_engine("sqlite://", poolclass=NullPool, future=True) if self._memory else self._original_engine
        )
        listen(self.engine, "close", self._receive_engine_close)
        if self._memory:
            copy_database_bind(self.engine, self._original_engine)
        self._metadata = MetaData()
        self._metadata.reflect(self.engine)
        self._tablenames = [t.name for t in self._metadata.sorted_tables]
        self._session = None
        self._context_open_count = 0
        if self._filter_configs is not None:
            stack = load_filters(self._filter_configs)
            apply_filter_stack(self, stack)

    def __enter__(self):
        if self._closed:
            return None
        self._context_open_count += 1
        if self._session is None:
            self._session = Session(self.engine, future=True)
        return self

    def __exit__(self, _exc_type, _exc_val, _exc_tb):
        self._context_open_count -= 1
        if self._context_open_count == 0:
            self._session.close()
            self._session = None
        return False

    def session(self):
        """Returns current session or None if session is closed.

        :meta private:

        Returns:
            Session: current session
        """
        return self._session

    @staticmethod
    def item_types() -> list[str]:
        return [x for x in DatabaseMapping._sq_name_by_item_type if not ITEM_CLASS_BY_TYPE[x].is_protected]

    @staticmethod
    def all_item_types() -> list[str]:
        return list(DatabaseMapping._sq_name_by_item_type)

    @staticmethod
    def item_factory(item_type):
        return ITEM_CLASS_BY_TYPE[item_type]

    def _query_commit_count(self) -> int:
        with self:
            return self.query(self.commit_sq).count()

    def make_item(self, item_type: str, **item) -> MappedItemBase:
        return ITEM_CLASS_BY_TYPE[item_type](self, **item)

    def _make_sq(self, item_type):
        sq_name = self._sq_name_by_item_type[item_type]
        return getattr(self, sq_name)

    @staticmethod
    def get_upgrade_db_prompt_data(url, create=False):
        """Returns data to prompt the user what to do if the DB at the given url is not the latest version.
        If it is, then returns None.

        :meta private:

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
                    f"The database at <br><center>'{sa_url}'</center><br> is at revision <b>{v_err.current}</b>. "
                    f"and needs to be upgraded to revision <b>{v_err.expected}</b> "
                    f"in order to be used with your current version of Spine_Database_API (and Spine Toolbox, if "
                    f"using the database through Spine Toolbox)."
                    "<p><b>WARNING</b>: After the upgrade, the database may no longer be used with previous versions."
                )
                if sa_url.drivername == "sqlite":
                    folder_name, file_name = os.path.split(sa_url.database)
                    file_name, _ = os.path.splitext(file_name)
                else:
                    folder_name = os.path.expanduser("~")
                    file_name = sa_url.database
                database = os.path.join(folder_name, file_name + "." + v_err.current)
                backup_url = str(URL.create("sqlite", database=database))
                option_to_kwargs = {
                    "Backup and upgrade": {"upgrade": True, "backup_url": backup_url},
                    "Just upgrade": {"upgrade": True},
                }
                notes = {"Backup and upgrade": f"The backup will be written at '{backup_url}'"}
                preferred = 0
            else:
                title = "Unsupported database version"
                text = (
                    f"The database at <br><center>'{sa_url}'</center><br> is at revision <b>{v_err.current}</b>. "
                    f"This revision requires a newer version of Spine_Database_API (and Spine Toolbox, if using "
                    f"the database through Spine Toolbox). Your current Spine_Database_API "
                    f"supports revisions only up to <b>{v_err.expected}</b>."
                    f"<p>Please upgrade Spine Toolbox (and Spine_Database_API as a consequence) to use this database. "
                    f"Alternatively, re-do your work in a compatible database version, or use a back-up if you have "
                    f"one. This database cannot be reverted back to an older version."
                    f"<ul>"
                    f"  <li>If you have installed through pip, and there is "
                    f"<a href="
                    "https://github.com/spine-tools/Spine-Toolbox#upgrade-when-using-pipx"
                    ">no newer "
                    f"Toolbox version available</a>, you need to "
                    f"<a href="
                    "https://github.com/spine-tools/Spine-Toolbox#installation-from-sources-using-git"
                    ">"
                    f"install using git</a> or wait for the next release (could be a month).</li>"
                    f"  <li>If you have grabbed a Toolbox zip-file, then you need to try to "
                    f"<a href="
                    "https://github.com/spine-tools/Spine-Toolbox#installation-with-python-and-pipx"
                    ">"
                    f"install using pip</a> or, to be safe, "
                    f"<a href="
                    "https://github.com/spine-tools/Spine-Toolbox#installation-from-sources-using-git"
                    ">"
                    f"install from sources using git</a> to get the latest Spine Toolbox."
                )
                option_to_kwargs = {}
                notes = None
                preferred = None
            return title, text, option_to_kwargs, notes, preferred

    @staticmethod
    def create_engine(sa_url, create=False, upgrade=False, backup_url="", sqlite_timeout=1800):
        if sa_url.drivername == "sqlite":
            extra_args = {"connect_args": {"timeout": sqlite_timeout}}
            if sa_url.database is None:
                extra_args["connect_args"]["check_same_thread"] = False
                extra_args["poolclass"] = StaticPool
        else:
            extra_args = {}
        try:
            engine = create_engine(sa_url, future=True, **extra_args)
            with engine.connect():
                pass
        except Exception as e:
            raise SpineDBAPIError(
                f"Could not connect to '{sa_url}': {str(e)}. "
                f"Please make sure that '{sa_url}' is a valid sqlalchemy URL."
            ) from None
        with engine.begin() as connection:
            if sa_url.drivername == "sqlite":
                connection.execute(text("BEGIN IMMEDIATE"))
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
                create_new_spine_database_from_engine(engine)
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
                dst_engine = create_engine(backup_url, future=True)
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
                with engine.begin() as connection:
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

    # pylint: disable=method-hidden
    def _create_import_alternative(self):
        """Creates the alternative to be used as default for all import operations."""
        self._import_alternative_name = "Base"

    def override_create_import_alternative(self, method):
        self._create_import_alternative = MethodType(method, self)
        self._import_alternative_name = None

    def get_table(self, tablename):
        # For tests
        return self._metadata.tables[tablename]

    def add(self, mapped_table: MappedTable, **kwargs) -> PublicItem:
        """Adds an item to the in-memory mapping.

        Example::

            with DatabaseMapping(db_url) as db_map:
                class_table = db_map.mapped_table("entity_class")
                db_map.add(class_table, name="musician")
                entity_table = db_map.mapped_table("entity")
                db_map.add(entity_table, entity_class_name="musician", name="Prince")
        """
        checked_item = mapped_table.make_candidate_item(kwargs)
        try:
            existing_item = mapped_table.find_item_by_unique_key(checked_item, fetch=False)
        except SpineDBAPIError:
            checked_item = mapped_table.add_item(checked_item)
        else:
            if not existing_item.removed:
                raise RuntimeError("logic error: item exists but no error was issued")
            existing_item.invalidate_id()
            mapped_table.remove_unique(existing_item)
            checked_item = mapped_table.add_item(checked_item)
            if (
                existing_item
                and existing_item["id"].db_id is not None
                and existing_item.status != Status.committed
                and not mapped_table.purged
                and not any(
                    self._mapped_tables[table].purged
                    for table in existing_item.ref_types()
                    if table in self._mapped_tables
                )
            ):
                checked_item.replaced_item_waiting_for_removal = existing_item
        return checked_item.public_item

    def add_by_type(self, item_type: str, **kwargs) -> PublicItem:
        return self.add(self._mapped_tables[item_type], **kwargs)

    def apply_many_by_type(self, item_type: str, method_name: str, items: list[dict], **kwargs) -> None:
        mapped_table = self._mapped_tables[item_type]
        method = getattr(self, method_name)
        for item in items:
            method(mapped_table, **kwargs, **item)

    def item(self, mapped_table: MappedTable, **kwargs) -> PublicItem:
        """Returns an item matching the keyword arguments.

        Example::

            with DatabaseMapping(db_url) as db_map:
                entity_table = db_map.mapped_table("entity")
                prince = db_map.get_item(entity_table, entity_class_name="musician", name="Prince")

        """
        item = mapped_table.find_item(kwargs)
        if not item.is_valid():
            if self._get_commit_count() != self._query_commit_count():
                self._do_fetch_more(mapped_table, offset=0, limit=None, real_commit_count=None, **kwargs)
                item = mapped_table.find_item(kwargs)
                mapped_table.reset_purging()
            else:
                raise SpineDBAPIError(f"{mapped_table.item_type} matching {kwargs} has been removed")
        return item.public_item

    def item_by_type(self, item_type: str, **kwargs) -> PublicItem:
        return self.item(self._mapped_tables[item_type], **kwargs)

    def find(self, mapped_table: MappedTable, **kwargs) -> list[PublicItem]:
        """Finds items that match the keyword arguments.

        Example::

            with DatabaseMapping(db_url) as db_map:
                entity_table = db_map.mapped_table("entity")
                entities = db_map.find(entity_table, entity_class_name="musician")
                for entity in entities:
                    print(f"{entity['name']}: {entity['description']}")
        """
        mapped_table.check_fields(kwargs, valid_types=(type(None),))
        fetched = self._fetched.get(mapped_table.item_type, -1) == self._get_commit_count()
        if not kwargs:
            if not fetched:
                self.do_fetch_all(mapped_table)
            return [i.public_item for i in mapped_table.values() if i.is_valid()]
        if not fetched:
            self._do_fetch_more(mapped_table, offset=0, limit=None, real_commit_count=None, **kwargs)
        return [i.public_item for i in mapped_table.values() if i.is_valid() and _fields_equal(i, kwargs)]

    def find_by_type(self, item_type: str, **kwargs) -> list[PublicItem]:
        return self.find(self._mapped_tables[item_type], **kwargs)

    @staticmethod
    def update(mapped_table: MappedTable, **kwargs) -> Optional[PublicItem]:
        """Updates an existing item.

        Returns the updated item or None if nothing was updated.

        Example::

            with DatabaseMapping(db_url) as db_map:
                entity_table = db_map.mapped_table("entity")
                prince = db_map.item(entity_table, entity_class_name="musician", name="Prince")
                db_map.update(
                    entity_table, id=prince["id"], name="the Artist", description="Formerly known as Prince."
                )
        """
        target_item = mapped_table.find_item(kwargs)
        merged_item, updated_fields = target_item.merge(kwargs)
        if merged_item is None:
            return None
        item_update = mapped_table.check_merged_item(merged_item, target_item, kwargs)
        mapped_table.update_item(item_update, target_item, updated_fields)
        return target_item.public_item

    def update_by_type(self, item_type: str, **kwargs) -> PublicItem:
        return self.update(self._mapped_tables[item_type], **kwargs)

    def add_or_update(self, mapped_table: MappedTable, **kwargs) -> Optional[PublicItem]:
        """Adds an item if it does not exist, otherwise updates it.

        Returns the added/updated item or None if nothing was changed.
        """
        try:
            return self.add(mapped_table, **kwargs)
        except SpineDBAPIError:
            pass
        return self.update(mapped_table, **kwargs)

    def add_or_update_by_type(self, item_type: str, **kwargs) -> PublicItem:
        return self.add_or_update(self._mapped_tables[item_type], **kwargs)

    @staticmethod
    def remove(mapped_table: MappedTable, **kwargs) -> None:
        """Removes an item matching the keyword arguments.

        Example::

            with DatabaseMapping(db_url) as db_map:
                entity_table = db_map.mapped_table("entity")
                prince = db_map.item(entity_table, entity_class_name="musician", name="Prince")
                db_map.remove_item(entity_table, id=prince["id"])
        """
        if "id" in kwargs:
            id_ = kwargs["id"]
        else:
            item = mapped_table.find_item_by_unique_key(kwargs)
            id_ = item["id"]
        item = mapped_table.item_to_remove(id_)
        removed_item = mapped_table.remove_item(item)
        if not removed_item:
            raise SpineDBAPIError("failed to remove")

    def remove_by_type(self, item_type: str, **kwargs) -> None:
        self.remove(self._mapped_tables[item_type], **kwargs)

    @staticmethod
    def restore(mapped_table: MappedTable, **kwargs) -> PublicItem:
        """Restores a previously removed item.

        Example::

            with DatabaseMapping(db_url) as db_map:
                entity_table = db_map.mapped_table("entity")
                db_map.restore(entity_table, entity_class_name="musician", name="Prince")
        """
        if "id" in kwargs:
            id_ = kwargs["id"]
        else:
            item = mapped_table.find_item_by_unique_key(kwargs)
            id_ = item["id"]
        restored_item = mapped_table.restore_item(id_)
        if not restored_item:
            raise SpineDBAPIError("failed to restore item")
        return restored_item.public_item

    def restore_by_type(self, item_type: str, **kwargs) -> PublicItem:
        return self.restore(self._mapped_tables[item_type], **kwargs)

    def get_item(self, item_type, fetch=True, skip_removed=True, **kwargs):
        """Finds and returns an item matching the arguments, or an empty dict if none found.

        This is legacy method. Use :meth:`item` instead.
        This method supports legacy item types, e.g. object and relationship_class.

        Args:
            item_type (str): One of <spine_item_types>.
            fetch (bool, optional): Whether to fetch the DB in case the item is not found in memory.
            skip_removed (bool, optional): Whether to ignore removed items.
            **kwargs: Fields and values for one the unique keys as specified for the item type
                in :ref:`db_mapping_schema`.

        Returns:
            :class:`PublicItem` or empty dict
        """
        item_type = self.real_item_type(item_type)
        mapped_table = self.mapped_table(item_type)
        mapped_table.check_fields(kwargs, valid_types=(type(None),))
        try:
            item = mapped_table.find_item(kwargs, fetch=fetch)
        except SpineDBAPIError:
            return {}
        if skip_removed and not item.is_valid():
            if fetch and self._get_commit_count() != self._query_commit_count():
                self._do_fetch_more(mapped_table, offset=0, limit=None, real_commit_count=None, **kwargs)
                try:
                    item = mapped_table.find_item(kwargs)
                except SpineDBAPIError:
                    return {}
                mapped_table.reset_purging()
            else:
                return {}
        return item.public_item

    def get_items(self, item_type, fetch=True, skip_removed=True, **kwargs):
        """Finds and returns all the items of one type.

        This is legacy method. Use :meth:`find` instead.
        This method supports legacy item types, e.g. object and relationship_class.

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
            self._do_fetch_more(mapped_table, offset=0, limit=None, real_commit_count=None, **kwargs)
        get_items = mapped_table.valid_values if skip_removed else mapped_table.values
        return [x.public_item for x in get_items() if all(x.get(k) == v for k, v in kwargs.items())]

    def item_active_in_scenario(self, item, scenario_id):
        """Checks if an item is active in a given scenario.

        Takes into account the ranks of the alternatives and figures
        out the final state of activity for the item.

        :meta private:

        Args:
            item (:class:`PublicItem`): Item value to check
            scenario_id (:class:`TempId`): The id of the scenario to test against

        Returns:
            result (bool or None): True if the item is active, False if not,
                None if no entity alternatives are specified.
        """
        scenario_table = self._mapped_tables["scenario"]
        scenario = scenario_table.find_item_by_id(scenario_id)
        entity_alternative_table = self._mapped_tables["entity_alternative"]
        entity_alternatives = self.find(entity_alternative_table, entity_id=item["id"])
        alts_ordered_by_rank = scenario["alternative_id_list"]
        alt_id_to_active = {}
        for ent_alt in entity_alternatives:
            alt_id_to_active[ent_alt["alternative_id"]] = ent_alt["active"]
        result = None
        for id_ in reversed(alts_ordered_by_rank):
            if id_ in alt_id_to_active:
                result = alt_id_to_active[id_]
                break
        return result

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

        This is legacy method. Use :meth:`add` instead.
        This method supports legacy item types, e.g. object and relationship_class.

        Args:
            item_type (str): One of <spine_item_types>.
            check (bool): Whether to check for data integrity.
            **kwargs: Fields and values as specified for the item type in :ref:`db_mapping_schema`.

        Returns:
            tuple(:class:`PublicItem` or None, str): The added item and any errors.
        """
        item_type = self.real_item_type(item_type)
        self._convert_legacy(item_type, kwargs)
        mapped_table = self.mapped_table(item_type)
        if not check:
            return mapped_table.add_item(kwargs).public_item, None
        try:
            return self.add(mapped_table, **kwargs), None
        except SpineDBAPIError as error:
            return None, str(error)

    def add_items(self, item_type, *items, check=True, strict=False):
        """Adds many items to the in-memory mapping.

        This is legacy method. Use the :meth:`add_entities`, :meth:`add_entity_classes` etc. methods instead.
        This method supports legacy item types, e.g. object and relationship_class.

        Args:
            item_type (str): One of <spine_item_types>.
            *items (Iterable(dict)): One or more :class:`dict` objects mapping fields to values of the item type,
                as specified in :ref:`db_mapping_schema`.
            check (bool): Whether to check for data integrity.
            strict (bool): Whether the method should raise :exc:`~.exception.SpineIntegrityError`
                if the insertion of one of the items violates an integrity constraint.

        Returns:
            tuple(list(:class:`PublicItem`),list(str)): items successfully added and found violations.
        """
        return self._modify_items(lambda x: self.add_item(item_type, check=check, **x), *items, strict=strict)

    def update_item(self, item_type, check=True, **kwargs):
        """Updates an item in the in-memory mapping.

        This is legacy method. Use :meth:`update` instead.
        This method supports legacy item types, e.g. object and relationship_class.

        Args:
            item_type (str): One of <spine_item_types>.
            check (bool): Whether to check for data integrity and legacy item types.
            **kwargs: Fields to update and their new values as specified for the item type in :ref:`db_mapping_schema`.

        Returns:
            tuple(:class:`PublicItem` or None, str): The updated item and any errors.
        """
        if check:
            item_type = self.real_item_type(item_type)
            self._convert_legacy(item_type, kwargs)
            mapped_table = self.mapped_table(item_type)
            try:
                return self.update(mapped_table, **kwargs), ""
            except SpineDBAPIError as error:
                return None, str(error)
        mapped_table = self.mapped_table(item_type)
        target_item = mapped_table.find_item(kwargs)
        merged_item, updated_fields = target_item.merge(kwargs)
        if merged_item is None:
            return None, ""
        candidate_item = self.make_item(item_type, **merged_item)
        candidate_item.polish()
        mapped_table.update_item(candidate_item, target_item, updated_fields)
        return target_item.public_item, ""

    def update_items(self, item_type, *items, check=True, strict=False):
        """Updates many items in the in-memory mapping.

        This is legacy method. Use the :meth:`update_entities`, :meth:`update_entity_classes` etc. methods instead.
        This method supports legacy item types, e.g. object and relationship_class.

        Args:
            item_type (str): One of <spine_item_types>.
            *items (Iterable(dict)): One or more :class:`dict` objects mapping fields to values of the item type,
                as specified in :ref:`db_mapping_schema` and including the `id`.
            check (bool): Whether to check for data integrity.
            strict (bool): Whether the method should raise :exc:`~.exception.SpineIntegrityError`
                if the update of one of the items violates an integrity constraint.

        Returns:
            tuple(list(:class:`PublicItem`),list(str)): items successfully updated and found violations.
        """
        return self._modify_items(lambda x: self.update_item(item_type, check=check, **x), *items, strict=strict)

    def add_update_item(self, item_type, check=True, **kwargs):
        """Adds an item to the in-memory mapping if it doesn't exist; otherwise updates the current one.

        This is legacy method. Use :meth:`add_or_update` instead.
        This method supports legacy item types, e.g. object and relationship_class.

        Args:
            item_type (str): One of <spine_item_types>.
            check (bool): Whether to check for data integrity.
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

        This is legacy method.
        Use :meth:`add_or_update_entities`, :meth:`add_or_update_entity_classes` etc. methods instead.
        This method supports legacy item types, e.g. object and relationship_class.

        Args:
            item_type (str): One of <spine_item_types>.
            *items (Iterable(dict)): One or more :class:`dict` objects mapping fields to values of the item type,
                as specified in :ref:`db_mapping_schema`.
            check (bool): Whether to check for data integrity.
            strict (bool): Whether the method should raise :exc:`~.exception.SpineIntegrityError`
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

        This is legacy method. Use :meth:`remove` instead.
        This method supports legacy item types, e.g. object and relationship_class.

        Args:
            item_type (str): One of <spine_item_types>.
            id_ (int): The id of the item to remove.
            check (bool): Whether to check for data integrity.

        Returns:
            tuple(:class:`PublicItem` or None, str): The removed item and any errors.
        """
        item_type = self.real_item_type(item_type)
        mapped_table = self.mapped_table(item_type)
        try:
            item = mapped_table.item_to_remove(id_)
        except SpineDBAPIError as error:
            return None, str(error)
        removed_item = mapped_table.remove_item(item)
        return (removed_item.public_item, None) if removed_item else (None, "failed to remove")

    def remove_items(self, item_type, *ids, check=True, strict=False):
        """Removes many items from the in-memory mapping.

        This is legacy method.
        Use :meth:`remove_entities`, :meth:`remove_entity_classes` etc. methods instead.
        This method supports legacy item types, e.g. object and relationship_class.

        Args:
            item_type (str): One of <spine_item_types>.
            *ids: Ids of items to be removed.
            check (bool): Whether to check for data integrity.
            strict (bool): Whether the method should raise :exc:`~.exception.SpineIntegrityError`
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

        This is legacy method. Use :meth:`restore` instead.
        This method supports legacy item types, e.g. object and relationship_class.

        Args:
            item_type (str): One of <spine_item_types>.
            id_ (int): The id of the item to restore.

        Returns:
            tuple(:class:`PublicItem` or None, str): The restored item if any and possible error.
        """
        item_type = self.real_item_type(item_type)
        mapped_table = self.mapped_table(item_type)
        restored_item = mapped_table.restore_item(id_)
        return (restored_item.public_item, None) if restored_item else (None, "failed to restore item")

    def restore_items(self, item_type, *ids):
        """Restores many previously removed items into the in-memory mapping.

        This is legacy method.
        Use :meth:`restore_entities`, :meth:`restore_entity_classes` etc. methods instead.
        This method supports legacy item types, e.g. object and relationship_class.

        Args:
            item_type (str): One of <spine_item_types>.
            *ids: Ids of items to be removed.

        Returns:
            tuple(list(:class:`PublicItem`),list(str)): items successfully restored and found violations.
        """
        return self._modify_items(lambda x: self.restore_item(item_type, x), *ids)

    def purge_items(self, item_type):
        """Removes all items of one type.

        This is legacy method. Use :meth:`remove_entity`, :meth:`remove_entity_class` etc.
        with ``id=Asterisk`` instead.
        This method supports legacy item types, e.g. object and relationship_class.

        Args:
            item_type (str): One of <spine_item_types>.

        Returns:
            bool: True if any data was removed, False otherwise.
        """
        return bool(self.remove_items(item_type, Asterisk))

    def _make_query(self, item_type, **kwargs):
        """Returns a :class:`~spinedb_api.query.Query` object to fetch items of given type.

        Args:
            item_type (str): item type
            **kwargs: query filters

        Returns:
            :class:`~spinedb_api.query.Query` or None if the mapping is closed.
        """
        sq = self._make_sq(item_type)
        qry = self._session.query(sq)
        for key, value in kwargs.items():
            if isinstance(value, tuple):
                continue
            value = resolve(value)
            if hasattr(sq.c, key):
                qry = qry.filter(getattr(sq.c, key) == value)
            elif key in (item_class := ITEM_CLASS_BY_TYPE[item_type])._external_fields:
                src_key, key = item_class._external_fields[key]
                ref_type = item_class._references[src_key]
                ref_sq = self._make_sq(ref_type)
                try:
                    qry = qry.filter(getattr(sq.c, src_key) == getattr(ref_sq.c, "id"), getattr(ref_sq.c, key) == value)
                except AttributeError:
                    pass
        return qry

    def _get_next_chunk(self, item_type, offset, limit, **kwargs):
        """Gets chunk of items from the DB.

        Returns:
            list(dict): list of dictionary items.
        """
        with self:
            qry = self._make_query(item_type, **kwargs)
            if not qry:
                return []
            if not limit:
                return [x._asdict() for x in qry]
            return [x._asdict() for x in qry.limit(limit).offset(offset)]

    def _do_fetch_more(
        self, mapped_table: MappedTable, offset: int, limit: Optional[int], real_commit_count: Optional[int], **kwargs
    ) -> list[MappedItemBase]:
        item_type = mapped_table.item_type
        ref_types = ITEM_CLASS_BY_TYPE[item_type].ref_types()
        if real_commit_count is None:
            real_commit_count = self._query_commit_count()
        if kwargs and item_type in ref_types:
            return self.do_fetch_all(self._mapped_tables[item_type], commit_count=real_commit_count)
        chunk = self._get_next_chunk(mapped_table.item_type, offset, limit, **kwargs)
        if not chunk:
            return []
        is_db_dirty = self._get_commit_count() != real_commit_count
        if is_db_dirty:
            # We need to fetch the most recent references because their ids might have changed in the DB
            item_type = mapped_table.item_type
            for ref_type in ref_types:
                if ref_type != item_type:
                    self.do_fetch_all(self._mapped_tables[ref_type], commit_count=real_commit_count)
        items = []
        new_items = []
        # Add items first
        for x in chunk:
            item, new = mapped_table.add_item_from_db(x, not is_db_dirty)
            if new:
                new_items.append(item)
            else:
                item.handle_refetch()
            items.append(item)
        # Once all items are added, add the unique key values
        # Otherwise items that refer to other items that come later in the query will be seen as corrupted
        for item in new_items:
            mapped_table.add_unique(item)
            item.become_referrer()
        return items

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
        mapped_table = self.mapped_table(item_type)
        return [
            x.public_item
            for x in self._do_fetch_more(mapped_table, offset=offset, limit=limit, real_commit_count=None, **kwargs)
        ]

    def fetch_all(self, *item_types) -> list[PublicItem]:
        """Fetches items from the DB into the in-memory mapping.
        Unlike :meth:`fetch_more`, this method fetches entire tables.

        Args:
            *item_types: One or more of <spine_item_types>. If none given, then the entire DB is fetched.
        """
        item_types = set(self.item_types()) if not item_types else set(item_types) & set(self.item_types())
        commit_count = self._query_commit_count()
        items = []
        for item_type in item_types:
            item_type = self.real_item_type(item_type)
            mapped_table = self.mapped_table(item_type)
            items += [item.public_item for item in self.do_fetch_all(mapped_table, commit_count)]
        return items

    def query(self, *entities, **kwargs):
        """Returns a :class:`~spinedb_api.query.Query` object to execute against the mapped DB.

        To perform custom ``SELECT`` statements, call this method with one or more of the documented
        subquery properties of :class:`~spinedb_api.db_mapping_query_mixin.DatabaseMappingQueryMixin` returning
        :class:`~sqlalchemy.sql.expression.Subquery` objetcs.
        For example, to select the entity class with ``id`` equal to 1::

            from spinedb_api import DatabaseMapping
            url = 'sqlite:///spine.db'
            ...
            with DatabaseMapping(url) as db_map:
                entity_record = db_map.query(db_map.entity_class_sq).filter_by(id=1).one_or_none()
                if entity_record is not None:
                    ...

        To perform more complex queries, use SQLAlchemy's :class:`~sqlalchemy.orm.query.Query` interface.
        For example, to select all entity class names and the names of their entities concatenated in a comma-separated
        string::

            from sqlalchemy import func

            with DatabaseMapping(ur) as db_map:
                classes = db_map.query(
                    db_map.entity_class_sq.c.name,
                    func.group_concat(db_map.entity_sq.c.name).label("entities")
                ).filter(
                    db_map.entity_sq.c.class_id == db_map.entity_class_sq.c.id
                ).group_by(
                    db_map.entity_class_sq.c.name
                ).all()
                for entity_class in classes:
                    print(f"{entity_class.name}: {entity_class.entities}")

        Returns:
            :class:`~sqlalchemy.orm.Query`: The resulting query.
        """
        try:
            return self._session.query(*entities, **kwargs)
        except AttributeError:
            raise SpineDBAPIError("session is None; did you forget to use the DB map inside a 'with' block?")

    def commit_session(self, comment: str, apply_compatibility_transforms: bool = True) -> CompatibilityTransformations:
        """Commits the changes from the in-memory mapping to the database.

        Args:
            comment: commit message
            apply_compatibility_transforms: if True, apply compatibility transforms

        Returns:
            compatibility transformations
        """
        if not comment:
            raise SpineDBAPIError("Commit message cannot be empty.")
        with self:
            dirty_items = self._dirty_items()
            if not dirty_items:
                raise NothingToCommit()
            commit = self._metadata.tables["commit"]
            commit_item = {"user": self.username, "date": datetime.now(timezone.utc), "comment": comment}
            connection = self._session.connection()
            try:
                # TODO: The below locks the DB in sqlite, how about other dialects?
                commit_id = connection.execute(commit.insert(), commit_item).inserted_primary_key[0]
            except DBAPIError as e:
                raise SpineDBAPIError(f"Fail to commit: {e.orig.args}") from e
            try:
                for tablename, (to_add, to_update, to_remove) in dirty_items:
                    for item in to_add + to_update + to_remove:
                        item.commit(commit_id)
                    # Remove before add, to help with keeping integrity constraints
                    if to_remove:
                        self._do_remove_items(connection, tablename, *{x["id"] for x in to_remove})
                    if to_update:
                        self._do_update_items(connection, tablename, *to_update)
                    if to_add:
                        self._do_add_items(connection, tablename, *to_add)
            except Exception as error:
                raise error
            self._session.commit()
            if self._memory:
                self._memory_dirty = True
            transformation_info = compatibility_transformations(
                self._session.connection(), apply=apply_compatibility_transforms
            )
            self._commit_count = self._query_commit_count()
        return transformation_info

    def rollback_session(self):
        """Discards all the changes from the in-memory mapping."""
        if not self._rollback():
            raise NothingToRollback()
        if self._memory:
            self._memory_dirty = False

    def has_external_commits(self):
        """Tests whether the database has had commits from other sources than this mapping.

        Returns:
            bool: True if database has external commits, False otherwise
        """
        return self._commit_count != self._query_commit_count()

    def add_ext_entity_metadata(self, *items, **kwargs):
        metadata_items = self.get_metadata_to_add_with_item_metadata_items(*items)
        self.add_items("metadata", *metadata_items, **kwargs)
        return self.add_items("entity_metadata", *items, **kwargs)

    def add_ext_parameter_value_metadata(self, *items, **kwargs):
        metadata_items = self.get_metadata_to_add_with_item_metadata_items(*items)
        self.add_items("metadata", *metadata_items, **kwargs)
        return self.add_items("parameter_value_metadata", *items, **kwargs)

    def get_metadata_to_add_with_item_metadata_items(self, *items):
        metadata_table = self._mapped_tables["metadata"]
        new_metadata = []
        for item in items:
            metadata = {"name": item["metadata_name"], "value": item["metadata_value"]}
            try:
                metadata_table.find_item(metadata)
            except SpineDBAPIError:
                new_metadata.append(metadata)
        return new_metadata

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
        for x in self._mapped_tables["entity_metadata"].valid_values():
            used_metadata_ids.add(x["metadata_id"])
        for x in self._mapped_tables["parameter_value_metadata"].valid_values():
            used_metadata_ids.add(x["metadata_id"])
        unused_metadata_ids = {x["id"] for x in self._mapped_tables["metadata"].valid_values()} - used_metadata_ids
        self.remove_items("metadata", *unused_metadata_ids)

    def get_filter_configs(self) -> list[dict]:
        """Returns the config dicts of filters applied to this database mapping."""
        return self._filter_configs


def _fields_equal(item: MappedItemBase, required: dict) -> bool:
    for key, required_value in required.items():
        item_value = item[key]
        if isinstance(required_value, (list, tuple)):
            if any(
                value_bit != required_bit if required_bit is not Asterisk else False
                for value_bit, required_bit in zip(item_value, required_value)
            ):
                return False
        else:
            if item_value != required_value:
                return False
    return True


def _pluralize(item_type: str) -> str:
    plural = {
        "entity": "entities",
        "entity_class": "entity_classes",
        "entity_metadata": "entity_metadata_items",
        "metadata": "metadata_items",
        "parameter_value_metadata": "parameter_value_metadata_items",
        "superclass_subclass": "superclass_subclasses",
    }.get(item_type)
    if plural:
        return plural
    return item_type + "s"


# Define convenience methods
for it in DatabaseMapping.item_types():
    setattr(DatabaseMapping, "add_" + it, partialmethod(DatabaseMapping.add_by_type, it))
    setattr(DatabaseMapping, "add_" + _pluralize(it), partialmethod(DatabaseMapping.apply_many_by_type, it, "add"))
    setattr(DatabaseMapping, it, partialmethod(DatabaseMapping.item_by_type, it))
    setattr(DatabaseMapping, "find_" + _pluralize(it), partialmethod(DatabaseMapping.find_by_type, it))
    setattr(DatabaseMapping, "add_or_update_" + it, partialmethod(DatabaseMapping.add_or_update_by_type, it))
    setattr(
        DatabaseMapping,
        "add_or_update_" + _pluralize(it),
        partialmethod(DatabaseMapping.apply_many_by_type, it, "add_or_update"),
    )
    setattr(DatabaseMapping, "update_" + it, partialmethod(DatabaseMapping.update_by_type, it))
    setattr(
        DatabaseMapping, "update_" + _pluralize(it), partialmethod(DatabaseMapping.apply_many_by_type, it, "update")
    )
    setattr(DatabaseMapping, "remove_" + it, partialmethod(DatabaseMapping.remove_by_type, it))
    setattr(
        DatabaseMapping, "remove_" + _pluralize(it), partialmethod(DatabaseMapping.apply_many_by_type, it, "remove")
    )
    setattr(DatabaseMapping, "restore_" + it, partialmethod(DatabaseMapping.restore_by_type, it))
    setattr(
        DatabaseMapping, "restore_" + _pluralize(it), partialmethod(DatabaseMapping.apply_many_by_type, it, "restore")
    )
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
            for f_names in factory.unique_keys
            for f_name in set(f_names) & set(factory.fields.keys())
        }

    def _kwargs(fields):
        def type_(f_dict):
            return f_dict["type"].__name__ + (", optional" if f_dict.get("optional", False) else "")

        return f"\n{padding}".join(
            [f"{f_name} ({type_(f_dict)}): {f_dict['value']}" for f_name, f_dict in fields.items()]
        )

    padding = 20 * " "
    children = {}
    for item_type in DatabaseMapping.item_types():
        factory = ITEM_CLASS_BY_TYPE[item_type]
        a = _a(item_type)
        get_kwargs = _kwargs(_uq_fields(factory))
        child = astroid.extract_node(
            f'''
            def {item_type}(self, **kwargs):
                """Returns {a} `{item_type}` item matching the keyword arguments.

                Args:
                    {get_kwargs}

                Returns:
                    :class:`PublicItem`
                """
            '''
        )
        children.setdefault("get", []).append(child)
        child = astroid.extract_node(
            f'''
            def find_{_pluralize(item_type)}(self, **kwargs):
                """Finds and returns all `{item_type}` items matching the keyword arguments.

                Args:
                    {get_kwargs}

                Returns:
                    list of :class:`PublicItem`: The items.
                """
            '''
        )
        children.setdefault("find", []).append(child)
        add_kwargs = _kwargs(factory.fields)
        child = astroid.extract_node(
            f'''
            def add_{item_type}(self, **kwargs):
                """Adds {a} `{item_type}` item to the in-memory mapping.

                Args:
                    {add_kwargs}

                Returns:
                    :class:`PublicItem`: The added item.
                """
            '''
        )
        children.setdefault("add", []).append(child)
        child = astroid.extract_node(
            f'''
            def add_{_pluralize(item_type)}(self, items):
                """Adds multiple `{item_type}` items to the in-memory mapping.

                Args:
                    items (list of dict): items to add
                """
            '''
        )
        children.setdefault("add many", []).append(child)
        update_kwargs = f"id (int): The id of the item to update.\n{padding}" + _kwargs(factory.fields)
        child = astroid.extract_node(
            f'''
            def update_{item_type}(self, **kwargs):
                """Updates {a} `{item_type}` item in the in-memory mapping.

                Args:
                    {update_kwargs}

                Returns:
                    :class:`PublicItem` or None: The updated item or None if nothing was updated.
                """
            '''
        )
        children.setdefault("update", []).append(child)
        child = astroid.extract_node(
            f'''
            def update_{_pluralize(item_type)}(self, items):
                """Updates multiple `{item_type}` items in the in-memory mapping.

                Args:
                    items (list of dict): items to update
                """
            '''
        )
        children.setdefault("update many", []).append(child)
        child = astroid.extract_node(
            f'''
            def add_or_update_{item_type}(self, **kwargs):
                """Adds {a} `{item_type}` item to the in-memory mapping if it doesn't exist;
                otherwise updates the current one.

                Args:
                    {add_kwargs}

                Returns:
                    :class:`PublicItem` or None: The added or updated item or None if nothing was added or updated.
                """
            '''
        )
        children.setdefault("add_or_update", []).append(child)
        child = astroid.extract_node(
            f'''
            def add_or_update_{_pluralize(item_type)}(self, items):
                """Adds multiple `{item_type}` items to the in-memory mapping if they don't exist;
                otherwise updates the items.

                Args:
                    items(list of dict): items to add or update
                """
            '''
        )
        children.setdefault("add_or_update many", []).append(child)
        child = astroid.extract_node(
            f'''
            def remove_{item_type}(self, **kwargs):
                """Removes {a} `{item_type}` item from the in-memory mapping.

                Args:
                    {add_kwargs}
                """
            '''
        )
        children.setdefault("remove", []).append(child)
        child = astroid.extract_node(
            f'''
            def remove_{_pluralize(item_type)}(self, items):
                """Removes multiple `{item_type}` items from the in-memory mapping.

                Args:
                    items(list of dict): items to remove
                """
            '''
        )
        children.setdefault("remove many", []).append(child)
        child = astroid.extract_node(
            f'''
            def restore_{item_type}(self, **kwargs):
                """Restores a previously removed `{item_type}` item into the in-memory mapping.

                Args:
                    {add_kwargs}

                Returns:
                    :class:`PublicItem`: The restored item.
                """
            '''
        )
        children.setdefault("restore", []).append(child)
        child = astroid.extract_node(
            f'''
            def restore_{_pluralize(item_type)}(self, items):
                """Restores multiple `{item_type}` items back into the in-memory mapping.

                Args:
                    items(list of dict): items to restore
                """
            '''
        )
        children.setdefault("restore many", []).append(child)
    for child_list in children.values():
        for child in child_list:
            child.parent = node
            node.body.append(child)
    return node


try:
    import astroid

    astroid.MANAGER.register_transform(astroid.ClassDef, _add_convenience_methods)
except ModuleNotFoundError:
    pass
