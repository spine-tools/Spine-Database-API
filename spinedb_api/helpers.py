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
General helper functions and classes.

:author: Manuel Marin (KTH)
:date:   15.8.2018
"""

import warnings
import os
from textwrap import fill
from sqlalchemy import create_engine, text, Table, MetaData, select, event, inspect
from sqlalchemy.ext.automap import generate_relationship
from sqlalchemy.pool import StaticPool
from sqlalchemy.exc import (
    DatabaseError,
    DBAPIError,
    IntegrityError,
    OperationalError,
    NoSuchTableError,
)
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.dialects.mysql import TINYINT, DOUBLE
from sqlalchemy.orm import interfaces
from sqlalchemy.engine import Engine
from .exception import SpineDBAPIError, SpineDBVersionError
from sqlalchemy import inspect
from alembic.config import Config
from alembic.script import ScriptDirectory
from alembic.migration import MigrationContext
from alembic.environment import EnvironmentContext
from alembic import command


# NOTE: Deactivated since foreign keys are too difficult to get right in the diff tables.
# For example, the diff_object table would need a `class_id` field and a `diff_class_id` field,
# plus a CHECK constraint that at least one of the two is NOT NULL.
# @event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    module_name = dbapi_connection.__class__.__module__
    if not module_name.lower().startswith("sqlite"):
        return
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


@compiles(TINYINT, "sqlite")
def compile_TINYINT_mysql_sqlite(element, compiler, **kw):
    """ Handles mysql TINYINT datatype as INTEGER in sqlite """
    return compiler.visit_INTEGER(element, **kw)


@compiles(DOUBLE, "sqlite")
def compile_DOUBLE_mysql_sqlite(element, compiler, **kw):
    """ Handles mysql DOUBLE datatype as REAL in sqlite """
    return compiler.visit_REAL(element, **kw)


def attr_dict(item):
    """A dictionary of all attributes of item."""
    return {c.key: getattr(item, c.key) for c in inspect(item).mapper.column_attrs}


def is_head(db_url, upgrade=False):
    """Check whether or not db_url is head.

    Args:
        db_url (str): database url
        upgrade (Bool): if True, upgrade db to head
    """
    config = Config()
    config.set_main_option("script_location", "spinedb_api:alembic")
    script = ScriptDirectory.from_config(config)
    head = script.get_current_head()
    engine = create_engine(db_url)
    with engine.connect() as connection:
        migration_context = MigrationContext.configure(connection)
        current_rev = migration_context.get_current_revision()
        if current_rev == head:
            return True
        if not upgrade:
            return False
        # Upgrade function
        def fn(rev, context):
            return script._upgrade_revs("head", rev)

        with EnvironmentContext(
            config,
            script,
            fn=fn,
            as_sql=False,
            starting_rev=None,
            destination_rev="head",
            tag=None,
        ) as environment_context:
            environment_context.configure(connection=connection, target_metadata=None)
            with environment_context.begin_transaction():
                environment_context.run_migrations()
    return True


def copy_database(
    dest_url,
    source_url,
    overwrite=True,
    upgrade=False,
    only_tables=set(),
    skip_tables=set(),
):
    """Copy the database from source_url into dest_url."""
    if not is_head(source_url, upgrade=upgrade):
        raise SpineDBVersionError(url=source_url)
    source_engine = create_engine(source_url)
    dest_engine = create_engine(dest_url)
    insp = inspect(dest_engine)
    meta = MetaData()
    meta.reflect(source_engine)
    if insp.get_table_names():
        if not overwrite:
            raise SpineDBAPIError(
                "The database at '{}' is not empty. "
                "If you want to overwrite it, please pass the argument `overwrite=True` "
                "to the function call.".format(dest_url)
            )
        meta.drop_all(dest_engine)
    source_meta = MetaData(bind=source_engine)
    dest_meta = MetaData(bind=dest_engine)
    for t in meta.sorted_tables:
        # Create table in dest
        source_table = Table(t, source_meta, autoload=True)
        source_table.create(dest_engine)
        if t.name not in ("alembic_version", "next_id"):
            # Skip tables according to `only_tables` and `skip_tables`
            if only_tables and t.name not in only_tables:
                continue
            if t.name in skip_tables:
                continue
        dest_table = Table(source_table, dest_meta, autoload=True)
        sel = select([source_table])
        result = source_engine.execute(sel)
        # Insert data from source into destination
        data = result.fetchall()
        if not data:
            continue
        ins = dest_table.insert()
        try:
            dest_engine.execute(ins, data)
        except IntegrityError as e:
            warnings.warn("Skipping table {0}: {1}".format(t.name, e.orig.args))


def custom_generate_relationship(
    base, direction, return_fn, attrname, local_cls, referred_cls, **kw
):
    if direction is interfaces.ONETOMANY:
        kw["cascade"] = "all, delete-orphan"
        kw["passive_deletes"] = True
    return generate_relationship(
        base, direction, return_fn, attrname, local_cls, referred_cls, **kw
    )


def is_unlocked(db_url, timeout=0):
    """Return True if the SQLite db_url is unlocked, after waiting at most timeout seconds.
    Otherwise return False."""
    if not db_url.startswith("sqlite"):
        return False
    try:
        engine = create_engine(db_url, connect_args={"timeout": timeout})
        engine.execute("BEGIN IMMEDIATE")
        return True
    except OperationalError:
        return False


def create_new_spine_database(db_url, for_spine_model=False):
    """Create a new Spine database at the given database url."""
    try:
        engine = create_engine(db_url)
    except DatabaseError as e:
        raise SpineDBAPIError(
            "Could not connect to '{}': {}".format(db_url, e.orig.args)
        )
    sql_list = list()
    sql = """
        CREATE TABLE IF NOT EXISTS "commit" (
            id INTEGER NOT NULL,
            comment VARCHAR(255) NOT NULL,
            date DATETIME NOT NULL,
            user VARCHAR(45),
            PRIMARY KEY (id),
            UNIQUE (id)
        );
    """
    sql_list.append(sql)
    sql = """
        CREATE TABLE IF NOT EXISTS object_class_category (
            id INTEGER NOT NULL,
            name VARCHAR(255) NOT NULL,
            description VARCHAR(255) DEFAULT NULL,
            commit_id INTEGER,
            PRIMARY KEY (id),
            FOREIGN KEY(commit_id) REFERENCES "commit" (id),
            UNIQUE(name)
        );
    """
    sql_list.append(sql)
    sql = """
        CREATE TABLE IF NOT EXISTS object_class (
            id INTEGER NOT NULL,
            name VARCHAR(255) NOT NULL,
            description VARCHAR(255) DEFAULT NULL,
            category_id INTEGER DEFAULT NULL,
            display_order INTEGER DEFAULT '99',
            display_icon INTEGER DEFAULT NULL,
            hidden INTEGER DEFAULT '0',
            commit_id INTEGER,
            PRIMARY KEY (id),
            FOREIGN KEY(commit_id) REFERENCES "commit" (id),
            FOREIGN KEY(category_id) REFERENCES object_class_category (id),
            UNIQUE(name)
        );
    """
    sql_list.append(sql)
    sql = """
        CREATE TABLE IF NOT EXISTS object_category (
            id INTEGER NOT NULL,
            object_class_id INTEGER NOT NULL,
            name VARCHAR(255) NOT NULL,
            description VARCHAR(255) DEFAULT NULL,
            commit_id INTEGER,
            PRIMARY KEY (id),
            FOREIGN KEY(object_class_id) REFERENCES object_class (id),
            FOREIGN KEY(commit_id) REFERENCES "commit" (id),
            UNIQUE(name)
        );
    """
    sql_list.append(sql)
    sql = """
        CREATE TABLE IF NOT EXISTS object (
            id INTEGER NOT NULL,
            class_id INTEGER NOT NULL,
            name VARCHAR(255) NOT NULL,
            description VARCHAR(255) DEFAULT NULL,
            category_id INTEGER DEFAULT NULL,
            commit_id INTEGER,
            PRIMARY KEY (id),
            FOREIGN KEY(commit_id) REFERENCES "commit" (id),
            FOREIGN KEY(class_id) REFERENCES object_class (id) ON DELETE CASCADE ON UPDATE CASCADE,
            FOREIGN KEY(category_id) REFERENCES object_category (id),
            UNIQUE(name)
        );
    """
    sql_list.append(sql)
    sql = """
        CREATE TABLE IF NOT EXISTS relationship_class (
            id INTEGER NOT NULL,
            dimension INTEGER NOT NULL,
            object_class_id INTEGER NOT NULL,
            name VARCHAR(155) NOT NULL,
            hidden INTEGER DEFAULT '0',
            commit_id INTEGER,
            PRIMARY KEY (id, dimension),
            FOREIGN KEY(commit_id) REFERENCES "commit" (id),
            FOREIGN KEY(object_class_id) REFERENCES object_class (id) ON UPDATE CASCADE
            CONSTRAINT relationship_class_unique_dimension_name UNIQUE (dimension, name)
        );
    """
    sql_list.append(sql)
    sql = """
        CREATE TABLE IF NOT EXISTS relationship (
            id INTEGER NOT NULL,
            dimension INTEGER NOT NULL,
            object_id INTEGER NOT NULL,
            class_id INTEGER NOT NULL,
            name VARCHAR(155) NOT NULL,
            commit_id INTEGER,
            PRIMARY KEY (id, dimension),
            FOREIGN KEY(commit_id) REFERENCES "commit" (id),
            FOREIGN KEY(class_id, dimension) REFERENCES relationship_class (id, dimension) ON DELETE CASCADE ON UPDATE CASCADE,
            FOREIGN KEY(object_id) REFERENCES object (id) ON UPDATE CASCADE
            CONSTRAINT relationship_unique_dimension_name UNIQUE (dimension, name)
        );
    """
    sql_list.append(sql)
    sql = """
        CREATE TABLE IF NOT EXISTS parameter (
            id INTEGER NOT NULL,
            name VARCHAR(155) NOT NULL,
            description VARCHAR(155) DEFAULT NULL,
            data_type VARCHAR(155) DEFAULT 'NUMERIC',
            relationship_class_id INTEGER DEFAULT NULL,
            object_class_id INTEGER DEFAULT NULL,
            can_have_time_series INTEGER DEFAULT '0',
            can_have_time_pattern INTEGER DEFAULT '1',
            can_be_stochastic INTEGER DEFAULT '0',
            default_value VARCHAR(155) DEFAULT '0',
            is_mandatory INTEGER DEFAULT '0',
            precision INTEGER DEFAULT '2',
            unit VARCHAR(155) DEFAULT NULL,
            minimum_value FLOAT DEFAULT NULL,
            maximum_value FLOAT DEFAULT NULL,
            commit_id INTEGER,
            PRIMARY KEY (id),
            FOREIGN KEY(commit_id) REFERENCES "commit" (id),
            FOREIGN KEY(object_class_id) REFERENCES object_class (id) ON DELETE CASCADE ON UPDATE CASCADE,
            CHECK (`relationship_class_id` IS NOT NULL OR `object_class_id` IS NOT NULL),
            UNIQUE(name)
        );
    """
    sql_list.append(sql)
    sql = """
        CREATE TABLE IF NOT EXISTS parameter_value (
            id INTEGER NOT NULL,
            parameter_id INTEGER NOT NULL,
            relationship_id INTEGER DEFAULT NULL,
            dummy_relationship_dimension INTEGER DEFAULT '1',
            object_id INTEGER DEFAULT NULL,
            "index" INTEGER DEFAULT '1',
            value VARCHAR(155) DEFAULT NULL,
            json VARCHAR(255) DEFAULT NULL,
            expression VARCHAR(255) DEFAULT NULL,
            time_pattern VARCHAR(155) DEFAULT NULL,
            time_series_id VARCHAR(155) DEFAULT NULL,
            stochastic_model_id VARCHAR(155) DEFAULT NULL,
            commit_id INTEGER,
            PRIMARY KEY (id),
            FOREIGN KEY(commit_id) REFERENCES "commit" (id),
            FOREIGN KEY(object_id) REFERENCES object (id) ON DELETE CASCADE ON UPDATE CASCADE,
            FOREIGN KEY(relationship_id, dummy_relationship_dimension)
                REFERENCES relationship (id, dimension) ON DELETE CASCADE ON UPDATE CASCADE,
            FOREIGN KEY(parameter_id) REFERENCES parameter (id) ON DELETE CASCADE ON UPDATE CASCADE,
            CHECK (`relationship_id` IS NOT NULL OR `object_id` IS NOT NULL),
            UNIQUE (parameter_id, object_id),
            UNIQUE (parameter_id, relationship_id)
        );
    """
    sql_list.append(sql)
    # TODO Fabiano - double creation of triggers?? to be clarified
    sql = """
        CREATE TRIGGER IF NOT EXISTS after_object_class_delete
            AFTER DELETE ON object_class
            FOR EACH ROW
        BEGIN
            DELETE FROM relationship_class
            WHERE id IN (
                SELECT id FROM relationship_class
                WHERE object_class_id = OLD.id
            );
        END
    """
    sql_list.append(sql)
    sql = """
        CREATE TRIGGER IF NOT EXISTS after_object_delete
            AFTER DELETE ON object
            FOR EACH ROW
        BEGIN
            DELETE FROM relationship
            WHERE id IN (
                SELECT id FROM relationship
                WHERE object_id = OLD.id
            );
        END
    """
    sql_list.append(sql)
    try:
        for sql in sql_list:
            engine.execute(text(sql))
    except DatabaseError as e:
        raise SpineDBAPIError(
            "Unable to create Spine database. Creation script failed: {}".format(
                e.orig.args
            )
        )
    is_head(db_url, upgrade=True)
    if for_spine_model:
        sql = """
            INSERT OR IGNORE INTO `object_class` (`id`, `name`, `description`, `category_id`, `display_order`, `display_icon`, `hidden`, `commit_id`) VALUES
            (1, 'direction', 'A flow direction', NULL, 1, 281105626296654, 0, NULL),
            (2, 'unit', 'An entity where an energy conversion process takes place', NULL, 2, 281470681805429, 0, NULL),
            (3, 'connection', 'An entity where an energy transfer takes place', NULL, 3, 280378317271233, 0, NULL),
            (4, 'storage', 'A storage', NULL, 4, 280376899531934, 0, NULL),
            (5, 'commodity', 'A commodity', NULL, 5, 281473533932880, 0, NULL),
            (6, 'node', 'An entity where an energy balance takes place', NULL, 6, 280740554077951, 0, NULL),
            (7, 'temporal_block', 'A temporal block', NULL, 7, 280376891207703, 0, NULL);
        """
        sql_list.append(sql)
        sql = """
            INSERT OR IGNORE INTO `object` (`class_id`, `name`, `description`, `category_id`, `commit_id`) VALUES
            (1, 'from_node', 'From a node, into something else', NULL, NULL),
            (1, 'to_node', 'Into a node, from something else', NULL, NULL);
        """
        sql_list.append(sql)
        sql = """
            INSERT OR IGNORE INTO `relationship_class` (`id`, `dimension`, `object_class_id`, `name`, `hidden`, `commit_id`) VALUES
            (1, 0, 2, 'unit__node__direction__temporal_block', 0, NULL),
            (1, 1, 6, 'unit__node__direction__temporal_block', 0, NULL),
            (1, 2, 1, 'unit__node__direction__temporal_block', 0, NULL),
            (1, 3, 7, 'unit__node__direction__temporal_block', 0, NULL),
            (2, 0, 3, 'connection__node__direction__temporal_block', 0, NULL),
            (2, 1, 6, 'connection__node__direction__temporal_block', 0, NULL),
            (2, 2, 1, 'connection__node__direction__temporal_block', 0, NULL),
            (2, 3, 7, 'connection__node__direction__temporal_block', 0, NULL),
            (3, 0, 6, 'node__commodity', 0, NULL),
            (3, 1, 5, 'node__commodity', 0, NULL),
            (4, 0, 2, 'unit_group__unit', 0, NULL),
            (4, 1, 2, 'unit_group__unit', 0, NULL),
            (5, 0, 5, 'commodity_group__commodity', 0, NULL),
            (5, 1, 5, 'commodity_group__commodity', 0, NULL),
            (6, 0, 6, 'node_group__node', 0, NULL),
            (6, 1, 6, 'node_group__node', 0, NULL),
            (7, 0, 2, 'unit_group__commodity_group', 0, NULL),
            (7, 1, 5, 'unit_group__commodity_group', 0, NULL),
            (8, 0, 5, 'commodity_group__node_group', 0, NULL),
            (8, 1, 6, 'commodity_group__node_group', 0, NULL),
            (9, 0, 2, 'unit__commodity', 0, NULL),
            (9, 1, 5, 'unit__commodity', 0, NULL),
            (10, 0, 2, 'unit__commodity__direction', 0, NULL),
            (10, 1, 5, 'unit__commodity__direction', 0, NULL),
            (10, 2, 1, 'unit__commodity__direction', 0, NULL),
            (11, 0, 2, 'unit__commodity__commodity', 0, NULL),
            (11, 1, 5, 'unit__commodity__commodity', 0, NULL),
            (11, 2, 5, 'unit__commodity__commodity', 0, NULL),
            (12, 0, 3, 'connection__node__node', 0, NULL),
            (12, 1, 6, 'connection__node__node', 0, NULL),
            (12, 2, 6, 'connection__node__node', 0, NULL),
            (13, 0, 6, 'node__temporal_block', 0, NULL),
            (13, 1, 7, 'node__temporal_block', 0, NULL),
            (14, 0, 4, 'storage__unit', 0, NULL),
            (14, 1, 2, 'storage__unit', 0, NULL),
            (15, 0, 4, 'storage__connection', 0, NULL),
            (15, 1, 3, 'storage__connection', 0, NULL),
            (16, 0, 4, 'storage__commodity', 0, NULL),
            (16, 1, 5, 'storage__commodity', 0, NULL);
        """
        sql_list.append(sql)
        sql = """
            INSERT OR IGNORE INTO `parameter_definition` (`id`, `name`, `object_class_id`, `default_value`, `commit_id`) VALUES
            (1, 'fom_cost', 2, 'null', NULL),
            (2, 'start_up_cost', 2, 'null', NULL),
            (3, 'shut_down_cost', 2, 'null', NULL),
            (4, 'number_of_units', 2, 1, NULL),
            (5, 'avail_factor', 2, 1, NULL),
            (6, 'min_down_time', 2, 0, NULL),
            (7, 'min_up_time', 2, 0, NULL),
            (8, 'start_datetime', 7, 'null', NULL),
            (9, 'end_datetime', 7, 'null', NULL),
            (10, 'time_slice_duration', 7, 'null', NULL),
            (11, 'demand', 6, 0, NULL),
            (12, 'online_variable_type', 2, '"integer_online_variable"', NULL),
            (13, 'fix_units_on', 2, 'null', NULL),
            (14, 'stor_state_cap', 4, 0, NULL),
            (15, 'frac_state_loss', 4, 0, NULL);
        """
        sql_list.append(sql)
        sql = """
            INSERT OR IGNORE INTO `parameter_definition` (`id`, `name`, `relationship_class_id`, `default_value`, `commit_id`) VALUES
            (1001, 'unit_conv_cap_to_flow', 9, 1, NULL),
            (1002, 'unit_capacity', 10, 'null', NULL),
            (1003, 'operating_cost', 10, 'null', NULL),
            (1004, 'vom_cost', 10, 'null', NULL),
            (1005, 'tax_net_flow', 8, 'null', NULL),
            (1006, 'tax_out_flow', 8, 'null', NULL),
            (1007, 'tax_in_flow', 8, 'null', NULL),
            (1008, 'fix_ratio_out_in', 11, 'null', NULL),
            (1009, 'fix_ratio_out_in', 12, 'null', NULL),
            (1010, 'max_ratio_out_in', 11, 'null', NULL),
            (1011, 'max_ratio_out_in', 12, 'null', NULL),
            (1012, 'min_ratio_out_in', 11, 'null', NULL),
            (1013, 'min_ratio_out_in', 12, 'null', NULL),
            (1014, 'minimum_operating_point', 9, 'null', NULL),
            (1017, 'stor_unit_discharg_eff', 15, 1, NULL),
            (1018, 'stor_unit_charg_eff', 15, 1, NULL),
            (1019, 'stor_conn_discharg_eff', 16, 1, NULL),
            (1020, 'stor_conn_charg_eff', 16, 1, NULL),
            (1021, 'max_cum_in_flow_bound', 7, 'null', NULL);
        """
        sql_list.append(sql)
        sql = """
            INSERT OR IGNORE INTO `parameter_tag` (`id`, `tag`, `description`, `commit_id`) VALUES
            (1, 'duration', 'duration in time', NULL),
            (2, 'date_time', 'a specific point in time', NULL),
            (3, 'time_series', 'time series data', NULL),
            (4, 'time_pattern', 'time patterned data', NULL);
        """
        sql_list.append(sql)
        sql = """
            INSERT OR IGNORE INTO `parameter_definition_tag` (`parameter_definition_id`, `parameter_tag_id`, `commit_id`) VALUES
            (11, 3, NULL),
            (10, 1, NULL),
            (8, 2, NULL),
            (9, 2, NULL);
        """
        sql_list.append(sql)
        try:
            for sql in sql_list:
                engine.execute(text(sql))
        except DatabaseError as e:
            raise SpineDBAPIError(
                "Unable to create Spine database. Creation script failed: {}".format(
                    e.orig.args
                )
            )
    return engine


def forward_sweep(root, func):
    """Recursively visit, using `get_children()`, the given sqlalchemy object.
    Apply `func` on every visited node."""
    current = root
    parent = {}
    children = {current: iter(current.get_children(column_collections=False))}
    while True:
        func(current)
        # Try and visit next children
        next_ = next(children[current], None)
        if next_ is not None:
            parent[next_] = current
            children[next_] = iter(next_.get_children(column_collections=False))
            current = next_
            continue
        # No (more) children, try and visit next sibling
        current_parent = parent[current]
        next_ = next(children[current_parent], None)
        if next_ is not None:
            parent[next_] = current_parent
            children[next_] = iter(next_.get_children(column_collections=False))
            current = next_
            continue
        # No (more) siblings, go back to parent
        current = current_parent
        if current == root:
            break
