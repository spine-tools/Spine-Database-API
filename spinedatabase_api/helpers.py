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
from sqlalchemy import create_engine, text, Table, MetaData, select, event
from sqlalchemy.ext.automap import generate_relationship
from sqlalchemy.pool import StaticPool
from sqlalchemy.exc import DatabaseError, IntegrityError, OperationalError
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.dialects.mysql import TINYINT, DOUBLE
from sqlalchemy.orm import interfaces
from sqlalchemy.engine import Engine
from .exception import SpineDBAPIError
from sqlalchemy import inspect
from alembic.config import Config
from alembic import command


def upgrade_to_head(db_url):
    # NOTE: this assumes alembic.ini is in the same folder as this file
    path = os.path.dirname(__file__)
    alembic_cfg = Config(os.path.join(path, "alembic.ini"))
    alembic_cfg.set_main_option("script_location", "spinedatabase_api:alembic")
    alembic_cfg.set_main_option("sqlalchemy.url", db_url)
    command.upgrade(alembic_cfg, "head")


def downgrade_to_base(db_url):
    # NOTE: this assumes alembic.ini is in the same folder as this file
    path = os.path.dirname(__file__)
    alembic_cfg = Config(os.path.join(path, "alembic.ini"))
    alembic_cfg.set_main_option("script_location", "spinedatabase_api:alembic")
    alembic_cfg.set_main_option("sqlalchemy.url", db_url)
    command.downgrade(alembic_cfg, "base")


# NOTE: Deactivated since foreign keys are too difficult to get right in the diff tables.
# For example, the diff_object table would need a `class_id` field and a `diff_class_id` field,
# plus a CHECK constraint that at least one of the two is NOT NULL.
# @event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    module_name = dbapi_connection.__class__.__module__
    if not module_name.lower().startswith('sqlite'):
        return
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()

@compiles(TINYINT, 'sqlite')
def compile_TINYINT_mysql_sqlite(element, compiler, **kw):
    """ Handles mysql TINYINT datatype as INTEGER in sqlite """
    return compiler.visit_INTEGER(element, **kw)


@compiles(DOUBLE, 'sqlite')
def compile_DOUBLE_mysql_sqlite(element, compiler, **kw):
    """ Handles mysql DOUBLE datatype as REAL in sqlite """
    return compiler.visit_REAL(element, **kw)


def attr_dict(item):
    """A dictionary of all attributes of item."""
    return {c.key: getattr(item, c.key) for c in inspect(item).mapper.column_attrs}


def copy_database(dest_url, source_url, overwrite=True, only_tables=set(), skip_tables=set()):
    """Copy the database from source_url into dest_url."""
    source_engine = create_engine(source_url)
    dest_engine = create_engine(dest_url)
    meta = MetaData()
    meta.reflect(source_engine)
    if overwrite:
        meta.drop_all(dest_engine)
    meta.create_all(dest_engine)
    # Copy tables
    source_meta = MetaData(bind=source_engine)
    dest_meta = MetaData(bind=dest_engine)
    for t in meta.sorted_tables:
        if t.name.startswith("diff"):
            continue
        if only_tables and t.name not in only_tables and t.name != "alembic_version":
            continue
        if t.name in skip_tables:
            continue
        source_table = Table(t, source_meta, autoload=True)
        dest_table = Table(t, dest_meta, autoload=True)
        sel = select([source_table])
        result = source_engine.execute(sel)
        if t.name == 'next_id':
            data = result.fetchone()
            dest_sel = select([dest_table])
            dest_result = dest_engine.execute(dest_sel)
            dest_data = dest_result.fetchone()
            if not dest_data:
                # No data in destination, just insert data from source
                ins = dest_table.insert()
                try:
                    dest_engine.execute(ins, data)
                except IntegrityError as e:
                    warnings.warn('Skipping table {0}: {1}'.format(t.name, e.orig.args))
            else:
                # Some data in destination, update with the maximum between the two
                new_data = dict()
                for key, value in data.items():
                    dest_value = dest_data[key]
                    try:
                        assert key != "user"
                        new_data[key] = max(value, dest_value)
                    except (AssertionError, TypeError):
                        new_data[key] = value
                upd = dest_table.update()
                try:
                    dest_engine.execute(upd, new_data)
                except IntegrityError as e:
                    warnings.warn('Skipping table {0}: {1}'.format(t.name, e.orig.args))
        else:
            data = result.fetchall()
            if not data:
                continue
            ins = dest_table.insert()
            try:
                dest_engine.execute(ins, data)
            except IntegrityError as e:
                warnings.warn('Skipping table {0}: {1}'.format(t.name, e.orig.args))


def custom_generate_relationship(base, direction, return_fn, attrname, local_cls, referred_cls, **kw):
    if direction is interfaces.ONETOMANY:
        kw['cascade'] = 'all, delete-orphan'
        kw['passive_deletes'] = True
    return generate_relationship(base, direction, return_fn, attrname, local_cls, referred_cls, **kw)


def is_unlocked(db_url, timeout=0):
    """Return True if the SQLite db_url is unlocked, after waiting at most timeout seconds.
    Otherwise return False."""
    if not db_url.startswith("sqlite"):
        return False
    try:
        engine = create_engine(db_url, connect_args={'timeout': timeout})
        engine.execute('BEGIN IMMEDIATE')
        return True
    except OperationalError:
        return False


def create_new_spine_database(db_url, for_spine_model=True):
    """Create a new Spine database at the given database url."""
    try:
        engine = create_engine(db_url)
    except DatabaseError as e:
        raise SpineDBAPIError("Could not connect to '{}': {}".format(db_url, e.orig.args))
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
    if for_spine_model:
        sql = """
            INSERT OR IGNORE INTO `object_class` (`id`, `name`, `description`, `category_id`, `display_order`, `display_icon`, `hidden`, `commit_id`) VALUES
            (1, 'direction', 'A flow direction, e.g., out of a node and into a unit', NULL, 1, NULL, 0, NULL),
            (2, 'unit', 'An entity where an energy conversion process takes place', NULL, 2, NULL, 0, NULL),
            (3, 'commodity', 'A commodity', NULL, 3, NULL, 0, NULL),
            (4, 'node', 'An entity where an energy balance takes place', NULL, 4, NULL, 0, NULL),
            (5, 'connection', 'An entity where an energy transfer takes place', NULL, 5, NULL, 0, NULL),
            (6, 'grid', 'A grid', NULL, 6, NULL, 0, NULL),
            (7, 'time_stage', 'A time stage', NULL, 7, NULL, 0, NULL),
            (8, 'unit_group', 'A group of units', NULL, 7, NULL, 0, NULL),
            (9, 'commodity_group', 'A group of commodities', NULL, 7, NULL, 0, NULL);
        """
        sql_list.append(sql)
        sql = """
            INSERT OR IGNORE INTO `object` (`class_id`, `name`, `description`, `category_id`, `commit_id`) VALUES
            (1, 'in', 'Into a unit, out of a node', NULL, NULL),
            (1, 'out', 'Out of a unit, into a node', NULL, NULL);
        """
        sql_list.append(sql)
        sql = """
            INSERT OR IGNORE INTO `relationship_class` (`id`, `dimension`, `object_class_id`, `name`, `hidden`, `commit_id`) VALUES
            (1, 0, 3, 'commodity__node__unit__direction', 0, NULL),
            (1, 1, 4, 'commodity__node__unit__direction', 0, NULL),
            (1, 2, 2, 'commodity__node__unit__direction', 0, NULL),
            (1, 3, 1, 'commodity__node__unit__direction', 0, NULL),
            (2, 0, 5, 'connection__from_node__to_node', 0, NULL),
            (2, 1, 4, 'connection__from_node__to_node', 0, NULL),
            (2, 2, 4, 'connection__from_node__to_node', 0, NULL),
            (3, 0, 2, 'unit__commodity', 0, NULL),
            (3, 1, 3, 'unit__commodity', 0, NULL),
            (4, 0, 2, 'unit__out_commodity_group__in_commodity_group', 0, NULL),
            (4, 1, 9, 'unit__out_commodity_group__in_commodity_group', 0, NULL),
            (4, 2, 9, 'unit__out_commodity_group__in_commodity_group', 0, NULL),
            (5, 0, 8, 'unit_group__unit', 0, NULL),
            (5, 1, 2, 'unit_group__unit', 0, NULL),
            (6, 0, 9, 'commodity_group__commodity', 0, NULL),
            (6, 1, 3, 'commodity_group__commodity', 0, NULL),
            (7, 0, 8, 'unit_group__commodity_group', 0, NULL),
            (7, 1, 9, 'unit_group__commodity_group', 0, NULL);
        """
        sql_list.append(sql)
        sql = """
            INSERT OR IGNORE INTO `parameter` (`name`, `object_class_id`, `commit_id`) VALUES
            ('avail_factor', 2, NULL),
            ('number_of_units', 2, NULL),
            ('demand', 4, NULL),
            ('trans_cap', 5, NULL),
            ('number_of_timesteps', 7, NULL);
        """
        sql_list.append(sql)
        sql = """
            INSERT OR IGNORE INTO `parameter` (`name`, `relationship_class_id`, `commit_id`) VALUES
            ('unit_capacity', 3, NULL),
            ('unit_conv_cap_to_flow', 3, NULL),
            ('conversion_cost', 3, NULL),
            ('fix_ratio_out_in_flow', 4, NULL),
            ('max_cum_in_flow_bound', 7, NULL);
        """
        sql_list.append(sql)
    # TODO Fabiano - double creation of triggers?? to be clarified
    sql = """
        CREATE TRIGGER after_object_class_delete
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
        CREATE TRIGGER after_object_delete
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
        raise SpineDBAPIError("Unable to create Spine database. Creation script failed: {}".format(e.orig.args))
    upgrade_to_head(db_url)
    return engine
