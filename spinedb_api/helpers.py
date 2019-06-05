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
from sqlalchemy import (
    create_engine,
    text,
    Table,
    Column,
    MetaData,
    select,
    event,
    inspect,
    String,
    Integer,
    BigInteger,
    Float,
    null,
    DateTime,
    ForeignKey,
    UniqueConstraint,
    CheckConstraint,
    ForeignKeyConstraint,
)
from sqlalchemy.ext.automap import generate_relationship
from sqlalchemy.engine import reflection
from sqlalchemy.pool import StaticPool
from sqlalchemy.exc import DatabaseError, DBAPIError, IntegrityError, OperationalError, NoSuchTableError
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

# Supported dialects and recommended dbapi. Restricted to mysql and sqlite for now:
# - sqlite works
# - mysql is trying to work
SUPPORTED_DIALECTS = {
    "mysql": "pymysql",
    "sqlite": "sqlite3",
    # "mssql": "pyodbc",
    # "postgresql": "psycopg2",
    # "oracle": "cx_oracle",
}


naming_convention = {
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "uq": "uq_%(table_name)s_%(column_0N_name)s",
}

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
            config, script, fn=fn, as_sql=False, starting_rev=None, destination_rev="head", tag=None
        ) as environment_context:
            environment_context.configure(connection=connection, target_metadata=None)
            with environment_context.begin_transaction():
                environment_context.run_migrations()
    return True


def copy_database(dest_url, source_url, overwrite=True, upgrade=False, only_tables=set(), skip_tables=set()):
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


def custom_generate_relationship(base, direction, return_fn, attrname, local_cls, referred_cls, **kw):
    # NOTE: Not in use at the moment
    if direction is interfaces.ONETOMANY:
        kw["cascade"] = "all, delete-orphan"
        kw["passive_deletes"] = True
    return generate_relationship(base, direction, return_fn, attrname, local_cls, referred_cls, **kw)


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


def schemas_are_equal(left_engine, right_engine):
    """Whether or not the left and right engine have the same schema.
    For now it only checks table names, but we should also check columns definitions and more...."""
    left_inspector = reflection.Inspector.from_engine(left_engine)
    right_inspector = reflection.Inspector.from_engine(right_engine)
    if sorted(right_inspector.get_table_names()) != sorted(left_inspector.get_table_names()):
        return False
    return True


def is_empty(db_url):
    try:
        engine = create_engine(db_url)
    except DatabaseError as e:
        raise SpineDBAPIError("Could not connect to '{}': {}".format(db_url, e.orig.args))
    insp = inspect(engine)
    if insp.get_table_names():
        return False
    return True


def create_new_spine_database(db_url, upgrade=True, for_spine_model=False):
    """Create a new Spine database at the given url."""
    try:
        engine = create_engine(db_url)
    except DatabaseError as e:
        raise SpineDBAPIError("Could not connect to '{}': {}".format(db_url, e.orig.args))
    # Drop existing tables. This is a Spine db now...
    meta = MetaData(engine)
    meta.reflect()
    meta.drop_all(engine)
    # Create new tables
    meta = MetaData(naming_convention=naming_convention)
    Table(
        "commit",
        meta,
        Column("id", Integer, primary_key=True),
        Column("comment", String(255), nullable=False),
        Column("date", DateTime, nullable=False),
        Column("user", String(45)),
    )
    object_class_category = Table(
        "object_class_category",
        meta,
        Column("id", Integer, primary_key=True),
        Column("name", String(255), nullable=False, unique=True),
        Column("description", String(255), server_default=null()),
        Column("commit_id", Integer, ForeignKey("commit.id")),
    )
    object_class = Table(
        "object_class",
        meta,
        Column("id", Integer, primary_key=True),
        Column("name", String(255), nullable=False, unique=True),
        Column("description", String(255), server_default=null()),
        Column("category_id", Integer, ForeignKey("object_class_category.id"), server_default=null()),
        Column("display_order", Integer, server_default="99"),
        Column("display_icon", BigInteger, server_default=null()),
        Column("hidden", Integer, server_default="0"),
        Column("commit_id", Integer, ForeignKey("commit.id")),
    )
    Table(
        "object_category",
        meta,
        Column("id", Integer, primary_key=True),
        Column("object_class_id", Integer, ForeignKey("object_class.id")),
        Column("name", String(255), nullable=False, unique=True),
        Column("description", String(255), server_default=null()),
        Column("commit_id", Integer, ForeignKey("commit.id")),
    )
    Table(
        "object",
        meta,
        Column("id", Integer, primary_key=True),
        Column("class_id", Integer, ForeignKey("object_class.id", onupdate="CASCADE", ondelete="CASCADE")),
        Column("name", String(255), nullable=False, unique=True),
        Column("description", String(255), server_default=null()),
        Column("category_id", Integer, ForeignKey("object_category.id")),
        Column("commit_id", Integer, ForeignKey("commit.id")),
    )
    Table(
        "relationship_class",
        meta,
        Column("id", Integer, primary_key=True),
        Column("dimension", Integer, primary_key=True),
        Column("object_class_id", Integer, ForeignKey("object_class.id")),
        Column("name", String(255), nullable=False),
        Column("hidden", Integer, server_default="0"),
        Column("commit_id", Integer, ForeignKey("commit.id")),
        UniqueConstraint("dimension", "name"),
    )
    Table(
        "relationship",
        meta,
        Column("id", Integer, primary_key=True),
        Column("dimension", Integer, primary_key=True),
        Column("object_id", Integer, ForeignKey("object.id")),
        Column("class_id", Integer, nullable=False),
        Column("name", String(255), nullable=False),
        Column("commit_id", Integer, ForeignKey("commit.id")),
        UniqueConstraint("dimension", "name"),
        ForeignKeyConstraint(
            ("class_id", "dimension"),
            ("relationship_class.id", "relationship_class.dimension"),
            onupdate="CASCADE",
            ondelete="CASCADE",
        ),
    )
    Table(
        "parameter",
        meta,
        Column("id", Integer, primary_key=True),
        Column("name", String(155), nullable=False, unique=True),
        Column("description", String(155), server_default=null()),
        Column("data_type", String(155), server_default="NUMERIC"),
        Column("relationship_class_id", Integer, default=null()),
        Column(
            "object_class_id",
            Integer,
            ForeignKey("object_class.id", onupdate="CASCADE", ondelete="CASCADE"),
            server_default=null(),
        ),
        Column("can_have_time_series", Integer, server_default="0"),
        Column("can_have_time_pattern", Integer, server_default="1"),
        Column("can_be_stochastic", Integer, server_default="0"),
        Column("default_value", String(155), server_default="0"),
        Column("is_mandatory", Integer, server_default="0"),
        Column("precision", Integer, server_default="2"),
        Column("unit", String(155), server_default=null()),
        Column("minimum_value", Float, server_default=null()),
        Column("maximum_value", Float, server_default=null()),
        Column("commit_id", Integer, ForeignKey("commit.id")),
        CheckConstraint("`relationship_class_id` IS NOT NULL OR `object_class_id` IS NOT NULL"),
    )
    Table(
        "parameter_value",
        meta,
        Column("id", Integer, primary_key=True),
        Column("parameter_id", Integer, ForeignKey("parameter.id", onupdate="CASCADE", ondelete="CASCADE")),
        Column("relationship_id", Integer, server_default=null()),
        Column("dummy_relationship_dimension", Integer, server_default="0"),
        Column(
            "object_id", Integer, ForeignKey("object.id", onupdate="CASCADE", ondelete="CASCADE"), server_default=null()
        ),
        Column("index", Integer, server_default="1"),
        Column("value", String(155), server_default=null()),
        Column("json", String(255), server_default=null()),
        Column("expression", String(155), server_default=null()),
        Column("time_pattern", String(155), server_default=null()),
        Column("time_series_id", String(155), server_default=null()),
        Column("stochastic_model_id", String(155), server_default=null()),
        Column("commit_id", Integer, ForeignKey("commit.id")),
        CheckConstraint("`relationship_id` IS NOT NULL OR `object_id` IS NOT NULL"),
        UniqueConstraint("parameter_id", "object_id"),
        UniqueConstraint("parameter_id", "relationship_id"),
        ForeignKeyConstraint(
            ("relationship_id", "dummy_relationship_dimension"),
            ("relationship.id", "relationship.dimension"),
            onupdate="CASCADE",
            ondelete="CASCADE",
        ),
    )
    try:
        meta.create_all(engine)
    except DatabaseError as e:
        raise SpineDBAPIError("Unable to create Spine database: {}".format(e.orig.args))
    if not upgrade:
        return engine
    is_head(db_url, upgrade=True)
    if not for_spine_model:
        return engine
    # Add specific data structure for Spine Model
    meta = MetaData(engine, reflect=True)
    object_class = meta.tables["object_class"]
    object_ = meta.tables["object"]
    relationship_class = meta.tables["relationship_class"]
    parameter_definition = meta.tables["parameter_definition"]
    parameter_tag = meta.tables["parameter_tag"]
    parameter_definition_tag = meta.tables["parameter_definition_tag"]
    obj_cls = lambda *x: dict(zip(("id", "name", "description", "display_order", "display_icon"), x))
    obj = lambda *x: dict(zip(("class_id", "name", "description"), x))
    rel_cls = lambda *x: dict(zip(("id", "dimension", "object_class_id", "name"), x))
    obj_par_def = lambda *x: dict(zip(("id", "name", "object_class_id", "default_value"), x))
    rel_par_def = lambda *x: dict(zip(("id", "name", "relationship_class_id", "default_value"), x))
    par_tag = lambda *x: dict(zip(("id", "tag", "description"), x))
    par_def_tag = lambda *x: dict(zip(("parameter_definition_id", "parameter_tag_id"), x))
    try:
        engine.execute(
            object_class.insert(),
            [
                obj_cls(1, "direction", "A flow direction", 1, 281105626296654, 0),
                obj_cls(2, "unit", "An entity where an energy conversion process takes place", 2, 281470681805429, 0),
                obj_cls(3, "connection", "An entity where an energy transfer takes place", 3, 280378317271233, 0),
                obj_cls(4, "storage", "A storage", 4, 280376899531934, 0),
                obj_cls(5, "commodity", "A commodity", 5, 281473533932880, 0),
                obj_cls(6, "node", "An entity where an energy balance takes place", 6, 280740554077951, 0),
                obj_cls(7, "temporal_block", "A temporal block", 7, 280376891207703, 0),
            ],
        )
        engine.execute(
            object_.insert(),
            [
                obj(1, "from_node", "From a node, into something else"),
                obj(1, "to_node", "Into a node, from something else"),
            ],
        )
        engine.execute(
            relationship_class.insert(),
            [
                rel_cls(1, 0, 2, "unit__node__direction__temporal_block"),
                rel_cls(1, 1, 6, "unit__node__direction__temporal_block"),
                rel_cls(1, 2, 1, "unit__node__direction__temporal_block"),
                rel_cls(1, 3, 7, "unit__node__direction__temporal_block"),
                rel_cls(2, 0, 3, "connection__node__direction__temporal_block"),
                rel_cls(2, 1, 6, "connection__node__direction__temporal_block"),
                rel_cls(2, 2, 1, "connection__node__direction__temporal_block"),
                rel_cls(2, 3, 7, "connection__node__direction__temporal_block"),
                rel_cls(3, 0, 6, "node__commodity"),
                rel_cls(3, 1, 5, "node__commodity"),
                rel_cls(4, 0, 2, "unit_group__unit"),
                rel_cls(4, 1, 2, "unit_group__unit"),
                rel_cls(5, 0, 5, "commodity_group__commodity"),
                rel_cls(5, 1, 5, "commodity_group__commodity"),
                rel_cls(6, 0, 6, "node_group__node"),
                rel_cls(6, 1, 6, "node_group__node"),
                rel_cls(7, 0, 2, "unit_group__commodity_group"),
                rel_cls(7, 1, 5, "unit_group__commodity_group"),
                rel_cls(8, 0, 5, "commodity_group__node_group"),
                rel_cls(8, 1, 6, "commodity_group__node_group"),
                rel_cls(9, 0, 2, "unit__commodity"),
                rel_cls(9, 1, 5, "unit__commodity"),
                rel_cls(10, 0, 2, "unit__commodity__direction"),
                rel_cls(10, 1, 5, "unit__commodity__direction"),
                rel_cls(10, 2, 1, "unit__commodity__direction"),
                rel_cls(11, 0, 2, "unit__commodity__commodity"),
                rel_cls(11, 1, 5, "unit__commodity__commodity"),
                rel_cls(11, 2, 5, "unit__commodity__commodity"),
                rel_cls(12, 0, 3, "connection__node__node"),
                rel_cls(12, 1, 6, "connection__node__node"),
                rel_cls(12, 2, 6, "connection__node__node"),
                rel_cls(13, 0, 6, "node__temporal_block"),
                rel_cls(13, 1, 7, "node__temporal_block"),
                rel_cls(14, 0, 4, "storage__unit"),
                rel_cls(14, 1, 2, "storage__unit"),
                rel_cls(15, 0, 4, "storage__connection"),
                rel_cls(15, 1, 3, "storage__connection"),
                rel_cls(16, 0, 4, "storage__commodity"),
                rel_cls(16, 1, 5, "storage__commodity"),
            ],
        )
        engine.execute(
            parameter_definition.insert(),
            [
                obj_par_def(1, "fom_cost", 2, "null"),
                obj_par_def(2, "start_up_cost", 2, "null"),
                obj_par_def(3, "shut_down_cost", 2, "null"),
                obj_par_def(4, "number_of_units", 2, 1),
                obj_par_def(5, "avail_factor", 2, 1),
                obj_par_def(6, "min_down_time", 2, 0),
                obj_par_def(7, "min_up_time", 2, 0),
                obj_par_def(8, "start_datetime", 7, "null"),
                obj_par_def(9, "end_datetime", 7, "null"),
                obj_par_def(10, "time_slice_duration", 7, "null"),
                obj_par_def(11, "demand", 6, 0),
                obj_par_def(12, "online_variable_type", 2, '"integer_online_variable"'),
                obj_par_def(13, "fix_units_on", 2, "null"),
                obj_par_def(14, "stor_state_cap", 4, 0),
                obj_par_def(15, "frac_state_loss", 4, 0),
            ],
        )
        engine.execute(
            parameter_definition.insert(),
            [
                rel_par_def(1001, "unit_conv_cap_to_flow", 9, 1),
                rel_par_def(1002, "unit_capacity", 10, "null"),
                rel_par_def(1003, "operating_cost", 10, "null"),
                rel_par_def(1004, "vom_cost", 10, "null"),
                rel_par_def(1005, "tax_net_flow", 8, "null"),
                rel_par_def(1006, "tax_out_flow", 8, "null"),
                rel_par_def(1007, "tax_in_flow", 8, "null"),
                rel_par_def(1008, "fix_ratio_out_in", 11, "null"),
                rel_par_def(1009, "fix_ratio_out_in", 12, "null"),
                rel_par_def(1010, "max_ratio_out_in", 11, "null"),
                rel_par_def(1011, "max_ratio_out_in", 12, "null"),
                rel_par_def(1012, "min_ratio_out_in", 11, "null"),
                rel_par_def(1013, "min_ratio_out_in", 12, "null"),
                rel_par_def(1014, "minimum_operating_point", 9, "null"),
                rel_par_def(1017, "stor_unit_discharg_eff", 15, 1),
                rel_par_def(1018, "stor_unit_charg_eff", 15, 1),
                rel_par_def(1019, "stor_conn_discharg_eff", 16, 1),
                rel_par_def(1020, "stor_conn_charg_eff", 16, 1),
                rel_par_def(1021, "max_cum_in_flow_bound", 7, "null"),
            ],
        )
        engine.execute(
            parameter_tag.insert(),
            [
                par_tag(1, "duration", "duration in time"),
                par_tag(2, "date_time", "a specific point in time"),
                par_tag(3, "time_series", "time series data"),
                par_tag(4, "time_pattern", "time patterned data"),
            ],
        )
        engine.execute(
            parameter_definition_tag.insert(),
            [par_def_tag(11, 3), par_def_tag(10, 1), par_def_tag(8, 2), par_def_tag(9, 2)],
        )
    except DatabaseError as e:
        raise SpineDBAPIError("Unable to add specific data structure for Spine Model: {}".format(e.orig.args))
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
