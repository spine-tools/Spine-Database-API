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
General helper functions and classes.

"""

import os
import json
import warnings
from operator import itemgetter
from urllib.parse import urlparse, urlunparse
from sqlalchemy import (
    Boolean,
    BigInteger,
    CheckConstraint,
    Column,
    DateTime,
    Float,
    ForeignKey,
    ForeignKeyConstraint,
    Integer,
    MetaData,
    PrimaryKeyConstraint,
    String,
    Table,
    Text,
    LargeBinary,
    UniqueConstraint,
    create_engine,
    false,
    func,
    inspect,
    null,
    select,
)
from sqlalchemy.ext.automap import generate_relationship
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.exc import DatabaseError, IntegrityError, OperationalError
from sqlalchemy.dialects.mysql import TINYINT, DOUBLE
from sqlalchemy.sql.expression import FunctionElement, bindparam, cast
from alembic.config import Config
from alembic.script import ScriptDirectory
from alembic.migration import MigrationContext
from alembic.environment import EnvironmentContext
from .exception import SpineDBAPIError, SpineDBVersionError

# Supported dialects and recommended dbapi. Restricted to mysql and sqlite for now:
# - sqlite works
# - mysql is trying to work
SUPPORTED_DIALECTS = {
    "mysql": "pymysql",
    "sqlite": "sqlite3",
}

UNSUPPORTED_DIALECTS = {
    "mssql": "pyodbc",
    "postgresql": "psycopg2",
    "oracle": "cx_oracle",

}

naming_convention = {
    "pk": "pk_%(table_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "uq": "uq_%(table_name)s_%(column_0N_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
}

model_meta = MetaData(naming_convention=naming_convention)

LONGTEXT_LENGTH = 2 ** 32 - 1


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
    """Handles mysql TINYINT datatype as INTEGER in sqlite."""
    return compiler.visit_INTEGER(element, **kw)


@compiles(DOUBLE, "sqlite")
def compile_DOUBLE_mysql_sqlite(element, compiler, **kw):
    """Handles mysql DOUBLE datatype as REAL in sqlite."""
    return compiler.visit_REAL(element, **kw)


class group_concat(FunctionElement):
    type = String()
    name = 'group_concat'


def _parse_group_concat_clauses(clauses):
    keys = ("group_concat_column", "order_by_column", "separator")
    d = dict(zip(keys, clauses))
    return d["group_concat_column"], d.get("order_by_column"), d.get("separator", bindparam("sep", ","))


@compiles(group_concat, "sqlite")
def compile_group_concat_sqlite(element, compiler, **kw):
    group_concat_column, _, separator = _parse_group_concat_clauses(element.clauses)
    return compiler.process(func.group_concat(group_concat_column, separator), **kw)


@compiles(group_concat, "mysql")
def compile_group_concat_mysql(element, compiler, **kw):
    group_concat_column, order_by_column, separator = _parse_group_concat_clauses(element.clauses)
    str_group_concat_column = cast(group_concat_column, String)
    if order_by_column is not None:
        str_group_concat_column = str_group_concat_column.op("ORDER BY")(order_by_column)
    return "group_concat(%s separator %s)" % (
        compiler.process(str_group_concat_column, **kw),
        compiler.process(separator, **kw),
    )


def _parse_metadata_fallback(metadata):
    yield ("unnamed", str(metadata))


def _parse_metadata(metadata):
    try:
        parsed = json.loads(metadata)
    except json.decoder.JSONDecodeError:
        yield from _parse_metadata_fallback(metadata)
        return
    if not isinstance(parsed, dict):
        yield from _parse_metadata_fallback(metadata)
        return
    for key, value in parsed.items():
        if isinstance(value, list):
            for val in value:
                yield (key, str(val))
            continue
        yield (key, str(value))


def is_head(db_url, upgrade=False):
    """Check whether or not db_url is head.

    Args:
        db_url (str): database url
        upgrade (Bool): if True, upgrade db to head
    """
    engine = create_engine(db_url)
    return is_head_engine(engine, upgrade=upgrade)


def is_head_engine(engine, upgrade=False):
    """Check whether or not engine is head.

    Args:
        engine (Engine): database engine
        upgrade (Bool): if True, upgrade db to head
    """
    config = Config()
    config.set_main_option("script_location", "spinedb_api:alembic")
    script = ScriptDirectory.from_config(config)
    head = script.get_current_head()
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
            environment_context.configure(connection=connection, target_metadata=model_meta)
            with environment_context.begin_transaction():
                environment_context.run_migrations()
    return True


def copy_database(dest_url, source_url, overwrite=True, upgrade=False, only_tables=(), skip_tables=()):
    """Copy the database from source_url into dest_url."""
    if not is_head(source_url, upgrade=upgrade):
        raise SpineDBVersionError(url=source_url)
    source_engine = create_engine(source_url)
    dest_engine = create_engine(dest_url)
    copy_database_bind(
        dest_engine,
        source_engine,
        overwrite=overwrite,
        upgrade=upgrade,
        only_tables=only_tables,
        skip_tables=skip_tables,
    )


def copy_database_bind(dest_bind, source_bind, overwrite=True, upgrade=False, only_tables=(), skip_tables=()):
    source_meta = MetaData(bind=source_bind)
    source_meta.reflect()
    if inspect(dest_bind).get_table_names():
        if not overwrite:
            raise SpineDBAPIError(
                f"The database at '{dest_bind}' is not empty. "
                "If you want to overwrite it, please pass the argument `overwrite=True` "
                "to the function call."
            )
        source_meta.drop_all(dest_bind)
    dest_meta = MetaData(bind=dest_bind)
    for source_table in source_meta.sorted_tables:
        # Create table in dest
        source_table.create(dest_bind)
        if source_table.name not in ("alembic_version", "next_id"):
            # Skip tables according to `only_tables` and `skip_tables`
            if only_tables and source_table.name not in only_tables:
                continue
            if source_table.name in skip_tables:
                continue
        dest_table = Table(source_table, dest_meta, autoload=True)
        sel = select([source_table])
        result = source_bind.execute(sel)
        # Insert data from source into destination
        data = result.fetchall()
        if not data:
            continue
        ins = dest_table.insert()
        try:
            dest_bind.execute(ins, data)
        except IntegrityError as e:
            warnings.warn("Skipping table {0}: {1}".format(source_table.name, e.orig.args))


def custom_generate_relationship(base, direction, return_fn, attrname, local_cls, referred_cls, **kw):
    """Make all relationships view only to avoid warnings."""
    kw["viewonly"] = True
    kw["cascade"] = ""
    kw["passive_deletes"] = False
    kw["sync_backref"] = False
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


def compare_schemas(left_engine, right_engine):
    """Whether or not the left and right engine have the same schema."""
    left_insp = inspect(left_engine)
    right_insp = inspect(right_engine)
    left_dict = schema_dict(left_insp)
    right_dict = schema_dict(right_insp)
    return str(left_dict) == str(right_dict)


def schema_dict(insp):
    return {
        table_name: {
            "columns": sorted(insp.get_columns(table_name), key=itemgetter("name")),
            "pk_constraint": insp.get_pk_constraint(table_name),
            "foreign_keys": sorted(insp.get_foreign_keys(table_name), key=lambda x: x["name"] or ""),
            "check_constraints": insp.get_check_constraints(table_name),
        }
        for table_name in insp.get_table_names()
    }


def is_empty(db_url):
    try:
        engine = create_engine(db_url)
    except DatabaseError as e:
        raise SpineDBAPIError("Could not connect to '{}': {}".format(db_url, e.orig.args)) from None
    insp = inspect(engine)
    if insp.get_table_names():
        return False
    return True


def create_spine_metadata():
    meta = MetaData(naming_convention=naming_convention)
    Table(
        "commit",
        meta,
        Column("id", Integer, primary_key=True),
        Column("comment", String(255), nullable=False),
        Column("date", DateTime, nullable=False),
        Column("user", String(45)),
    )
    Table(
        "alternative",
        meta,
        Column("id", Integer, primary_key=True),
        Column("name", String(255), nullable=False),
        Column("description", Text(), server_default=null()),
        Column("commit_id", Integer, ForeignKey("commit.id")),
        UniqueConstraint("name"),
    )
    Table(
        "scenario",
        meta,
        Column("id", Integer, primary_key=True),
        Column("name", String(255), nullable=False),
        Column("description", Text(), server_default=null()),
        Column("active", Boolean(name="active"), server_default=false(), nullable=False),
        Column("commit_id", Integer, ForeignKey("commit.id")),
        UniqueConstraint("name"),
    )
    Table(
        "scenario_alternative",
        meta,
        Column("id", Integer, primary_key=True),
        Column(
            "scenario_id", Integer, ForeignKey("scenario.id", onupdate="CASCADE", ondelete="CASCADE"), nullable=False
        ),
        Column(
            "alternative_id",
            Integer,
            ForeignKey("alternative.id", onupdate="CASCADE", ondelete="CASCADE"),
            nullable=False,
        ),
        Column("rank", Integer, nullable=False),
        Column("commit_id", Integer, ForeignKey("commit.id")),
        UniqueConstraint("scenario_id", "rank"),
        UniqueConstraint("scenario_id", "alternative_id"),
    )
    Table(
        "entity_class_type",
        meta,
        Column("id", Integer, primary_key=True),
        Column("name", String(255), nullable=False),
        Column("commit_id", Integer, ForeignKey("commit.id")),
    )
    Table(
        "entity_type",
        meta,
        Column("id", Integer, primary_key=True),
        Column("name", String(255), nullable=False),
        Column("commit_id", Integer, ForeignKey("commit.id")),
    )
    Table(
        "entity_class",
        meta,
        Column("id", Integer, primary_key=True),
        Column(
            "type_id",
            Integer,
            ForeignKey("entity_class_type.id", onupdate="CASCADE", ondelete="CASCADE"),
            nullable=False,
        ),
        Column("name", String(255), nullable=False),
        Column("description", Text(), server_default=null()),
        Column("display_order", Integer, server_default="99"),
        Column("display_icon", BigInteger, server_default=null()),
        Column("hidden", Integer, server_default="0"),
        Column("commit_id", Integer, ForeignKey("commit.id")),
        UniqueConstraint("id", "type_id"),
        UniqueConstraint("type_id", "name"),
    )
    Table(
        "object_class",
        meta,
        Column("entity_class_id", Integer, primary_key=True),
        Column("type_id", Integer, nullable=False),
        ForeignKeyConstraint(
            ("entity_class_id", "type_id"), ("entity_class.id", "entity_class.type_id"), ondelete="CASCADE"
        ),
        CheckConstraint("`type_id` = 1", name="type_id"),  # make sure object class can only have object type
    )
    Table(
        "relationship_class",
        meta,
        Column("entity_class_id", Integer, primary_key=True),
        Column("type_id", Integer, nullable=False),
        ForeignKeyConstraint(
            ("entity_class_id", "type_id"), ("entity_class.id", "entity_class.type_id"), ondelete="CASCADE"
        ),
        CheckConstraint("`type_id` = 2", name="type_id"),
    )
    Table(
        "relationship_entity_class",
        meta,
        Column(
            "entity_class_id",
            Integer,
            ForeignKey("relationship_class.entity_class_id", onupdate="CASCADE", ondelete="CASCADE"),
            primary_key=True,
        ),
        Column("dimension", Integer, primary_key=True),
        Column("member_class_id", Integer, nullable=False),
        Column("member_class_type_id", Integer, nullable=False),
        UniqueConstraint("entity_class_id", "dimension", "member_class_id", name="uq_relationship_entity_class"),
        ForeignKeyConstraint(("member_class_id", "member_class_type_id"), ("entity_class.id", "entity_class.type_id")),
        CheckConstraint("`member_class_type_id` != 2", name="member_class_type_id"),
    )
    Table(
        "entity",
        meta,
        Column("id", Integer, primary_key=True),
        Column("type_id", Integer, ForeignKey("entity_type.id", onupdate="CASCADE", ondelete="CASCADE")),
        Column("class_id", Integer, ForeignKey("entity_class.id", onupdate="CASCADE", ondelete="CASCADE")),
        Column("name", String(255), nullable=False),
        Column("description", Text(), server_default=null()),
        Column("commit_id", Integer, ForeignKey("commit.id")),
        UniqueConstraint("id", "class_id"),
        UniqueConstraint("id", "type_id", "class_id"),
        UniqueConstraint("class_id", "name"),
    )
    Table(
        "object",
        meta,
        Column("entity_id", Integer, primary_key=True),
        Column("type_id", Integer, nullable=False),
        ForeignKeyConstraint(("entity_id", "type_id"), ("entity.id", "entity.type_id"), ondelete="CASCADE"),
        CheckConstraint("`type_id` = 1", name="type_id"),  # make sure object can only have object type
    )
    Table(
        "relationship",
        meta,
        Column("entity_id", Integer, primary_key=True),
        Column("entity_class_id", Integer, nullable=False),
        Column("type_id", Integer, nullable=False),
        UniqueConstraint("entity_id", "entity_class_id"),
        ForeignKeyConstraint(("entity_id", "type_id"), ("entity.id", "entity.type_id"), ondelete="CASCADE"),
        CheckConstraint("`type_id` = 2", name="type_id"),
    )
    Table(
        "relationship_entity",
        meta,
        Column("entity_id", Integer, primary_key=True),
        Column("entity_class_id", Integer, nullable=False),
        Column("dimension", Integer, primary_key=True),
        Column("member_id", Integer, nullable=False),
        Column("member_class_id", Integer, nullable=False),
        ForeignKeyConstraint(
            ("member_id", "member_class_id"), ("entity.id", "entity.class_id"), onupdate="CASCADE", ondelete="CASCADE"
        ),
        ForeignKeyConstraint(
            ("entity_class_id", "dimension", "member_class_id"),
            (
                "relationship_entity_class.entity_class_id",
                "relationship_entity_class.dimension",
                "relationship_entity_class.member_class_id",
            ),
            onupdate="CASCADE",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ("entity_id", "entity_class_id"),
            ("relationship.entity_id", "relationship.entity_class_id"),
            onupdate="CASCADE",
            ondelete="CASCADE",
        ),
    )
    Table(
        "entity_group",
        meta,
        Column("id", Integer, primary_key=True),
        Column("entity_id", Integer, nullable=False),
        Column("entity_class_id", Integer, nullable=False),
        Column("member_id", Integer, nullable=False),
        UniqueConstraint("entity_id", "member_id"),
        ForeignKeyConstraint(
            ("entity_id", "entity_class_id"), ("entity.id", "entity.class_id"), onupdate="CASCADE", ondelete="CASCADE"
        ),
        ForeignKeyConstraint(
            ("member_id", "entity_class_id"), ("entity.id", "entity.class_id"), onupdate="CASCADE", ondelete="CASCADE"
        ),
    )
    Table(
        "parameter_definition",
        meta,
        Column("id", Integer, primary_key=True),
        Column(
            "entity_class_id",
            Integer,
            ForeignKey("entity_class.id", onupdate="CASCADE", ondelete="CASCADE"),
            nullable=False,
        ),
        Column("name", String(155), nullable=False),
        Column("description", Text(), server_default=null()),
        Column("default_type", String(255)),
        Column("default_value", LargeBinary(LONGTEXT_LENGTH), server_default=null()),
        Column("commit_id", Integer, ForeignKey("commit.id")),
        Column("parameter_value_list_id", Integer),
        UniqueConstraint("id", "entity_class_id"),
        UniqueConstraint("entity_class_id", "name"),
        UniqueConstraint("id", "parameter_value_list_id"),
    )
    Table(
        "parameter_tag",
        meta,
        Column("id", Integer, primary_key=True),
        Column("tag", String(155), nullable=False, unique=True),
        Column("description", Text(), server_default=null()),
        Column("commit_id", Integer, ForeignKey("commit.id")),
    )
    Table(
        "parameter_definition_tag",
        meta,
        Column("id", Integer, primary_key=True),
        Column(
            "parameter_definition_id",
            Integer,
            ForeignKey("parameter_definition.id", name="fk_parameter_tag_parameter_definition"),
            nullable=False,
        ),
        Column("parameter_tag_id", Integer, ForeignKey("parameter_tag.id"), nullable=False),
        Column("commit_id", Integer, ForeignKey("commit.id")),
        UniqueConstraint("parameter_definition_id", "parameter_tag_id", name="uq_parameter_definition_tag"),
    )
    Table(
        "parameter_value",
        meta,
        Column("id", Integer, primary_key=True),
        Column("parameter_definition_id", Integer, nullable=False),
        Column("entity_id", Integer, nullable=False),
        Column("entity_class_id", Integer, nullable=False),
        Column("type", String(255)),
        Column("value", LargeBinary(LONGTEXT_LENGTH), server_default=null()),
        Column("commit_id", Integer, ForeignKey("commit.id")),
        Column("alternative_id", Integer, ForeignKey("alternative.id"), nullable=False),
        UniqueConstraint("parameter_definition_id", "entity_id", "alternative_id", name="uq_parameter_value"),
        ForeignKeyConstraint(
            ("entity_id", "entity_class_id"), ("entity.id", "entity.class_id"), onupdate="CASCADE", ondelete="CASCADE"
        ),
        ForeignKeyConstraint(
            ("parameter_definition_id", "entity_class_id"),
            ("parameter_definition.id", "parameter_definition.entity_class_id"),
            onupdate="CASCADE",
            ondelete="CASCADE",
        ),
    )
    Table(
        "parameter_value_list",
        meta,
        Column("id", Integer, primary_key=True),
        Column("name", String(155), nullable=False),
        Column("commit_id", Integer, ForeignKey("commit.id")),
    )
    Table(
        "list_value",
        meta,
        Column("id", Integer, primary_key=True),
        Column("parameter_value_list_id", Integer, ForeignKey("parameter_value_list.id"), nullable=False),
        Column("index", Integer, nullable=False),
        Column("type", String(255)),
        Column("value", LargeBinary(LONGTEXT_LENGTH), server_default=null()),
        Column("commit_id", Integer, ForeignKey("commit.id")),
        UniqueConstraint("parameter_value_list_id", "index"),
    )
    Table(
        "tool",
        meta,
        Column("id", Integer, primary_key=True),
        Column("name", String(155), nullable=False),
        Column("description", Text(), server_default=null()),
        Column("commit_id", Integer, ForeignKey("commit.id")),
    )
    Table(
        "feature",
        meta,
        Column("id", Integer, primary_key=True),
        Column("parameter_definition_id", Integer, nullable=False),
        Column("parameter_value_list_id", Integer, nullable=False),
        Column("description", Text(), server_default=null()),
        Column("commit_id", Integer, ForeignKey("commit.id")),
        UniqueConstraint("parameter_definition_id", "parameter_value_list_id"),
        UniqueConstraint("id", "parameter_value_list_id"),
        ForeignKeyConstraint(
            ("parameter_definition_id", "parameter_value_list_id"),
            ("parameter_definition.id", "parameter_definition.parameter_value_list_id"),
            onupdate="CASCADE",
            ondelete="CASCADE",
        ),
    )
    Table(
        "tool_feature",
        meta,
        Column("id", Integer, primary_key=True),
        Column("tool_id", Integer, ForeignKey("tool.id")),
        Column("feature_id", Integer, nullable=False),
        Column("parameter_value_list_id", Integer, nullable=False),
        Column("required", Boolean(name="required"), server_default=false(), nullable=False),
        Column("commit_id", Integer, ForeignKey("commit.id")),
        UniqueConstraint("tool_id", "feature_id"),
        UniqueConstraint("id", "parameter_value_list_id"),
        ForeignKeyConstraint(
            ("feature_id", "parameter_value_list_id"),
            ("feature.id", "feature.parameter_value_list_id"),
            onupdate="CASCADE",
            ondelete="CASCADE",
        ),
    )
    Table(
        "tool_feature_method",
        meta,
        Column("id", Integer, primary_key=True),
        Column("tool_feature_id", Integer, nullable=False),
        Column("parameter_value_list_id", Integer, nullable=False),
        Column("method_index", Integer),
        Column("commit_id", Integer, ForeignKey("commit.id")),
        UniqueConstraint("tool_feature_id", "method_index"),
        ForeignKeyConstraint(
            ("tool_feature_id", "parameter_value_list_id"),
            ("tool_feature.id", "tool_feature.parameter_value_list_id"),
            onupdate="CASCADE",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ("parameter_value_list_id", "method_index"),
            ("list_value.parameter_value_list_id", "list_value.index"),
            onupdate="CASCADE",
            ondelete="CASCADE",
        ),
    )
    Table(
        "metadata",
        meta,
        Column("id", Integer, primary_key=True),
        Column("name", String(155), nullable=False),
        Column("value", String(255), nullable=False),
        Column("commit_id", Integer, ForeignKey("commit.id")),
        UniqueConstraint("name", "value"),
    )
    Table(
        "parameter_value_metadata",
        meta,
        Column("id", Integer, primary_key=True),
        Column(
            "parameter_value_id",
            Integer,
            ForeignKey("parameter_value.id", onupdate="CASCADE", ondelete="CASCADE"),
            nullable=False,
        ),
        Column(
            "metadata_id", Integer, ForeignKey("metadata.id", onupdate="CASCADE", ondelete="CASCADE"), nullable=False
        ),
        Column("commit_id", Integer, ForeignKey("commit.id")),
        UniqueConstraint("parameter_value_id", "metadata_id"),
    )
    Table(
        "entity_metadata",
        meta,
        Column("id", Integer, primary_key=True),
        Column("entity_id", Integer, ForeignKey("entity.id", onupdate="CASCADE", ondelete="CASCADE"), nullable=False),
        Column(
            "metadata_id", Integer, ForeignKey("metadata.id", onupdate="CASCADE", ondelete="CASCADE"), nullable=False
        ),
        Column("commit_id", Integer, ForeignKey("commit.id")),
        UniqueConstraint("entity_id", "metadata_id"),
    )
    Table(
        "alembic_version",
        meta,
        Column("version_num", String(32), nullable=False),
        PrimaryKeyConstraint("version_num", name="alembic_version_pkc"),
    )
    return meta


def create_new_spine_database(db_url):
    """Create a new Spine database at the given url."""
    try:
        engine = create_engine(db_url)
    except DatabaseError as e:
        raise SpineDBAPIError("Could not connect to '{}': {}".format(db_url, e.orig.args)) from None
    # Drop existing tables. This is a Spine db now...
    meta = MetaData(engine)
    meta.reflect()
    meta.drop_all()
    # Create new tables
    meta = create_spine_metadata()
    try:
        meta.create_all(engine)
        engine.execute("INSERT INTO `commit` VALUES (1, 'Create the database', CURRENT_TIMESTAMP, 'spinedb_api')")
        engine.execute("INSERT INTO alternative VALUES (1, 'Base', 'Base alternative', 1)")
        engine.execute("INSERT INTO entity_class_type VALUES (1, 'object', 1), (2, 'relationship', 1)")
        engine.execute("INSERT INTO entity_type VALUES (1, 'object', 1), (2, 'relationship', 1)")
        engine.execute("INSERT INTO alembic_version VALUES ('989fccf80441')")
    except DatabaseError as e:
        raise SpineDBAPIError("Unable to create Spine database: {}".format(e)) from None
    return engine


def _create_first_spine_database(db_url):
    """Creates a Spine database with the very first version at the given url.
    Used internally.
    """
    try:
        engine = create_engine(db_url)
    except DatabaseError as e:
        raise SpineDBAPIError("Could not connect to '{}': {}".format(db_url, e.orig.args)) from None
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
    Table(
        "object_class_category",
        meta,
        Column("id", Integer, primary_key=True),
        Column("name", String(255), nullable=False, unique=True),
        Column("description", String(255), server_default=null()),
        Column("commit_id", Integer, ForeignKey("commit.id")),
    )
    Table(
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
        CheckConstraint(
            "`relationship_class_id` IS NOT NULL OR `object_class_id` IS NOT NULL",
            name="obj_or_rel_class_id_is_not_null",
        ),
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
        CheckConstraint("`relationship_id` IS NOT NULL OR `object_id` IS NOT NULL", name="obj_or_rel_id_is_not_null"),
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
    return engine


def forward_sweep(root, fn):
    """Recursively visit, using `get_children()`, the given sqlalchemy object.
    Apply `fn` on every visited node."""
    current = root
    parent = {}
    children = {current: iter(current.get_children(column_collections=False))}
    while True:
        fn(current)
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


def get_relationship_entity_class_items(item, object_class_type):
    return [
        {
            "entity_class_id": item["id"],
            "dimension": dimension,
            "member_class_id": object_class_id,
            "member_class_type_id": object_class_type,
        }
        for dimension, object_class_id in enumerate(item["object_class_id_list"])
    ]


def get_relationship_entity_items(item, relationship_entity_type, object_entity_type):
    return [
        {
            "entity_id": item["id"],
            "type_id": relationship_entity_type,
            "entity_class_id": item["class_id"],
            "dimension": dimension,
            "member_id": object_id,
            "member_class_type_id": object_entity_type,
            "member_class_id": object_class_id,
        }
        for dimension, (object_id, object_class_id) in enumerate(
            zip(item["object_id_list"], item["object_class_id_list"])
        )
    ]


def labelled_columns(table):
    return [c.label(c.name) for c in table.columns]


class AsteriskType:
    def __repr__(self):
        return "Asterisk"


Asterisk = AsteriskType()


def fix_name_ambiguity(input_list, offset=0, prefix=""):
    """Modify repeated entries in name list by appending an increasing integer."""
    result = []
    ocurrences = {}
    for item in input_list:
        n_ocurrences = input_list.count(item)
        if n_ocurrences > 1:
            ocurrence = ocurrences.get(item, 1)
            ocurrences[item] = ocurrence + 1
            item += prefix + str(offset + ocurrence)
        result.append(item)
    return result


def vacuum(url):
    engine = create_engine(url)
    if not engine.url.drivername.startswith("sqlite"):
        return 0, "bytes"
    size_before = os.path.getsize(engine.url.database)
    engine.execute("vacuum")
    freed = size_before - os.path.getsize(engine.url.database)
    k = 0
    units = ("bytes", "KB", "MB", "GB", "TB")
    while freed > 1e3 and k < len(units):
        freed /= 1e3
        k += 1
    return freed, units[k]


def remove_credentials_from_url(url):
    """Removes username and password information from URLs.

    Args:
        url (str): URL

    Returns:
        str: sanitized URL
    """
    parsed = urlparse(url)
    if parsed.username is None:
        return url
    return urlunparse(parsed._replace(netloc=parsed.netloc.partition("@")[-1]))
