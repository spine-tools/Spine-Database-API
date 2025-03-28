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
""" General helper functions. """

from collections.abc import Callable, Iterable, Iterator, Sequence
import enum
from itertools import groupby
import json
from operator import itemgetter
import os
from typing import Any
import warnings
from alembic.config import Config
from alembic.environment import EnvironmentContext
from alembic.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    ForeignKeyConstraint,
    Integer,
    LargeBinary,
    MetaData,
    PrimaryKeyConstraint,
    String,
    Table,
    Text,
    UniqueConstraint,
    create_engine,
    false,
    func,
    inspect,
    null,
    select,
    text,
    true,
)
from sqlalchemy.dialects.mysql import DOUBLE, TINYINT
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DatabaseError, IntegrityError
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import FunctionElement, bindparam, cast
from sqlalchemy.sql.selectable import SelectBase
from .exception import SpineDBAPIError, SpineDBVersionError

SUPPORTED_DIALECTS = {
    "mysql": "pymysql",
    "sqlite": "sqlite3",
}
"""Currently supported dialects and recommended dbapi."""


UNSUPPORTED_DIALECTS = {
    "mssql": "pyodbc",
    "postgresql": "psycopg2",
}
"""Dialects and recommended dbapi that are not supported by DatabaseMapping but are supported by SqlAlchemy."""


naming_convention = {
    "pk": "pk_%(table_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "uq": "uq_%(table_name)s_%(column_0N_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
}

model_meta = MetaData(naming_convention=naming_convention)

LONGTEXT_LENGTH = 2**32 - 1


def name_from_elements(elements: Sequence[str]) -> str:
    """Creates an entity name by combining a list of element names into a single string."""
    if len(elements) == 1:
        return elements[0] + "__"
    return "__".join(elements)


def name_from_dimensions(dimensions: Sequence[str]) -> str:
    """Creates an entity class name by combining a list of dimension names into a single string."""
    return name_from_elements(dimensions)


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
    name = "group_concat"
    inherit_cache = True


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
    return (
        f"group_concat({compiler.process(str_group_concat_column, **kw)} "
        f"separator {compiler.process(separator, **kw)})"
    )


def _parse_metadata_fallback(metadata: Any) -> Iterator[tuple[str, str]]:
    yield ("unnamed", str(metadata))


def _parse_metadata(metadata: str) -> Iterator[tuple[str, str]]:
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


def _is_head(db_url: str, upgrade=False) -> bool:
    """Check whether the database at db_url is at the head revision."""
    engine = create_engine(db_url, future=True)
    return is_head_engine(engine, upgrade=upgrade)


def is_head_engine(engine: Engine, upgrade: bool = False) -> bool:
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


def copy_database(
    dest_url: str,
    source_url: str,
    overwrite: bool = True,
    upgrade: bool = False,
    only_tables: Sequence[str] = (),
    skip_tables: Sequence[str] = (),
) -> None:
    """Copy the database from one url to another.

    Args:
        dest_url: The destination url.
        source_url: The source url.
        overwrite: whether to overwrite the destination.
        upgrade: whether to upgrade the source to the latest Spine schema revision.
        only_tables: If given, only these tables are copied.
        skip_tables: If given, these tables are skipped.
    """
    if not _is_head(source_url, upgrade=upgrade):
        raise SpineDBVersionError(url=source_url)
    source_engine = create_engine(source_url, future=True)
    dest_engine = create_engine(dest_url, future=True)
    copy_database_bind(
        dest_engine,
        source_engine,
        overwrite=overwrite,
        only_tables=only_tables,
        skip_tables=skip_tables,
    )


def copy_database_bind(
    dest_bind: Engine,
    source_bind: Engine,
    overwrite: bool = True,
    only_tables: Sequence[str] = (),
    skip_tables: Sequence[str] = (),
) -> None:
    source_meta = MetaData()
    source_meta.reflect(bind=source_bind)
    if inspect(dest_bind).get_table_names():
        if not overwrite:
            raise SpineDBAPIError(
                f"The database at '{dest_bind}' is not empty. "
                "If you want to overwrite it, please pass the argument `overwrite=True` "
                "to the function call."
            )
        source_meta.drop_all(dest_bind)
    for table in source_meta.sorted_tables:
        table.create(dest_bind)
        if table.name not in ("alembic_version", "next_id"):
            if (only_tables and table.name not in only_tables) or table.name in skip_tables:
                continue
        with source_bind.begin() as connection:
            result = connection.execute(select(table))
            data = list(row._asdict() for row in result.fetchall())
        if not data:
            continue
        with dest_bind.begin() as connection:
            try:
                connection.execute(table.insert(), data)
            except IntegrityError as e:
                warnings.warn(f"Skipping table {table.name}: {e.orig.args}")


def compare_schemas(left_engine: Engine, right_engine: Engine) -> bool:
    left_insp = inspect(left_engine)
    right_insp = inspect(right_engine)
    left_dict = schema_dict(left_insp)
    right_dict = schema_dict(right_insp)
    return str(left_dict) == str(right_dict)


def schema_dict(insp) -> dict:
    return {
        table_name: {
            "columns": sorted(insp.get_columns(table_name), key=itemgetter("name")),
            "pk_constraint": insp.get_pk_constraint(table_name),
            "foreign_keys": sorted(insp.get_foreign_keys(table_name), key=lambda x: x["name"] or ""),
            "check_constraints": insp.get_check_constraints(table_name),
        }
        for table_name in insp.get_table_names()
    }


def is_empty(db_url: str) -> bool:
    try:
        engine = create_engine(db_url, future=True)
    except DatabaseError as e:
        raise SpineDBAPIError(f"Could not connect to '{db_url}': {e.orig.args}") from None
    insp = inspect(engine)
    if insp.get_table_names():
        return False
    return True


def get_head_alembic_version() -> str:
    config = Config()
    config.set_main_option("script_location", "spinedb_api:alembic")
    script = ScriptDirectory.from_config(config)
    return script.get_current_head()


def create_spine_metadata() -> MetaData:
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
        "entity_class",
        meta,
        Column("id", Integer, primary_key=True),
        Column("name", String(255), nullable=False),
        Column("description", Text(), server_default=null()),
        Column("display_order", Integer, server_default="99"),
        Column("display_icon", BigInteger, server_default=null()),
        Column("hidden", Integer, server_default="0"),
        Column("active_by_default", Boolean(name="active_by_default"), server_default=true(), nullable=False),
    )
    Table(
        "superclass_subclass",
        meta,
        Column("id", Integer, primary_key=True),
        Column(
            "superclass_id",
            Integer,
            ForeignKey("entity_class.id", onupdate="CASCADE", ondelete="CASCADE"),
            nullable=False,
        ),
        Column(
            "subclass_id",
            Integer,
            ForeignKey("entity_class.id", onupdate="CASCADE", ondelete="CASCADE"),
            nullable=False,
            unique=True,
        ),
    )
    Table(
        "entity_class_dimension",
        meta,
        Column(
            "entity_class_id",
            Integer,
            ForeignKey("entity_class.id", onupdate="CASCADE", ondelete="CASCADE"),
            primary_key=True,
        ),
        Column(
            "dimension_id",
            Integer,
            ForeignKey("entity_class.id", onupdate="CASCADE", ondelete="CASCADE"),
            primary_key=True,
        ),
        Column("position", Integer, primary_key=True),
        UniqueConstraint("entity_class_id", "dimension_id", "position", name="uq_entity_class_dimension"),
    )
    Table(
        "entity",
        meta,
        Column("id", Integer, primary_key=True),
        Column("class_id", Integer, ForeignKey("entity_class.id", onupdate="CASCADE", ondelete="CASCADE")),
        Column("name", String(255), nullable=False),
        Column("description", Text(), server_default=null()),
        Column("commit_id", Integer, ForeignKey("commit.id")),
        UniqueConstraint("class_id", "name"),
    )
    Table(
        "entity_element",
        meta,
        Column("entity_id", Integer, primary_key=True),
        Column("entity_class_id", Integer, nullable=False),
        Column("element_id", Integer, nullable=False),
        Column("dimension_id", Integer, nullable=False),
        Column("position", Integer, primary_key=True),
        ForeignKeyConstraint(
            ("entity_id", "entity_class_id"), ("entity.id", "entity.class_id"), onupdate="CASCADE", ondelete="CASCADE"
        ),
        ForeignKeyConstraint(
            ("element_id", "dimension_id"), ("entity.id", "entity.class_id"), onupdate="CASCADE", ondelete="CASCADE"
        ),
        ForeignKeyConstraint(
            ("entity_class_id", "dimension_id", "position"),
            (
                "entity_class_dimension.entity_class_id",
                "entity_class_dimension.dimension_id",
                "entity_class_dimension.position",
            ),
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
        "entity_alternative",
        meta,
        Column("id", Integer, primary_key=True),
        Column("entity_id", Integer, ForeignKey("entity.id", onupdate="CASCADE", ondelete="CASCADE"), nullable=False),
        Column(
            "alternative_id",
            Integer,
            ForeignKey("alternative.id", onupdate="CASCADE", ondelete="CASCADE"),
            nullable=False,
        ),
        Column("active", Boolean(name="active"), server_default=true(), nullable=False),
        Column("commit_id", Integer, ForeignKey("commit.id")),
        UniqueConstraint("entity_id", "alternative_id"),
    )
    Table(
        "display_mode",
        meta,
        Column("id", Integer, primary_key=True),
        Column("name", String(255), nullable=False, unique=True),
        Column("description", Text(), server_default=null()),
    )
    Table(
        "entity_class_display_mode",
        meta,
        Column("id", Integer, primary_key=True),
        Column(
            "display_mode_id",
            Integer,
            ForeignKey(
                "display_mode.id",
                onupdate="CASCADE",
                ondelete="CASCADE",
            ),
            nullable=False,
        ),
        Column(
            "entity_class_id",
            Integer,
            ForeignKey("entity_class.id", onupdate="CASCADE", ondelete="CASCADE"),
            nullable=False,
        ),
        Column("display_order", Integer, nullable=False),
        Column(
            "display_status",
            Enum(DisplayStatus, name="display_status_enum", create_constraint=True),
            server_default=DisplayStatus.visible.name,
            nullable=False,
        ),
        Column("display_font_color", String(6), server_default=null()),
        Column("display_background_color", String(6), server_default=null()),
        UniqueConstraint("display_mode_id", "entity_class_id"),
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
        "parameter_type",
        meta,
        Column("id", Integer, primary_key=True),
        Column(
            "parameter_definition_id",
            Integer,
            ForeignKey("parameter_definition.id", onupdate="CASCADE", ondelete="CASCADE"),
            nullable=False,
        ),
        Column("rank", Integer, nullable=False),
        Column("type", String(255), nullable=False),
        UniqueConstraint("parameter_definition_id", "type", "rank"),
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
        "entity_location",
        meta,
        Column("id", Integer, primary_key=True),
        Column("entity_id", Integer, ForeignKey("entity.id", onupdate="CASCADE", ondelete="CASCADE"), nullable=False),
        Column("lat", Float),
        Column("lon", Float),
        Column("alt", Float),
        Column("shape_name", String(155)),
        Column("shape_blob", Text, server_default=null()),
        UniqueConstraint("entity_id"),
    )
    Table(
        "alembic_version",
        meta,
        Column("version_num", String(32), nullable=False),
        PrimaryKeyConstraint("version_num", name="alembic_version_pkc"),
    )
    return meta


def create_new_spine_database(db_url: str) -> Engine:
    """Create a new Spine database at the given url."""
    try:
        engine = create_engine(db_url, future=True)
    except DatabaseError as e:
        raise SpineDBAPIError(f"Could not connect to '{db_url}': {e.orig.args}") from None
    create_new_spine_database_from_engine(engine)
    return engine


def create_new_spine_database_from_engine(engine: Engine) -> None:
    # Drop existing tables. This is a Spine db now...
    meta = MetaData()
    meta.reflect(engine)
    meta.drop_all(engine)
    # Create new tables
    meta = create_spine_metadata()
    version = get_head_alembic_version()
    try:
        meta.create_all(engine)
        with engine.begin() as connection:
            connection.execute(
                text("INSERT INTO `commit` VALUES (1, 'Create the database', CURRENT_TIMESTAMP, 'spinedb_api')")
            )
            connection.execute(text("INSERT INTO alternative VALUES (1, 'Base', 'Base alternative', 1)"))
            connection.execute(text(f"INSERT INTO alembic_version VALUES ('{version}')"))
    except DatabaseError as e:
        raise SpineDBAPIError(f"Unable to create Spine database: {e}") from None


def _create_first_spine_database(db_url: str) -> Engine:
    """Creates a Spine database with the very first version at the given url."""
    try:
        engine = create_engine(db_url, future=True)
    except DatabaseError as e:
        raise SpineDBAPIError(f"Could not connect to '{db_url}': {e.orig.args}") from None
    # Drop existing tables. This is a Spine db now...
    meta = MetaData()
    meta.reflect(engine)
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
        raise SpineDBAPIError(f"Unable to create Spine database: {e.orig.args}") from e
    return engine


def forward_sweep(root: SelectBase, fn: Callable[[SelectBase, ...], None], *args) -> None:
    """Recursively visit, using `get_children()`, the given sqlalchemy object.
    Apply `fn` on every visited node."""
    current = root
    parent = {}
    children = {current: iter(current.get_children(column_collections=False))}
    while True:
        fn(current, *args)
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
        if current is root:
            break


def labelled_columns(table: Table) -> list:
    return [c.label(c.name) for c in table.columns]


class AsteriskType:
    def __repr__(self):
        return "Asterisk"


Asterisk = AsteriskType()


def fix_name_ambiguity(input_list: Sequence[str], offset: int = 0, prefix: str = "") -> list[str]:
    """Modify repeated entries in name list by appending an increasing integer."""
    result = []
    occurrences = {}
    for item in input_list:
        n_occurrences = input_list.count(item)
        if n_occurrences > 1:
            occurrence = occurrences.get(item, 1)
            occurrences[item] = occurrence + 1
            item += prefix + str(offset + occurrence)
        result.append(item)
    return result


def vacuum(url: str) -> tuple[int, str]:
    engine = create_engine(url, future=True)
    if not engine.url.drivername.startswith("sqlite"):
        return 0, "bytes"
    size_before = os.path.getsize(engine.url.database)
    with engine.begin() as connection:
        connection.execute(text("vacuum"))
    freed = size_before - os.path.getsize(engine.url.database)
    k = 0
    units = ("bytes", "KB", "MB", "GB", "TB")
    while freed > 1e3 and k < len(units):
        freed /= 1e3
        k += 1
    return freed, units[k]


def remove_credentials_from_url(url: str) -> str:
    """Removes username and password information from URLs."""
    if "@" not in url:
        return url
    head, tail = url.rsplit("@", maxsplit=1)
    scheme, _ = head.split("://", maxsplit=1)
    return scheme + "://" + tail


def group_consecutive(list_of_numbers: Iterable[int]) -> Iterator[tuple[int, int]]:
    for _k, g in groupby(enumerate(sorted(list_of_numbers)), lambda x: x[0] - x[1]):
        group = list(map(itemgetter(1), g))
        yield group[0], group[-1]


_TRUTHS = {s.casefold() for s in ("yes", "true", "y", "t", "1")}
_FALSES = {s.casefold() for s in ("no", "false", "n", "f", "0")}


def string_to_bool(string: str) -> bool:
    """Converts string to boolean.

    Recognizes "yes", "true", "y", "t" and "1" as True, "no", "false", "n", "f" and "0" as False.
    Case-insensitive.
    Raises Value error if value is not recognized.
    """
    string = string.casefold()
    if string in _TRUTHS:
        return True
    if string in _FALSES:
        return False
    raise ValueError(string)


@enum.unique
class DisplayStatus(enum.Enum):
    """Custom enum for entity class display status."""

    visible = enum.auto()
    hidden = enum.auto()
    greyed_out = enum.auto()
