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

"""
Unit tests for migration scripts.

:author: M. Marin (KTH)
:date:   19.9.2019
"""

import pprint
import unittest
from sqlalchemy.exc import DatabaseError
from sqlalchemy import (
    create_engine,
    Table,
    Column,
    MetaData,
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
    inspect,
)
from spinedb_api import SpineDBAPIError, naming_convention
from spinedb_api.helpers import create_new_spine_database, is_head_from_engine


def create_new_spine_database_by_upgrades(db_url):
    """Creates a new Spine database at the given url by using the migration scripts."""
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
    is_head_from_engine(engine, upgrade=True)
    return engine


def schema_dict(insp):
    return {
        table_name: {
            "columns": sorted(insp.get_columns(table_name), key=lambda x: x["name"]),
            "pk_constraint": insp.get_pk_constraint(table_name),
            "foreign_keys": sorted(insp.get_foreign_keys(table_name), key=lambda x: x["name"]),
            "check_constraints": insp.get_check_constraints(table_name),
        }
        for table_name in insp.get_table_names()
    }


class TestMigration(unittest.TestCase):
    def test_upgrade(self):
        """Tests that the upgrade scripts produce the same schema as the function to create
        a Spine db anew.
        """
        left_engine = create_new_spine_database_by_upgrades("sqlite://")
        left_insp = inspect(left_engine)
        left_dict = schema_dict(left_insp)
        right_engine = create_new_spine_database("sqlite://")
        right_insp = inspect(right_engine)
        right_dict = schema_dict(right_insp)
        self.maxDiff = None
        self.assertEqual(pprint.pformat(left_dict), pprint.pformat(right_dict))

        left_ver = left_engine.execute("SELECT version_num FROM alembic_version").fetchall()
        right_ver = right_engine.execute("SELECT version_num FROM alembic_version").fetchall()
        self.assertEqual(left_ver, right_ver)

        left_ent_typ = left_engine.execute("SELECT * FROM entity_type").fetchall()
        right_ent_typ = right_engine.execute("SELECT * FROM entity_type").fetchall()
        left_ent_cls_typ = left_engine.execute("SELECT * FROM entity_class_type").fetchall()
        right_ent_cls_typ = right_engine.execute("SELECT * FROM entity_class_type").fetchall()
        self.assertEqual(left_ent_typ, right_ent_typ)
        self.assertEqual(left_ent_cls_typ, right_ent_cls_typ)
