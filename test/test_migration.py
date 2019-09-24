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
from sqlalchemy import inspect
from spinedb_api.helpers import (
    create_new_spine_database,
    _create_first_spine_database,
    is_head_from_engine,
    schema_dict,
)


class TestMigration(unittest.TestCase):
    def test_upgrade(self):
        """Tests that the upgrade scripts produce the same schema as the function to create
        a Spine db anew.
        """
        left_engine = _create_first_spine_database("sqlite://")
        is_head_from_engine(left_engine, upgrade=True)
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
