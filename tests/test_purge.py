######################################################################################################################
# Copyright (C) 2017-2022 Spine project consortium
# This file is part of Spine Database API.
# Spine Toolbox is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option)
# any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################
import pathlib
import tempfile
import unittest

from spinedb_api import DatabaseMapping
from spinedb_api.purge import purge_url


class TestPurgeUrl(unittest.TestCase):
    def setUp(self):
        self._temp_dir = tempfile.TemporaryDirectory()
        path = pathlib.Path(self._temp_dir.name, "database.sqlite")
        self._url = "sqlite:///" + str(path)

    def tearDown(self):
        self._temp_dir.cleanup()

    def test_purge_entity_classes(self):
        with DatabaseMapping(self._url, create=True) as db_map:
            db_map.add_item("entity_class", name="Soup")
            db_map.commit_session("Add test data")
        purge_url(self._url, {"alternative": False, "entity_class": True})
        with DatabaseMapping(self._url) as db_map:
            entities = db_map.query(db_map.entity_class_sq).all()
            self.assertEqual(entities, [])
            alternatives = db_map.query(db_map.alternative_sq).all()
            self.assertEqual(len(alternatives), 1)


if __name__ == '__main__':
    unittest.main()
