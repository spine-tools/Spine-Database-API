######################################################################################################################
# Copyright (C) 2017-2022 Spine project consortium
# Copyright Spine Database API contributors
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
from spinedb_api.helpers import Asterisk


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
            classes = db_map.query(db_map.entity_class_sq).all()
            self.assertEqual(classes, [])
            alternatives = db_map.query(db_map.alternative_sq).all()
            self.assertEqual(len(alternatives), 1)

    def test_purge_then_add(self):
        with DatabaseMapping(self._url, create=True) as db_map:
            db_map.remove_item("entity_class", Asterisk)
            db_map.add_item("entity_class", name="Soup")
            db_map.commit_session("Yummy")
            self.assertEqual([x["name"] for x in db_map.get_items("entity_class")], ["Soup"])
        with DatabaseMapping(self._url, create=True) as db_map:
            self.assertEqual([x["name"] for x in db_map.get_items("entity_class")], ["Soup"])

    def test_add_then_purge_then_unpurge(self):
        with DatabaseMapping(self._url, create=True) as db_map:
            db_map.add_item("entity_class", name="Soup")
            db_map.remove_item("entity_class", Asterisk)
            self.assertFalse(db_map.get_items("entity_class"))
            db_map.restore_item("entity_class", Asterisk)
            self.assertEqual([x["name"] for x in db_map.get_items("entity_class")], ["Soup"])

    def test_add_then_purge_then_add(self):
        with DatabaseMapping(self._url, create=True) as db_map:
            db_map.add_item("entity_class", name="Soup")
            db_map.remove_item("entity_class", Asterisk)
            self.assertFalse(db_map.get_items("entity_class"))
            db_map.add_item("entity_class", name="Poison")
            self.assertEqual([x["name"] for x in db_map.get_items("entity_class")], ["Poison"])

    def test_add_then_purge_then_add_then_purge_again(self):
        with DatabaseMapping(self._url, create=True) as db_map:
            db_map.add_item("entity_class", name="Soup")
            db_map.remove_item("entity_class", Asterisk)
            self.assertFalse(db_map.get_items("entity_class"))
            db_map.add_item("entity_class", name="Poison")
            db_map.remove_item("entity_class", Asterisk)
            self.assertFalse(db_map.get_items("entity_class"))

    def test_dont_keep_purging_after_commit(self):
        """Tests that if I purge and then commit, then add more stuff then commit again, the stuff I added
        after the first commit is not purged afterwards. In other words, the commit resets the purge need."""
        with DatabaseMapping(self._url, create=True) as db_map:
            db_map.add_item("entity_class", name="Soup")
            db_map.remove_item("entity_class", Asterisk)
            db_map.commit_session("Yummy but nope")
            self.assertFalse(db_map.get_items("entity_class"))
            db_map.add_item("entity_class", name="Poison")
            db_map.commit_session("Deadly")
            self.assertEqual([x["name"] for x in db_map.get_items("entity_class")], ["Poison"])
        with DatabaseMapping(self._url, create=True) as db_map:
            self.assertEqual([x["name"] for x in db_map.get_items("entity_class")], ["Poison"])

    def test_purge_externally(self):
        with DatabaseMapping(self._url, create=True) as db_map:
            db_map.add_item("entity_class", name="Soup")
            db_map.commit_session("Add test data")
        with DatabaseMapping(self._url, create=True) as db_map:
            db_map.fetch_all()
            self.assertEqual([x["name"] for x in db_map.get_items("entity_class")], ["Soup"])
            purge_url(self._url, {"entity_class": True})
            self.assertEqual([x["name"] for x in db_map.get_items("entity_class")], ["Soup"])
            # Mapped items survive an external purge!
            # It is up to the client to resolve the situation.
            # For example, toolbox does it via SpineDBManager.notify_session_committed
            # which calls DatabaseMapping.reset
        with DatabaseMapping(self._url, create=True) as db_map:
            self.assertFalse(db_map.get_items("entity_class"))


if __name__ == "__main__":
    unittest.main()
