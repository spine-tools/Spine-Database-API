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
import gc
import pathlib
import tempfile
import unittest
from unittest import mock
from spinedb_api import DatabaseMapping
from spinedb_api.helpers import Asterisk, create_new_spine_database
from spinedb_api.purge import purge_url


class TestPurgeUrl(unittest.TestCase):
    def setUp(self):
        self._temp_dir = tempfile.TemporaryDirectory()
        path = pathlib.Path(self._temp_dir.name, "database.sqlite")
        self._url = "sqlite:///" + str(path)

    def tearDown(self):
        gc.collect()
        self._temp_dir.cleanup()

    def test_purge_entity_classes(self):
        with DatabaseMapping(self._url, create=True) as db_map:
            db_map.add_item("entity_class", name="Soup")
            db_map.commit_session("Add test data")
        db_map.close()
        self.assertTrue(purge_url(self._url, {"alternative": False, "entity_class": True}))
        with DatabaseMapping(self._url) as db_map:
            classes = db_map.query(db_map.entity_class_sq).all()
            self.assertEqual(classes, [])
            alternatives = db_map.query(db_map.alternative_sq).all()
            self.assertEqual(len(alternatives), 1)
        db_map.close()

    def test_logging(self):
        create_new_spine_database(self._url)
        logger = mock.MagicMock()
        logger.msg = mock.MagicMock()
        logger.msg.emit = mock.MagicMock()
        self.assertTrue(purge_url(self._url, None, logger))
        logger.msg.emit.assert_has_calls([mock.call("Purging database..."), mock.call("Database purged")])

    def test_error_logging_on_broken_url_with_credentials(self):
        broken_url = "anondb://paycheck:supers3cr3t@example.com/database"
        logger = mock.MagicMock()
        logger.msg_warning = mock.MagicMock()
        logger.msg_warning.emit = mock.MagicMock()
        self.assertFalse(purge_url(broken_url, None, logger))
        logger.msg_warning.emit.assert_called_once_with(
            "Failed to purge url <b>anondb://example.com/database</b>: "
            "Could not connect to 'anondb://paycheck:***@example.com/database': "
            "Can't load plugin: sqlalchemy.dialects:anondb. "
            "Please make sure that 'anondb://paycheck:***@example.com/database' is a valid sqlalchemy URL."
        )

    def test_purge_then_add(self):
        with DatabaseMapping(self._url, create=True) as db_map:
            db_map.remove_item("entity_class", Asterisk)
            db_map.add_item("entity_class", name="Soup")
            db_map.commit_session("Yummy")
            self.assertEqual([x["name"] for x in db_map.get_items("entity_class")], ["Soup"])
        db_map.close()
        with DatabaseMapping(self._url, create=True) as db_map:
            self.assertEqual([x["name"] for x in db_map.get_items("entity_class")], ["Soup"])
        db_map.close()

    def test_add_then_purge_then_unpurge(self):
        with DatabaseMapping(self._url, create=True) as db_map:
            db_map.add_item("entity_class", name="Soup")
            db_map.remove_item("entity_class", Asterisk)
            self.assertFalse(db_map.get_items("entity_class"))
            db_map.restore_item("entity_class", Asterisk)
            self.assertEqual([x["name"] for x in db_map.get_items("entity_class")], ["Soup"])
        db_map.close()

    def test_add_then_purge_then_add(self):
        with DatabaseMapping(self._url, create=True) as db_map:
            db_map.add_item("entity_class", name="Soup")
            db_map.remove_item("entity_class", Asterisk)
            self.assertFalse(db_map.get_items("entity_class"))
            db_map.add_item("entity_class", name="Poison")
            self.assertEqual([x["name"] for x in db_map.get_items("entity_class")], ["Poison"])
        db_map.close()

    def test_add_then_purge_then_add_then_purge_again(self):
        with DatabaseMapping(self._url, create=True) as db_map:
            db_map.add_item("entity_class", name="Soup")
            db_map.remove_item("entity_class", Asterisk)
            self.assertFalse(db_map.get_items("entity_class"))
            db_map.add_item("entity_class", name="Poison")
            db_map.remove_item("entity_class", Asterisk)
            self.assertFalse(db_map.get_items("entity_class"))
        db_map.close()

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
        db_map.close()
        with DatabaseMapping(self._url, create=True) as db_map:
            self.assertEqual([x["name"] for x in db_map.get_items("entity_class")], ["Poison"])
        db_map.close()

    def test_purge_externally(self):
        with DatabaseMapping(self._url, create=True) as db_map:
            db_map.add_item("entity_class", name="Soup")
            db_map.commit_session("Add test data")
        db_map.close()
        with DatabaseMapping(self._url, create=True) as db_map:
            db_map.fetch_all()
            self.assertEqual([x["name"] for x in db_map.get_items("entity_class")], ["Soup"])
            self.assertTrue(purge_url(self._url, {"entity_class": True}))
            self.assertEqual([x["name"] for x in db_map.get_items("entity_class")], ["Soup"])
            # Mapped items survive an external purge!
            # It is up to the client to resolve the situation.
            # For example, toolbox does it via SpineDBManager.notify_session_committed
            # which calls DatabaseMapping.reset
        db_map.close()
        with DatabaseMapping(self._url, create=True) as db_map:
            self.assertFalse(db_map.get_items("entity_class"))
        db_map.close()


if __name__ == "__main__":
    unittest.main()
