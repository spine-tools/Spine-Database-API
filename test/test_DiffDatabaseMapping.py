######################################################################################################################
# Copyright (C) 2017 - 2018 Spine project consortium
# This file is part of Spine Toolbox.
# Spine Toolbox is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option)
# any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

"""
Unit tests for DiffDatabaseMapping class.

:author: P. Vennström (VTT)
:date:   29.11.2018
"""
#import sys
#sys.path.append('/spinedatabase_api')

from spinedatabase_api.diff_database_mapping import DiffDatabaseMapping
from spinedatabase_api.helpers import create_new_spine_database
import unittest
from sqlalchemy.orm import Session

class TestDiffDatabaseMapping(unittest.TestCase):

    def setUp(self):
        """Overridden method. Runs before each test.
        Create a empty in memory database
        """

        # create a in memory database
        input_db = create_new_spine_database('sqlite://')
        self.db_map = DiffDatabaseMapping("", username='IntegrationTest', create_all=False)
        self.db_map.engine = input_db
        self.db_map.engine.connect()
        
        #empty database from items
        self.db_map.engine.execute("DELETE FROM object_class")
        self.db_map.engine.execute("DELETE FROM object")
        self.db_map.engine.execute("DELETE FROM relationship_class")
        self.db_map.engine.execute("DELETE FROM relationship")
        self.db_map.engine.execute("DELETE FROM parameter")
        self.db_map.engine.execute("DELETE FROM parameter_value")
        self.db_map.engine.execute("DELETE FROM [commit]")
        
        # initialize diff db
        self.db_map.session = Session(self.db_map.engine, autoflush=False)
        self.db_map.create_mapping()
        self.db_map.create_diff_tables_and_mapping()
        self.db_map.init_next_id()


    def tearDown(self):
        """Overridden method. Runs after each test.
        Use this to free resources after a test if needed.
        """
        # close database connection
        self.db_map.close()
    
    def test_insert_many_objects_and_commiting(self):
        """Tests inserting many objects into db"""
        c_id = self.db_map.add_object_classes(*[{'name': 'testclass'}]).all()[0].id
        self.db_map.add_objects(*[{'name': str(i), 'class_id': c_id} for i in range(1001)])
        self.db_map.commit_session('test_commit')
        self.assertEqual(len(self.db_map.session.query(self.db_map.Object).all()), 1001)
    
    def test_insert_and_retrive_many_objects(self):
        """Tests inserting many objects into db and retriving them."""
        c_id = self.db_map.add_object_classes(*[{'name': 'testclass'}]).all()[0].id
        objects = self.db_map.add_objects(*[{'name': str(i), 'class_id': c_id} for i in range(1001)])
        self.assertEqual(len(objects.all()), 1001)
    
    def test_check_relationship_wide_with_multiples_of_same_object(self):
        """Tests check of valid relationship where one object repeats itself doesn't throw an error"""
        check_rel = {"name": 'unique_name', 'object_id_list': [1, 1], 'class_id': 1}
        self.db_map.check_wide_relationship(check_rel, [], {1: [1, 1]}, {1:1, 1:1})


if __name__ == '__main__':
    unittest.main()
