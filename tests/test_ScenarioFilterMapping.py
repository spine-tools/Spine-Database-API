######################################################################################################################
# Copyright (C) 2017 - 2020 Spine project consortium
# This file is part of Spine Database API.
# Spine Database API is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser
# General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

"""
Provides :class:`.ScenarioFilterMapping`.

:author: Antti Soininen (VTT)
:date:   19.8.2020
"""
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from spinedb_api import (
    create_new_spine_database,
    DiffDatabaseMapping,
    import_alternatives,
    import_object_classes,
    import_objects,
    import_object_parameters,
    import_object_parameter_values,
    import_scenario_alternatives,
    import_scenarios,
    ScenarioFilterMapping,
)


class TestScenarioFilterMapping(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._temp_dir = TemporaryDirectory()
        cls._db_url = "sqlite" + Path(cls._temp_dir.name, "test_scenario_filter_mapping.sqlite").as_uri().lstrip("file")

    def setUp(self):
        create_new_spine_database(self._db_url)
        self._out_map = DiffDatabaseMapping(self._db_url)
        self._db_map = ScenarioFilterMapping(self._db_url)

    def tearDown(self):
        self._out_map.connection.close()
        self._db_map.connection.close()

    def test_parameter_value_sq_works_normally_without_scenarios_or_alternatives(self):
        import_object_classes(self._out_map, ["object_class"])
        import_objects(self._out_map, [("object_class", "object")])
        import_object_parameters(self._out_map, [("object_class", "parameter")])
        import_object_parameter_values(self._out_map, [("object_class", "object", "parameter", 23.0)])
        self._out_map.commit_session("Add test data")
        parameters = self._db_map.query(self._db_map.parameter_value_sq).all()
        self.assertEqual(len(parameters), 1)
        self.assertEqual(parameters[0].value, "23.0")

    def test_parameter_value_sq_with_single_active_scenario_and_alternative(self):
        import_alternatives(self._out_map, ["alternative"])
        import_object_classes(self._out_map, ["object_class"])
        import_objects(self._out_map, [("object_class", "object")])
        import_object_parameters(self._out_map, [("object_class", "parameter")])
        import_object_parameter_values(self._out_map, [("object_class", "object", "parameter", -1.0)])
        import_object_parameter_values(self._out_map, [("object_class", "object", "parameter", 23.0, "alternative")])
        import_scenarios(self._out_map, [("scenario", True)])
        import_scenario_alternatives(self._out_map, [("scenario", "alternative")])
        self._out_map.commit_session("Add test data")
        parameters = self._db_map.query(self._db_map.parameter_value_sq).all()
        self.assertEqual(len(parameters), 1)
        self.assertEqual(parameters[0].value, "23.0")

    def test_parameter_value_sq_with_single_overridden_active_scenario_and_alternative(self):
        import_alternatives(self._out_map, ["alternative"])
        import_object_classes(self._out_map, ["object_class"])
        import_objects(self._out_map, [("object_class", "object")])
        import_object_parameters(self._out_map, [("object_class", "parameter")])
        import_object_parameter_values(self._out_map, [("object_class", "object", "parameter", -1.0)])
        import_object_parameter_values(self._out_map, [("object_class", "object", "parameter", 23.0, "alternative")])
        import_scenarios(self._out_map, [("scenario", False)])
        import_scenario_alternatives(self._out_map, [("scenario", "alternative")])
        self._out_map.commit_session("Add test data")
        self._db_map.override_activate_scenarios("scenario")
        parameters = self._db_map.query(self._db_map.parameter_value_sq).all()
        self.assertEqual(len(parameters), 1)
        self.assertEqual(parameters[0].value, "23.0")

    def test_parameter_value_sq_with_single_overridden_active_scenario(self):
        import_alternatives(self._out_map, ["alternative"])
        import_object_classes(self._out_map, ["object_class"])
        import_objects(self._out_map, [("object_class", "object")])
        import_object_parameters(self._out_map, [("object_class", "parameter")])
        import_object_parameter_values(self._out_map, [("object_class", "object", "parameter", -1.0)])
        import_object_parameter_values(self._out_map, [("object_class", "object", "parameter", 23.0, "alternative")])
        import_scenarios(self._out_map, [("scenario", False)])
        import_scenario_alternatives(self._out_map, [("scenario", "alternative")])
        self._out_map.commit_session("Add test data")
        self._db_map.override_activate_scenarios("scenario")
        parameters = self._db_map.query(self._db_map.parameter_value_sq).all()
        self.assertEqual(len(parameters), 1)
        self.assertEqual(parameters[0].value, "23.0")

    def test_parameter_value_sq_with_single_overridden_active_alternative(self):
        import_alternatives(self._out_map, ["alternative"])
        import_object_classes(self._out_map, ["object_class"])
        import_objects(self._out_map, [("object_class", "object")])
        import_object_parameters(self._out_map, [("object_class", "parameter")])
        import_object_parameter_values(self._out_map, [("object_class", "object", "parameter", -1.0)])
        import_object_parameter_values(self._out_map, [("object_class", "object", "parameter", 23.0, "alternative")])
        import_scenarios(self._out_map, [("scenario", False)])
        import_scenario_alternatives(self._out_map, [("scenario", "alternative")])
        self._out_map.commit_session("Add test data")
        self._db_map.override_active_alternatives("alternative")
        parameters = self._db_map.query(self._db_map.parameter_value_sq).all()
        self.assertEqual(len(parameters), 1)
        self.assertEqual(parameters[0].value, "23.0")


if __name__ == '__main__':
    unittest.main()
