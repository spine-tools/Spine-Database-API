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
Unit tests for ``alternative_value_filter`` module.

:author: Antti Soininen (VTT)
:date:   21.8.2020
"""
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from sqlalchemy.engine.url import URL
from spinedb_api import (
    apply_alternative_value_filter,
    create_new_spine_database,
    DatabaseMapping,
    DiffDatabaseMapping,
    import_alternatives,
    import_object_classes,
    import_object_parameter_values,
    import_object_parameters,
    import_objects,
    import_relationship_classes,
    import_relationship_parameter_values,
    import_relationship_parameters,
    import_relationships,
    import_scenario_alternatives,
    import_scenarios,
)


class TestAlternativeValueFilter(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._temp_dir = TemporaryDirectory()
        cls._db_url = URL("sqlite", database=Path(cls._temp_dir.name, "test_scenario_filter_mapping.sqlite").as_posix())

    def setUp(self):
        create_new_spine_database(self._db_url)
        self._out_map = DiffDatabaseMapping(self._db_url)
        self._db_map = DatabaseMapping(self._db_url)
        self._diff_db_map = DiffDatabaseMapping(self._db_url)

    def tearDown(self):
        self._out_map.connection.close()
        self._db_map.connection.close()
        self._diff_db_map.connection.close()

    def test_apply_alternative_value_filter_without_scenarios_or_alternatives(self):
        self._build_data_without_scenarios_or_alternatives()
        self._out_map.commit_session("Add test data")
        for db_map in [self._db_map, self._diff_db_map]:
            apply_alternative_value_filter(db_map)
            parameters = db_map.query(db_map.parameter_value_sq).all()
            self.assertEqual(len(parameters), 1)
            self.assertEqual(parameters[0].value, "23.0")

    def test_apply_alternative_value_filter_without_scenarios_or_alternatives_uncommitted_data(self):
        self._build_data_without_scenarios_or_alternatives()
        apply_alternative_value_filter(self._out_map)
        parameters = self._out_map.query(self._out_map.parameter_value_sq).all()
        self.assertEqual(len(parameters), 1)
        self.assertEqual(parameters[0].value, "23.0")
        self._out_map.rollback_session()

    def _build_data_without_scenarios_or_alternatives(self):
        import_object_classes(self._out_map, ["object_class"])
        import_objects(self._out_map, [("object_class", "object")])
        import_object_parameters(self._out_map, [("object_class", "parameter")])
        import_object_parameter_values(self._out_map, [("object_class", "object", "parameter", 23.0)])

    def test_apply_alternative_value_filter(self):
        self._build_data_with_single_scenario()
        self._out_map.commit_session("Add test data")
        for db_map in [self._db_map, self._diff_db_map]:
            apply_alternative_value_filter(db_map)
            parameters = db_map.query(db_map.parameter_value_sq).all()
            self.assertEqual(len(parameters), 1)
            self.assertEqual(parameters[0].value, "23.0")

    def test_apply_alternative_value_filter_uncommitted_data(self):
        self._build_data_with_single_scenario()
        apply_alternative_value_filter(self._out_map)
        parameters = self._out_map.query(self._out_map.parameter_value_sq).all()
        self.assertEqual(len(parameters), 1)
        self.assertEqual(parameters[0].value, "23.0")
        self._out_map.rollback_session()

    def _build_data_with_single_scenario(self):
        import_alternatives(self._out_map, ["alternative"])
        import_object_classes(self._out_map, ["object_class"])
        import_objects(self._out_map, [("object_class", "object")])
        import_object_parameters(self._out_map, [("object_class", "parameter")])
        import_object_parameter_values(self._out_map, [("object_class", "object", "parameter", -1.0)])
        import_object_parameter_values(self._out_map, [("object_class", "object", "parameter", 23.0, "alternative")])
        import_scenarios(self._out_map, [("scenario", True)])
        import_scenario_alternatives(self._out_map, [("scenario", "alternative")])

    def test_apply_alternative_value_filter_with_single_overridden_active_scenario(self):
        self._build_data_with_single_scenario()
        import_scenarios(self._out_map, [("scenario", False)])
        self._out_map.commit_session("Add test data")
        for db_map in (self._db_map, self._diff_db_map):
            apply_alternative_value_filter(db_map, ["scenario"])
            parameters = db_map.query(db_map.parameter_value_sq).all()
            self.assertEqual(len(parameters), 1)
            self.assertEqual(parameters[0].value, "23.0")

    def test_apply_alternative_value_filter_with_single_overridden_active_scenario_uncommitted_data(self):
        self._build_data_with_single_scenario()
        import_scenarios(self._out_map, [("scenario", False)])
        apply_alternative_value_filter(self._out_map, ["scenario"])
        parameters = self._out_map.query(self._out_map.parameter_value_sq).all()
        self.assertEqual(len(parameters), 1)
        self.assertEqual(parameters[0].value, "23.0")
        self._out_map.rollback_session()

    def test_apply_alternative_value_filter_with_single_overridden_active_alternative(self):
        self._build_data_with_single_scenario()
        import_scenarios(self._out_map, [("scenario", False)])
        self._out_map.commit_session("Add test data")
        for db_map in [self._db_map, self._diff_db_map]:
            apply_alternative_value_filter(db_map, overridden_active_alternatives=["alternative"])
            parameters = db_map.query(db_map.parameter_value_sq).all()
            self.assertEqual(len(parameters), 1)
            self.assertEqual(parameters[0].value, "23.0")

    def test_apply_alternative_value_filter_with_single_overridden_active_alternative_uncommitted_data(self):
        self._build_data_with_single_scenario()
        import_scenarios(self._out_map, [("scenario", False)])
        apply_alternative_value_filter(self._out_map, overridden_active_alternatives=["alternative"])
        parameters = self._out_map.query(self._out_map.parameter_value_sq).all()
        self.assertEqual(len(parameters), 1)
        self.assertEqual(parameters[0].value, "23.0")
        self._out_map.rollback_session()

    def test_apply_alternative_value_filter_works_for_object_parameter_value_sq(self):
        self._build_data_with_single_scenario()
        self._out_map.commit_session("Add test data")
        for db_map in [self._db_map, self._diff_db_map]:
            apply_alternative_value_filter(db_map)
            parameters = db_map.query(db_map.object_parameter_value_sq).all()
            self.assertEqual(len(parameters), 1)
            self.assertEqual(parameters[0].value, "23.0")

    def test_apply_alternative_value_filter_works_for_relationship_parameter_value_sq(self):
        self._build_data_with_single_scenario()
        import_relationship_classes(self._out_map, [("relationship_class", ["object_class"])])
        import_relationship_parameters(self._out_map, [("relationship_class", "relationship_parameter")])
        import_relationships(self._out_map, [("relationship_class", ["object"])])
        import_relationship_parameter_values(
            self._out_map, [("relationship_class", ["object"], "relationship_parameter", -1)]
        )
        import_relationship_parameter_values(
            self._out_map, [("relationship_class", ["object"], "relationship_parameter", 23.0, "alternative")]
        )
        self._out_map.commit_session("Add test data")
        for db_map in [self._db_map, self._diff_db_map]:
            apply_alternative_value_filter(db_map)
            parameters = db_map.query(db_map.relationship_parameter_value_sq).all()
            self.assertEqual(len(parameters), 1)
            self.assertEqual(parameters[0].value, "23.0")

    def test_apply_alternative_value_filter_falls_back_to_Base_alternative(self):
        import_alternatives(self._out_map, ["alternative"])
        import_object_classes(self._out_map, ["object_class"])
        import_objects(self._out_map, [("object_class", "object")])
        import_object_parameters(self._out_map, [("object_class", "parameter")])
        import_object_parameter_values(self._out_map, [("object_class", "object", "parameter", -1.0)])
        import_scenarios(self._out_map, [("scenario", True)])
        import_scenario_alternatives(self._out_map, [("scenario", "alternative")])
        self._out_map.commit_session("Add test data")
        for db_map in [self._db_map, self._diff_db_map]:
            apply_alternative_value_filter(db_map)
            parameters = db_map.query(db_map.parameter_value_sq).all()
            self.assertEqual(len(parameters), 1)
            self.assertEqual(parameters[0].value, "-1.0")

    def test_apply_alternative_value_filter_falls_back_to_Base_alternative_with_multiple_alternatives(self):
        import_alternatives(self._out_map, ["alternative"])
        import_object_classes(self._out_map, ["object_class"])
        import_objects(self._out_map, [("object_class", "object")])
        import_object_parameters(self._out_map, [("object_class", "parameter")])
        import_object_parameters(self._out_map, [("object_class", "parameter2")])
        import_object_parameter_values(self._out_map, [("object_class", "object", "parameter", -1.0)])
        import_object_parameter_values(self._out_map, [("object_class", "object", "parameter2", -2.0)])
        import_object_parameter_values(self._out_map, [("object_class", "object", "parameter2", 23.0, "alternative")])
        import_scenarios(self._out_map, [("scenario", True)])
        import_scenario_alternatives(self._out_map, [("scenario", "alternative")])
        self._out_map.commit_session("Add test data")
        for db_map in [self._db_map, self._diff_db_map]:
            apply_alternative_value_filter(db_map)
            parameters = db_map.query(db_map.parameter_value_sq).all()
            self.assertEqual(len(parameters), 2)
            self.assertEqual(parameters[0].value, "-1.0")
            self.assertEqual(parameters[1].value, "23.0")

    def test_apply_alternative_value_filter_selects_highest_ranked_alternative(self):
        import_alternatives(self._out_map, ["alternative3"])
        import_alternatives(self._out_map, ["alternative1"])
        import_alternatives(self._out_map, ["alternative2"])
        import_object_classes(self._out_map, ["object_class"])
        import_objects(self._out_map, [("object_class", "object")])
        import_object_parameters(self._out_map, [("object_class", "parameter")])
        import_object_parameter_values(self._out_map, [("object_class", "object", "parameter", -1.0)])
        import_object_parameter_values(self._out_map, [("object_class", "object", "parameter", 10.0, "alternative1")])
        import_object_parameter_values(self._out_map, [("object_class", "object", "parameter", 2000.0, "alternative2")])
        import_object_parameter_values(self._out_map, [("object_class", "object", "parameter", 300.0, "alternative3")])
        import_scenarios(self._out_map, [("scenario", True)])
        import_scenario_alternatives(
            self._out_map,
            [
                ("scenario", "alternative2"),
                ("scenario", "alternative3", "alternative2"),
                ("scenario", "alternative1", "alternative3"),
            ],
        )
        self._out_map.commit_session("Add test data")
        for db_map in [self._db_map, self._diff_db_map]:
            apply_alternative_value_filter(db_map)
            parameters = db_map.query(db_map.parameter_value_sq).all()
            self.assertEqual(len(parameters), 1)
            self.assertEqual(parameters[0].value, "2000.0")


if __name__ == '__main__':
    unittest.main()
