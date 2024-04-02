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

"""
Unit tests for ``alternative_filter`` module.

"""
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from sqlalchemy.engine.url import URL
from spinedb_api import (
    apply_alternative_filter_to_parameter_value_sq,
    create_new_spine_database,
    DatabaseMapping,
    from_database,
    import_alternatives,
    import_object_classes,
    import_object_parameter_values,
    import_object_parameters,
    import_objects,
    SpineDBAPIError,
)
from spinedb_api.filters.alternative_filter import (
    alternative_filter_config,
    alternative_filter_from_dict,
    alternative_filter_config_to_shorthand,
    alternative_filter_shorthand_to_config,
    alternative_names_from_dict,
)


class TestAlternativeFilter(unittest.TestCase):
    _db_url = None
    _temp_dir = None

    @classmethod
    def setUpClass(cls):
        cls._temp_dir = TemporaryDirectory()
        cls._db_url = URL("sqlite", database=Path(cls._temp_dir.name, "test_scenario_filter_mapping.sqlite").as_posix())

    def setUp(self):
        create_new_spine_database(self._db_url)
        self._out_db_map = DatabaseMapping(self._db_url)
        self._db_map = DatabaseMapping(self._db_url)

    def tearDown(self):
        self._out_db_map.close()
        self._db_map.close()

    def test_alternative_filter_without_scenarios_or_alternatives(self):
        self._build_data_without_alternatives()
        self._out_db_map.commit_session("Add test data")
        apply_alternative_filter_to_parameter_value_sq(self._db_map, [])
        parameters = self._db_map.query(self._db_map.parameter_value_sq).all()
        self.assertEqual(parameters, [])

    def test_alternative_filter_without_scenarios_or_alternatives_uncommitted_data(self):
        self._build_data_without_alternatives()
        apply_alternative_filter_to_parameter_value_sq(self._out_db_map, alternatives=[])
        parameters = self._out_db_map.query(self._out_db_map.parameter_value_sq).all()
        self.assertEqual(parameters, [])
        self._out_db_map.rollback_session()

    def test_alternative_filter(self):
        self._build_data_with_single_alternative()
        self._out_db_map.commit_session("Add test data")
        apply_alternative_filter_to_parameter_value_sq(self._db_map, ["alternative"])
        parameters = self._db_map.query(self._db_map.parameter_value_sq).all()
        self.assertEqual(len(parameters), 1)
        self.assertEqual(parameters[0].value, b"23.0")

    def test_alternative_filter_uncommitted_data(self):
        self._build_data_with_single_alternative()
        with self.assertRaises(SpineDBAPIError):
            apply_alternative_filter_to_parameter_value_sq(self._out_db_map, ["alternative"])
        parameters = self._out_db_map.query(self._out_db_map.parameter_value_sq).all()
        self.assertEqual(len(parameters), 0)
        self._out_db_map.rollback_session()

    def test_alternative_filter_from_dict(self):
        self._build_data_with_single_alternative()
        self._out_db_map.commit_session("Add test data")
        config = alternative_filter_config(["alternative"])
        alternative_filter_from_dict(self._db_map, config)
        parameters = self._db_map.query(self._db_map.parameter_value_sq).all()
        self.assertEqual(len(parameters), 1)
        self.assertEqual(parameters[0].value, b"23.0")

    def _build_data_without_alternatives(self):
        import_object_classes(self._out_db_map, ["object_class"])
        import_objects(self._out_db_map, [("object_class", "object")])
        import_object_parameters(self._out_db_map, [("object_class", "parameter")])
        import_object_parameter_values(self._out_db_map, [("object_class", "object", "parameter", 23.0)])

    def _build_data_with_single_alternative(self):
        import_alternatives(self._out_db_map, ["alternative"])
        import_object_classes(self._out_db_map, ["object_class"])
        import_objects(self._out_db_map, [("object_class", "object")])
        import_object_parameters(self._out_db_map, [("object_class", "parameter")])
        import_object_parameter_values(self._out_db_map, [("object_class", "object", "parameter", -1.0)])
        import_object_parameter_values(self._out_db_map, [("object_class", "object", "parameter", 23.0, "alternative")])


class TestAlternativeFilterWithMemoryDatabase(unittest.TestCase):
    def setUp(self):
        self._db_map = DatabaseMapping("sqlite://", create=True)
        import_object_classes(self._db_map, ["object_class"])
        import_objects(self._db_map, [("object_class", "object")])
        import_object_parameters(self._db_map, [("object_class", "parameter")])
        import_object_parameter_values(self._db_map, [("object_class", "object", "parameter", -1.0)])
        self._db_map.commit_session("Add initial data.")

    def tearDown(self):
        self._db_map.close()

    def test_alternative_names_with_colons(self):
        self._add_value_in_alternative(23.0, "new@2023-23-23T11:12:13")
        config = alternative_filter_config(["new@2023-23-23T11:12:13"])
        alternative_filter_from_dict(self._db_map, config)
        parameters = self._db_map.query(self._db_map.parameter_value_sq).all()
        self.assertEqual(len(parameters), 1)
        self.assertEqual(parameters[0].value, b"23.0")

    def test_multiple_alternatives(self):
        self._add_value_in_alternative(23.0, "new@2023-23-23T11:12:13")
        self._add_value_in_alternative(101.1, "new@2005-05-05T22:23:24")
        config = alternative_filter_config(["new@2005-05-05T22:23:24", "new@2023-23-23T11:12:13"])
        alternative_filter_from_dict(self._db_map, config)
        parameters = self._db_map.query(self._db_map.parameter_value_sq).all()
        self.assertEqual(len(parameters), 2)
        values = {from_database(p.value) for p in parameters}
        self.assertEqual(values, {23.0, 101.1})

    def _add_value_in_alternative(self, value, alternative):
        import_alternatives(self._db_map, [alternative])
        import_object_parameter_values(self._db_map, [("object_class", "object", "parameter", value, alternative)])
        self._db_map.commit_session(f"Add value in {alternative}")


class TestAlternativeFilterWithoutDatabase(unittest.TestCase):
    def test_alternative_filter_config(self):
        config = alternative_filter_config(["alternative1", "alternative2"])
        self.assertEqual(config, {"type": "alternative_filter", "alternatives": ["alternative1", "alternative2"]})

    def test_alternative_names_from_dict(self):
        config = alternative_filter_config(["alternative1", "alternative2"])
        self.assertEqual(alternative_names_from_dict(config), ["alternative1", "alternative2"])

    def test_alternative_filter_config_to_shorthand(self):
        config = alternative_filter_config(["alternative1", "alternative2"])
        shorthand = alternative_filter_config_to_shorthand(config)
        self.assertEqual(shorthand, "alternatives:'alternative1':'alternative2'")

    def test_alternative_filter_shorthand_to_config(self):
        config = alternative_filter_shorthand_to_config("alternatives:'alternative1':'alternative2'")
        self.assertEqual(config, {"type": "alternative_filter", "alternatives": ["alternative1", "alternative2"]})

    def test_quoted_alternative_names(self):
        config = alternative_filter_shorthand_to_config("alternatives:'alt:er:na:ti:ve':'alternative2'")
        self.assertEqual(config, {"type": "alternative_filter", "alternatives": ["alt:er:na:ti:ve", "alternative2"]})


if __name__ == "__main__":
    unittest.main()
