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
Unit tests for ``value_transformer`` module.

"""
from pathlib import Path
import unittest
from tempfile import TemporaryDirectory
from sqlalchemy.engine.url import URL
from spinedb_api import (
    DatabaseMapping,
    import_object_classes,
    import_object_parameter_values,
    import_object_parameters,
    import_objects,
    append_filter_config,
    create_new_spine_database,
    from_database,
    Map,
    TimeSeriesFixedResolution,
)
from spinedb_api.filters.value_transformer import (
    value_transformer_config,
    value_transformer_config_to_shorthand,
    value_transformer_shorthand_to_config,
)


class TestValueTransformerFunctions(unittest.TestCase):
    def test_config(self):
        instructions = {
            "class1": {"parameter1": [{"operation": "negate"}], "parameter2": [{"operation": "reciprocal"}]},
            "class2": {"parameter2": [{"operation": "enchant"}], "parameter3": [{"operation": "integrate"}]},
        }
        config = value_transformer_config(instructions)
        expected = {"type": "value_transformer", "instructions": instructions}
        self.assertEqual(config, expected)

    def test_config_to_shorthand(self):
        instructions = {
            "class1": {"parameter1": [{"operation": "negate"}], "parameter2": [{"operation": "reciprocal"}]},
            "class2": {"parameter2": [{"operation": "enchant"}], "parameter3": [{"operation": "integrate"}]},
        }
        config = value_transformer_config(instructions)
        shorthand = value_transformer_config_to_shorthand(config)
        expected = (
            "value_transform:class1:parameter1:negate:class1:parameter2:reciprocal"
            + ":class2:parameter2:enchant:class2:parameter3:integrate"
        )
        self.assertEqual(shorthand, expected)

    def test_config_to_shorthand_multiple_instructions_for_single_parameter(self):
        instructions = {"class": {"parameter": [{"operation": "negate"}, {"operation": "reciprocal"}]}}
        config = value_transformer_config(instructions)
        shorthand = value_transformer_config_to_shorthand(config)
        expected = "value_transform:class:parameter:negate:class:parameter:reciprocal"
        self.assertEqual(shorthand, expected)

    def test_shorthand_to_config(self):
        shorthand = (
            "value_transform:class1:parameter1:negate:class1:parameter2:reciprocal"
            + ":class2:parameter2:enchant:class2:parameter3:integrate"
        )
        config = value_transformer_shorthand_to_config(shorthand)
        expected = {
            "type": "value_transformer",
            "instructions": {
                "class1": {"parameter1": [{"operation": "negate"}], "parameter2": [{"operation": "reciprocal"}]},
                "class2": {"parameter2": [{"operation": "enchant"}], "parameter3": [{"operation": "integrate"}]},
            },
        }
        self.assertEqual(config, expected)

    def test_shorthand_to_config_with_multiple_instructions_for_single_parameter(self):
        shorthand = "value_transform:class:parameter:negate:class:parameter:reciprocal"
        config = value_transformer_shorthand_to_config(shorthand)
        expected = {
            "type": "value_transformer",
            "instructions": {"class": {"parameter": [{"operation": "negate"}, {"operation": "reciprocal"}]}},
        }
        self.assertEqual(config, expected)


class TestValueTransformerUsingDatabase(unittest.TestCase):
    _db_url = None
    _temp_dir = None

    @classmethod
    def setUpClass(cls):
        cls._temp_dir = TemporaryDirectory()
        cls._db_url = URL("sqlite", database=Path(cls._temp_dir.name, "test_value_transformer.sqlite").as_posix())

    @classmethod
    def tearDownClass(cls):
        cls._temp_dir.cleanup()

    def setUp(self):
        create_new_spine_database(self._db_url)
        self._out_db_map = DatabaseMapping(self._db_url)

    def tearDown(self):
        self._out_db_map.close()

    def test_negate_manipulator(self):
        import_object_classes(self._out_db_map, ("class",))
        import_object_parameters(self._out_db_map, (("class", "parameter"),))
        import_objects(self._out_db_map, (("class", "object"),))
        import_object_parameter_values(self._out_db_map, (("class", "object", "parameter", -2.3),))
        self._out_db_map.commit_session("Add test data.")
        instructions = {"class": {"parameter": [{"operation": "negate"}]}}
        config = value_transformer_config(instructions)
        url = append_filter_config(str(self._db_url), config)
        db_map = DatabaseMapping(url)
        try:
            values = [from_database(row.value, row.type) for row in db_map.query(db_map.parameter_value_sq)]
            self.assertEqual(values, [2.3])
        finally:
            db_map.close()

    def test_negate_manipulator_with_nested_map(self):
        import_object_classes(self._out_db_map, ("class",))
        import_object_parameters(self._out_db_map, (("class", "parameter"),))
        import_objects(self._out_db_map, (("class", "object"),))
        value = Map(["A"], [Map(["1"], [2.3])])
        import_object_parameter_values(self._out_db_map, (("class", "object", "parameter", value),))
        self._out_db_map.commit_session("Add test data.")
        instructions = {"class": {"parameter": [{"operation": "negate"}]}}
        config = value_transformer_config(instructions)
        url = append_filter_config(str(self._db_url), config)
        db_map = DatabaseMapping(url)
        try:
            values = [from_database(row.value, row.type) for row in db_map.query(db_map.parameter_value_sq)]
            expected = Map(["A"], [Map(["1"], [-2.3])])
            self.assertEqual(values, [expected])
        finally:
            db_map.close()

    def test_multiply_manipulator(self):
        import_object_classes(self._out_db_map, ("class",))
        import_object_parameters(self._out_db_map, (("class", "parameter"),))
        import_objects(self._out_db_map, (("class", "object"),))
        import_object_parameter_values(self._out_db_map, (("class", "object", "parameter", -2.3),))
        self._out_db_map.commit_session("Add test data.")
        instructions = {"class": {"parameter": [{"operation": "multiply", "rhs": 10.0}]}}
        config = value_transformer_config(instructions)
        url = append_filter_config(str(self._db_url), config)
        db_map = DatabaseMapping(url)
        try:
            values = [from_database(row.value, row.type) for row in db_map.query(db_map.parameter_value_sq)]
            self.assertEqual(values, [-23.0])
        finally:
            db_map.close()

    def test_invert_manipulator(self):
        import_object_classes(self._out_db_map, ("class",))
        import_object_parameters(self._out_db_map, (("class", "parameter"),))
        import_objects(self._out_db_map, (("class", "object"),))
        import_object_parameter_values(self._out_db_map, (("class", "object", "parameter", -2.3),))
        self._out_db_map.commit_session("Add test data.")
        instructions = {"class": {"parameter": [{"operation": "invert"}]}}
        config = value_transformer_config(instructions)
        url = append_filter_config(str(self._db_url), config)
        db_map = DatabaseMapping(url)
        try:
            values = [from_database(row.value, row.type) for row in db_map.query(db_map.parameter_value_sq)]
            self.assertEqual(values, [-1.0 / 2.3])
        finally:
            db_map.close()

    def test_multiple_instructions(self):
        import_object_classes(self._out_db_map, ("class",))
        import_object_parameters(self._out_db_map, (("class", "parameter"),))
        import_objects(self._out_db_map, (("class", "object"),))
        import_object_parameter_values(self._out_db_map, (("class", "object", "parameter", -2.3),))
        self._out_db_map.commit_session("Add test data.")
        instructions = {"class": {"parameter": [{"operation": "invert"}, {"operation": "negate"}]}}
        config = value_transformer_config(instructions)
        url = append_filter_config(str(self._db_url), config)
        db_map = DatabaseMapping(url)
        try:
            values = [from_database(row.value, row.type) for row in db_map.query(db_map.parameter_value_sq)]
            self.assertEqual(values, [1.0 / 2.3])
        finally:
            db_map.close()

    def test_index_generator_on_time_series(self):
        import_object_classes(self._out_db_map, ("class",))
        import_object_parameters(self._out_db_map, (("class", "parameter"),))
        import_objects(self._out_db_map, (("class", "object"),))
        value = TimeSeriesFixedResolution("2021-06-07T08:00", "1D", [-5.0, -2.3], False, False)
        import_object_parameter_values(self._out_db_map, (("class", "object", "parameter", value),))
        self._out_db_map.commit_session("Add test data.")
        instructions = {"class": {"parameter": [{"operation": "generate_index", "expression": "float(i)"}]}}
        config = value_transformer_config(instructions)
        url = append_filter_config(str(self._db_url), config)
        db_map = DatabaseMapping(url)
        try:
            values = [from_database(row.value, row.type) for row in db_map.query(db_map.parameter_value_sq)]
            expected = Map([1.0, 2.0], [-5.0, -2.3])
            self.assertEqual(values, [expected])
        finally:
            db_map.close()


if __name__ == "__main__":
    unittest.main()
