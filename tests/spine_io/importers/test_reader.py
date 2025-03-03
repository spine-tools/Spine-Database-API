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
import unittest
from unittest import mock
from spinedb_api.exception import ReaderError
from spinedb_api.import_mapping.import_mapping import AlternativeMapping, EntityClassMapping, EntityMapping
from spinedb_api.mapping import Position
from spinedb_api.spine_io.importers.reader import Reader


class TestReader(unittest.TestCase):
    def test_get_mapped_data_can_handle_reader_error_in_data_iterator(self):
        def failing_iterator():
            if True:
                raise ReaderError("error in iterator")
            yield from []

        reader = Reader(None)
        reader.get_data_iterator = lambda *args: (failing_iterator(), [])
        table_mappings = {"table 1": []}
        table_options = {}
        table_column_convert_specs = {}
        table_default_column_convert_fns = {}
        table_row_convert_specs = {}
        mapped_data, errors = reader.get_mapped_data(
            table_mappings,
            table_options,
            table_column_convert_specs,
            table_default_column_convert_fns,
            table_row_convert_specs,
        )
        self.assertEqual(errors, ["error in iterator"])

    def test_get_mapped_data(self):
        reader = Reader(None)
        reader.get_data_iterator = lambda *args: (iter([["A", "b"]]), [])
        table_mappings = {
            "table 1": [{"entity mapping": {"mapping": [EntityClassMapping(0).to_dict(), EntityMapping(1).to_dict()]}}]
        }
        table_options = {}
        table_column_convert_specs = {}
        table_default_column_convert_fns = {}
        table_row_convert_specs = {}
        mapped_data, errors = reader.get_mapped_data(
            table_mappings,
            table_options,
            table_column_convert_specs,
            table_default_column_convert_fns,
            table_row_convert_specs,
        )
        self.assertEqual(errors, [])
        self.assertEqual(mapped_data, {"entity_classes": [("A",)], "entities": [("A", "b")]})

    def test_resolve_values_for_fixed_position_mappings(self):
        reader = Reader(None)
        root_mapping = AlternativeMapping(Position.fixed, value="5, 23")
        table_mappings = {"table 1": [("alternative mapping", root_mapping)]}
        table_options = {"table 1": {"my option": 13}}
        with mock.patch.object(reader, "get_table_cell") as get_table_cell:
            get_table_cell.return_value = "cat"
            parsed_tables_mappings = reader.resolve_values_for_fixed_position_mappings(
                table_mappings, table_options, True
            )
            get_table_cell.assert_called_once_with("table 1", 5, 23, {"my option": 13})
        self.assertEqual(len(parsed_tables_mappings), 1)
        mappings = parsed_tables_mappings["table 1"]
        self.assertEqual(len(mappings), 1)
        self.assertEqual(mappings[0], ("alternative mapping", AlternativeMapping(Position.hidden, value="cat")))

    def test_resolve_values_for_fixed_position_mappings_reraises_reader_error_when_its_fatal(self):
        reader = Reader(None)
        root_mapping = AlternativeMapping(Position.fixed, value="5, 23")
        table_mappings = {"table 1": [("alternative mapping", root_mapping)]}
        table_options = {"table 1": {"my option": 13}}
        with mock.patch.object(reader, "get_table_cell") as get_table_cell:
            get_table_cell.side_effect = ReaderError()
            with self.assertRaises(ReaderError):
                parsed_tables_mappings = reader.resolve_values_for_fixed_position_mappings(
                    table_mappings, table_options, True
                )

    def test_resolve_values_for_fixed_position_mappings_sets_value_to_none_on_reader_error_that_isnt_fatal(self):
        reader = Reader(None)
        root_mapping = AlternativeMapping(Position.fixed, value="5, 23")
        table_mappings = {"table 1": [("alternative mapping", root_mapping)]}
        table_options = {"table 1": {"my option": 13}}
        with mock.patch.object(reader, "get_table_cell") as get_table_cell:
            get_table_cell.side_effect = ReaderError()
            parsed_tables_mappings = reader.resolve_values_for_fixed_position_mappings(
                table_mappings, table_options, False
            )
            get_table_cell.assert_called_once_with("table 1", 5, 23, {"my option": 13})
        self.assertEqual(len(parsed_tables_mappings), 1)
        mappings = parsed_tables_mappings["table 1"]
        self.assertEqual(len(mappings), 1)
        self.assertEqual(mappings[0], ("alternative mapping", AlternativeMapping(Position.hidden, value=None)))

    def test_reader_appends_data_iterator_exceptions_to_return_value(self):
        def raise_exception(*args):
            raise ReaderError("this is expected")

        reader = Reader(None)
        reader.get_data_iterator = raise_exception
        table_mappings = {
            "table 1": [{"entity mapping": {"mapping": [EntityClassMapping(0).to_dict(), EntityMapping(1).to_dict()]}}]
        }
        table_options = {}
        table_column_convert_specs = {}
        table_default_column_convert_fns = {}
        table_row_convert_specs = {}
        mapped_data, errors = reader.get_mapped_data(
            table_mappings,
            table_options,
            table_column_convert_specs,
            table_default_column_convert_fns,
            table_row_convert_specs,
        )
        self.assertEqual(errors, ["this is expected"])
        self.assertEqual(mapped_data, {})


if __name__ == "__main__":
    unittest.main()
