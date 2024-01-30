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
Integration tests for Excel import and export.

"""

from pathlib import PurePath
from tempfile import TemporaryDirectory
import unittest
import json
from spinedb_api import DatabaseMapping, import_data, from_database
from spinedb_api.spine_io.exporters.excel import export_spine_database_to_xlsx
from spinedb_api.spine_io.importers.excel_reader import get_mapped_data_from_xlsx
from tests.test_import_functions import assert_import_equivalent

_TEMP_EXCEL_FILENAME = "excel.xlsx"


class TestExcelIntegration(unittest.TestCase):
    def test_array(self):
        array = b'{"type": "array", "data": [1, 2, 3]}'
        array = from_database(array, type_="array")
        self._check_parameter_value(array)

    def test_time_series(self):
        ts = b'{"type": "time_series", "index": {"start": "1999-12-31 23:00:00", "resolution": "1h"}, "data": [0.1, 0.2]}'
        ts = from_database(ts, type_="time_series")
        self._check_parameter_value(ts)

    def test_map(self):
        map_ = json.dumps(
            {
                "type": "map",
                "index_type": "str",
                "data": [
                    [
                        "realization",
                        {
                            "type": "map",
                            "index_type": "date_time",
                            "data": [
                                [
                                    "2000-01-01T00:00:00",
                                    {
                                        "type": "time_series",
                                        "index": {"start": "2000-01-01T00:00:00", "resolution": "1h"},
                                        "data": [0.732885319, 0.658604529],
                                    },
                                ]
                            ],
                        },
                    ],
                    [
                        "forecast1",
                        {
                            "type": "map",
                            "index_type": "date_time",
                            "data": [
                                [
                                    "2000-01-01T00:00:00",
                                    {
                                        "type": "time_series",
                                        "index": {"start": "2000-01-01T00:00:00", "resolution": "1h"},
                                        "data": [0.65306041, 0.60853286],
                                    },
                                ]
                            ],
                        },
                    ],
                    [
                        "forecast_tail",
                        {
                            "type": "map",
                            "index_type": "date_time",
                            "data": [
                                [
                                    "2000-01-01T00:00:00",
                                    {
                                        "type": "time_series",
                                        "index": {"start": "2000-01-01T00:00:00", "resolution": "1h"},
                                        "data": [0.680549132, 0.636555097],
                                    },
                                ]
                            ],
                        },
                    ],
                ],
            }
        ).encode("UTF8")
        map_ = from_database(map_, type_="map")
        self._check_parameter_value(map_)

    def _check_parameter_value(self, val):
        input_data = {
            "entity_classes": {("dog",)},
            "entities": {("dog", "pluto")},
            "parameter_definitions": [("dog", "bone")],
            "parameter_values": [("dog", "pluto", "bone", val)],
        }
        db_map = DatabaseMapping("sqlite://", create=True)
        import_data(db_map, **input_data)
        db_map.commit_session("yeah")
        with TemporaryDirectory() as directory:
            path = str(PurePath(directory, _TEMP_EXCEL_FILENAME))
            export_spine_database_to_xlsx(db_map, path)
            output_data, errors = get_mapped_data_from_xlsx(path)
        db_map.close()
        self.assertEqual([], errors)
        input_param_vals = input_data.pop("parameter_values")
        output_param_vals = output_data.pop("parameter_values")
        self.assertEqual(1, len(output_param_vals))
        input_obj_param_val = input_param_vals[0]
        output_obj_param_val = output_param_vals[0]
        for input_, output in zip(input_obj_param_val[:3], output_obj_param_val[:3]):
            self.assertEqual(input_, output)
        input_val = input_obj_param_val[3]
        output_val = output_obj_param_val[3]
        self.assertEqual(set(indexed_values(output_val)), set(indexed_values(input_val)))
        assert_import_equivalent(self, input_data, output_data, strict=False)


def indexed_values(value, k=1, prefix=()):
    try:
        for index, new_value in zip(value.indexes, value.values):
            yield from indexed_values(new_value, k=k + 1, prefix=(*prefix, str(index)))
    except AttributeError:
        yield str(prefix), value


if __name__ == "__main__":
    unittest.main()
