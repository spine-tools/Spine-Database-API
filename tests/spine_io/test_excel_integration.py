######################################################################################################################
# Copyright (C) 2017-2021 Spine project consortium
# This file is part of Spine Engine.
# Spine Engine is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option)
# any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

"""
Integration tests for Excel import and export.

:author: P. Vennström (VTT), A. Soininen (VTT)
:date:   31.1.2020
"""

from pathlib import PurePath
from tempfile import TemporaryDirectory
import unittest
import json
from spinedb_api import DatabaseMapping, import_data, from_database
from spine_engine.spine_io.exporters.excel import export_spine_database_to_xlsx
from spine_engine.spine_io.importers.excel_reader import get_mapped_data_from_xlsx

_TEMP_EXCEL_FILENAME = "excel.xlsx"


class TestExcelIntegration(unittest.TestCase):
    def test_array(self):
        array = '{"type": "array", "data": [1, 2, 3]}'
        array = from_database(array)
        self._check_parameter_value(array)

    def test_time_series(self):
        ts = (
            '{"type": "time_series", "index": {"start": "1999-12-31 23:00:00", "resolution": "1h"}, "data": [0.1, 0.2]}'
        )
        ts = from_database(ts)
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
                                        "index": {"start": "2000-01-01 00:00:00", "resolution": "1h"},
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
                                        "index": {"start": "2000-01-01 00:00:00", "resolution": "1h"},
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
                                        "index": {"start": "2000-01-01 00:00:00", "resolution": "1h"},
                                        "data": [0.680549132, 0.636555097],
                                    },
                                ]
                            ],
                        },
                    ],
                ],
            }
        )
        map_ = from_database(map_)
        self._check_parameter_value(map_)

    def _check_parameter_value(self, val):
        input_data = {
            "object_classes": ["dog"],
            "objects": [("dog", "pluto")],
            "object_parameters": [("dog", "bone")],
            "object_parameter_values": [("dog", "pluto", "bone", val)],
        }
        db_map = DatabaseMapping("sqlite://", create=True)
        import_data(db_map, **input_data)
        db_map.commit_session("yeah")
        with TemporaryDirectory() as directory:
            path = str(PurePath(directory, _TEMP_EXCEL_FILENAME))
            export_spine_database_to_xlsx(db_map, path)
            output_data, errors = get_mapped_data_from_xlsx(path)
        db_map.connection.close()
        self.assertEqual([], errors)
        input_obj_param_vals = input_data.pop("object_parameter_values")
        output_obj_param_vals = output_data.pop("object_parameter_values")
        self.assertEqual(1, len(output_obj_param_vals))
        input_obj_param_val = input_obj_param_vals[0]
        output_obj_param_val = output_obj_param_vals[0]
        self.assertEqual(input_obj_param_val[:3], output_obj_param_val[:3])
        input_val = input_obj_param_val[3]
        output_val = output_obj_param_val[3]
        self.assertEqual(sorted(input_val.indexed_values()), sorted(output_val.indexed_values()))
        for key, value in input_data.items():
            self.assertEqual(value, output_data[key])


if __name__ == "__main__":
    unittest.main()
