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

from spinedb_api.exception import ConnectorError
from spinedb_api.spine_io.importers.reader import SourceConnection


class TestSourceConnection(unittest.TestCase):
    def test_get_mapped_data_can_handle_connector_error_in_data_iterator(self):
        def failing_iterator():
            if True:
                raise ConnectorError("error in iterator")
            yield from []

        reader = SourceConnection(None)
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


if __name__ == "__main__":
    unittest.main()
