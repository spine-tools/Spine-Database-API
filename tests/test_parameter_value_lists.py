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
from spinedb_api import DatabaseMapping


def test_fetch_parameter_definition_with_value_list(tmp_path):
    url = "sqlite:///" + str(tmp_path / "test.db")
    with DatabaseMapping(url, create=True) as db_map:
        db_map.add_parameter_value_list(name="Enumeration")
        db_map.add_entity_class(name="Widget")
        db_map.add_parameter_definition(
            entity_class_name="Widget", name="Widget", parameter_value_list_name="Enumeration"
        )
        db_map.commit_session("Add test data.")
    with DatabaseMapping(url) as db_map:
        definitions = db_map.find_parameter_definitions()
        assert len(definitions) == 1
        assert definitions[0]["parameter_value_list_name"] == "Enumeration"
