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
import pathlib
import shutil
from spinedb_api import (
    DatabaseMapping,
    DateTime,
    Duration,
    Map,
    TimePattern,
    TimeSeriesFixedResolution,
    TimeSeriesVariableResolution,
)


class TestMigrationFrom070a0eb89e88:
    def test_from_070a0eb89e88_to_latest(self, tmp_path):
        source_path = pathlib.Path(__file__).parent / "legacy_databases" / "070a0eb89e88.sqlite"
        destination_path = tmp_path / "db.sqlite"
        shutil.copyfile(source_path, destination_path)
        url = "sqlite:///" + str(destination_path)
        with DatabaseMapping(url, upgrade=True) as db_map:
            self._assert_parameter_value_list_and_list_values(db_map)
            self._assert_entity_classes(db_map)
            self._assert_entities(db_map)
            self._assert_parameter_definitions(db_map)
            self._assert_parameter_values(db_map)

    @staticmethod
    def _assert_parameter_value_list_and_list_values(db_map):
        parameter_value_lists = db_map.find_parameter_value_lists()
        assert len(parameter_value_lists) == 1
        assert parameter_value_lists[0]["name"] == "Enumeration"
        list_values = db_map.find_list_values()
        assert len(list_values) == 3
        reds = db_map.find_list_values(parameter_value_list_name="Enumeration", index=0)
        assert len(reds) == 1
        red = reds[0]
        assert red["parameter_value_list_name"] == "Enumeration"
        assert red["parsed_value"] == "red"
        assert red["index"] == 0
        greens = db_map.find_list_values(parameter_value_list_name="Enumeration", index=1)
        assert len(greens) == 1
        green = greens[0]
        assert green["parameter_value_list_name"] == "Enumeration"
        assert green["parsed_value"] == "green"
        assert green["index"] == 1
        blues = db_map.find_list_values(parameter_value_list_name="Enumeration", index=2)
        assert len(blues) == 1
        blue = blues[0]
        assert blue["parameter_value_list_name"] == "Enumeration"
        assert blue["parsed_value"] == "blue"
        assert blue["index"] == 2

    @staticmethod
    def _assert_entity_classes(db_map):
        entity_classes = db_map.find_entity_classes()
        assert len(entity_classes) == 3
        widget_class = db_map.entity_class(name="Widget")
        assert widget_class["name"] == "Widget"
        assert widget_class["description"] == "Widgets of all kinds."
        assert widget_class["dimension_name_list"] == ()
        assert widget_class["display_icon"] == 280379751657962
        gadget_class = db_map.entity_class(name="Gadget")
        assert gadget_class["name"] == "Gadget"
        assert gadget_class["description"] == "Gadget is not widget."
        assert gadget_class["dimension_name_list"] == ()
        assert gadget_class["display_icon"] == 280741980139948
        gadget_widget_class = db_map.entity_class(name="Gadget__Widget")
        assert gadget_widget_class["name"] == "Gadget__Widget"
        assert gadget_widget_class["description"] is None
        assert gadget_widget_class["dimension_name_list"] == ("Gadget", "Widget")
        assert gadget_widget_class["display_icon"] is None

    @staticmethod
    def _assert_entities(db_map):
        entities = db_map.find_entities()
        assert len(entities) == 5
        router = db_map.entity(entity_class_name="Gadget", name="router")
        assert router["name"] == "router"
        assert router["description"] is None
        assert router["element_name_list"] == ()
        mouse = db_map.entity(entity_class_name="Gadget", name="mouse")
        assert mouse["name"] == "mouse"
        assert mouse["description"] is None
        assert mouse["element_name_list"] == ()
        calculator = db_map.entity(entity_class_name="Widget", name="calculator")
        assert calculator["name"] == "calculator"
        assert calculator["description"] == "Standard phone calculator."
        assert calculator["element_name_list"] == ()
        clock = db_map.entity(entity_class_name="Widget", name="clock")
        assert clock["name"] == "clock"
        assert clock["description"] == "Fancy digital clock."
        assert clock["element_name_list"] == ()
        router_calculator = db_map.entity(entity_class_name="Gadget__Widget", entity_byname=("router", "calculator"))
        assert router_calculator["name"] == "router__calculator"
        assert router_calculator["description"] is None
        assert router_calculator["element_name_list"] == ("router", "calculator")

    @staticmethod
    def _assert_parameter_definitions(db_map):
        parameter_definitions = db_map.find_parameter_definitions()
        assert len(parameter_definitions) == 11
        float_definition = db_map.parameter_definition(entity_class_name="Widget", name="float")
        assert float_definition["name"] == "float"
        assert float_definition["description"] == "Parameter with float values."
        assert float_definition["parsed_value"] == 2.3
        assert float_definition["parameter_value_list_name"] is None
        null_definition = db_map.parameter_definition(entity_class_name="Widget", name="none")
        assert null_definition["name"] == "none"
        assert null_definition["description"] == "Parameter with no values."
        assert null_definition["parsed_value"] is None
        assert null_definition["parameter_value_list_name"] is None
        value_list_definition = db_map.parameter_definition(entity_class_name="Widget", name="value_list")
        assert value_list_definition["name"] == "value_list"
        assert value_list_definition["description"] == "Parameter with list values."
        assert value_list_definition["parsed_value"] == "red"
        assert value_list_definition["parameter_value_list_name"] == "Enumeration"
        string_definition = db_map.parameter_definition(entity_class_name="Widget", name="string")
        assert string_definition["name"] == "string"
        assert string_definition["description"] == "Parameter with string values."
        assert string_definition["parsed_value"] == "a default string"
        assert string_definition["parameter_value_list_name"] is None
        boolean_definition = db_map.parameter_definition(entity_class_name="Widget", name="boolean")
        assert boolean_definition["name"] == "boolean"
        assert boolean_definition["description"] == "Parameter with boolean values."
        assert isinstance(boolean_definition["parsed_value"], bool)
        assert boolean_definition["parsed_value"]
        assert boolean_definition["parameter_value_list_name"] is None
        date_time_definition = db_map.parameter_definition(entity_class_name="Widget", name="date_time")
        assert date_time_definition["name"] == "date_time"
        assert date_time_definition["description"] == "Parameter with datetime values."
        assert date_time_definition["parsed_value"] == DateTime("2020-04-22T14:20:00")
        assert date_time_definition["parameter_value_list_name"] is None
        duration_definition = db_map.parameter_definition(entity_class_name="Widget", name="duration")
        assert duration_definition["name"] == "duration"
        assert duration_definition["description"] == "Parameter with duration values."
        assert duration_definition["parsed_value"] == Duration("5h")
        assert duration_definition["parameter_value_list_name"] is None
        time_pattern_definition = db_map.parameter_definition(entity_class_name="Widget", name="time_pattern")
        assert time_pattern_definition["name"] == "time_pattern"
        assert time_pattern_definition["description"] == "Parameter with time pattern values."
        assert time_pattern_definition["parsed_value"] == TimePattern(["D1-7"], [1.1])
        assert time_pattern_definition["parameter_value_list_name"] is None
        time_series_fixed_resolution_definition = db_map.parameter_definition(
            entity_class_name="Widget", name="time_series_fixed_resolution"
        )
        assert time_series_fixed_resolution_definition["name"] == "time_series_fixed_resolution"
        assert time_series_fixed_resolution_definition["description"] == "Parameter with time series values."
        assert time_series_fixed_resolution_definition["parsed_value"] == TimeSeriesFixedResolution(
            "2020-04-22 00:00:00", "3h", [1.1, 2.2, 3.3], ignore_year=True, repeat=False
        )
        assert time_series_fixed_resolution_definition["parameter_value_list_name"] is None
        time_series_variable_resolution_definition = db_map.parameter_definition(
            entity_class_name="Widget", name="time_series_variable_resolution"
        )
        assert time_series_variable_resolution_definition["name"] == "time_series_variable_resolution"
        assert time_series_variable_resolution_definition["description"] == "Parameter with time series values."
        assert time_series_variable_resolution_definition["parsed_value"] == TimeSeriesVariableResolution(
            ["2000-01-01T00:00:00", "2000-01-01T01:00:00", "2000-01-01T02:00:00"],
            [1.1, 2.2, 3.3],
            ignore_year=False,
            repeat=True,
        )
        assert time_series_variable_resolution_definition["parameter_value_list_name"] is None
        map_definition = db_map.parameter_definition(entity_class_name="Widget", name="map")
        assert map_definition["name"] == "map"
        assert map_definition["description"] == "Parameter with map values."
        assert map_definition["parsed_value"] == Map(
            ["A", "B"], [Map(["T00", "T01"], [1.1, 2.2]), Map(["T00", "T01"], [3.3, 4.4])]
        )
        assert map_definition["parameter_value_list_name"] is None

    @staticmethod
    def _assert_parameter_values(db_map):
        parameter_values = db_map.find_parameter_values()
        assert len(parameter_values) == 11
        float_value = db_map.parameter_value(
            entity_class_name="Widget",
            parameter_definition_name="float",
            entity_byname=("clock",),
            alternative_name="Base",
        )
        assert float_value["parsed_value"] == -2.3
        null_value = db_map.parameter_value(
            entity_class_name="Widget",
            parameter_definition_name="none",
            entity_byname=("clock",),
            alternative_name="Base",
        )
        assert null_value["parsed_value"] is None
        value_list_value = db_map.parameter_value(
            entity_class_name="Widget",
            parameter_definition_name="value_list",
            entity_byname=("clock",),
            alternative_name="Base",
        )
        assert value_list_value["parsed_value"] == "blue"
        string_value = db_map.parameter_value(
            entity_class_name="Widget",
            parameter_definition_name="string",
            entity_byname=("clock",),
            alternative_name="Base",
        )
        assert string_value["parsed_value"] == "a real value"
        boolean_value = db_map.parameter_value(
            entity_class_name="Widget",
            parameter_definition_name="boolean",
            entity_byname=("clock",),
            alternative_name="Base",
        )
        assert isinstance(boolean_value["parsed_value"], bool)
        assert not boolean_value["parsed_value"]
        date_time_value = db_map.parameter_value(
            entity_class_name="Widget",
            parameter_definition_name="date_time",
            entity_byname=("clock",),
            alternative_name="Base",
        )
        assert date_time_value["parsed_value"] == DateTime("2025-09-23T14:56:00")
        duration_value = db_map.parameter_value(
            entity_class_name="Widget",
            parameter_definition_name="duration",
            entity_byname=("clock",),
            alternative_name="Base",
        )
        assert duration_value["parsed_value"] == Duration("9M")
        time_pattern_value = db_map.parameter_value(
            entity_class_name="Widget",
            parameter_definition_name="time_pattern",
            entity_byname=("clock",),
            alternative_name="Base",
        )
        assert time_pattern_value["parsed_value"] == TimePattern(["M1-12"], [-2.3])
        time_series_fixed_resolution_value = db_map.parameter_value(
            entity_class_name="Widget",
            parameter_definition_name="time_series_fixed_resolution",
            entity_byname=("clock",),
            alternative_name="Base",
        )
        assert time_series_fixed_resolution_value["parsed_value"] == TimeSeriesFixedResolution(
            "2025-09-23 00:00:00", "6h", [-1.1, -2.2], ignore_year=False, repeat=False
        )
        time_series_variable_resolution_value = db_map.parameter_value(
            entity_class_name="Widget",
            parameter_definition_name="time_series_variable_resolution",
            entity_byname=("clock",),
            alternative_name="Base",
        )
        assert time_series_variable_resolution_value["parsed_value"] == TimeSeriesVariableResolution(
            ["2025-09-23T00:00:00", "2025-09-23T06:00:00", "2025-09-23T12:00:00"],
            [-1.1, -2.2, -3.3],
            ignore_year=False,
            repeat=False,
        )
        map_value = db_map.parameter_value(
            entity_class_name="Widget",
            parameter_definition_name="map",
            entity_byname=("clock",),
            alternative_name="Base",
        )
        assert map_value["parsed_value"] == Map(
            ["A", "A", "B", "B"], [-1.1, Map(["a"], [-2.2]), -3.3, Map(["b"], [-4.4])]
        )
