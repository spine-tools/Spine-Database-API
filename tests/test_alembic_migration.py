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
import pytest
from spinedb_api import (
    Array,
    DatabaseMapping,
    DateTime,
    Duration,
    Map,
    SpineDBAPIError,
    TimePattern,
    TimeSeriesFixedResolution,
    TimeSeriesVariableResolution,
)


class TestMigrationFrom070a0eb89e88:
    # Test migration from a very old revision (e.g. no alternatives or object groups yet).
    def test_migration_to_latest(self, tmp_path):
        source_path = pathlib.Path(__file__).parent / "legacy_databases" / "070a0eb89e88.sqlite"
        destination_path = tmp_path / "db.sqlite"
        shutil.copyfile(source_path, destination_path)
        url = "sqlite:///" + str(destination_path)
        with DatabaseMapping(url, upgrade=True) as db_map:
            self._assert_alternatives(db_map)
            self._assert_parameter_value_lists(db_map)
            self._assert_entity_classes(db_map)
            self._assert_entities(db_map)
            self._assert_parameter_definitions(db_map)
            self._assert_parameter_values(db_map)

    @staticmethod
    def _assert_alternatives(db_map):
        assert len(db_map.find_alternatives()) == 1
        assert db_map.alternative(name="Base")["description"] == "Base alternative"

    @staticmethod
    def _assert_parameter_value_lists(db_map):
        parameter_value_lists = db_map.find_parameter_value_lists()
        assert len(parameter_value_lists) == 1
        enumeration = db_map.parameter_value_list(name="Enumeration")
        assert enumeration["parsed_value_list"] == ["red", "green", "blue"]

    @staticmethod
    def _assert_entity_classes(db_map):
        entity_classes = db_map.find_entity_classes()
        assert len(entity_classes) == 3
        widget_class = db_map.entity_class(name="Widget")
        assert widget_class["description"] == "Widgets of all kinds."
        assert widget_class["dimension_name_list"] == ()
        assert widget_class["display_icon"] == 280379751657962
        assert widget_class["active_by_default"]
        gadget_class = db_map.entity_class(name="Gadget")
        assert gadget_class["description"] == "Gadget is not widget."
        assert gadget_class["dimension_name_list"] == ()
        assert gadget_class["display_icon"] == 280741980139948
        assert gadget_class["active_by_default"]
        gadget_widget_class = db_map.entity_class(name="Gadget__Widget")
        assert gadget_widget_class["description"] is None
        assert gadget_widget_class["dimension_name_list"] == ("Gadget", "Widget")
        assert gadget_widget_class["display_icon"] is None
        assert gadget_widget_class["active_by_default"]

    @staticmethod
    def _assert_entities(db_map):
        entities = db_map.find_entities()
        assert len(entities) == 5
        router = db_map.entity(entity_class_name="Gadget", name="router")
        assert router["description"] is None
        assert router["element_name_list"] == ()
        mouse = db_map.entity(entity_class_name="Gadget", name="mouse")
        assert mouse["description"] is None
        assert mouse["element_name_list"] == ()
        calculator = db_map.entity(entity_class_name="Widget", name="calculator")
        assert calculator["description"] == "Standard phone calculator."
        assert calculator["element_name_list"] == ()
        clock = db_map.entity(entity_class_name="Widget", name="clock")
        assert clock["description"] == "Fancy digital clock."
        assert clock["element_name_list"] == ()
        router_calculator = db_map.entity(entity_class_name="Gadget__Widget", entity_byname=("router", "calculator"))
        assert router_calculator["description"] is None
        assert router_calculator["element_name_list"] == ("router", "calculator")

    @staticmethod
    def _assert_parameter_definitions(db_map):
        parameter_definitions = db_map.find_parameter_definitions()
        assert len(parameter_definitions) == 11
        float_definition = db_map.parameter_definition(entity_class_name="Widget", name="float")
        assert float_definition["description"] == "Parameter with float values."
        assert float_definition["parsed_value"] == 2.3
        assert float_definition["parameter_value_list_name"] is None
        null_definition = db_map.parameter_definition(entity_class_name="Widget", name="none")
        assert null_definition["description"] == "Parameter with no values."
        assert null_definition["parsed_value"] is None
        assert null_definition["parameter_value_list_name"] is None
        value_list_definition = db_map.parameter_definition(entity_class_name="Widget", name="value_list")
        assert value_list_definition["description"] == "Parameter with list values."
        assert value_list_definition["parsed_value"] == "red"
        assert value_list_definition["parameter_value_list_name"] == "Enumeration"
        string_definition = db_map.parameter_definition(entity_class_name="Widget", name="string")
        assert string_definition["description"] == "Parameter with string values."
        assert string_definition["parsed_value"] == "a default string"
        assert string_definition["parameter_value_list_name"] is None
        boolean_definition = db_map.parameter_definition(entity_class_name="Widget", name="boolean")
        assert boolean_definition["description"] == "Parameter with boolean values."
        assert isinstance(boolean_definition["parsed_value"], bool)
        assert boolean_definition["parsed_value"]
        assert boolean_definition["parameter_value_list_name"] is None
        date_time_definition = db_map.parameter_definition(entity_class_name="Widget", name="date_time")
        assert date_time_definition["description"] == "Parameter with datetime values."
        assert date_time_definition["parsed_value"] == DateTime("2020-04-22T14:20:00")
        assert date_time_definition["parameter_value_list_name"] is None
        duration_definition = db_map.parameter_definition(entity_class_name="Widget", name="duration")
        assert duration_definition["description"] == "Parameter with duration values."
        assert duration_definition["parsed_value"] == Duration("5h")
        assert duration_definition["parameter_value_list_name"] is None
        time_pattern_definition = db_map.parameter_definition(entity_class_name="Widget", name="time_pattern")
        assert time_pattern_definition["description"] == "Parameter with time pattern values."
        assert time_pattern_definition["parsed_value"] == TimePattern(["D1-7"], [1.1])
        assert time_pattern_definition["parameter_value_list_name"] is None
        time_series_fixed_resolution_definition = db_map.parameter_definition(
            entity_class_name="Widget", name="time_series_fixed_resolution"
        )
        assert time_series_fixed_resolution_definition["description"] == "Parameter with time series values."
        assert time_series_fixed_resolution_definition["parsed_value"] == TimeSeriesFixedResolution(
            "2020-04-22 00:00:00", "3h", [1.1, 2.2, 3.3], ignore_year=True, repeat=False
        )
        assert time_series_fixed_resolution_definition["parameter_value_list_name"] is None
        time_series_variable_resolution_definition = db_map.parameter_definition(
            entity_class_name="Widget", name="time_series_variable_resolution"
        )
        assert time_series_variable_resolution_definition["description"] == "Parameter with time series values."
        assert time_series_variable_resolution_definition["parsed_value"] == TimeSeriesVariableResolution(
            ["2000-01-01T00:00:00", "2000-01-01T01:00:00", "2000-01-01T02:00:00"],
            [1.1, 2.2, 3.3],
            ignore_year=False,
            repeat=True,
        )
        assert time_series_variable_resolution_definition["parameter_value_list_name"] is None
        map_definition = db_map.parameter_definition(entity_class_name="Widget", name="map")
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


class TestMigrationFrom39e860a11b05:
    # This revision includes object groups, alternatives and scenarios that were not present in 070a0eb89e88.
    # Also, the Array value type was introduced between the revisions.
    def test_migration_to_latest(self, tmp_path):
        source_path = pathlib.Path(__file__).parent / "legacy_databases" / "39e860a11b05.sqlite"
        destination_path = tmp_path / "db.sqlite"
        shutil.copyfile(source_path, destination_path)
        url = "sqlite:///" + str(destination_path)
        with DatabaseMapping(url, upgrade=True) as db_map:
            self._assert_alternatives(db_map)
            self._assert_scenarios(db_map)
            self._assert_entity_classes(db_map)
            self._assert_entities(db_map)
            self._assert_entity_groups(db_map)
            self._assert_parameter_definitions(db_map)
            self._assert_parameter_values(db_map)

    @staticmethod
    def _assert_alternatives(db_map):
        alternatives = db_map.find_alternatives()
        assert len(alternatives) == 3
        base = db_map.alternative(name="Base")
        assert base["description"] == "Base alternative"
        high_greed = db_map.alternative(name="HighGreed")
        assert high_greed["description"] == "We want your money."
        low_greed = db_map.alternative(name="LowGreed")
        assert low_greed["description"] == "We can do with less."

    @staticmethod
    def _assert_scenarios(db_map):
        scenarios = db_map.find_scenarios()
        assert len(scenarios) == 3
        base = db_map.scenario(name="base")
        assert base["description"] == "Base prices."
        assert base["alternative_name_list"] == ["Base"]
        high_cost = db_map.scenario(name="high_cost")
        assert high_cost["description"] == "High prices."
        assert high_cost["alternative_name_list"] == ["Base", "HighGreed"]
        low_cost = db_map.scenario(name="low_cost")
        assert low_cost["description"] == "Low prices."
        assert low_cost["alternative_name_list"] == ["Base", "LowGreed"]

    @staticmethod
    def _assert_entity_classes(db_map):
        entity_classes = db_map.find_entity_classes()
        assert len(entity_classes) == 2
        widget_class = db_map.entity_class(name="Widget")
        assert widget_class["description"] is None
        assert widget_class["dimension_name_list"] == ()
        assert widget_class["display_icon"] == 280743406203308
        widget_class = db_map.entity_class(name="Chain")
        assert widget_class["description"] is None
        assert widget_class["dimension_name_list"] == ()
        assert widget_class["display_icon"] == 280923065020609

    @staticmethod
    def _assert_entities(db_map):
        assert len(db_map.find_entities()) == 4
        notepad = db_map.entity(entity_class_name="Widget", name="notepad")
        assert notepad["description"] is None
        wallet = db_map.entity(entity_class_name="Widget", name="wallet")
        assert wallet["description"] is None
        open_source = db_map.entity(entity_class_name="Widget", name="open_source")
        assert open_source["description"] is None
        links = db_map.entity(entity_class_name="Chain", name="links")
        assert links["description"] is None

    @staticmethod
    def _assert_entity_groups(db_map):
        group = db_map.find_entity_groups(entity_class_name="Widget", group_name="open_source")
        assert len(group) == 1
        group_item = group[0]
        assert group_item["member_name"] == "notepad"

    @staticmethod
    def _assert_parameter_definitions(db_map):
        assert len(db_map.find_parameter_definitions()) == 3
        price = db_map.parameter_definition(entity_class_name="Widget", name="price")
        assert price["description"] is None
        assert price["parsed_value"] is None
        assert price["parameter_value_list_name"] is None
        link_strength = db_map.parameter_definition(entity_class_name="Chain", name="link_strength")
        assert link_strength["description"] is None
        assert link_strength["parsed_value"] == Array([1.1, 2.2])
        assert link_strength["parameter_value_list_name"] is None
        yield_time = db_map.parameter_definition(entity_class_name="Chain", name="yield_time")
        assert yield_time["description"] is None
        assert yield_time["parsed_value"] == Array([Duration("2h"), Duration("3h")])
        assert yield_time["parameter_value_list_name"] is None

    @staticmethod
    def _assert_parameter_values(db_map):
        assert (len(db_map.find_parameter_values())) == 6
        assert (
            db_map.parameter_value(
                entity_class_name="Widget",
                entity_byname=("notepad",),
                parameter_definition_name="price",
                alternative_name="Base",
            )["parsed_value"]
            == 0.0
        )
        assert (
            db_map.parameter_value(
                entity_class_name="Widget",
                entity_byname=("wallet",),
                parameter_definition_name="price",
                alternative_name="Base",
            )["parsed_value"]
            == 2.2
        )
        assert (
            db_map.parameter_value(
                entity_class_name="Widget",
                entity_byname=("wallet",),
                parameter_definition_name="price",
                alternative_name="HighGreed",
            )["parsed_value"]
            == 3.3
        )
        assert (
            db_map.parameter_value(
                entity_class_name="Widget",
                entity_byname=("wallet",),
                parameter_definition_name="price",
                alternative_name="LowGreed",
            )["parsed_value"]
            == 1.1
        )
        link_strength = db_map.parameter_value(
            entity_class_name="Chain",
            entity_byname=("links",),
            parameter_definition_name="link_strength",
            alternative_name="Base",
        )
        assert link_strength["parsed_value"] == Array([-1.1, -2.2])
        yield_time = db_map.parameter_value(
            entity_class_name="Chain",
            entity_byname=("links",),
            parameter_definition_name="yield_time",
            alternative_name="Base",
        )
        assert yield_time["parsed_value"] == Array([Duration("2M"), Duration("3M")])


class TestMigrationFrom989fccf80441:
    # This revision includes tool feature methods that have been superseded by entity alternatives.
    def test_migration_to_latest(self, tmp_path):
        source_path = pathlib.Path(__file__).parent / "legacy_databases" / "989fccf80441.sqlite"
        destination_path = tmp_path / "db.sqlite"
        shutil.copyfile(source_path, destination_path)
        url = "sqlite:///" + str(destination_path)
        with DatabaseMapping(url, upgrade=True) as db_map:
            self._assert_alternatives(db_map)
            self._assert_parameter_value_lists(db_map)
            self._assert_entity_classes(db_map)
            self._assert_entities(db_map)
            self._assert_parameter_definitions(db_map)
            self._assert_parameter_values(db_map)
            self._assert_entity_alternatives(db_map)

    @staticmethod
    def _assert_alternatives(db_map):
        assert len(db_map.find_alternatives()) == 3
        base = db_map.alternative(name="Base")
        assert base["description"] == "Base alternative"
        inverted_activities = db_map.alternative(name="inverted_activities")
        assert inverted_activities["description"] == ""
        expected_activities = db_map.alternative(name="expected_activities")
        assert expected_activities["description"] == ""

    @staticmethod
    def _assert_parameter_value_lists(db_map):
        assert len(db_map.find_parameter_value_lists()) == 2
        booleans = db_map.parameter_value_list(name="Booleans")
        assert booleans["parsed_value_list"] == [True, False]
        yes_no = db_map.parameter_value_list(name="YesNo")
        assert yes_no["parsed_value_list"] == ["yes", "no"]

    @staticmethod
    def _assert_entity_classes(db_map):
        assert len(db_map.find_entity_classes()) == 6
        widget_class = db_map.entity_class(name="Widget")
        assert widget_class["description"] is None
        assert widget_class["dimension_name_list"] == ()
        assert widget_class["display_icon"] is None
        assert not widget_class["active_by_default"]
        gadget_class = db_map.entity_class(name="Gadget")
        assert gadget_class["description"] is None
        assert gadget_class["dimension_name_list"] == ()
        assert gadget_class["display_icon"] is None
        assert not gadget_class["active_by_default"]
        activist = db_map.entity_class(name="Activist")
        assert activist["description"] is None
        assert activist["dimension_name_list"] == ()
        assert activist["display_icon"] is None
        assert activist["active_by_default"]
        passivist = db_map.entity_class(name="Passivist")
        assert passivist["description"] is None
        assert passivist["dimension_name_list"] == ()
        assert passivist["display_icon"] is None
        assert not passivist["active_by_default"]
        positive = db_map.entity_class(name="Positive")
        assert positive["description"] is None
        assert positive["dimension_name_list"] == ()
        assert positive["display_icon"] is None
        assert positive["active_by_default"]
        negative = db_map.entity_class(name="Negative")
        assert negative["description"] is None
        assert negative["dimension_name_list"] == ()
        assert negative["display_icon"] is None
        assert not negative["active_by_default"]

    @staticmethod
    def _assert_entities(db_map):
        assert len(db_map.find_entities()) == 5
        assert db_map.entity(entity_class_name="Widget", name="undefined_activity")["description"] is None
        assert db_map.entity(entity_class_name="Widget", name="active")["description"] is None
        assert db_map.entity(entity_class_name="Widget", name="inactive")["description"] is None
        assert db_map.entity(entity_class_name="Gadget", name="yes_active")["description"] is None
        assert db_map.entity(entity_class_name="Gadget", name="no_active")["description"] is None

    @staticmethod
    def _assert_parameter_definitions(db_map):
        widget_is_active = db_map.parameter_definition(entity_class_name="Widget", name="is_active")
        assert widget_is_active["description"] is None
        assert widget_is_active["parsed_value"] is None
        assert widget_is_active["parameter_value_list_name"] == "Booleans"
        gadget_is_active = db_map.parameter_definition(entity_class_name="Gadget", name="is_active")
        assert gadget_is_active["description"] is None
        assert gadget_is_active["parsed_value"] is None
        assert gadget_is_active["parameter_value_list_name"] == "YesNo"

    @staticmethod
    def _assert_parameter_values(db_map):
        assert len(db_map.find_parameter_values()) == 0

    @staticmethod
    def _assert_entity_alternatives(db_map):
        assert len(db_map.find_entity_alternatives()) == 10
        with pytest.raises(SpineDBAPIError):
            db_map.entity_alternative(
                alternative_name="Base", entity_class_name="Widget", entity_byname=("undefined_activity",)
            )
        with pytest.raises(SpineDBAPIError):
            db_map.entity_alternative(alternative_name="Base", entity_class_name="Widget", entity_byname=("active",))
        with pytest.raises(SpineDBAPIError):
            db_map.entity_alternative(alternative_name="Base", entity_class_name="Widget", entity_byname=("inactive",))
        with pytest.raises(SpineDBAPIError):
            db_map.entity_alternative(
                alternative_name="Base", entity_class_name="Gadget", entity_byname=("yes_active",)
            )
        with pytest.raises(SpineDBAPIError):
            db_map.entity_alternative(alternative_name="Base", entity_class_name="Gadget", entity_byname=("no_active",))
        assert not db_map.entity_alternative(
            alternative_name="expected_activities", entity_class_name="Widget", entity_byname=("undefined_activity",)
        )["active"]
        assert db_map.entity_alternative(
            alternative_name="expected_activities", entity_class_name="Widget", entity_byname=("active",)
        )["active"]
        assert not db_map.entity_alternative(
            alternative_name="expected_activities", entity_class_name="Widget", entity_byname=("inactive",)
        )["active"]
        assert db_map.entity_alternative(
            alternative_name="expected_activities", entity_class_name="Gadget", entity_byname=("yes_active",)
        )["active"]
        assert not db_map.entity_alternative(
            alternative_name="expected_activities", entity_class_name="Gadget", entity_byname=("no_active",)
        )["active"]
        assert not db_map.entity_alternative(
            alternative_name="inverted_activities", entity_class_name="Widget", entity_byname=("undefined_activity",)
        )["active"]
        assert not db_map.entity_alternative(
            alternative_name="inverted_activities", entity_class_name="Widget", entity_byname=("active",)
        )["active"]
        assert db_map.entity_alternative(
            alternative_name="inverted_activities", entity_class_name="Widget", entity_byname=("inactive",)
        )["active"]
        assert not db_map.entity_alternative(
            alternative_name="inverted_activities", entity_class_name="Gadget", entity_byname=("yes_active",)
        )["active"]
        assert db_map.entity_alternative(
            alternative_name="inverted_activities", entity_class_name="Gadget", entity_byname=("no_active",)
        )["active"]
