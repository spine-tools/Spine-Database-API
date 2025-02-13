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
import datetime
import unittest
import numpy as np
import pandas as pd
from spinedb_api import DatabaseMapping, Map, to_database
import spinedb_api.dataframes as spine_df
from spinedb_api.parameter_value import FLOAT_VALUE_TYPE, DateTime, TimeSeriesVariableResolution
from tests.mock_helpers import AssertSuccessTestCase

# Copy-on-write will become default in Pandas 3.0.
# We want to receive excessive deprecation warnings during tests to stay future-proof.
# The setting below can be removed once we require pandas >= 3.0.
pd.options.mode.copy_on_write = "warn"


class TestToDataframe(AssertSuccessTestCase):
    def test_simple_value(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="Object"))
            self._assert_success(db_map.add_parameter_definition_item(name="y", entity_class_name="Object"))
            self._assert_success(db_map.add_entity_item(name="fork", entity_class_name="Object"))
            value_item = self._assert_success(
                db_map.add_parameter_value_item(
                    entity_class_name="Object",
                    entity_byname=("fork",),
                    parameter_definition_name="y",
                    alternative_name="Base",
                    parsed_value=2.3,
                )
            )
            dataframe = spine_df.to_dataframe(value_item)
            expected = pd.DataFrame(
                {
                    "entity_class_name": pd.Series(["Object"], dtype="category"),
                    "Object": pd.Series(["fork"], dtype="string"),
                    "parameter_definition_name": pd.Series(["y"], dtype="category"),
                    "alternative_name": pd.Series(["Base"], dtype="category"),
                    "value": [2.3],
                }
            )
            self.assertTrue(dataframe.equals(expected))

    def test_simple_map_value(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="Object"))
            self._assert_success(db_map.add_parameter_definition_item(name="y", entity_class_name="Object"))
            self._assert_success(db_map.add_entity_item(name="fork", entity_class_name="Object"))
            value_item = self._assert_success(
                db_map.add_parameter_value_item(
                    entity_class_name="Object",
                    entity_byname=("fork",),
                    parameter_definition_name="y",
                    alternative_name="Base",
                    parsed_value=Map(["A", "B"], [1.1, 1.2], index_name="letter"),
                )
            )
            dataframe = spine_df.to_dataframe(value_item)
            expected = pd.DataFrame(
                {
                    "entity_class_name": pd.Series(2 * ["Object"], dtype="category"),
                    "Object": pd.Series(["fork", "fork"], dtype="string"),
                    "parameter_definition_name": pd.Series(["y", "y"], dtype="category"),
                    "alternative_name": pd.Series(["Base", "Base"], dtype="category"),
                    "letter": ["A", "B"],
                    "value": [1.1, 1.2],
                }
            )
            self.assertTrue(dataframe.equals(expected))

    def test_unnamed_indexes_are_named_as_col_x(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="Object"))
            self._assert_success(db_map.add_parameter_definition_item(name="y", entity_class_name="Object"))
            self._assert_success(db_map.add_entity_item(name="fork", entity_class_name="Object"))
            value_item = self._assert_success(
                db_map.add_parameter_value_item(
                    entity_class_name="Object",
                    entity_byname=("fork",),
                    parameter_definition_name="y",
                    alternative_name="Base",
                    parsed_value=Map(["A"], [1.1]),
                )
            )
            dataframe = spine_df.to_dataframe(value_item)
            expected = pd.DataFrame(
                {
                    "entity_class_name": pd.Series(["Object"], dtype="category"),
                    "Object": pd.Series(["fork"], dtype="string"),
                    "parameter_definition_name": pd.Series(["y"], dtype="category"),
                    "alternative_name": pd.Series(["Base"], dtype="category"),
                    "col_1": ["A"],
                    "value": [1.1],
                }
            )
            self.assertTrue(dataframe.equals(expected))

    def test_time_series_value(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="Object"))
            self._assert_success(db_map.add_parameter_definition_item(name="y", entity_class_name="Object"))
            self._assert_success(db_map.add_entity_item(name="fork", entity_class_name="Object"))
            value_item = self._assert_success(
                db_map.add_parameter_value_item(
                    entity_class_name="Object",
                    entity_byname=("fork",),
                    parameter_definition_name="y",
                    alternative_name="Base",
                    parsed_value=TimeSeriesVariableResolution(
                        ["2025-02-05T12:30", "2025-02-05T12:45"], [1.1, 1.2], repeat=False, ignore_year=False
                    ),
                )
            )
            dataframe = spine_df.to_dataframe(value_item)
            expected = pd.DataFrame(
                {
                    "entity_class_name": pd.Series(2 * ["Object"], dtype="category"),
                    "Object": pd.Series(["fork", "fork"], dtype="string"),
                    "parameter_definition_name": pd.Series(["y", "y"], dtype="category"),
                    "alternative_name": pd.Series(["Base", "Base"], dtype="category"),
                    "t": np.array(["2025-02-05T12:30", "2025-02-05T12:45"], dtype="datetime64[s]"),
                    "value": [1.1, 1.2],
                }
            )
            self.assertTrue(dataframe.equals(expected))
            self.assertEqual(dataframe.attrs, {"t": {"ignore_year": "false", "repeat": "false"}})

    def test_time_series_value_of_multidimensional_entity(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="Object"))
            self._assert_success(db_map.add_entity_item(name="fork", entity_class_name="Object"))
            self._assert_success(db_map.add_entity_class_item(name="Subject"))
            self._assert_success(db_map.add_entity_item(name="spoon", entity_class_name="Subject"))
            self._assert_success(
                db_map.add_entity_class_item(name="from_object__to_subject", dimension_name_list=("Object", "Subject"))
            )
            self._assert_success(
                db_map.add_parameter_definition_item(name="y", entity_class_name="from_object__to_subject")
            )
            self._assert_success(
                db_map.add_entity_item(entity_class_name="from_object__to_subject", entity_byname=("fork", "spoon"))
            )
            value_item = self._assert_success(
                db_map.add_parameter_value_item(
                    entity_class_name="from_object__to_subject",
                    entity_byname=("fork", "spoon"),
                    parameter_definition_name="y",
                    alternative_name="Base",
                    parsed_value=TimeSeriesVariableResolution(
                        ["2025-02-05T12:30", "2025-02-05T12:45"], [1.1, 1.2], repeat=False, ignore_year=False
                    ),
                )
            )
            dataframe = spine_df.to_dataframe(value_item)
            expected = pd.DataFrame(
                {
                    "entity_class_name": pd.Series(2 * ["from_object__to_subject"], dtype="category"),
                    "Object": pd.Series(2 * ["fork"], dtype="string"),
                    "Subject": pd.Series(2 * ["spoon"], dtype="string"),
                    "parameter_definition_name": pd.Series(["y", "y"], dtype="category"),
                    "alternative_name": pd.Series(["Base", "Base"], dtype="category"),
                    "t": np.array(["2025-02-05T12:30", "2025-02-05T12:45"], dtype="datetime64[s]"),
                    "value": [1.1, 1.2],
                }
            )
            self.assertTrue(dataframe.equals(expected))
            self.assertEqual(dataframe.attrs, {"t": {"ignore_year": "false", "repeat": "false"}})


class TestAddOrUpdateFrom(AssertSuccessTestCase):
    def test_add_simple_parameter_value(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="Object"))
            self._assert_success(db_map.add_parameter_definition_item(name="length", entity_class_name="Object"))
            self._assert_success(db_map.add_entity_item(name="spoon", entity_class_name="Object"))
            dataframe = pd.DataFrame(
                {
                    "entity_class_name": ["Object"],
                    "Object": ["spoon"],
                    "parameter_definition_name": ["length"],
                    "alternative_name": ["Base"],
                    "value": [2.3],
                }
            )
            spine_df.add_or_update_from(dataframe, db_map)
            value_item = db_map.get_parameter_value_item(
                entity_class_name="Object",
                entity_byname=("spoon",),
                parameter_definition_name="length",
                alternative_name="Base",
            )
            self.assertEqual(value_item["parsed_value"], 2.3)

    def test_add_simple_parameter_value_into_multidimensional_entity(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="Object"))
            self._assert_success(db_map.add_entity_item(name="spoon", entity_class_name="Object"))
            self._assert_success(db_map.add_entity_class_item(name="Subject"))
            self._assert_success(db_map.add_entity_item(name="fork", entity_class_name="Subject"))
            self._assert_success(
                db_map.add_entity_class_item(name="RequiredCombinations", dimension_name_list=["Subject", "Object"])
            )
            self._assert_success(
                db_map.add_entity_item(entity_class_name="RequiredCombinations", entity_byname=("fork", "spoon"))
            )
            self._assert_success(
                db_map.add_parameter_definition_item(
                    name="subjectivity_fraction", entity_class_name="RequiredCombinations"
                )
            )
            dataframe = pd.DataFrame(
                {
                    "entity_class_name": ["RequiredCombinations"],
                    "Subject": ["fork"],
                    "Object": ["spoon"],
                    "parameter_definition_name": ["subjectivity_fraction"],
                    "alternative_name": ["Base"],
                    "value": [0.23],
                }
            )
            spine_df.add_or_update_from(dataframe, db_map)
            value_item = db_map.get_parameter_value_item(
                entity_class_name="RequiredCombinations",
                entity_byname=("fork", "spoon"),
                parameter_definition_name="subjectivity_fraction",
                alternative_name="Base",
            )
            self.assertEqual(value_item["parsed_value"], 0.23)

    def test_add_datetime_value(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="Object"))
            self._assert_success(db_map.add_parameter_definition_item(name="length", entity_class_name="Object"))
            self._assert_success(db_map.add_entity_item(name="spoon", entity_class_name="Object"))
            dataframe = pd.DataFrame(
                {
                    "entity_class_name": ["Object"],
                    "Object": ["spoon"],
                    "parameter_definition_name": ["length"],
                    "alternative_name": ["Base"],
                    "value": [datetime.datetime(2025, 2, 6, 9, 30)],
                }
            )
            spine_df.add_or_update_from(dataframe, db_map)
            value_item = db_map.get_parameter_value_item(
                entity_class_name="Object",
                entity_byname=("spoon",),
                parameter_definition_name="length",
                alternative_name="Base",
            )
            self.assertEqual(value_item["parsed_value"], DateTime("2025-02-06T09:30"))

    def test_add_map_parameter_value_with_time_stamps(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="Object"))
            self._assert_success(db_map.add_parameter_definition_item(name="length", entity_class_name="Object"))
            self._assert_success(db_map.add_entity_item(name="spoon", entity_class_name="Object"))
            dataframe = pd.DataFrame(
                {
                    "entity_class_name": ["Object", "Object"],
                    "Object": ["spoon", "spoon"],
                    "parameter_definition_name": ["length", "length"],
                    "alternative_name": ["Base", "Base"],
                    "my_index": [datetime.datetime(2025, 2, 6, 9, 45), datetime.datetime(2025, 2, 6, 10, 0)],
                    "value": [2.3, 2.5],
                }
            )
            spine_df.add_or_update_from(dataframe, db_map)
            value_item = db_map.get_parameter_value_item(
                entity_class_name="Object",
                entity_byname=("spoon",),
                parameter_definition_name="length",
                alternative_name="Base",
            )
            self.assertEqual(
                value_item["parsed_value"],
                Map([DateTime("2025-02-06T09:45"), DateTime("2025-02-06T10:00")], [2.3, 2.5], index_name="my_index"),
            )

    def test_add_map_parameter_value(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="Object"))
            self._assert_success(db_map.add_parameter_definition_item(name="length", entity_class_name="Object"))
            self._assert_success(db_map.add_entity_item(name="spoon", entity_class_name="Object"))
            dataframe = pd.DataFrame(
                {
                    "entity_class_name": ["Object", "Object"],
                    "Object": ["spoon", "spoon"],
                    "parameter_definition_name": ["length", "length"],
                    "alternative_name": ["Base", "Base"],
                    "my_index": ["A", "B"],
                    "value": [2.3, 2.5],
                }
            )
            spine_df.add_or_update_from(dataframe, db_map)
            value_item = db_map.get_parameter_value_item(
                entity_class_name="Object",
                entity_byname=("spoon",),
                parameter_definition_name="length",
                alternative_name="Base",
            )
            self.assertEqual(value_item["parsed_value"], Map(["A", "B"], [2.3, 2.5], index_name="my_index"))

    def test_add_multidimensional_map_parameter_value(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="Object"))
            self._assert_success(db_map.add_parameter_definition_item(name="length", entity_class_name="Object"))
            self._assert_success(db_map.add_entity_item(name="spoon", entity_class_name="Object"))
            dataframe = pd.DataFrame(
                {
                    "entity_class_name": 3 * ["Object"],
                    "Object": 3 * ["spoon"],
                    "parameter_definition_name": 3 * ["length"],
                    "alternative_name": 3 * ["Base"],
                    "my_index_1": ["A", "A", "B"],
                    "my_index_2": ["a", "b", "a"],
                    "my_index_3": ["1", "2", "1"],
                    "value": [1.1, 1.2, 2.1],
                }
            )
            spine_df.add_or_update_from(dataframe, db_map)
            value_item = db_map.get_parameter_value_item(
                entity_class_name="Object",
                entity_byname=("spoon",),
                parameter_definition_name="length",
                alternative_name="Base",
            )
            self.assertEqual(
                value_item["parsed_value"],
                Map(
                    ["A", "B"],
                    [
                        Map(
                            ["a", "b"],
                            [Map(["1"], [1.1], index_name="my_index_3"), Map(["2"], [1.2], index_name="my_index_3")],
                            index_name="my_index_2",
                        ),
                        Map(["a"], [Map(["1"], [2.2], index_name="my_index_3")], index_name="my_index_2"),
                    ],
                    index_name="my_index_1",
                ),
            )


class TestFetchAsDataframe(AssertSuccessTestCase):
    def test_fetch_from_empty_database(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            sq = spine_df.parameter_value_sq(db_map)
            fetched_maps = spine_df.FetchedMaps.fetch(db_map)
            dataframe = spine_df.fetch_as_dataframe(db_map, sq, fetched_maps)
            self.assertTrue(dataframe.empty)

    def test_fetch_scalar(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="Object"))
            self._assert_success(db_map.add_parameter_definition_item(name="y", entity_class_name="Object"))
            self._assert_success(db_map.add_entity_item(name="octopus", entity_class_name="Object"))
            value, value_type = to_database(2.3)
            self._assert_success(
                db_map.add_parameter_value_item(
                    entity_class_name="Object",
                    entity_byname=("octopus",),
                    parameter_definition_name="y",
                    alternative_name="Base",
                    value=value,
                    type=value_type,
                )
            )
            db_map.commit_session("Add test data")
            sq = spine_df.parameter_value_sq(db_map)
            fetched_maps = spine_df.FetchedMaps.fetch(db_map)
            dataframe = spine_df.fetch_as_dataframe(db_map, sq, fetched_maps)
            expected = pd.DataFrame(
                {
                    "entity_class_name": pd.Series(["Object"], dtype="category"),
                    "Object": pd.Series(["octopus"], dtype="string"),
                    "parameter_definition_name": pd.Series(["y"], dtype="category"),
                    "alternative_name": pd.Series(["Base"], dtype="category"),
                    "value": 2.3,
                }
            )
            self.assertTrue(dataframe.equals(expected))

    def test_fetch_simple_map(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="Object"))
            self._assert_success(db_map.add_parameter_definition_item(name="y", entity_class_name="Object"))
            self._assert_success(db_map.add_entity_item(name="octopus", entity_class_name="Object"))
            value, value_type = to_database(Map(["A", "B"], [2.3, 2.4], index_name="Letter"))
            self._assert_success(
                db_map.add_parameter_value_item(
                    entity_class_name="Object",
                    entity_byname=("octopus",),
                    parameter_definition_name="y",
                    alternative_name="Base",
                    value=value,
                    type=value_type,
                )
            )
            db_map.commit_session("Add test data")
            sq = spine_df.parameter_value_sq(db_map)
            fetched_maps = spine_df.FetchedMaps.fetch(db_map)
            dataframe = spine_df.fetch_as_dataframe(db_map, sq, fetched_maps)
            expected = pd.DataFrame(
                {
                    "entity_class_name": pd.Series(2 * ["Object"], dtype="category"),
                    "Object": pd.Series(["octopus", "octopus"], dtype="string"),
                    "parameter_definition_name": pd.Series(["y", "y"], dtype="category"),
                    "alternative_name": pd.Series(["Base", "Base"], dtype="category"),
                    "Letter": ["A", "B"],
                    "value": [2.3, 2.4],
                }
            )
            self.assertTrue(dataframe.equals(expected))


class TestFetchingAndConvertingItemsGiveEquivalentDataFrames(AssertSuccessTestCase):
    def test_simple_value(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="Object"))
            self._assert_success(db_map.add_parameter_definition_item(name="mass", entity_class_name="Object"))
            self._assert_success(db_map.add_entity_item(name="ladle", entity_class_name="Object"))
            value_item = self._assert_success(
                db_map.add_parameter_value_item(
                    entity_class_name="Object",
                    entity_byname=("ladle",),
                    parameter_definition_name="mass",
                    alternative_name="Base",
                    parsed_value=2.3,
                )
            )
            db_map.commit_session("Add test data.")
            maps = spine_df.FetchedMaps.fetch(db_map)
            subquery = spine_df.parameter_value_sq(db_map)
            queried_dataframe = spine_df.fetch_as_dataframe(db_map, subquery, maps)
            converted_dataframe = spine_df.to_dataframe(value_item)
            self.assertTrue(converted_dataframe.equals(queried_dataframe))

    def test_multidimensional_case(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="Node"))
            self._assert_success(db_map.add_entity_item(name="point", entity_class_name="Node"))
            self._assert_success(db_map.add_entity_class_item(name="Edge"))
            self._assert_success(db_map.add_entity_item(name="line", entity_class_name="Edge"))
            self._assert_success(db_map.add_entity_class_item(dimension_name_list=("Node", "Edge")))
            self._assert_success(db_map.add_parameter_definition_item(name="weight", entity_class_name="Node__Edge"))
            self._assert_success(
                db_map.add_entity_item(element_name_list=("point", "line"), entity_class_name="Node__Edge")
            )
            value_item = self._assert_success(
                db_map.add_parameter_value_item(
                    entity_class_name="Node__Edge",
                    entity_byname=("point", "line"),
                    parameter_definition_name="weight",
                    alternative_name="Base",
                    parsed_value=Map(["A", "B"], [1.1, 1.2], index_name="my_index"),
                )
            )
            db_map.commit_session("Add test data.")
            maps = spine_df.FetchedMaps.fetch(db_map)
            subquery = spine_df.parameter_value_sq(db_map)
            subquery = db_map.query(subquery).filter(subquery.c.entity_class_name == "Node__Edge").subquery()
            queried_dataframe = spine_df.fetch_as_dataframe(db_map, subquery, maps)
            converted_dataframe = spine_df.to_dataframe(value_item)
            self.assertTrue(converted_dataframe.equals(queried_dataframe))


class TestFetchEntityElementMap(AssertSuccessTestCase):
    def test_two_dimensional_entity(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="Subject"))
            self._assert_success(db_map.add_entity_class_item(name="Verb"))
            self._assert_success(db_map.add_entity_class_item(name="Phrase", dimension_name_list=("Verb", "Subject")))
            subject = self._assert_success(db_map.add_entity_item(name="me", entity_class_name="Subject"))
            verb = self._assert_success(db_map.add_entity_item(name="walk", entity_class_name="Verb"))
            phrase = self._assert_success(
                db_map.add_entity_item(entity_byname=("walk", "me"), entity_class_name="Phrase")
            )
            db_map.commit_session("Add test data.")
            element_map = spine_df._fetch_entity_element_map(db_map)
            self.assertEqual(len(element_map), 1)
            self.assertEqual(element_map[phrase["id"].db_id], [verb["id"].db_id, subject["id"].db_id])


class TestResolveElements(unittest.TestCase):
    def test_single_value(self):
        raw_data = pd.DataFrame(
            {
                "entity_class_id": [1],
                "entity_class_name": pd.Series(["Object"], dtype="category"),
                "entity_id": [2],
                "parameter_definition_name": pd.Series(["Y"], dtype="category"),
                "alternative_name": pd.Series(["Base"], dtype="category"),
                "value": [2.3],
            }
        )
        entity_class_name_map = {1: "Object"}
        entity_name_and_class_map = {2: ("fork", 1)}
        entity_element_map = {}
        resolved = spine_df._resolve_elements(
            raw_data, entity_class_name_map, entity_name_and_class_map, entity_element_map
        )
        expected = pd.DataFrame(
            {
                "entity_class_name": pd.Series(["Object"], dtype="category"),
                "Object": pd.Series(["fork"], dtype="string"),
                "parameter_definition_name": pd.Series(["Y"], dtype="category"),
                "alternative_name": pd.Series(["Base"], dtype="category"),
                "value": [2.3],
            }
        )
        self.assertTrue(resolved.equals(expected))

    def test_multidimensional_entity(self):
        raw_data = pd.DataFrame(
            {
                "entity_class_id": [1],
                "entity_class_name": pd.Series(["Relationship"], dtype="category"),
                "entity_id": [3],
                "parameter_definition_name": pd.Series(["Y"], dtype="category"),
                "alternative_name": pd.Series(["Base"], dtype="category"),
                "value": 2.3,
            }
        )
        entity_class_name_map = {1: "Relationship", 2: "Right", 3: "Left"}
        entity_name_and_class_map = {1: ("right", 2), 2: ("left", 3), 3: ("left__right", 1)}
        entity_element_map = {3: [2, 1]}
        resolved = spine_df._resolve_elements(
            raw_data, entity_class_name_map, entity_name_and_class_map, entity_element_map
        )
        expected = pd.DataFrame(
            {
                "entity_class_name": pd.Series(["Relationship"], dtype="category"),
                "Left": pd.Series(["left"], dtype="string"),
                "Right": pd.Series(["right"], dtype="string"),
                "parameter_definition_name": pd.Series(["Y"], dtype="category"),
                "alternative_name": pd.Series(["Base"], dtype="category"),
                "value": [2.3],
            }
        )
        self.assertTrue(resolved.equals(expected))

    def test_relationship_with_same_class_in_both_dimensions(self):
        raw_data = pd.DataFrame(
            {
                "entity_class_id": [2],
                "entity_class_name": pd.Series(["Relationship"], dtype="category"),
                "entity_id": [2],
                "parameter_definition_name": pd.Series(["Y"], dtype="category"),
                "alternative_name": pd.Series(["Base"], dtype="category"),
                "value": 2.3,
            }
        )
        entity_class_name_map = {1: "Both", 2: "Relationship"}
        entity_name_and_class_map = {1: ("both", 1), 2: ("both__both", 2)}
        entity_element_map = {2: [1, 1]}
        resolved = spine_df._resolve_elements(
            raw_data, entity_class_name_map, entity_name_and_class_map, entity_element_map
        )
        expected = pd.DataFrame(
            {
                "entity_class_name": pd.Series(["Relationship"], dtype="category"),
                "Both_1": pd.Series(["both"], dtype="string"),
                "Both_2": pd.Series(["both"], dtype="string"),
                "parameter_definition_name": pd.Series(["Y"], dtype="category"),
                "alternative_name": pd.Series(["Base"], dtype="category"),
                "value": [2.3],
            }
        )
        self.assertTrue(resolved.equals(expected))


class TestExpandValues(unittest.TestCase):
    def test_scalar_wont_get_expanded(self):
        value = 2.3
        dataframe = pd.DataFrame({"Object": ["spoon"], "value": [value], "type": [FLOAT_VALUE_TYPE]})
        resolved = spine_df._expand_values(dataframe)
        expected = pd.DataFrame({"Object": ["spoon"], "value": [2.3]})
        self.assertTrue(resolved.equals(expected))

    def test_expand_simple_map(self):
        value = pd.DataFrame({"x": ["A"], "value": [2.3]})
        dataframe = pd.DataFrame({"Object": ["spoon"], "value": [value], "type": [Map.type_()]})
        resolved = spine_df._expand_values(dataframe)
        expected = pd.DataFrame({"Object": ["spoon"], "x": ["A"], "value": [2.3]})
        self.assertTrue(resolved.equals(expected))

    def test_expand_multirow_map(self):
        value = pd.DataFrame({"x": ["A", "B"], "value": [2.3, 2.4]})
        dataframe = pd.DataFrame({"Object": ["spoon"], "value": [value], "type": [Map.type_()]})
        resolved = spine_df._expand_values(dataframe)
        expected = pd.DataFrame({"Object": ["spoon", "spoon"], "x": ["A", "B"], "value": [2.3, 2.4]})
        self.assertTrue(resolved.equals(expected))

    def test_expand_multiple_multirow_maps_with_same_indexes(self):
        value1 = pd.DataFrame({"x": ["A", "B"], "value": [2.3, 2.4]})
        value2 = pd.DataFrame({"x": ["C", "D"], "value": [2.5, 2.6]})
        dataframe = pd.DataFrame(
            {"Object": ["spoon", "fork"], "value": [value1, value2], "type": [Map.type_(), Map.type_()]}
        )
        resolved = spine_df._expand_values(dataframe)
        expected = pd.DataFrame(
            {"Object": ["spoon", "spoon", "fork", "fork"], "x": ["A", "B", "C", "D"], "value": [2.3, 2.4, 2.5, 2.6]}
        )
        self.assertTrue(resolved.equals(expected))

    def test_expand_multiple_multirow_maps_with_overlapping_indexes(self):
        value1 = pd.DataFrame({"i": ["A", "B"], "j": ["a", "b"], "value": [2.3, 2.4]})
        value2 = pd.DataFrame({"j": ["C", "D"], "k": ["c", "d"], "value": [2.5, 2.6]})
        dataframe = pd.DataFrame(
            {"Object": ["spoon", "fork"], "value": [value1, value2], "type": [Map.type_(), Map.type_()]}
        )
        resolved = spine_df._expand_values(dataframe)
        expected = pd.DataFrame(
            {
                "Object": ["spoon", "spoon", "fork", "fork"],
                "i": ["A", "B", np.nan, np.nan],
                "j": ["a", "b", "C", "D"],
                "k": [np.nan, np.nan, "c", "d"],
                "value": [2.3, 2.4, 2.5, 2.6],
            }
        )
        self.assertTrue(resolved.equals(expected))
