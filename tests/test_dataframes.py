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
import numpy as np
import pandas as pd
from spinedb_api import DatabaseMapping, Map, to_database
import spinedb_api.dataframes as spine_df
from spinedb_api.parameter_value import FLOAT_VALUE_TYPE
from tests.mock_helpers import AssertSuccessTestCase


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
                {"Object": ["octopus"], "parameter_definition_name": ["y"], "alternative_name": ["Base"], "value": 2.3}
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
                    "Object": ["octopus", "octopus"],
                    "parameter_definition_name": ["y", "y"],
                    "alternative_name": ["Base", "Base"],
                    "Letter": ["A", "B"],
                    "value": [2.3, 2.4],
                }
            )
            self.assertTrue(dataframe.equals(expected))


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
            element_map = spine_df.fetch_entity_element_map(db_map)
            self.assertEqual(len(element_map), 1)
            self.assertEqual(element_map[phrase["id"].db_id], [verb["id"].db_id, subject["id"].db_id])


class TestResolveElements(unittest.TestCase):
    def test_single_value(self):
        raw_data = pd.DataFrame(
            {
                "entity_class_id": [1],
                "parameter_definition_name": ["Y"],
                "entity_id": [2],
                "alternative_name": ["Base"],
                "value": 2.3,
            }
        )
        entity_class_name_map = {1: "Object"}
        entity_name_and_class_map = {2: ("fork", 1)}
        entity_element_map = {}
        resolved = spine_df.resolve_elements(
            raw_data, entity_class_name_map, entity_name_and_class_map, entity_element_map
        )
        expected = pd.DataFrame(
            {"Object": ["fork"], "parameter_definition_name": ["Y"], "alternative_name": ["Base"], "value": [2.3]}
        )
        self.assertTrue(resolved.equals(expected))

    def test_multidimensional_entity(self):
        raw_data = pd.DataFrame(
            {
                "entity_class_id": [1],
                "parameter_definition_name": ["Y"],
                "entity_id": [3],
                "alternative_name": ["Base"],
                "value": 2.3,
            }
        )
        entity_class_name_map = {1: "Relationship", 2: "Right", 3: "Left"}
        entity_name_and_class_map = {1: ("right", 2), 2: ("left", 3), 3: ("left__right", 1)}
        entity_element_map = {3: [2, 1]}
        resolved = spine_df.resolve_elements(
            raw_data, entity_class_name_map, entity_name_and_class_map, entity_element_map
        )
        expected = pd.DataFrame(
            {
                "Left": ["left"],
                "Right": ["right"],
                "parameter_definition_name": ["Y"],
                "alternative_name": ["Base"],
                "value": [2.3],
            }
        )
        self.assertTrue(resolved.equals(expected))

    def test_relationship_with_same_class_in_both_dimensions(self):
        raw_data = pd.DataFrame(
            {
                "entity_class_id": [2],
                "parameter_definition_name": ["Y"],
                "entity_id": [2],
                "alternative_name": ["Base"],
                "value": 2.3,
            }
        )
        entity_class_name_map = {1: "Both", 2: "Relationship"}
        entity_name_and_class_map = {1: ("both", 1), 2: ("both__both", 2)}
        entity_element_map = {2: [1, 1]}
        resolved = spine_df.resolve_elements(
            raw_data, entity_class_name_map, entity_name_and_class_map, entity_element_map
        )
        expected = pd.DataFrame(
            {
                "Both_1": ["both"],
                "Both_2": ["both"],
                "parameter_definition_name": ["Y"],
                "alternative_name": ["Base"],
                "value": [2.3],
            }
        )
        self.assertTrue(resolved.equals(expected))


class TestExpandValues(unittest.TestCase):
    def test_scalar_wont_get_expanded(self):
        value = 2.3
        dataframe = pd.DataFrame({"Object": ["spoon"], "value": [value], "type": [FLOAT_VALUE_TYPE]})
        resolved = spine_df.expand_values(dataframe)
        expected = pd.DataFrame({"Object": ["spoon"], "value": [2.3]})
        self.assertTrue(resolved.equals(expected))

    def test_expand_simple_map(self):
        value = pd.DataFrame({"x": ["A"], "value": [2.3]})
        dataframe = pd.DataFrame({"Object": ["spoon"], "value": [value], "type": [Map.type_()]})
        resolved = spine_df.expand_values(dataframe)
        expected = pd.DataFrame({"Object": ["spoon"], "x": ["A"], "value": [2.3]})
        self.assertTrue(resolved.equals(expected))

    def test_expand_multirow_map(self):
        value = pd.DataFrame({"x": ["A", "B"], "value": [2.3, 2.4]})
        dataframe = pd.DataFrame({"Object": ["spoon"], "value": [value], "type": [Map.type_()]})
        resolved = spine_df.expand_values(dataframe)
        expected = pd.DataFrame({"Object": ["spoon", "spoon"], "x": ["A", "B"], "value": [2.3, 2.4]})
        self.assertTrue(resolved.equals(expected))

    def test_expand_multiple_multirow_maps_with_same_indexes(self):
        value1 = pd.DataFrame({"x": ["A", "B"], "value": [2.3, 2.4]})
        value2 = pd.DataFrame({"x": ["C", "D"], "value": [2.5, 2.6]})
        dataframe = pd.DataFrame(
            {"Object": ["spoon", "fork"], "value": [value1, value2], "type": [Map.type_(), Map.type_()]}
        )
        resolved = spine_df.expand_values(dataframe)
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
        resolved = spine_df.expand_values(dataframe)
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
