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
""" Unit tests for ``alternative_filter`` module. """
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from sqlalchemy.engine.url import URL
from spinedb_api import (
    DatabaseMapping,
    SpineDBAPIError,
    apply_alternative_filter_to_parameter_value_sq,
    from_database,
    import_alternatives,
    to_database,
)
from spinedb_api.filters.alternative_filter import (
    alternative_filter_config,
    alternative_filter_config_to_shorthand,
    alternative_filter_from_dict,
    alternative_filter_shorthand_to_config,
    alternative_names_from_dict,
)
from spinedb_api.import_functions import (
    import_entities,
    import_entity_classes,
    import_parameter_definitions,
    import_parameter_values,
)
from tests.mock_helpers import AssertSuccessTestCase


class TestAlternativeFilter(AssertSuccessTestCase):
    def test_alternative_filter_without_scenarios_or_alternatives(self):
        with TemporaryDirectory() as temp_dir:
            url = URL.create("sqlite", database=Path(temp_dir, "test_scenario_filter_mapping.sqlite").as_posix())
            with DatabaseMapping(url, create=True) as db_map:
                self._build_data_without_alternatives(db_map)
            with DatabaseMapping(url) as db_map:
                apply_alternative_filter_to_parameter_value_sq(db_map, [])
                parameters = db_map.query(db_map.parameter_value_sq).all()
                self.assertEqual(parameters, [])

    def test_alternative_filter_without_scenarios_or_alternatives_uncommitted_data(self):
        with TemporaryDirectory() as temp_dir:
            url = URL.create("sqlite", database=Path(temp_dir, "test_scenario_filter_mapping.sqlite").as_posix())
            with DatabaseMapping(url, create=True) as db_map:
                self._build_data_without_alternatives(db_map, commit=False)
                apply_alternative_filter_to_parameter_value_sq(db_map, alternatives=[])
                parameters = db_map.query(db_map.parameter_value_sq).all()
                self.assertEqual(parameters, [])
                db_map.rollback_session()

    def test_alternative_filter(self):
        with TemporaryDirectory() as temp_dir:
            url = URL.create("sqlite", database=Path(temp_dir, "test_scenario_filter_mapping.sqlite").as_posix())
            with DatabaseMapping(url, create=True) as db_map:
                self._build_data_with_single_alternative(db_map)
            with DatabaseMapping(url) as db_map:
                apply_alternative_filter_to_parameter_value_sq(db_map, ["alternative"])
                parameters = db_map.query(db_map.parameter_value_sq).all()
                self.assertEqual(len(parameters), 1)
                self.assertEqual(parameters[0].value, to_database(-23.0)[0])

    def test_alternative_filter_uncommitted_data(self):
        with TemporaryDirectory() as temp_dir:
            url = URL.create("sqlite", database=Path(temp_dir, "test_scenario_filter_mapping.sqlite").as_posix())
            with DatabaseMapping(url, create=True) as db_map:
                self._build_data_with_single_alternative(db_map, commit=False)
                with self.assertRaises(SpineDBAPIError):
                    apply_alternative_filter_to_parameter_value_sq(db_map, ["alternative"])
                parameters = db_map.query(db_map.parameter_value_sq).all()
                self.assertEqual(len(parameters), 0)
                db_map.rollback_session()

    def test_alternative_filter_from_dict(self):
        with TemporaryDirectory() as temp_dir:
            url = URL.create("sqlite", database=Path(temp_dir, "test_scenario_filter_mapping.sqlite").as_posix())
            with DatabaseMapping(url, create=True) as db_map:
                self._build_data_with_single_alternative(db_map)
            with DatabaseMapping(url) as db_map:
                config = alternative_filter_config(["alternative"])
                alternative_filter_from_dict(db_map, config)
                parameters = db_map.query(db_map.parameter_value_sq).all()
                self.assertEqual(len(parameters), 1)
                self.assertEqual(parameters[0].value, to_database(-23.0)[0])

    def test_alternative_names_with_colons(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._build_data_with_single_alternative(db_map)
            self._add_value_in_alternative(db_map, 23.0, "new@2023-23-23T11:12:13")
            config = alternative_filter_config(["new@2023-23-23T11:12:13"])
            alternative_filter_from_dict(db_map, config)
            parameters = db_map.query(db_map.parameter_value_sq).all()
            self.assertEqual(len(parameters), 1)
            self.assertEqual(parameters[0].value, to_database(23.0)[0])

    def test_multiple_alternatives(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._build_data_with_single_alternative(db_map)
            self._add_value_in_alternative(db_map, 23.0, "new@2023-23-23T11:12:13")
            self._add_value_in_alternative(db_map, 101.1, "new@2005-05-05T22:23:24")
            config = alternative_filter_config(["new@2005-05-05T22:23:24", "new@2023-23-23T11:12:13"])
            alternative_filter_from_dict(db_map, config)
            parameters = db_map.query(db_map.parameter_value_sq).all()
            self.assertEqual(len(parameters), 2)
            values = {from_database(p.value, p.type) for p in parameters}
            self.assertEqual(values, {23.0, 101.1})

    def test_filters_alternatives(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_alternative_item(name="visible"))
            db_map.commit_session("Add alternative")
            config = alternative_filter_config(["visible"])
            alternative_filter_from_dict(db_map, config)
            alternatives = db_map.query(db_map.alternative_sq).all()
            self.assertEqual(len(alternatives), 1)
            self.assertEqual(alternatives[0].name, "visible")

    def test_filters_scenarios(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_alternative_item(name="visible"))
            self._assert_success(db_map.add_scenario_item(name="Empty"))
            self._assert_success(db_map.add_scenario_item(name="Base and visible"))
            self._assert_success(
                db_map.add_scenario_alternative_item(scenario_name="Base and visible", alternative_name="Base", rank=1)
            )
            self._assert_success(
                db_map.add_scenario_alternative_item(
                    scenario_name="Base and visible", alternative_name="visible", rank=2
                )
            )
            self._assert_success(db_map.add_scenario_item(name="Base only"))
            self._assert_success(
                db_map.add_scenario_alternative_item(scenario_name="Base only", alternative_name="Base", rank=1)
            )
            self._assert_success(db_map.add_scenario_item(name="Visible only"))
            self._assert_success(
                db_map.add_scenario_alternative_item(scenario_name="Visible only", alternative_name="visible", rank=1)
            )
            db_map.commit_session("Add scenarios")
            config = alternative_filter_config(["visible"])
            alternative_filter_from_dict(db_map, config)
            scenarios = db_map.query(db_map.scenario_sq).all()
            self.assertEqual(len(scenarios), 2)
            self.assertCountEqual([s.name for s in scenarios], ["Empty", "Visible only"])

    def test_filters_scenario_alternatives(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_alternative_item(name="visible"))
            self._assert_success(db_map.add_scenario_item(name="Base and visible"))
            self._assert_success(
                db_map.add_scenario_alternative_item(scenario_name="Base and visible", alternative_name="Base", rank=1)
            )
            self._assert_success(
                db_map.add_scenario_alternative_item(
                    scenario_name="Base and visible", alternative_name="visible", rank=2
                )
            )
            db_map.commit_session("Add scenario with two alternatives")
            config = alternative_filter_config(["visible"])
            alternative_filter_from_dict(db_map, config)
            scenario_alternatives = db_map.query(db_map.scenario_alternative_sq).all()
            self.assertEqual(len(scenario_alternatives), 0)

    def test_filters_entities_in_class_thats_active_by_default(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="ActiveByDefault", active_by_default=True))
            self._assert_success(db_map.add_entity_item(name="visible_by_default", entity_class_name="ActiveByDefault"))
            self._assert_success(db_map.add_entity_item(name="visible", entity_class_name="ActiveByDefault"))
            self._assert_success(
                db_map.add_entity_alternative_item(
                    entity_class_name="ActiveByDefault",
                    entity_byname=("visible",),
                    alternative_name="Base",
                    active=True,
                )
            )
            self._assert_success(db_map.add_entity_item(name="invisible", entity_class_name="ActiveByDefault"))
            self._assert_success(
                db_map.add_entity_alternative_item(
                    entity_class_name="ActiveByDefault",
                    entity_byname=("invisible",),
                    alternative_name="Base",
                    active=False,
                )
            )
            db_map.commit_session("Add entities.")
            config = alternative_filter_config(["Base"])
            alternative_filter_from_dict(db_map, config)
            entities = db_map.query(db_map.entity_sq).all()
            self.assertEqual(len(entities), 2)
            self.assertCountEqual([e.name for e in entities], ["visible", "visible_by_default"])

    def test_filters_entities_in_class_that_isnt_active_by_default(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="InactiveByDefault", active_by_default=False))
            self._assert_success(
                db_map.add_entity_item(name="invisible_by_default", entity_class_name="InactiveByDefault")
            )
            self._assert_success(db_map.add_entity_item(name="visible", entity_class_name="InactiveByDefault"))
            self._assert_success(
                db_map.add_entity_alternative_item(
                    entity_class_name="InactiveByDefault",
                    entity_byname=("visible",),
                    alternative_name="Base",
                    active=True,
                )
            )
            self._assert_success(db_map.add_entity_item(name="invisible", entity_class_name="InactiveByDefault"))
            self._assert_success(
                db_map.add_entity_alternative_item(
                    entity_class_name="InactiveByDefault",
                    entity_byname=("invisible",),
                    alternative_name="Base",
                    active=False,
                )
            )
            db_map.commit_session("Add entities.")
            config = alternative_filter_config(["Base"])
            alternative_filter_from_dict(db_map, config)
            entities = db_map.query(db_map.entity_sq).all()
            self.assertEqual(len(entities), 1)
            self.assertCountEqual([e.name for e in entities], ["visible"])

    def test_filters_multidimensional_entities_that_have_inactive_elements(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="Object"))
            self._assert_success(db_map.add_entity_item(name="invisible", entity_class_name="Object"))
            self._assert_success(
                db_map.add_entity_alternative_item(
                    entity_class_name="Object", entity_byname=("invisible",), alternative_name="Base", active=False
                )
            )
            self._assert_success(db_map.add_entity_item(name="visible", entity_class_name="Object"))
            self._assert_success(
                db_map.add_entity_alternative_item(
                    entity_class_name="Object", entity_byname=("visible",), alternative_name="Base", active=True
                )
            )
            self._assert_success(db_map.add_entity_class_item(name="Relationship", dimension_name_list=["Object"]))
            self._assert_success(db_map.add_entity_item(entity_byname=("invisible",), entity_class_name="Relationship"))
            self._assert_success(db_map.add_entity_item(entity_byname=("visible",), entity_class_name="Relationship"))
            db_map.commit_session("Add entities.")
            config = alternative_filter_config(["Base"])
            alternative_filter_from_dict(db_map, config)
            entities = db_map.query(db_map.entity_sq).all()
            self.assertEqual(len(entities), 2)
            self.assertCountEqual([e.name for e in entities], ["visible", "visible__"])

    def test_filters_parameters_values_of_inactive_entities(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="Object"))
            self._assert_success(db_map.add_entity_item(name="invisible", entity_class_name="Object"))
            self._assert_success(
                db_map.add_entity_alternative_item(
                    entity_class_name="Object", entity_byname=("invisible",), alternative_name="Base", active=False
                )
            )
            self._assert_success(db_map.add_entity_item(name="visible", entity_class_name="Object"))
            self._assert_success(
                db_map.add_entity_alternative_item(
                    entity_class_name="Object", entity_byname=("visible",), alternative_name="Base", active=True
                )
            )
            self._assert_success(db_map.add_parameter_definition_item(name="y", entity_class_name="Object"))
            value, value_type = to_database(-2.3)
            self._assert_success(
                db_map.add_parameter_value_item(
                    entity_class_name="Object",
                    entity_byname=("invisible",),
                    parameter_definition_name="y",
                    alternative_name="Base",
                    value=value,
                    type=value_type,
                )
            )
            value, value_type = to_database(2.3)
            self._assert_success(
                db_map.add_parameter_value_item(
                    entity_class_name="Object",
                    entity_byname=("visible",),
                    parameter_definition_name="y",
                    alternative_name="Base",
                    value=value,
                    type=value_type,
                )
            )
            db_map.commit_session("Add values.")
            config = alternative_filter_config(["Base"])
            alternative_filter_from_dict(db_map, config)
            values = db_map.query(db_map.parameter_value_sq).all()
            self.assertEqual(len(values), 1)
            self.assertEqual(from_database(values[0].value, values[0].type), 2.3)

    def test_filters_parameters_values_by_active_by_default(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="ActiveByDefault", active_by_default=True))
            self._assert_success(db_map.add_parameter_definition_item(name="x", entity_class_name="ActiveByDefault"))
            self._assert_success(db_map.add_entity_item(name="visible", entity_class_name="ActiveByDefault"))
            value, value_type = to_database(-2.3)
            self._assert_success(
                db_map.add_parameter_value_item(
                    entity_class_name="ActiveByDefault",
                    entity_byname=("visible",),
                    parameter_definition_name="x",
                    alternative_name="Base",
                    value=value,
                    type=value_type,
                )
            )
            self._assert_success(db_map.add_entity_class_item(name="InactiveByDefault", active_by_default=False))
            self._assert_success(db_map.add_parameter_definition_item(name="y", entity_class_name="InactiveByDefault"))
            self._assert_success(db_map.add_entity_item(name="invisible", entity_class_name="InactiveByDefault"))
            value, value_type = to_database(2.3)
            self._assert_success(
                db_map.add_parameter_value_item(
                    entity_class_name="InactiveByDefault",
                    entity_byname=("invisible",),
                    parameter_definition_name="y",
                    alternative_name="Base",
                    value=value,
                    type=value_type,
                )
            )
            db_map.commit_session("Add values.")
            config = alternative_filter_config(["Base"])
            alternative_filter_from_dict(db_map, config)
            values = db_map.query(db_map.parameter_value_sq).all()
            self.assertEqual(len(values), 1)
            self.assertEqual(from_database(values[0].value, values[0].type), -2.3)

    def test_filters_parameter_values_of_multidimensional_entities_inactive_by_elements(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="Object"))
            self._assert_success(db_map.add_entity_item(name="invisible", entity_class_name="Object"))
            self._assert_success(
                db_map.add_entity_alternative_item(
                    entity_class_name="Object", entity_byname=("invisible",), alternative_name="Base", active=False
                )
            )
            self._assert_success(db_map.add_entity_item(name="visible", entity_class_name="Object"))
            self._assert_success(
                db_map.add_entity_alternative_item(
                    entity_class_name="Object", entity_byname=("visible",), alternative_name="Base", active=True
                )
            )
            self._assert_success(db_map.add_entity_class_item(name="Relationship", dimension_name_list=("Object",)))
            self._assert_success(
                db_map.add_entity_item(element_name_list=("invisible",), entity_class_name="Relationship")
            )
            self._assert_success(
                db_map.add_entity_item(element_name_list=("visible",), entity_class_name="Relationship")
            )
            self._assert_success(db_map.add_parameter_definition_item(name="y", entity_class_name="Relationship"))
            value, value_type = to_database(2.3)
            self._assert_success(
                db_map.add_parameter_value_item(
                    entity_class_name="Relationship",
                    entity_byname=("invisible",),
                    parameter_definition_name="y",
                    alternative_name="Base",
                    value=value,
                    type=value_type,
                )
            )
            value, value_type = to_database(-2.3)
            self._assert_success(
                db_map.add_parameter_value_item(
                    entity_class_name="Relationship",
                    entity_byname=("visible",),
                    parameter_definition_name="y",
                    alternative_name="Base",
                    value=value,
                    type=value_type,
                )
            )
            db_map.commit_session("Add values.")
            config = alternative_filter_config(["Base"])
            alternative_filter_from_dict(db_map, config)
            values = db_map.query(db_map.parameter_value_sq).all()
            self.assertEqual(len(values), 1)
            self.assertEqual(from_database(values[0].value, values[0].type), -2.3)

    def test_entity_groups(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="Visible", active_by_default=True))
            self._assert_success(db_map.add_entity_item(name="default_active", entity_class_name="Visible"))
            self._assert_success(db_map.add_entity_item(name="active", entity_class_name="Visible"))
            self._assert_success(
                db_map.add_entity_alternative_item(
                    entity_byname=("active",), entity_class_name="Visible", alternative_name="Base", active=True
                )
            )
            self._assert_success(db_map.add_entity_item(name="inactive", entity_class_name="Visible"))
            self._assert_success(
                db_map.add_entity_alternative_item(
                    entity_byname=("inactive",), entity_class_name="Visible", alternative_name="Base", active=False
                )
            )
            self._assert_success(db_map.add_entity_item(name="default_active_group", entity_class_name="Visible"))
            self._assert_success(
                db_map.add_entity_group_item(
                    entity_class_name="Visible", group_name="default_active_group", member_name="default_active"
                )
            )
            self._assert_success(
                db_map.add_entity_group_item(
                    entity_class_name="Visible", group_name="default_active_group", member_name="active"
                )
            )
            self._assert_success(
                db_map.add_entity_group_item(
                    entity_class_name="Visible", group_name="default_active_group", member_name="inactive"
                )
            )
            self._assert_success(db_map.add_entity_item(name="active_group", entity_class_name="Visible"))
            self._assert_success(
                db_map.add_entity_alternative_item(
                    entity_byname=("active_group",), entity_class_name="Visible", alternative_name="Base", active=True
                )
            )
            self._assert_success(
                db_map.add_entity_group_item(
                    entity_class_name="Visible", group_name="active_group", member_name="default_active"
                )
            )
            self._assert_success(
                db_map.add_entity_group_item(
                    entity_class_name="Visible", group_name="active_group", member_name="active"
                )
            )
            self._assert_success(
                db_map.add_entity_group_item(
                    entity_class_name="Visible", group_name="active_group", member_name="inactive"
                )
            )
            self._assert_success(db_map.add_entity_item(name="inactive_group", entity_class_name="Visible"))
            self._assert_success(
                db_map.add_entity_alternative_item(
                    entity_byname=("inactive_group",),
                    entity_class_name="Visible",
                    alternative_name="Base",
                    active=False,
                )
            )
            self._assert_success(
                db_map.add_entity_group_item(
                    entity_class_name="Visible", group_name="inactive_group", member_name="default_active"
                )
            )
            self._assert_success(
                db_map.add_entity_group_item(
                    entity_class_name="Visible", group_name="inactive_group", member_name="active"
                )
            )
            self._assert_success(
                db_map.add_entity_group_item(
                    entity_class_name="Visible", group_name="inactive_group", member_name="inactive"
                )
            )
            self._assert_success(db_map.add_entity_class_item(name="Invisible", active_by_default=False))
            self._assert_success(db_map.add_entity_item(name="default_inactive", entity_class_name="Invisible"))
            self._assert_success(db_map.add_entity_item(name="active", entity_class_name="Invisible"))
            self._assert_success(
                db_map.add_entity_alternative_item(
                    entity_byname=("active",), entity_class_name="Invisible", alternative_name="Base", active=True
                )
            )
            self._assert_success(db_map.add_entity_item(name="inactive", entity_class_name="Invisible"))
            self._assert_success(
                db_map.add_entity_alternative_item(
                    entity_byname=("inactive",), entity_class_name="Invisible", alternative_name="Base", active=False
                )
            )
            self._assert_success(db_map.add_entity_item(name="default_inactive_group", entity_class_name="Invisible"))
            self._assert_success(
                db_map.add_entity_group_item(
                    entity_class_name="Invisible", group_name="default_inactive_group", member_name="default_inactive"
                )
            )
            self._assert_success(
                db_map.add_entity_group_item(
                    entity_class_name="Invisible", group_name="default_inactive_group", member_name="active"
                )
            )
            self._assert_success(
                db_map.add_entity_group_item(
                    entity_class_name="Invisible", group_name="default_inactive_group", member_name="inactive"
                )
            )
            self._assert_success(db_map.add_entity_item(name="active_group", entity_class_name="Invisible"))
            self._assert_success(
                db_map.add_entity_alternative_item(
                    entity_byname=("active_group",), entity_class_name="Invisible", alternative_name="Base", active=True
                )
            )
            self._assert_success(
                db_map.add_entity_group_item(
                    entity_class_name="Invisible", group_name="active_group", member_name="default_inactive"
                )
            )
            self._assert_success(
                db_map.add_entity_group_item(
                    entity_class_name="Invisible", group_name="active_group", member_name="active"
                )
            )
            self._assert_success(
                db_map.add_entity_group_item(
                    entity_class_name="Invisible", group_name="active_group", member_name="inactive"
                )
            )
            self._assert_success(db_map.add_entity_item(name="inactive_group", entity_class_name="Invisible"))
            self._assert_success(
                db_map.add_entity_alternative_item(
                    entity_byname=("inactive_group",),
                    entity_class_name="Invisible",
                    alternative_name="Base",
                    active=False,
                )
            )
            self._assert_success(
                db_map.add_entity_group_item(
                    entity_class_name="Invisible", group_name="inactive_group", member_name="default_inactive"
                )
            )
            self._assert_success(
                db_map.add_entity_group_item(
                    entity_class_name="Invisible", group_name="inactive_group", member_name="active"
                )
            )
            self._assert_success(
                db_map.add_entity_group_item(
                    entity_class_name="Invisible", group_name="inactive_group", member_name="inactive"
                )
            )
            db_map.commit_session("Add test data.")
            config = alternative_filter_config(["Base"])
            alternative_filter_from_dict(db_map, config)
            groups = db_map.query(db_map.entity_group_sq).all()
            self.assertEqual(len(groups), 5)
            class_names = {row.id: row.name for row in db_map.query(db_map.entity_class_sq)}
            entity_names = {row.id: row.name for row in db_map.query(db_map.entity_sq)}
            data = {}
            for row in groups:
                data.setdefault(class_names[row.entity_class_id], {}).setdefault(
                    entity_names[row.entity_id], set()
                ).add(entity_names[row.member_id])
            expected = {
                "Visible": {
                    "default_active_group": {"default_active", "active"},
                    "active_group": {"default_active", "active"},
                },
                "Invisible": {"active_group": {"active"}},
            }
            self.assertEqual(data, expected)

    def _build_data_without_alternatives(self, db_map, commit=True):
        self._assert_imports(import_entity_classes(db_map, ["object_class"]))
        self._assert_imports(import_entities(db_map, [("object_class", "object")]))
        self._assert_imports(import_parameter_definitions(db_map, [("object_class", "parameter")]))
        self._assert_imports(import_parameter_values(db_map, [("object_class", "object", "parameter", -23.0)]))
        if commit:
            db_map.commit_session("Add test data")

    def _build_data_with_single_alternative(self, db_map, commit=True):
        self._assert_imports(import_alternatives(db_map, ["alternative"]))
        self._assert_imports(import_entity_classes(db_map, ["object_class"]))
        self._assert_imports(import_entities(db_map, [("object_class", "object")]))
        self._assert_imports(import_parameter_definitions(db_map, [("object_class", "parameter")]))
        self._assert_imports(import_parameter_values(db_map, [("object_class", "object", "parameter", -1.0)]))
        self._assert_imports(
            import_parameter_values(db_map, [("object_class", "object", "parameter", -23.0, "alternative")])
        )
        if commit:
            db_map.commit_session("Add test data")

    def _add_value_in_alternative(self, db_map, value, alternative):
        self._assert_imports(import_alternatives(db_map, [alternative]))
        self._assert_imports(
            import_parameter_values(db_map, [("object_class", "object", "parameter", value, alternative)])
        )
        db_map.commit_session(f"Add value in {alternative}")


class TestAlternativeFilterWithoutDatabase(unittest.TestCase):
    def test_alternative_filter_config(self):
        config = alternative_filter_config(["alternative1", "alternative2"])
        self.assertEqual(config, {"type": "alternative_filter", "alternatives": ["alternative1", "alternative2"]})

    def test_alternative_names_from_dict(self):
        config = alternative_filter_config(["alternative1", "alternative2"])
        self.assertEqual(alternative_names_from_dict(config), ["alternative1", "alternative2"])

    def test_alternative_filter_config_to_shorthand(self):
        config = alternative_filter_config(["alternative1", "alternative2"])
        shorthand = alternative_filter_config_to_shorthand(config)
        self.assertEqual(shorthand, "alternatives:'alternative1':'alternative2'")

    def test_alternative_filter_shorthand_to_config(self):
        config = alternative_filter_shorthand_to_config("alternatives:'alternative1':'alternative2'")
        self.assertEqual(config, {"type": "alternative_filter", "alternatives": ["alternative1", "alternative2"]})

    def test_quoted_alternative_names(self):
        config = alternative_filter_shorthand_to_config("alternatives:'alt:er:na:ti:ve':'alternative2'")
        self.assertEqual(config, {"type": "alternative_filter", "alternatives": ["alt:er:na:ti:ve", "alternative2"]})


if __name__ == "__main__":
    unittest.main()
