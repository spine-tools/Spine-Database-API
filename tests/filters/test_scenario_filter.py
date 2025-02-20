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
""" Unit tests for ``alternative_value_filter`` module. """
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from spinedb_api import (
    DatabaseMapping,
    SpineDBAPIError,
    append_filter_config,
    apply_filter_stack,
    apply_scenario_filter_to_subqueries,
    from_database,
    to_database,
)
from spinedb_api.filters.scenario_filter import (
    scenario_filter_config,
    scenario_filter_config_to_shorthand,
    scenario_filter_from_dict,
    scenario_filter_shorthand_to_config,
    scenario_name_from_dict,
)
from tests.mock_helpers import AssertSuccessTestCase


class TestScenarioFilterInMemory(AssertSuccessTestCase):
    def test_filter_entities_with_default_activity_only(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_entity_class_item(name="visible", active_by_default=True))
            self._assert_success(db_map.add_entity_item(name="visible_object", entity_class_name="visible"))
            self._assert_success(db_map.add_entity_class_item(name="hidden", active_by_default=False))
            self._assert_success(db_map.add_entity_item(name="invisible_object", entity_class_name="hidden"))
            self._assert_success(db_map.add_scenario_item(name="S"))
            db_map.commit_session("Add data.")
            apply_filter_stack(db_map, [scenario_filter_config("S")])
            entities = db_map.query(db_map.wide_entity_sq).all()
            self.assertEqual(len(entities), 1)
            self.assertEqual(entities[0].name, "visible_object")

    def test_filter_entities_with_default_activity(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_alternative_item(name="alt"))
            self._assert_success(db_map.add_entity_class_item(name="visible_by_default", active_by_default=True))
            self._assert_success(db_map.add_entity_item(name="visible", entity_class_name="visible_by_default"))
            self._assert_success(db_map.add_entity_item(name="hidden", entity_class_name="visible_by_default"))
            self._assert_success(
                db_map.add_entity_alternative_item(
                    entity_class_name="visible_by_default",
                    entity_byname=("hidden",),
                    alternative_name="alt",
                    active=False,
                )
            )
            self._assert_success(db_map.add_entity_class_item(name="hidden_by_default", active_by_default=False))
            self._assert_success(db_map.add_entity_item(name="visible", entity_class_name="hidden_by_default"))
            self._assert_success(
                db_map.add_entity_alternative_item(
                    entity_class_name="hidden_by_default",
                    entity_byname=("visible",),
                    alternative_name="alt",
                    active=True,
                )
            )
            self._assert_success(db_map.add_entity_item(name="hidden", entity_class_name="hidden_by_default"))
            self._assert_success(db_map.add_scenario_item(name="S"))
            self._assert_success(
                db_map.add_scenario_alternative_item(scenario_name="S", alternative_name="alt", rank=0)
            )
            db_map.commit_session("Add data.")
            apply_filter_stack(db_map, [scenario_filter_config("S")])
            entities = db_map.query(db_map.wide_entity_sq).all()
            self.assertEqual(len(entities), 2)
            self.assertEqual(entities[0].name, "visible")
            self.assertEqual(entities[1].name, "visible")

    def test_filter_entity_that_is_not_active_in_scenario(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_alternative_item(name="alt"))
            self._assert_success(db_map.add_scenario_item(name="scen"))
            self._assert_success(
                db_map.add_scenario_alternative_item(scenario_name="scen", alternative_name="Base", rank=0)
            )
            self._assert_success(db_map.add_entity_class_item(name="Gadget", active_by_default=False))
            self._assert_success(db_map.add_entity_item(name="fork", entity_class_name="Gadget"))
            self._assert_success(
                db_map.add_entity_alternative_item(
                    entity_byname=("fork",), entity_class_name="Gadget", alternative_name="alt", active=True
                )
            )
            db_map.commit_session("Add test data.")
            apply_filter_stack(db_map, [scenario_filter_config("scen")])
            entities = db_map.query(db_map.wide_entity_sq).all()
            self.assertEqual(len(entities), 0)

    def test_filter_entity_groups(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_scenario_item(name="scen"))
            self._assert_success(
                db_map.add_scenario_alternative_item(scenario_name="scen", alternative_name="Base", rank=0)
            )
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
            apply_filter_stack(db_map, [scenario_filter_config("scen")])
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


class DataBuilderTestCase(AssertSuccessTestCase):
    def _build_data_with_single_scenario(self, db_map, commit=True):
        self._assert_success(db_map.add_alternative_item(name="alternative"))
        self._assert_success(db_map.add_entity_class_item(name="object_class"))
        self._assert_success(db_map.add_entity_item(entity_class_name="object_class", name="object"))
        self._assert_success(db_map.add_parameter_definition_item(entity_class_name="object_class", name="parameter"))
        value, value_type = to_database(-1.0)
        self._assert_success(
            db_map.add_parameter_value_item(
                entity_class_name="object_class",
                entity_byname=("object",),
                parameter_definition_name="parameter",
                value=value,
                type=value_type,
                alternative_name="Base",
            )
        )
        value, value_type = to_database(23.0)
        self._assert_success(
            db_map.add_parameter_value_item(
                entity_class_name="object_class",
                entity_byname=("object",),
                parameter_definition_name="parameter",
                value=value,
                type=value_type,
                alternative_name="alternative",
            )
        )
        self._assert_success(db_map.add_scenario_item(name="scenario"))
        self._assert_success(
            db_map.add_scenario_alternative_item(scenario_name="scenario", alternative_name="alternative", rank=1)
        )
        for entity_class in db_map.get_entity_class_items():
            entity_class.update(active_by_default=True)
        if commit:
            db_map.commit_session("Add test data.")


class TestScenarioFilter(DataBuilderTestCase):

    def test_scenario_filter(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + str(Path(temp_dir, "db.sqlite"))
            with DatabaseMapping(url, create=True) as db_map:
                self._build_data_with_single_scenario(db_map)
            with DatabaseMapping(url) as db_map:
                apply_scenario_filter_to_subqueries(db_map, "scenario")
                parameters = db_map.query(db_map.parameter_value_sq).all()
                self.assertEqual(len(parameters), 1)
                self.assertEqual(parameters[0].value, b"23.0")
                alternatives = [a._asdict() for a in db_map.query(db_map.alternative_sq)]
                self.assertEqual(alternatives, [{"name": "alternative", "description": None, "id": 2, "commit_id": 2}])
                scenarios = [s._asdict() for s in db_map.query(db_map.wide_scenario_sq)]
                self.assertEqual(
                    scenarios,
                    [
                        {
                            "name": "scenario",
                            "description": None,
                            "active": False,
                            "alternative_name_list": "alternative",
                            "alternative_id_list": "2",
                            "id": 1,
                            "commit_id": 2,
                        }
                    ],
                )

    def test_scenario_filter_uncommitted_data(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._build_data_with_single_scenario(db_map, commit=False)
            with self.assertRaises(SpineDBAPIError):
                apply_scenario_filter_to_subqueries(db_map, "scenario")
            parameters = db_map.query(db_map.parameter_value_sq).all()
            self.assertEqual(len(parameters), 0)
            alternatives = [a._asdict() for a in db_map.query(db_map.alternative_sq)]
            self.assertEqual(
                alternatives, [{"name": "Base", "description": "Base alternative", "id": 1, "commit_id": 1}]
            )
            scenarios = db_map.query(db_map.wide_scenario_sq).all()
            self.assertEqual(len(scenarios), 0)
            db_map.rollback_session()

    def test_scenario_filter_works_for_entity_sq(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + str(Path(temp_dir, "db.sqlite"))
            with DatabaseMapping(url, create=True) as db_map:
                self._assert_success(db_map.add_alternative_item(name="alternative1"))
                self._assert_success(db_map.add_alternative_item(name="alternative2"))
                self._assert_success(db_map.add_entity_class_item(name="class1"))
                self._assert_success(db_map.add_entity_class_item(name="class2"))
                self._assert_success(
                    db_map.add_entity_class_item(name="class1__class2", dimension_name_list=["class1", "class2"])
                )
                self._assert_success(db_map.add_entity_item(name="obj1", entity_class_name="class1"))
                self._assert_success(db_map.add_entity_item(name="obj2", entity_class_name="class2"))
                self._assert_success(db_map.add_entity_item(name="obj22", entity_class_name="class2"))
                self._assert_success(
                    db_map.add_entity_item(element_name_list=["obj1", "obj2"], entity_class_name="class1__class2")
                )
                self._assert_success(
                    db_map.add_entity_item(element_name_list=["obj1", "obj22"], entity_class_name="class1__class2")
                )
                self._assert_success(
                    db_map.add_entity_alternative_item(
                        entity_class_name="class2",
                        entity_byname=("obj2",),
                        alternative_name="alternative1",
                        active=True,
                    )
                )
                self._assert_success(
                    db_map.add_entity_alternative_item(
                        entity_class_name="class2",
                        entity_byname=("obj2",),
                        alternative_name="alternative2",
                        active=False,
                    )
                )
                self._assert_success(
                    db_map.add_entity_alternative_item(
                        entity_class_name="class2",
                        entity_byname=("obj22",),
                        alternative_name="alternative1",
                        active=False,
                    )
                )
                self._assert_success(
                    db_map.add_entity_alternative_item(
                        entity_class_name="class2",
                        entity_byname=("obj22",),
                        alternative_name="alternative2",
                        active=True,
                    )
                )
                self._assert_success(db_map.add_scenario_item(name="scenario1"))
                self._assert_success(
                    db_map.add_scenario_alternative_item(
                        scenario_name="scenario1", alternative_name="alternative1", rank=1
                    )
                )
                self._assert_success(
                    db_map.add_scenario_alternative_item(
                        scenario_name="scenario1", alternative_name="alternative2", rank=2
                    )
                )
                for entity_class in db_map.get_entity_class_items():
                    entity_class.update(active_by_default=True)
                db_map.commit_session("Add test data")
            with DatabaseMapping(url) as db_map:
                entities = db_map.query(db_map.entity_sq).all()
                self.assertEqual(len(entities), 5)
                apply_scenario_filter_to_subqueries(db_map, "scenario1")
                # After this, obj2 should be excluded because it is inactive in the highest-ranked alternative2
                # The multidimensional entity 'class1__class2, (obj1, obj2)' should also be excluded because involves obj2
                entities = db_map.query(db_map.wide_entity_sq).all()
                self.assertEqual(len(entities), 3)
                entity_names = {
                    name
                    for x in entities
                    for name in (x.element_name_list.split(",") if x.element_name_list else (x.name,))
                }
                self.assertFalse("obj2" in entity_names)

    def test_scenario_filter_works_for_object_parameter_value_sq(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + str(Path(temp_dir, "db.sqlite"))
            with DatabaseMapping(url, create=True) as db_map:
                self._build_data_with_single_scenario(db_map)
            with DatabaseMapping(url) as db_map:
                apply_scenario_filter_to_subqueries(db_map, "scenario")
                parameters = db_map.query(db_map.object_parameter_value_sq).all()
                self.assertEqual(len(parameters), 1)
                self.assertEqual(parameters[0].value, b"23.0")
                alternatives = [a._asdict() for a in db_map.query(db_map.alternative_sq)]
                self.assertEqual(alternatives, [{"name": "alternative", "description": None, "id": 2, "commit_id": 2}])
                scenarios = [s._asdict() for s in db_map.query(db_map.wide_scenario_sq)]
                self.assertEqual(
                    scenarios,
                    [
                        {
                            "name": "scenario",
                            "description": None,
                            "active": False,
                            "alternative_name_list": "alternative",
                            "alternative_id_list": "2",
                            "id": 1,
                            "commit_id": 2,
                        }
                    ],
                )

    def test_scenario_filter_works_for_relationship_parameter_value_sq(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + str(Path(temp_dir, "db.sqlite"))
            with DatabaseMapping(url, create=True) as db_map:
                self._build_data_with_single_scenario(db_map, commit=False)
                self._assert_success(
                    db_map.add_entity_class_item(name="relationship_class", dimension_name_list=["object_class"])
                )
                self._assert_success(
                    db_map.add_parameter_definition_item(
                        entity_class_name="relationship_class", name="relationship_parameter"
                    )
                )
                self._assert_success(
                    db_map.add_entity_item(entity_class_name="relationship_class", element_name_list=["object"])
                )
                value, value_type = to_database(-1.0)
                self._assert_success(
                    db_map.add_parameter_value_item(
                        entity_class_name="relationship_class",
                        entity_byname=("object",),
                        parameter_definition_name="relationship_parameter",
                        value=value,
                        type=value_type,
                        alternative_name="Base",
                    )
                )
                value, value_type = to_database(23.0)
                self._assert_success(
                    db_map.add_parameter_value_item(
                        entity_class_name="relationship_class",
                        entity_byname=("object",),
                        parameter_definition_name="relationship_parameter",
                        value=value,
                        type=value_type,
                        alternative_name="alternative",
                    )
                )
                db_map.commit_session("Add test data")
            with DatabaseMapping(url) as db_map:
                apply_scenario_filter_to_subqueries(db_map, "scenario")
                parameters = db_map.query(db_map.relationship_parameter_value_sq).all()
                self.assertEqual(len(parameters), 1)
                self.assertEqual((parameters[0].value, parameters[0].type), to_database(23.0))
                alternatives = [a._asdict() for a in db_map.query(db_map.alternative_sq)]
                self.assertEqual(alternatives, [{"name": "alternative", "description": None, "id": 2, "commit_id": 2}])
                scenarios = [s._asdict() for s in db_map.query(db_map.wide_scenario_sq)]
                self.assertEqual(
                    scenarios,
                    [
                        {
                            "name": "scenario",
                            "description": None,
                            "active": False,
                            "alternative_name_list": "alternative",
                            "alternative_id_list": "2",
                            "id": 1,
                            "commit_id": 2,
                        }
                    ],
                )

    def test_scenario_filter_selects_highest_ranked_alternative(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + str(Path(temp_dir, "db.sqlite"))
            with DatabaseMapping(url, create=True) as db_map:
                self._assert_success(db_map.add_alternative_item(name="alternative3"))
                self._assert_success(db_map.add_alternative_item(name="alternative1"))
                self._assert_success(db_map.add_alternative_item(name="alternative2"))
                self._assert_success(db_map.add_entity_class_item(name="object_class"))
                self._assert_success(db_map.add_entity_item(entity_class_name="object_class", name="object"))
                self._assert_success(
                    db_map.add_parameter_definition_item(entity_class_name="object_class", name="parameter")
                )
                value, value_type = to_database(-1.0)
                self._assert_success(
                    db_map.add_parameter_value_item(
                        entity_class_name="object_class",
                        entity_byname=("object",),
                        parameter_definition_name="parameter",
                        value=value,
                        type=value_type,
                        alternative_name="Base",
                    )
                )
                value, value_type = to_database(10.0)
                self._assert_success(
                    db_map.add_parameter_value_item(
                        entity_class_name="object_class",
                        entity_byname=("object",),
                        parameter_definition_name="parameter",
                        value=value,
                        type=value_type,
                        alternative_name="alternative1",
                    )
                )
                value, value_type = to_database(2000.0)
                self._assert_success(
                    db_map.add_parameter_value_item(
                        entity_class_name="object_class",
                        entity_byname=("object",),
                        parameter_definition_name="parameter",
                        value=value,
                        type=value_type,
                        alternative_name="alternative2",
                    )
                )
                value, value_type = to_database(300.0)
                self._assert_success(
                    db_map.add_parameter_value_item(
                        entity_class_name="object_class",
                        entity_byname=("object",),
                        parameter_definition_name="parameter",
                        value=value,
                        type=value_type,
                        alternative_name="alternative3",
                    )
                )
                self._assert_success(db_map.add_scenario_item(name="scenario"))
                self._assert_success(
                    db_map.add_scenario_alternative_item(
                        scenario_name="scenario", alternative_name="alternative2", rank=3
                    )
                )
                self._assert_success(
                    db_map.add_scenario_alternative_item(
                        scenario_name="scenario", alternative_name="alternative3", rank=2
                    )
                )
                self._assert_success(
                    db_map.add_scenario_alternative_item(
                        scenario_name="scenario", alternative_name="alternative1", rank=1
                    )
                )
                db_map.commit_session("Add test data")
            with DatabaseMapping(url, create=True) as db_map:
                apply_scenario_filter_to_subqueries(db_map, "scenario")
                parameters = db_map.query(db_map.parameter_value_sq).all()
                self.assertEqual(len(parameters), 1)
                self.assertEqual((parameters[0].value, parameters[0].type), to_database(2000.0))
                alternatives = [a._asdict() for a in db_map.query(db_map.alternative_sq)]
                self.assertEqual(
                    alternatives,
                    [
                        {"name": "alternative3", "description": None, "id": 2, "commit_id": 2},
                        {"name": "alternative1", "description": None, "id": 3, "commit_id": 2},
                        {"name": "alternative2", "description": None, "id": 4, "commit_id": 2},
                    ],
                )
                scenarios = [s._asdict() for s in db_map.query(db_map.wide_scenario_sq)]
                self.assertEqual(
                    scenarios,
                    [
                        {
                            "name": "scenario",
                            "description": None,
                            "active": False,
                            "alternative_name_list": "alternative1,alternative3,alternative2",
                            "alternative_id_list": "3,2,4",
                            "id": 1,
                            "commit_id": 2,
                        }
                    ],
                )

    def test_scenario_filter_selects_highest_ranked_alternative_of_active_scenario(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + str(Path(temp_dir, "db.sqlite"))
            with DatabaseMapping(url, create=True) as db_map:
                self._assert_success(db_map.add_alternative_item(name="alternative3"))
                self._assert_success(db_map.add_alternative_item(name="alternative1"))
                self._assert_success(db_map.add_alternative_item(name="alternative2"))
                self._assert_success(db_map.add_alternative_item(name="non_active_alternative"))
                self._assert_success(db_map.add_entity_class_item(name="object_class"))
                self._assert_success(db_map.add_entity_item(entity_class_name="object_class", name="object"))
                self._assert_success(
                    db_map.add_parameter_definition_item(entity_class_name="object_class", name="parameter")
                )
                value, value_type = to_database(-1.0)
                self._assert_success(
                    db_map.add_parameter_value_item(
                        entity_class_name="object_class",
                        entity_byname=("object",),
                        parameter_definition_name="parameter",
                        value=value,
                        type=value_type,
                        alternative_name="Base",
                    )
                )
                value, value_type = to_database(10.0)
                self._assert_success(
                    db_map.add_parameter_value_item(
                        entity_class_name="object_class",
                        entity_byname=("object",),
                        parameter_definition_name="parameter",
                        value=value,
                        type=value_type,
                        alternative_name="alternative1",
                    )
                )
                value, value_type = to_database(2000.0)
                self._assert_success(
                    db_map.add_parameter_value_item(
                        entity_class_name="object_class",
                        entity_byname=("object",),
                        parameter_definition_name="parameter",
                        value=value,
                        type=value_type,
                        alternative_name="alternative2",
                    )
                )
                value, value_type = to_database(300.0)
                self._assert_success(
                    db_map.add_parameter_value_item(
                        entity_class_name="object_class",
                        entity_byname=("object",),
                        parameter_definition_name="parameter",
                        value=value,
                        type=value_type,
                        alternative_name="alternative3",
                    )
                )
                self._assert_success(db_map.add_scenario_item(name="scenario"))
                self._assert_success(db_map.add_scenario_item(name="non_active_scenario"))
                self._assert_success(
                    db_map.add_scenario_alternative_item(
                        scenario_name="scenario", alternative_name="alternative1", rank=1
                    )
                )
                self._assert_success(
                    db_map.add_scenario_alternative_item(
                        scenario_name="scenario", alternative_name="alternative3", rank=2
                    )
                )
                self._assert_success(
                    db_map.add_scenario_alternative_item(
                        scenario_name="scenario", alternative_name="alternative2", rank=3
                    )
                )
                self._assert_success(
                    db_map.add_scenario_alternative_item(
                        scenario_name="non_active_scenario", alternative_name="alternative1", rank=1
                    )
                )
                self._assert_success(
                    db_map.add_scenario_alternative_item(
                        scenario_name="non_active_scenario", alternative_name="alternative3", rank=2
                    )
                )
                self._assert_success(
                    db_map.add_scenario_alternative_item(
                        scenario_name="non_active_scenario", alternative_name="alternative2", rank=3
                    )
                )
                self._assert_success(
                    db_map.add_scenario_alternative_item(
                        scenario_name="non_active_scenario", alternative_name="non_active_alternative", rank=4
                    )
                )
                db_map.commit_session("Add test data")
            with DatabaseMapping(url, create=True) as db_map:
                apply_scenario_filter_to_subqueries(db_map, "scenario")
                parameters = db_map.query(db_map.parameter_value_sq).all()
                self.assertEqual(len(parameters), 1)
                self.assertEqual((parameters[0].value, parameters[0].type), to_database(2000.0))
                alternatives = [a._asdict() for a in db_map.query(db_map.alternative_sq)]
                self.assertEqual(
                    alternatives,
                    [
                        {"name": "alternative3", "description": None, "id": 2, "commit_id": 2},
                        {"name": "alternative1", "description": None, "id": 3, "commit_id": 2},
                        {"name": "alternative2", "description": None, "id": 4, "commit_id": 2},
                    ],
                )
                scenarios = [s._asdict() for s in db_map.query(db_map.wide_scenario_sq)]
                self.assertEqual(
                    scenarios,
                    [
                        {
                            "name": "scenario",
                            "description": None,
                            "active": False,
                            "alternative_name_list": "alternative1,alternative3,alternative2",
                            "alternative_id_list": "3,2,4",
                            "id": 1,
                            "commit_id": 2,
                        }
                    ],
                )

    def test_scenario_filter_for_multiple_objects_and_parameters(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + str(Path(temp_dir, "db.sqlite"))
            with DatabaseMapping(url, create=True) as db_map:
                self._assert_success(db_map.add_alternative_item(name="alternative"))
                self._assert_success(db_map.add_entity_class_item(name="object_class"))
                self._assert_success(db_map.add_entity_item(entity_class_name="object_class", name="object1"))
                self._assert_success(db_map.add_entity_item(entity_class_name="object_class", name="object2"))
                self._assert_success(
                    db_map.add_parameter_definition_item(entity_class_name="object_class", name="parameter1")
                )
                self._assert_success(
                    db_map.add_parameter_definition_item(entity_class_name="object_class", name="parameter2")
                )
                value, value_type = to_database(-1.0)
                self._assert_success(
                    db_map.add_parameter_value_item(
                        entity_class_name="object_class",
                        entity_byname=("object1",),
                        parameter_definition_name="parameter1",
                        value=value,
                        type=value_type,
                        alternative_name="Base",
                    )
                )
                value, value_type = to_database(10.0)
                self._assert_success(
                    db_map.add_parameter_value_item(
                        entity_class_name="object_class",
                        entity_byname=("object1",),
                        parameter_definition_name="parameter1",
                        value=value,
                        type=value_type,
                        alternative_name="alternative",
                    )
                )
                value, value_type = to_database(-1.0)
                self._assert_success(
                    db_map.add_parameter_value_item(
                        entity_class_name="object_class",
                        entity_byname=("object1",),
                        parameter_definition_name="parameter2",
                        value=value,
                        type=value_type,
                        alternative_name="Base",
                    )
                )
                value, value_type = to_database(11.0)
                self._assert_success(
                    db_map.add_parameter_value_item(
                        entity_class_name="object_class",
                        entity_byname=("object1",),
                        parameter_definition_name="parameter2",
                        value=value,
                        type=value_type,
                        alternative_name="alternative",
                    )
                )
                value, value_type = to_database(-2.0)
                self._assert_success(
                    db_map.add_parameter_value_item(
                        entity_class_name="object_class",
                        entity_byname=("object2",),
                        parameter_definition_name="parameter1",
                        value=value,
                        type=value_type,
                        alternative_name="Base",
                    )
                )
                value, value_type = to_database(20.0)
                self._assert_success(
                    db_map.add_parameter_value_item(
                        entity_class_name="object_class",
                        entity_byname=("object2",),
                        parameter_definition_name="parameter1",
                        value=value,
                        type=value_type,
                        alternative_name="alternative",
                    )
                )
                value, value_type = to_database(-2.0)
                self._assert_success(
                    db_map.add_parameter_value_item(
                        entity_class_name="object_class",
                        entity_byname=("object2",),
                        parameter_definition_name="parameter2",
                        value=value,
                        type=value_type,
                        alternative_name="Base",
                    )
                )
                value, value_type = to_database(22.0)
                self._assert_success(
                    db_map.add_parameter_value_item(
                        entity_class_name="object_class",
                        entity_byname=("object2",),
                        parameter_definition_name="parameter2",
                        value=value,
                        type=value_type,
                        alternative_name="alternative",
                    )
                )
                self._assert_success(db_map.add_scenario_item(name="scenario"))
                self._assert_success(
                    db_map.add_scenario_alternative_item(
                        scenario_name="scenario", alternative_name="alternative", rank=1
                    )
                )
                for item in db_map.get_entity_class_items():
                    item.update(active_by_default=True)
                db_map.commit_session("Add test data")
            with DatabaseMapping(url, create=True) as db_map:
                apply_scenario_filter_to_subqueries(db_map, "scenario")
                parameters = db_map.query(db_map.parameter_value_sq).all()
                self.assertEqual(len(parameters), 4)
                object_names = {o.id: o.name for o in db_map.query(db_map.object_sq)}
                alternative_names = {a.id: a.name for a in db_map.query(db_map.alternative_sq)}
                parameter_names = {d.id: d.name for d in db_map.query(db_map.parameter_definition_sq)}
                datamined_values = {}
                for parameter in parameters:
                    self.assertEqual(alternative_names[parameter.alternative_id], "alternative")
                    parameter_values = datamined_values.setdefault(object_names[parameter.entity_id], {})
                    parameter_values[parameter_names[parameter.parameter_definition_id]] = parameter.value
                self.assertEqual(
                    datamined_values,
                    {
                        "object1": {"parameter1": b"10.0", "parameter2": b"11.0"},
                        "object2": {"parameter1": b"20.0", "parameter2": b"22.0"},
                    },
                )
                alternatives = [a._asdict() for a in db_map.query(db_map.alternative_sq)]
                self.assertEqual(alternatives, [{"name": "alternative", "description": None, "id": 2, "commit_id": 2}])
                scenarios = [s._asdict() for s in db_map.query(db_map.wide_scenario_sq)]
                self.assertEqual(
                    scenarios,
                    [
                        {
                            "name": "scenario",
                            "description": None,
                            "active": False,
                            "alternative_name_list": "alternative",
                            "alternative_id_list": "2",
                            "id": 1,
                            "commit_id": 2,
                        }
                    ],
                )

    def test_filters_scenarios_and_alternatives(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + str(Path(temp_dir, "db.sqlite"))
            with DatabaseMapping(url, create=True) as db_map:
                self._assert_success(db_map.add_scenario_item(name="scenario1"))
                self._assert_success(db_map.add_scenario_item(name="scenario2"))
                self._assert_success(db_map.add_alternative_item(name="alternative1"))
                self._assert_success(db_map.add_alternative_item(name="alternative2"))
                self._assert_success(db_map.add_alternative_item(name="alternative3"))
                self._assert_success(
                    db_map.add_scenario_alternative_item(
                        scenario_name="scenario1", alternative_name="alternative1", rank=1
                    )
                )
                self._assert_success(
                    db_map.add_scenario_alternative_item(
                        scenario_name="scenario1", alternative_name="alternative2", rank=2
                    )
                )
                self._assert_success(
                    db_map.add_scenario_alternative_item(
                        scenario_name="scenario2", alternative_name="alternative2", rank=1
                    )
                )
                self._assert_success(
                    db_map.add_scenario_alternative_item(
                        scenario_name="scenario2", alternative_name="alternative3", rank=2
                    )
                )
                db_map.commit_session("Add test data.")
            with DatabaseMapping(url, create=True) as db_map:
                apply_scenario_filter_to_subqueries(db_map, "scenario2")
                alternatives = [a._asdict() for a in db_map.query(db_map.alternative_sq)]
                self.assertEqual(
                    alternatives,
                    [
                        {"name": "alternative2", "description": None, "id": 3, "commit_id": 2},
                        {"name": "alternative3", "description": None, "id": 4, "commit_id": 2},
                    ],
                )
                scenarios = [s._asdict() for s in db_map.query(db_map.wide_scenario_sq)]
                self.assertEqual(
                    scenarios,
                    [
                        {
                            "name": "scenario2",
                            "description": None,
                            "active": False,
                            "alternative_name_list": "alternative2,alternative3",
                            "alternative_id_list": "3,4",
                            "id": 2,
                            "commit_id": 2,
                        }
                    ],
                )

    def test_filters_entity_alternatives(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + str(Path(temp_dir, "db.sqlite"))
            with DatabaseMapping(url, create=True) as db_map:
                self._assert_success(db_map.add_entity_class_item(name="Object"))
                self._assert_success(db_map.add_entity_item(name="invisible widget", entity_class_name="Object"))
                self._assert_success(db_map.add_entity_item(name="visible widget", entity_class_name="Object"))
                self._assert_success(
                    db_map.add_entity_alternative_item(
                        entity_class_name="Object",
                        entity_byname=("invisible widget",),
                        alternative_name="Base",
                        active=False,
                    )
                )
                self._assert_success(
                    db_map.add_entity_alternative_item(
                        entity_class_name="Object",
                        entity_byname=("visible widget",),
                        alternative_name="Base",
                        active=True,
                    )
                )
                self._assert_success(db_map.add_scenario_item(name="Scenario"))
                self._assert_success(
                    db_map.add_scenario_alternative_item(scenario_name="Scenario", alternative_name="Base", rank=1)
                )
                db_map.commit_session("Add test data")
            filtered_url = append_filter_config(url, scenario_filter_config("Scenario"))
            with DatabaseMapping(filtered_url) as db_map:
                entity_alternatives = db_map.query(db_map.entity_alternative_sq).all()
                self.assertEqual(len(entity_alternatives), 1)

    def test_parameter_values_for_entities_that_have_been_filtered_out(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + str(Path(temp_dir, "db.sqlite"))
            with DatabaseMapping(url, create=True) as db_map:
                self._assert_success(db_map.add_entity_class_item(name="Object"))
                self._assert_success(db_map.add_parameter_definition_item(name="y", entity_class_name="Object"))
                self._assert_success(db_map.add_entity_item(name="invisible widget", entity_class_name="Object"))
                self._assert_success(
                    db_map.add_entity_alternative_item(
                        entity_class_name="Object",
                        entity_byname=("invisible widget",),
                        alternative_name="Base",
                        active=False,
                    )
                )
                value, value_type = to_database(2.3)
                self._assert_success(
                    db_map.add_parameter_value_item(
                        entity_class_name="Object",
                        parameter_definition_name="y",
                        entity_byname=("invisible widget",),
                        alternative_name="Base",
                        value=value,
                        type=value_type,
                    )
                )
                self._assert_success(db_map.add_scenario_item(name="Scenario"))
                self._assert_success(
                    db_map.add_scenario_alternative_item(scenario_name="Scenario", alternative_name="Base", rank=1)
                )
                db_map.commit_session("Add test data")
            filtered_url = append_filter_config(url, scenario_filter_config("Scenario"))
            with DatabaseMapping(filtered_url) as db_map:
                values = db_map.query(db_map.parameter_value_sq).all()
                self.assertEqual(len(values), 0)

    def test_parameter_values_for_entities_that_have_been_filtered_out_by_default(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + str(Path(temp_dir, "db.sqlite"))
            with DatabaseMapping(url, create=True) as db_map:
                self._assert_success(db_map.add_entity_class_item(name="Invisible", active_by_default=False))
                self._assert_success(db_map.add_entity_class_item(name="Visible", active_by_default=True))
                self._assert_success(db_map.add_parameter_definition_item(name="y", entity_class_name="Invisible"))
                self._assert_success(db_map.add_parameter_definition_item(name="z", entity_class_name="Visible"))
                self._assert_success(db_map.add_entity_item(name="invisible widget", entity_class_name="Invisible"))
                self._assert_success(db_map.add_entity_item(name="visible widget", entity_class_name="Visible"))
                value, value_type = to_database(-2.3)
                self._assert_success(
                    db_map.add_parameter_value_item(
                        entity_class_name="Invisible",
                        parameter_definition_name="y",
                        entity_byname=("invisible widget",),
                        alternative_name="Base",
                        value=value,
                        type=value_type,
                    )
                )
                value, value_type = to_database(2.3)
                self._assert_success(
                    db_map.add_parameter_value_item(
                        entity_class_name="Visible",
                        parameter_definition_name="z",
                        entity_byname=("visible widget",),
                        alternative_name="Base",
                        value=value,
                        type=value_type,
                    )
                )
                self._assert_success(db_map.add_scenario_item(name="Scenario"))
                self._assert_success(
                    db_map.add_scenario_alternative_item(scenario_name="Scenario", alternative_name="Base", rank=1)
                )
                db_map.commit_session("Add test data")
            filtered_url = append_filter_config(url, scenario_filter_config("Scenario"))
            with DatabaseMapping(filtered_url) as db_map:
                entities = db_map.query(db_map.entity_sq).all()
                self.assertEqual(len(entities), 1)
                self.assertEqual(entities[0].name, "visible widget")
                values = db_map.query(db_map.parameter_value_sq).all()
                self.assertEqual(len(values), 1)
                self.assertEqual(from_database(values[0].value, values[0].type), 2.3)

    def test_parameter_values_for_entities_that_swim_against_active_by_default(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + str(Path(temp_dir, "db.sqlite"))
            with DatabaseMapping(url, create=True) as db_map:
                self._assert_success(db_map.add_entity_class_item(name="Invisible", active_by_default=False))
                self._assert_success(db_map.add_entity_class_item(name="Visible", active_by_default=True))
                self._assert_success(db_map.add_parameter_definition_item(name="y", entity_class_name="Invisible"))
                self._assert_success(db_map.add_parameter_definition_item(name="z", entity_class_name="Visible"))
                self._assert_success(db_map.add_entity_item(name="invisible widget", entity_class_name="Invisible"))
                self._assert_success(db_map.add_entity_item(name="visible widget", entity_class_name="Visible"))
                self._assert_success(
                    db_map.add_entity_alternative_item(
                        entity_class_name="Invisible",
                        entity_byname=("invisible widget",),
                        alternative_name="Base",
                        active=True,
                    )
                )
                self._assert_success(
                    db_map.add_entity_alternative_item(
                        entity_class_name="Visible",
                        entity_byname=("visible widget",),
                        alternative_name="Base",
                        active=False,
                    )
                )
                value, value_type = to_database(-2.3)
                self._assert_success(
                    db_map.add_parameter_value_item(
                        entity_class_name="Invisible",
                        parameter_definition_name="y",
                        entity_byname=("invisible widget",),
                        alternative_name="Base",
                        value=value,
                        type=value_type,
                    )
                )
                value, value_type = to_database(2.3)
                self._assert_success(
                    db_map.add_parameter_value_item(
                        entity_class_name="Visible",
                        parameter_definition_name="z",
                        entity_byname=("visible widget",),
                        alternative_name="Base",
                        value=value,
                        type=value_type,
                    )
                )
                self._assert_success(db_map.add_scenario_item(name="Scenario"))
                self._assert_success(
                    db_map.add_scenario_alternative_item(scenario_name="Scenario", alternative_name="Base", rank=1)
                )
                db_map.commit_session("Add test data")
            filtered_url = append_filter_config(url, scenario_filter_config("Scenario"))
            with DatabaseMapping(filtered_url) as db_map:
                entities = db_map.query(db_map.entity_sq).all()
                self.assertEqual(len(entities), 1)
                self.assertEqual(entities[0].name, "invisible widget")
                values = db_map.query(db_map.parameter_value_sq).all()
                self.assertEqual(len(values), 1)
                self.assertEqual(from_database(values[0].value, values[0].type), -2.3)

    def test_parameter_values_of_multidimensional_entity_whose_elements_have_entity_alternatives(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._assert_success(db_map.add_scenario_item(name="base"))
            self._assert_success(
                db_map.add_scenario_alternative_item(scenario_name="base", alternative_name="Base", rank=1)
            )
            self._assert_success(db_map.add_entity_class_item(name="Object"))
            self._assert_success(db_map.add_entity_item(name="visible", entity_class_name="Object"))
            self._assert_success(
                db_map.add_entity_alternative_item(
                    entity_class_name="Object", entity_byname=("visible",), alternative_name="Base", active=True
                )
            )
            self._assert_success(db_map.add_entity_item(name="invisible", entity_class_name="Object"))
            self._assert_success(
                db_map.add_entity_alternative_item(
                    entity_class_name="Object", entity_byname=("invisible",), alternative_name="Base", active=False
                )
            )
            self._assert_success(db_map.add_entity_class_item(name="Relationship", dimension_name_list=("Object",)))
            self._assert_success(
                db_map.add_entity_item(element_name_list=("visible",), entity_class_name="Relationship")
            )
            self._assert_success(
                db_map.add_entity_item(element_name_list=("invisible",), entity_class_name="Relationship")
            )
            self._assert_success(db_map.add_parameter_definition_item(name="y", entity_class_name="Relationship"))
            value, value_type = to_database(2.3)
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
            value, value_type = to_database(-2.3)
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
            db_map.commit_session("Add test data")
            config = scenario_filter_config("base")
            scenario_filter_from_dict(db_map, config)
            values = db_map.query(db_map.parameter_value_sq).all()
            self.assertEqual(len(values), 1)
            self.assertEqual(from_database(values[0].value, values[0].type), 2.3)

    def test_entity_alternative_not_in_scenario_is_dropped_when_another_active_one_is_in_scenario(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + str(Path(temp_dir, "db.sqlite"))
            with DatabaseMapping(url, create=True) as db_map:
                self._assert_success(db_map.add_scenario_item(name="Scenario"))
                self._assert_success(db_map.add_alternative_item(name="alt"))
                self._assert_success(
                    db_map.add_scenario_alternative_item(scenario_name="Scenario", alternative_name="alt", rank=1)
                )
                self._assert_success(db_map.add_entity_class_item(name="Object", active_by_default=True))
                self._assert_success(db_map.add_entity_item(name="cube", entity_class_name="Object"))
                self._assert_success(
                    db_map.add_entity_alternative_item(
                        entity_class_name="Object", entity_byname=("cube",), alternative_name="Base", active=False
                    )
                )
                self._assert_success(
                    db_map.add_entity_alternative_item(
                        entity_class_name="Object", entity_byname=("cube",), alternative_name="alt", active=True
                    )
                )
                db_map.commit_session("Add test data")
            with DatabaseMapping(url) as db_map:
                config = scenario_filter_config("Scenario")
                scenario_filter_from_dict(db_map, config)
                entity_alternatives = db_map.get_entity_alternative_items()
                self.assertEqual(len(entity_alternatives), 1)
                self.assertEqual(entity_alternatives[0]["entity_class_name"], "Object")
                self.assertEqual(entity_alternatives[0]["entity_byname"], ("cube",))
                self.assertEqual(entity_alternatives[0]["alternative_name"], "alt")
                self.assertTrue(entity_alternatives[0]["active"])

    def test_entity_with_irrelevant_entity_alternative_is_passed(self):
        with TemporaryDirectory() as temp_dir:
            url = "sqlite:///" + str(Path(temp_dir, "db.sqlite"))
            with DatabaseMapping(url, create=True) as db_map:
                self._assert_success(db_map.add_scenario_item(name="Scenario"))
                self._assert_success(db_map.add_entity_class_item(name="Object", active_by_default=True))
                self._assert_success(db_map.add_entity_item(name="cube", entity_class_name="Object"))
                self._assert_success(
                    db_map.add_entity_alternative_item(
                        entity_class_name="Object", entity_byname=("cube",), alternative_name="Base", active=True
                    )
                )
                db_map.commit_session("Add test data")
            with DatabaseMapping(url) as db_map:
                config = scenario_filter_config("Scenario")
                scenario_filter_from_dict(db_map, config)
                entities = db_map.get_entity_items()
                self.assertEqual(len(entities), 1)
                self.assertEqual(entities[0]["name"], "cube")


class TestScenarioFilterUtils(DataBuilderTestCase):
    def test_scenario_filter_config(self):
        config = scenario_filter_config("scenario name")
        self.assertEqual(config, {"type": "scenario_filter", "scenario": "scenario name"})

    def test_scenario_filter_from_dict(self):
        with DatabaseMapping("sqlite://", create=True) as db_map:
            self._build_data_with_single_scenario(db_map)
            config = scenario_filter_config("scenario")
            scenario_filter_from_dict(db_map, config)
            parameters = db_map.query(db_map.parameter_value_sq).all()
            self.assertEqual(len(parameters), 1)
            self.assertEqual((parameters[0].value, parameters[0].type), to_database(23.0))

    def test_scenario_name_from_dict(self):
        config = scenario_filter_config("scenario name")
        self.assertEqual(scenario_name_from_dict(config), "scenario name")

    def test_scenario_filter_config_to_shorthand(self):
        config = scenario_filter_config("scenario name")
        shorthand = scenario_filter_config_to_shorthand(config)
        self.assertEqual(shorthand, "scenario:scenario name")

    def test_scenario_filter_shorthand_to_config(self):
        config = scenario_filter_shorthand_to_config("scenario:scenario name")
        self.assertEqual(config, {"type": "scenario_filter", "scenario": "scenario name"})


if __name__ == "__main__":
    unittest.main()
