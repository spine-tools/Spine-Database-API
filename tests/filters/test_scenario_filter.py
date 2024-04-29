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
from sqlalchemy.engine.url import URL
from spinedb_api import (
    apply_filter_stack,
    apply_scenario_filter_to_subqueries,
    create_new_spine_database,
    DatabaseMapping,
    import_alternatives,
    import_entity_classes,
    import_entities,
    import_entity_alternatives,
    import_object_classes,
    import_object_parameter_values,
    import_object_parameters,
    import_objects,
    import_relationship_classes,
    import_relationship_parameter_values,
    import_relationship_parameters,
    import_relationships,
    import_scenario_alternatives,
    import_scenarios,
    SpineDBAPIError,
)
from spinedb_api.filters.scenario_filter import (
    scenario_filter_config,
    scenario_filter_config_to_shorthand,
    scenario_filter_from_dict,
    scenario_filter_shorthand_to_config,
    scenario_name_from_dict,
)


class TestScenarioFilterInMemory(unittest.TestCase):
    def _assert_success(self, result):
        item, error = result
        self.assertIsNone(error)
        return item

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
            self.assertEqual(entities[0]["name"], "visible_object")

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
            self.assertEqual(entities[0]["name"], "visible")
            self.assertEqual(entities[1]["name"], "visible")

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


class TestScenarioFilter(unittest.TestCase):
    _db_url = None
    _temp_dir = None

    @classmethod
    def setUpClass(cls):
        cls._temp_dir = TemporaryDirectory()
        cls._db_url = URL("sqlite", database=Path(cls._temp_dir.name, "test_scenario_filter_mapping.sqlite").as_posix())

    def setUp(self):
        create_new_spine_database(self._db_url)
        self._out_db_map = DatabaseMapping(self._db_url)
        self._db_map = DatabaseMapping(self._db_url)

    def tearDown(self):
        self._out_db_map.close()
        self._db_map.close()

    def _build_data_with_single_scenario(self):
        import_alternatives(self._out_db_map, ["alternative"])
        import_object_classes(self._out_db_map, ["object_class"])
        import_objects(self._out_db_map, [("object_class", "object")])
        import_object_parameters(self._out_db_map, [("object_class", "parameter")])
        import_object_parameter_values(self._out_db_map, [("object_class", "object", "parameter", -1.0)])
        import_object_parameter_values(self._out_db_map, [("object_class", "object", "parameter", 23.0, "alternative")])
        import_scenarios(self._out_db_map, [("scenario", True)])
        import_scenario_alternatives(self._out_db_map, [("scenario", "alternative")])

    def test_scenario_filter(self):
        _build_data_with_single_scenario(self._out_db_map)
        apply_scenario_filter_to_subqueries(self._db_map, "scenario")
        parameters = self._db_map.query(self._db_map.parameter_value_sq).all()
        self.assertEqual(len(parameters), 1)
        self.assertEqual(parameters[0].value, b"23.0")
        alternatives = [dict(a) for a in self._db_map.query(self._db_map.alternative_sq)]
        self.assertEqual(alternatives, [{"name": "alternative", "description": None, "id": 2, "commit_id": 2}])
        scenarios = [dict(s) for s in self._db_map.query(self._db_map.wide_scenario_sq).all()]
        self.assertEqual(
            scenarios,
            [
                {
                    "name": "scenario",
                    "description": None,
                    "active": True,
                    "alternative_name_list": "alternative",
                    "alternative_id_list": "2",
                    "id": 1,
                    "commit_id": 2,
                }
            ],
        )

    def test_scenario_filter_uncommitted_data(self):
        _build_data_with_single_scenario(self._out_db_map, commit=False)
        with self.assertRaises(SpineDBAPIError):
            apply_scenario_filter_to_subqueries(self._out_db_map, "scenario")
        parameters = self._out_db_map.query(self._out_db_map.parameter_value_sq).all()
        self.assertEqual(len(parameters), 0)
        alternatives = [dict(a) for a in self._out_db_map.query(self._out_db_map.alternative_sq)]
        self.assertEqual(alternatives, [{"name": "Base", "description": "Base alternative", "id": 1, "commit_id": 1}])
        scenarios = self._out_db_map.query(self._out_db_map.wide_scenario_sq).all()
        self.assertEqual(len(scenarios), 0)
        self._out_db_map.rollback_session()

    def test_scenario_filter_works_for_entity_sq(self):
        import_alternatives(self._out_db_map, ["alternative1", "alternative2"])
        import_entity_classes(
            self._out_db_map, [("class1", ()), ("class2", ()), ("class1__class2", ("class1", "class2"))]
        )
        import_entities(
            self._out_db_map,
            [
                ("class1", "obj1"),
                ("class2", "obj2"),
                ("class2", "obj22"),
                ("class1__class2", ("obj1", "obj2")),
                ("class1__class2", ("obj1", "obj22")),
            ],
        )
        import_entity_alternatives(
            self._out_db_map,
            [
                ("class2", "obj2", "alternative1", True),
                ("class2", "obj2", "alternative2", False),
                ("class2", "obj22", "alternative1", False),
                ("class2", "obj22", "alternative2", True),
            ],
        )
        import_scenarios(self._out_db_map, [("scenario1", True)])
        import_scenario_alternatives(
            self._out_db_map, [("scenario1", "alternative2"), ("scenario1", "alternative1", "alternative2")]
        )
        for entity_class in self._out_db_map.get_entity_class_items():
            entity_class.update(active_by_default=True)
        self._out_db_map.commit_session("Add test data")
        entities = self._db_map.query(self._db_map.entity_sq).all()
        self.assertEqual(len(entities), 5)
        apply_scenario_filter_to_subqueries(self._db_map, "scenario1")
        # After this, obj2 should be excluded because it is inactive in the highest-ranked alternative2
        # The multidimensional entity 'class1__class2, (obj1, obj2)' should also be excluded because involves obj2
        entities = self._db_map.query(self._db_map.wide_entity_sq).all()
        self.assertEqual(len(entities), 3)
        entity_names = {
            name
            for x in entities
            for name in (x["element_name_list"].split(",") if x["element_name_list"] else (x["name"],))
        }
        self.assertFalse("obj2" in entity_names)

    def test_scenario_filter_works_for_object_parameter_value_sq(self):
        _build_data_with_single_scenario(self._out_db_map)
        apply_scenario_filter_to_subqueries(self._db_map, "scenario")
        parameters = self._db_map.query(self._db_map.object_parameter_value_sq).all()
        self.assertEqual(len(parameters), 1)
        self.assertEqual(parameters[0].value, b"23.0")
        alternatives = [dict(a) for a in self._db_map.query(self._db_map.alternative_sq)]
        self.assertEqual(alternatives, [{"name": "alternative", "description": None, "id": 2, "commit_id": 2}])
        scenarios = [dict(s) for s in self._db_map.query(self._db_map.wide_scenario_sq).all()]
        self.assertEqual(
            scenarios,
            [
                {
                    "name": "scenario",
                    "description": None,
                    "active": True,
                    "alternative_name_list": "alternative",
                    "alternative_id_list": "2",
                    "id": 1,
                    "commit_id": 2,
                }
            ],
        )

    def test_scenario_filter_works_for_relationship_parameter_value_sq(self):
        _build_data_with_single_scenario(self._out_db_map, commit=False)
        import_relationship_classes(self._out_db_map, [("relationship_class", ["object_class"])])
        import_relationship_parameters(self._out_db_map, [("relationship_class", "relationship_parameter")])
        import_relationships(self._out_db_map, [("relationship_class", ["object"])])
        import_relationship_parameter_values(
            self._out_db_map, [("relationship_class", ["object"], "relationship_parameter", -1)]
        )
        import_relationship_parameter_values(
            self._out_db_map, [("relationship_class", ["object"], "relationship_parameter", 23.0, "alternative")]
        )
        self._out_db_map.commit_session("Add test data")
        apply_scenario_filter_to_subqueries(self._db_map, "scenario")
        parameters = self._db_map.query(self._db_map.relationship_parameter_value_sq).all()
        self.assertEqual(len(parameters), 1)
        self.assertEqual(parameters[0].value, b"23.0")
        alternatives = [dict(a) for a in self._db_map.query(self._db_map.alternative_sq)]
        self.assertEqual(alternatives, [{"name": "alternative", "description": None, "id": 2, "commit_id": 2}])
        scenarios = [dict(s) for s in self._db_map.query(self._db_map.wide_scenario_sq).all()]
        self.assertEqual(
            scenarios,
            [
                {
                    "name": "scenario",
                    "description": None,
                    "active": True,
                    "alternative_name_list": "alternative",
                    "alternative_id_list": "2",
                    "id": 1,
                    "commit_id": 2,
                }
            ],
        )

    def test_scenario_filter_selects_highest_ranked_alternative(self):
        import_alternatives(self._out_db_map, ["alternative3"])
        import_alternatives(self._out_db_map, ["alternative1"])
        import_alternatives(self._out_db_map, ["alternative2"])
        import_object_classes(self._out_db_map, ["object_class"])
        import_objects(self._out_db_map, [("object_class", "object")])
        import_object_parameters(self._out_db_map, [("object_class", "parameter")])
        import_object_parameter_values(self._out_db_map, [("object_class", "object", "parameter", -1.0)])
        import_object_parameter_values(
            self._out_db_map, [("object_class", "object", "parameter", 10.0, "alternative1")]
        )
        import_object_parameter_values(
            self._out_db_map, [("object_class", "object", "parameter", 2000.0, "alternative2")]
        )
        import_object_parameter_values(
            self._out_db_map, [("object_class", "object", "parameter", 300.0, "alternative3")]
        )
        import_scenarios(self._out_db_map, [("scenario", True)])
        import_scenario_alternatives(
            self._out_db_map,
            [
                ("scenario", "alternative2"),
                ("scenario", "alternative3", "alternative2"),
                ("scenario", "alternative1", "alternative3"),
            ],
        )
        self._out_db_map.commit_session("Add test data")
        apply_scenario_filter_to_subqueries(self._db_map, "scenario")
        parameters = self._db_map.query(self._db_map.parameter_value_sq).all()
        self.assertEqual(len(parameters), 1)
        self.assertEqual(parameters[0].value, b"2000.0")
        alternatives = [dict(a) for a in self._db_map.query(self._db_map.alternative_sq)]
        self.assertEqual(
            alternatives,
            [
                {"name": "alternative3", "description": None, "id": 2, "commit_id": 2},
                {"name": "alternative1", "description": None, "id": 3, "commit_id": 2},
                {"name": "alternative2", "description": None, "id": 4, "commit_id": 2},
            ],
        )
        scenarios = [dict(s) for s in self._db_map.query(self._db_map.wide_scenario_sq).all()]
        self.assertEqual(
            scenarios,
            [
                {
                    "name": "scenario",
                    "description": None,
                    "active": True,
                    "alternative_name_list": "alternative1,alternative3,alternative2",
                    "alternative_id_list": "3,2,4",
                    "id": 1,
                    "commit_id": 2,
                }
            ],
        )

    def test_scenario_filter_selects_highest_ranked_alternative_of_active_scenario(self):
        import_alternatives(
            self._out_db_map, ["alternative3", "alternative1", "alternative2", "non_active_alternative"]
        )
        import_object_classes(self._out_db_map, ["object_class"])
        import_objects(self._out_db_map, [("object_class", "object")])
        import_object_parameters(self._out_db_map, [("object_class", "parameter")])
        import_object_parameter_values(
            self._out_db_map,
            [
                ("object_class", "object", "parameter", -1.0),
                ("object_class", "object", "parameter", 10.0, "alternative1"),
                ("object_class", "object", "parameter", 2000.0, "alternative2"),
                ("object_class", "object", "parameter", 300.0, "alternative3"),
            ],
        )
        import_scenarios(self._out_db_map, [("scenario", True), "non_active_scenario"])
        import_scenario_alternatives(
            self._out_db_map,
            [
                ("scenario", "alternative2"),
                ("scenario", "alternative3", "alternative2"),
                ("scenario", "alternative1", "alternative3"),
            ],
        )
        import_scenario_alternatives(
            self._out_db_map,
            [
                ("non_active_scenario", "non_active_alternative"),
                ("scenario", "alternative2", "non_active_alternative"),
                ("scenario", "alternative3", "alternative2"),
                ("scenario", "alternative1", "alternative3"),
            ],
        )
        self._out_db_map.commit_session("Add test data")
        apply_scenario_filter_to_subqueries(self._db_map, "scenario")
        parameters = self._db_map.query(self._db_map.parameter_value_sq).all()
        self.assertEqual(len(parameters), 1)
        self.assertEqual(parameters[0].value, b"2000.0")
        alternatives = [dict(a) for a in self._db_map.query(self._db_map.alternative_sq)]
        self.assertEqual(
            alternatives,
            [
                {"name": "alternative3", "description": None, "id": 2, "commit_id": 2},
                {"name": "alternative1", "description": None, "id": 3, "commit_id": 2},
                {"name": "alternative2", "description": None, "id": 4, "commit_id": 2},
            ],
        )
        scenarios = [dict(s) for s in self._db_map.query(self._db_map.wide_scenario_sq).all()]
        self.assertEqual(
            scenarios,
            [
                {
                    "name": "scenario",
                    "description": None,
                    "active": True,
                    "alternative_name_list": "alternative1,alternative3,alternative2",
                    "alternative_id_list": "3,2,4",
                    "id": 1,
                    "commit_id": 2,
                }
            ],
        )

    def test_scenario_filter_for_multiple_objects_and_parameters(self):
        import_alternatives(self._out_db_map, ["alternative"])
        import_object_classes(self._out_db_map, ["object_class"])
        import_objects(self._out_db_map, [("object_class", "object1")])
        import_objects(self._out_db_map, [("object_class", "object2")])
        import_object_parameters(self._out_db_map, [("object_class", "parameter1")])
        import_object_parameters(self._out_db_map, [("object_class", "parameter2")])
        import_object_parameter_values(self._out_db_map, [("object_class", "object1", "parameter1", -1.0)])
        import_object_parameter_values(
            self._out_db_map, [("object_class", "object1", "parameter1", 10.0, "alternative")]
        )
        import_object_parameter_values(self._out_db_map, [("object_class", "object1", "parameter2", -1.0)])
        import_object_parameter_values(
            self._out_db_map, [("object_class", "object1", "parameter2", 11.0, "alternative")]
        )
        import_object_parameter_values(self._out_db_map, [("object_class", "object2", "parameter1", -2.0)])
        import_object_parameter_values(
            self._out_db_map, [("object_class", "object2", "parameter1", 20.0, "alternative")]
        )
        import_object_parameter_values(self._out_db_map, [("object_class", "object2", "parameter2", -2.0)])
        import_object_parameter_values(
            self._out_db_map, [("object_class", "object2", "parameter2", 22.0, "alternative")]
        )
        import_scenarios(self._out_db_map, [("scenario", True)])
        import_scenario_alternatives(self._out_db_map, [("scenario", "alternative")])
        for item in self._out_db_map.get_entity_class_items():
            item.update(active_by_default=True)
        self._out_db_map.commit_session("Add test data")
        apply_scenario_filter_to_subqueries(self._db_map, "scenario")
        parameters = self._db_map.query(self._db_map.parameter_value_sq).all()
        self.assertEqual(len(parameters), 4)
        object_names = {o.id: o.name for o in self._db_map.query(self._db_map.object_sq).all()}
        alternative_names = {a.id: a.name for a in self._db_map.query(self._db_map.alternative_sq).all()}
        parameter_names = {d.id: d.name for d in self._db_map.query(self._db_map.parameter_definition_sq).all()}
        datamined_values = dict()
        for parameter in parameters:
            self.assertEqual(alternative_names[parameter.alternative_id], "alternative")
            parameter_values = datamined_values.setdefault(object_names[parameter.entity_id], dict())
            parameter_values[parameter_names[parameter.parameter_definition_id]] = parameter.value
        self.assertEqual(
            datamined_values,
            {
                "object1": {"parameter1": b"10.0", "parameter2": b"11.0"},
                "object2": {"parameter1": b"20.0", "parameter2": b"22.0"},
            },
        )
        alternatives = [dict(a) for a in self._db_map.query(self._db_map.alternative_sq)]
        self.assertEqual(alternatives, [{"name": "alternative", "description": None, "id": 2, "commit_id": 2}])
        scenarios = [dict(s) for s in self._db_map.query(self._db_map.wide_scenario_sq).all()]
        self.assertEqual(
            scenarios,
            [
                {
                    "name": "scenario",
                    "description": None,
                    "active": True,
                    "alternative_name_list": "alternative",
                    "alternative_id_list": "2",
                    "id": 1,
                    "commit_id": 2,
                }
            ],
        )

    def test_filters_scenarios_and_alternatives(self):
        import_scenarios(self._out_db_map, ("scenario1", "scenario2"))
        import_alternatives(self._out_db_map, ("alternative1", "alternative2", "alternative3"))
        import_scenario_alternatives(
            self._out_db_map,
            (
                ("scenario1", "alternative2"),
                ("scenario1", "alternative1", "alternative2"),
                ("scenario2", "alternative3"),
                ("scenario2", "alternative2", "alternative3"),
            ),
        )
        self._out_db_map.commit_session("Add test data.")
        apply_scenario_filter_to_subqueries(self._db_map, "scenario2")
        alternatives = [dict(a) for a in self._db_map.query(self._db_map.alternative_sq)]
        self.assertEqual(
            alternatives,
            [
                {"name": "alternative2", "description": None, "id": 3, "commit_id": 2},
                {"name": "alternative3", "description": None, "id": 4, "commit_id": 2},
            ],
        )
        scenarios = [dict(s) for s in self._db_map.query(self._db_map.wide_scenario_sq).all()]
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


class TestScenarioFilterUtils(unittest.TestCase):
    def test_scenario_filter_config(self):
        config = scenario_filter_config("scenario name")
        self.assertEqual(config, {"type": "scenario_filter", "scenario": "scenario name"})

    def test_scenario_filter_from_dict(self):
        db_map = DatabaseMapping("sqlite://", create=True)
        _build_data_with_single_scenario(db_map)
        config = scenario_filter_config("scenario")
        scenario_filter_from_dict(db_map, config)
        parameters = db_map.query(db_map.parameter_value_sq).all()
        self.assertEqual(len(parameters), 1)
        self.assertEqual(parameters[0].value, b"23.0")

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


def _build_data_with_single_scenario(db_map, commit=True):
    import_alternatives(db_map, ["alternative"])
    import_object_classes(db_map, ["object_class"])
    import_objects(db_map, [("object_class", "object")])
    import_object_parameters(db_map, [("object_class", "parameter")])
    import_object_parameter_values(db_map, [("object_class", "object", "parameter", -1.0)])
    import_object_parameter_values(db_map, [("object_class", "object", "parameter", 23.0, "alternative")])
    import_scenarios(db_map, [("scenario", True)])
    import_scenario_alternatives(db_map, [("scenario", "alternative")])
    for entity_class in db_map.get_entity_class_items():
        entity_class.update(active_by_default=True)
    if commit:
        db_map.commit_session("Add test data.")


if __name__ == "__main__":
    unittest.main()
