######################################################################################################################
# Copyright (C) 2017-2022 Spine project consortium
# This file is part of Spine Database API.
# Spine Database API is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser
# General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

"""
Unit tests for DatabaseMapping class.

"""
import unittest
from unittest.mock import patch
from sqlalchemy.engine.url import URL
from spinedb_api import (
    DatabaseMapping,
    to_database,
    import_functions,
    from_database,
    SpineDBAPIError,
    SpineIntegrityError,
)

IN_MEMORY_DB_URL = "sqlite://"


class TestDatabaseMappingBase(unittest.TestCase):
    _db_map = None

    @classmethod
    def setUpClass(cls):
        cls._db_map = DatabaseMapping(IN_MEMORY_DB_URL, create=True)

    @classmethod
    def tearDownClass(cls):
        cls._db_map.connection.close()

    def test_construction_with_filters(self):
        db_url = IN_MEMORY_DB_URL + "?spinedbfilter=fltr1&spinedbfilter=fltr2"
        with patch("spinedb_api.db_mapping.apply_filter_stack") as mock_apply:
            with patch(
                "spinedb_api.db_mapping.load_filters", return_value=[{"fltr1": "config1", "fltr2": "config2"}]
            ) as mock_load:
                db_map = DatabaseMapping(db_url, create=True)
                db_map.connection.close()
                mock_load.assert_called_once_with(["fltr1", "fltr2"])
                mock_apply.assert_called_once_with(db_map, [{"fltr1": "config1", "fltr2": "config2"}])

    def test_construction_with_sqlalchemy_url_and_filters(self):
        sa_url = URL("sqlite")
        sa_url.query = {"spinedbfilter": ["fltr1", "fltr2"]}
        with patch("spinedb_api.db_mapping.apply_filter_stack") as mock_apply:
            with patch(
                "spinedb_api.db_mapping.load_filters", return_value=[{"fltr1": "config1", "fltr2": "config2"}]
            ) as mock_load:
                db_map = DatabaseMapping(sa_url, create=True)
                db_map.connection.close()
                mock_load.assert_called_once_with(["fltr1", "fltr2"])
                mock_apply.assert_called_once_with(db_map, [{"fltr1": "config1", "fltr2": "config2"}])

    def test_entity_class_type_sq(self):
        columns = ["id", "name", "commit_id"]
        self.assertEqual(len(self._db_map.entity_class_type_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.entity_class_type_sq.c, column_name))

    def test_entity_type_sq(self):
        columns = ["id", "name", "commit_id"]
        self.assertEqual(len(self._db_map.entity_type_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.entity_type_sq.c, column_name))

    def test_entity_sq(self):
        columns = ["id", "type_id", "class_id", "name", "description", "commit_id"]
        self.assertEqual(len(self._db_map.entity_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.entity_sq.c, column_name))

    def test_object_class_sq(self):
        columns = ["id", "name", "description", "display_order", "display_icon", "hidden", "commit_id"]
        self.assertEqual(len(self._db_map.object_class_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.object_class_sq.c, column_name))

    def test_object_sq(self):
        columns = ["id", "class_id", "name", "description", "commit_id"]
        self.assertEqual(len(self._db_map.object_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.object_sq.c, column_name))

    def test_relationship_class_sq(self):
        columns = ["id", "dimension", "object_class_id", "name", "description", "display_icon", "hidden", "commit_id"]
        self.assertEqual(len(self._db_map.relationship_class_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.relationship_class_sq.c, column_name))

    def test_relationship_sq(self):
        columns = ["id", "dimension", "object_id", "class_id", "name", "commit_id"]
        self.assertEqual(len(self._db_map.relationship_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.relationship_sq.c, column_name))

    def test_entity_group_sq(self):
        columns = ["id", "entity_id", "entity_class_id", "member_id"]
        self.assertEqual(len(self._db_map.entity_group_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.entity_group_sq.c, column_name))

    def test_parameter_definition_sq(self):
        columns = [
            "id",
            "name",
            "description",
            "entity_class_id",
            "object_class_id",
            "relationship_class_id",
            "default_value",
            "default_type",
            "list_value_id",
            "commit_id",
            "parameter_value_list_id",
        ]
        self.assertEqual(len(self._db_map.parameter_definition_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.parameter_definition_sq.c, column_name))

    def test_parameter_value_sq(self):
        columns = [
            "id",
            "parameter_definition_id",
            "entity_class_id",
            "entity_id",
            "object_class_id",
            "relationship_class_id",
            "object_id",
            "relationship_id",
            "value",
            "type",
            "list_value_id",
            "commit_id",
            "alternative_id",
        ]
        self.assertEqual(len(self._db_map.parameter_value_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.parameter_value_sq.c, column_name))

    def test_parameter_value_list_sq(self):
        columns = ["id", "name", "commit_id"]
        self.assertEqual(len(self._db_map.parameter_value_list_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.parameter_value_list_sq.c, column_name))

    def test_ext_object_sq(self):
        columns = ["id", "class_id", "class_name", "name", "description", "group_id", "commit_id"]
        self.assertEqual(len(self._db_map.ext_object_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.ext_object_sq.c, column_name))

    def test_ext_relationship_class_sq(self):
        columns = [
            "id",
            "name",
            "description",
            "display_icon",
            "dimension",
            "object_class_id",
            "object_class_name",
            "commit_id",
        ]
        self.assertEqual(len(self._db_map.ext_relationship_class_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.ext_relationship_class_sq.c, column_name))

    def test_wide_relationship_class_sq(self):
        columns = [
            "id",
            "name",
            "description",
            "display_icon",
            "commit_id",
            "object_class_id_list",
            "object_class_name_list",
        ]
        self.assertEqual(len(self._db_map.wide_relationship_class_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.wide_relationship_class_sq.c, column_name))

    def test_ext_relationship_sq(self):
        columns = [
            "id",
            "name",
            "class_id",
            "class_name",
            "dimension",
            "object_id",
            "object_name",
            "object_class_id",
            "object_class_name",
            "commit_id",
        ]
        self.assertEqual(len(self._db_map.ext_relationship_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.ext_relationship_sq.c, column_name))

    def test_wide_relationship_sq(self):
        columns = [
            "id",
            "name",
            "class_id",
            "class_name",
            "commit_id",
            "object_id_list",
            "object_name_list",
            "object_class_id_list",
            "object_class_name_list",
        ]
        self.assertEqual(len(self._db_map.wide_relationship_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.wide_relationship_sq.c, column_name))

    def test_object_parameter_definition_sq(self):
        columns = [
            "id",
            "entity_class_id",
            "entity_class_name",
            "object_class_id",
            "object_class_name",
            "parameter_name",
            "value_list_id",
            "value_list_name",
            "default_value",
            "default_type",
            "description",
        ]
        self.assertEqual(len(self._db_map.object_parameter_definition_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.object_parameter_definition_sq.c, column_name))

    def test_relationship_parameter_definition_sq(self):
        columns = [
            "id",
            "entity_class_id",
            "entity_class_name",
            "relationship_class_id",
            "relationship_class_name",
            "object_class_id_list",
            "object_class_name_list",
            "parameter_name",
            "value_list_id",
            "value_list_name",
            "default_value",
            "default_type",
            "description",
        ]
        self.assertEqual(len(self._db_map.relationship_parameter_definition_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.relationship_parameter_definition_sq.c, column_name))

    def test_object_parameter_value_sq(self):
        columns = [
            "id",
            "entity_class_id",
            "object_class_id",
            "object_class_name",
            "entity_id",
            "object_id",
            "object_name",
            "parameter_id",
            "parameter_name",
            "alternative_id",
            "alternative_name",
            "value",
            "type",
        ]
        self.assertEqual(len(self._db_map.object_parameter_value_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.object_parameter_value_sq.c, column_name))

    def test_relationship_parameter_value_sq(self):
        columns = [
            "id",
            "entity_class_id",
            "relationship_class_id",
            "relationship_class_name",
            "object_class_id_list",
            "object_class_name_list",
            "entity_id",
            "relationship_id",
            "object_id_list",
            "object_name_list",
            "parameter_id",
            "parameter_name",
            "alternative_id",
            "alternative_name",
            "value",
            "type",
        ]
        self.assertEqual(len(self._db_map.relationship_parameter_value_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.relationship_parameter_value_sq.c, column_name))

    def test_wide_parameter_value_list_sq(self):
        columns = ["id", "name", "value_index_list", "value_id_list", "commit_id"]
        self.assertEqual(len(self._db_map.wide_parameter_value_list_sq.c), len(columns))
        for column_name in columns:
            self.assertTrue(hasattr(self._db_map.wide_parameter_value_list_sq.c, column_name))


class TestDatabaseMappingBaseQueries(unittest.TestCase):
    def setUp(self):
        self._db_map = DatabaseMapping(IN_MEMORY_DB_URL, create=True)

    def tearDown(self):
        self._db_map.connection.close()

    def create_object_classes(self):
        obj_classes = ['class1', 'class2']
        import_functions.import_object_classes(self._db_map, obj_classes)
        return obj_classes

    def create_objects(self):
        objects = [('class1', 'obj11'), ('class1', 'obj12'), ('class2', 'obj21')]
        import_functions.import_objects(self._db_map, objects)
        return objects

    def create_relationship_classes(self):
        relationship_classes = [('rel1', ['class1']), ('rel2', ['class1', 'class2'])]
        import_functions.import_relationship_classes(self._db_map, relationship_classes)
        return relationship_classes

    def create_relationships(self):
        relationships = [('rel1', ['obj11']), ('rel2', ['obj11', 'obj21'])]
        import_functions.import_relationships(self._db_map, relationships)
        return relationships

    def test_commit_sq_hides_pending_commit(self):
        commits = self._db_map.query(self._db_map.commit_sq).all()
        self.assertEqual(len(commits), 1)

    def test_alternative_sq(self):
        import_functions.import_alternatives(self._db_map, (("alt1", "test alternative"),))
        alternative_rows = self._db_map.query(self._db_map.alternative_sq).all()
        expected_names_and_descriptions = {"Base": "Base alternative", "alt1": "test alternative"}
        self.assertEqual(len(alternative_rows), len(expected_names_and_descriptions))
        for row in alternative_rows:
            self.assertTrue(row.name in expected_names_and_descriptions)
            self.assertEqual(row.description, expected_names_and_descriptions[row.name])
            expected_names_and_descriptions.pop(row.name)
        self.assertEqual(expected_names_and_descriptions, {})

    def test_scenario_sq(self):
        import_functions.import_scenarios(self._db_map, (("scen1", True, "test scenario"),))
        scenario_rows = self._db_map.query(self._db_map.scenario_sq).all()
        self.assertEqual(len(scenario_rows), 1)
        self.assertEqual(scenario_rows[0].name, "scen1")
        self.assertEqual(scenario_rows[0].description, "test scenario")
        self.assertTrue(scenario_rows[0].active)

    def test_ext_linked_scenario_alternative_sq(self):
        import_functions.import_scenarios(self._db_map, (("scen1", True),))
        import_functions.import_alternatives(self._db_map, ("alt1", "alt2", "alt3"))
        import_functions.import_scenario_alternatives(self._db_map, (("scen1", "alt2"),))
        import_functions.import_scenario_alternatives(self._db_map, (("scen1", "alt3"),))
        import_functions.import_scenario_alternatives(self._db_map, (("scen1", "alt1"),))
        scenario_alternative_rows = self._db_map.query(self._db_map.ext_linked_scenario_alternative_sq).all()
        self.assertEqual(len(scenario_alternative_rows), 3)
        expected_befores = {"alt2": "alt3", "alt3": "alt1", "alt1": None}
        expected_ranks = {"alt2": 1, "alt3": 2, "alt1": 3}
        for row in scenario_alternative_rows:
            self.assertEqual(row.scenario_name, "scen1")
            self.assertIn(row.alternative_name, expected_befores)
            self.assertEqual(row.rank, expected_ranks[row.alternative_name])
            expected_before_alternative = expected_befores.pop(row.alternative_name)
            self.assertEqual(row.before_alternative_name, expected_before_alternative)
            if expected_before_alternative is not None:
                self.assertIsNotNone(row.before_alternative_id)
                self.assertEqual(row.before_rank, expected_ranks[row.before_alternative_name])
            else:
                self.assertIsNone(row.before_alternative_id)
                self.assertIsNone(row.before_rank)
        self.assertEqual(expected_befores, {})

    def test_entity_class_sq(self):
        obj_classes = self.create_object_classes()
        relationship_classes = self.create_relationship_classes()
        results = self._db_map.query(self._db_map.entity_class_sq).all()
        # Check that number of results matches total entities
        self.assertEqual(len(results), len(obj_classes) + len(relationship_classes))
        # Check result values
        for row, class_name in zip(results, obj_classes + [rel[0] for rel in relationship_classes]):
            self.assertEqual(row.name, class_name)

    def test_entity_sq(self):
        self.create_object_classes()
        objects = self.create_objects()
        self.create_relationship_classes()
        relationships = self.create_relationships()
        entity_rows = self._db_map.query(self._db_map.entity_sq).all()
        self.assertEqual(len(entity_rows), len(objects) + len(relationships))
        object_names = [o[1] for o in objects]
        relationship_names = [r[0] + "_" + "__".join(r[1]) for r in relationships]
        for row, expected_name in zip(entity_rows, object_names + relationship_names):
            self.assertEqual(row.name, expected_name)

    def test_object_class_sq_picks_object_classes_only(self):
        obj_classes = self.create_object_classes()
        self.create_relationship_classes()
        class_rows = self._db_map.query(self._db_map.object_class_sq).all()
        self.assertEqual(len(class_rows), len(obj_classes))
        for row, expected_name in zip(class_rows, obj_classes):
            self.assertEqual(row.name, expected_name)

    def test_object_sq_picks_objects_only(self):
        self.create_object_classes()
        objects = self.create_objects()
        self.create_relationship_classes()
        self.create_relationships()
        object_rows = self._db_map.query(self._db_map.object_sq).all()
        self.assertEqual(len(object_rows), len(objects))
        for row, expected_object in zip(object_rows, objects):
            self.assertEqual(row.name, expected_object[1])

    def test_wide_relationship_class_sq(self):
        self.create_object_classes()
        relationship_classes = self.create_relationship_classes()
        class_rows = self._db_map.query(self._db_map.wide_relationship_class_sq).all()
        self.assertEqual(len(class_rows), 2)
        for row, relationship_class in zip(class_rows, relationship_classes):
            self.assertEqual(row.name, relationship_class[0])
            self.assertEqual(row.object_class_name_list, ",".join(relationship_class[1]))

    def test_wide_relationship_sq(self):
        self.create_object_classes()
        self.create_objects()
        relationship_classes = self.create_relationship_classes()
        object_classes = {rel_class[0]: rel_class[1] for rel_class in relationship_classes}
        relationships = self.create_relationships()
        relationship_rows = self._db_map.query(self._db_map.wide_relationship_sq).all()
        self.assertEqual(len(relationship_rows), 2)
        for row, relationship in zip(relationship_rows, relationships):
            self.assertEqual(row.name, relationship[0] + "_" + "__".join(relationship[1]))
            self.assertEqual(row.class_name, relationship[0])
            self.assertEqual(row.object_class_name_list, ",".join(object_classes[relationship[0]]))
            self.assertEqual(row.object_name_list, ",".join(relationship[1]))

    def test_parameter_definition_sq_for_object_class(self):
        self.create_object_classes()
        import_functions.import_object_parameters(self._db_map, (("class1", "par1"),))
        definition_rows = self._db_map.query(self._db_map.parameter_definition_sq).all()
        self.assertEqual(len(definition_rows), 1)
        self.assertEqual(definition_rows[0].name, "par1")
        self.assertIsNotNone(definition_rows[0].object_class_id)
        self.assertIsNone(definition_rows[0].relationship_class_id)

    def test_parameter_definition_sq_for_relationship_class(self):
        self.create_object_classes()
        self.create_relationship_classes()
        import_functions.import_relationship_parameters(self._db_map, (("rel1", "par1"),))
        definition_rows = self._db_map.query(self._db_map.parameter_definition_sq).all()
        self.assertEqual(len(definition_rows), 1)
        self.assertEqual(definition_rows[0].name, "par1")
        self.assertIsNone(definition_rows[0].object_class_id)
        self.assertIsNotNone(definition_rows[0].relationship_class_id)

    def test_entity_parameter_definition_sq_for_object_class(self):
        self.create_object_classes()
        self.create_relationship_classes()
        import_functions.import_object_parameters(self._db_map, (("class1", "par1"),))
        definition_rows = self._db_map.query(self._db_map.entity_parameter_definition_sq).all()
        self.assertEqual(len(definition_rows), 1)
        self.assertEqual(definition_rows[0].parameter_name, "par1")
        self.assertEqual(definition_rows[0].entity_class_name, "class1")
        self.assertEqual(definition_rows[0].object_class_name, "class1")
        self.assertIsNone(definition_rows[0].relationship_class_id)
        self.assertIsNone(definition_rows[0].relationship_class_name)
        self.assertIsNone(definition_rows[0].object_class_id_list)
        self.assertIsNone(definition_rows[0].object_class_name_list)

    def test_entity_parameter_definition_sq_for_relationship_class(self):
        object_classes = self.create_object_classes()
        self.create_relationship_classes()
        import_functions.import_relationship_parameters(self._db_map, (("rel2", "par1"),))
        definition_rows = self._db_map.query(self._db_map.entity_parameter_definition_sq).all()
        self.assertEqual(len(definition_rows), 1)
        self.assertEqual(definition_rows[0].parameter_name, "par1")
        self.assertEqual(definition_rows[0].entity_class_name, "rel2")
        self.assertIsNotNone(definition_rows[0].relationship_class_id)
        self.assertEqual(definition_rows[0].relationship_class_name, "rel2")
        self.assertIsNotNone(definition_rows[0].object_class_id_list)
        self.assertEqual(definition_rows[0].object_class_name_list, ",".join(object_classes))
        self.assertIsNone(definition_rows[0].object_class_name)

    def test_entity_parameter_definition_sq_with_multiple_relationship_classes_but_single_parameter(self):
        self.create_object_classes()
        self.create_relationship_classes()
        obj_parameter_definitions = [('class1', 'par1a'), ('class1', 'par1b')]
        rel_parameter_definitions = [('rel1', 'rpar1a')]
        import_functions.import_object_parameters(self._db_map, obj_parameter_definitions)
        import_functions.import_relationship_parameters(self._db_map, rel_parameter_definitions)
        results = self._db_map.query(self._db_map.entity_parameter_definition_sq).all()
        # Check that number of results matches total entities
        self.assertEqual(len(results), len(obj_parameter_definitions) + len(rel_parameter_definitions))
        # Check result values
        for row, par_def in zip(results, obj_parameter_definitions + rel_parameter_definitions):
            self.assertTupleEqual((row.entity_class_name, row.parameter_name), par_def)

    def test_entity_parameter_values(self):
        self.create_object_classes()
        self.create_objects()
        self.create_relationship_classes()
        self.create_relationships()
        obj_parameter_definitions = [('class1', 'par1a'), ('class1', 'par1b'), ('class2', 'par2a')]
        rel_parameter_definitions = [('rel1', 'rpar1a'), ('rel2', 'rpar2a')]
        import_functions.import_object_parameters(self._db_map, obj_parameter_definitions)
        import_functions.import_relationship_parameters(self._db_map, rel_parameter_definitions)
        object_parameter_values = [
            ('class1', 'obj11', 'par1a', 123),
            ('class1', 'obj11', 'par1b', 333),
            ('class2', 'obj21', 'par2a', 'empty'),
        ]
        _, errors = import_functions.import_object_parameter_values(self._db_map, object_parameter_values)
        self.assertFalse(errors)
        relationship_parameter_values = [('rel1', ['obj11'], 'rpar1a', 1.1), ('rel2', ['obj11', 'obj21'], 'rpar2a', 42)]
        _, errors = import_functions.import_relationship_parameter_values(self._db_map, relationship_parameter_values)
        self.assertFalse(errors)
        results = self._db_map.query(self._db_map.entity_parameter_value_sq).all()
        # Check that number of results matches total entities
        self.assertEqual(len(results), len(object_parameter_values) + len(relationship_parameter_values))
        # Check result values
        for row, par_val in zip(results, object_parameter_values + relationship_parameter_values):
            self.assertEqual(row.entity_class_name, par_val[0])
            if row.object_name:  # This is an object parameter
                self.assertEqual(row.object_name, par_val[1])
            else:  # This is a relationship parameter
                self.assertEqual(row.object_name_list, ','.join(par_val[1]))
            self.assertEqual(row.parameter_name, par_val[2])
            self.assertEqual(from_database(row.value, row.type), par_val[3])

    def test_wide_parameter_value_list_sq(self):
        _, errors = import_functions.import_parameter_value_lists(
            self._db_map, (("list1", "value1"), ("list1", "value2"), ("list2", "valueA"))
        )
        self.assertEqual(errors, [])
        value_lists = self._db_map.query(self._db_map.wide_parameter_value_list_sq).all()
        self.assertEqual(len(value_lists), 2)
        self.assertEqual(value_lists[0].name, "list1")
        self.assertEqual(value_lists[1].name, "list2")


class TestDatabaseMappingUpdateMixin(unittest.TestCase):
    def setUp(self):
        self._db_map = DatabaseMapping(IN_MEMORY_DB_URL, create=True)

    def tearDown(self):
        self._db_map.connection.close()

    def test_update_method_of_tool_feature_method(self):
        import_functions.import_object_classes(self._db_map, ("object_class1", "object_class2"))
        import_functions.import_parameter_value_lists(
            self._db_map, (("value_list", "value1"), ("value_list", "value2"))
        )
        import_functions.import_object_parameters(
            self._db_map, (("object_class1", "parameter1", "value1", "value_list"), ("object_class1", "parameter2"))
        )
        import_functions.import_features(self._db_map, (("object_class1", "parameter1"),))
        import_functions.import_tools(self._db_map, ("tool1",))
        import_functions.import_tool_features(self._db_map, (("tool1", "object_class1", "parameter1"),))
        import_functions.import_tool_feature_methods(
            self._db_map, (("tool1", "object_class1", "parameter1", "value2"),)
        )
        self._db_map.commit_session("Populate with initial data.")
        updated_ids, errors = self._db_map.update_tool_feature_methods(
            {"id": 1, "method_index": 0, "method": to_database("value1")[0]}
        )
        self.assertEqual(errors, [])
        self.assertEqual(updated_ids, {1})
        self._db_map.commit_session("Update data.")
        tool_feature_methods = self._db_map.query(self._db_map.ext_tool_feature_method_sq).all()
        self.assertEqual(len(tool_feature_methods), 1)
        tool_feature_method = tool_feature_methods[0]
        self.assertEqual(tool_feature_method.method, to_database("value1")[0])

    def test_update_wide_relationship_class(self):
        _ = import_functions.import_object_classes(self._db_map, ("object_class_1",))
        _ = import_functions.import_relationship_classes(self._db_map, (("my_class", ("object_class_1",)),))
        self._db_map.commit_session("Add test data")
        updated_ids, errors = self._db_map.update_wide_relationship_classes({"id": 2, "name": "renamed"})
        self.assertEqual(errors, [])
        self.assertEqual(updated_ids, {2})
        self._db_map.commit_session("Update data.")
        classes = self._db_map.query(self._db_map.wide_relationship_class_sq).all()
        self.assertEqual(len(classes), 1)
        self.assertEqual(classes[0].name, "renamed")

    def test_update_wide_relationship_class_does_not_update_member_class_id(self):
        import_functions.import_object_classes(self._db_map, ("object_class_1", "object_class_2"))
        import_functions.import_relationship_classes(self._db_map, (("my_class", ("object_class_1",)),))
        self._db_map.commit_session("Add test data")
        updated_ids, errors = self._db_map.update_wide_relationship_classes(
            {"id": 3, "name": "renamed", "object_class_id_list": [2]}
        )
        self.assertEqual([str(err) for err in errors], ["Can't update fixed fields 'object_class_id_list'"])
        self.assertEqual(updated_ids, {3})
        self._db_map.commit_session("Update data.")
        classes = self._db_map.query(self._db_map.wide_relationship_class_sq).all()
        self.assertEqual(len(classes), 1)
        self.assertEqual(classes[0].name, "renamed")
        self.assertEqual(classes[0].object_class_name_list, "object_class_1")

    def test_update_wide_relationship(self):
        import_functions.import_object_classes(self._db_map, ("object_class_1", "object_class_2"))
        import_functions.import_objects(
            self._db_map,
            (("object_class_1", "object_11"), ("object_class_1", "object_12"), ("object_class_2", "object_21")),
        )
        import_functions.import_relationship_classes(
            self._db_map, (("my_class", ("object_class_1", "object_class_2")),)
        )
        import_functions.import_relationships(self._db_map, (("my_class", ("object_11", "object_21")),))
        self._db_map.commit_session("Add test data")
        updated_ids, errors = self._db_map.update_wide_relationships(
            {"id": 4, "name": "renamed", "object_id_list": [2, 3]}
        )
        self.assertEqual(errors, [])
        self.assertEqual(updated_ids, {4})
        self._db_map.commit_session("Update data.")
        relationships = self._db_map.query(self._db_map.wide_relationship_sq).all()
        self.assertEqual(len(relationships), 1)
        self.assertEqual(relationships[0].name, "renamed")
        self.assertEqual(relationships[0].object_name_list, "object_12,object_21")

    def test_update_parameter_value_by_id_only(self):
        import_functions.import_object_classes(self._db_map, ("object_class1",))
        import_functions.import_object_parameters(self._db_map, (("object_class1", "parameter1"),))
        import_functions.import_objects(self._db_map, (("object_class1", "object1"),))
        import_functions.import_object_parameter_values(
            self._db_map, (("object_class1", "object1", "parameter1", "something"),)
        )
        self._db_map.commit_session("Populate with initial data.")
        updated_ids, errors = self._db_map.update_parameter_values({"id": 1, "value": b"something else"})
        self.assertEqual(errors, [])
        self.assertEqual(updated_ids, {1})
        self._db_map.commit_session("Update data.")
        pvals = self._db_map.query(self._db_map.parameter_value_sq).all()
        self.assertEqual(len(pvals), 1)
        pval = pvals[0]
        self.assertEqual(pval.value, b"something else")

    def test_update_parameter_definition_by_id_only(self):
        import_functions.import_object_classes(self._db_map, ("object_class1",))
        import_functions.import_object_parameters(self._db_map, (("object_class1", "parameter1"),))
        self._db_map.commit_session("Populate with initial data.")
        updated_ids, errors = self._db_map.update_parameter_definitions({"id": 1, "name": "parameter2"})
        self.assertEqual(errors, [])
        self.assertEqual(updated_ids, {1})
        self._db_map.commit_session("Update data.")
        pdefs = self._db_map.query(self._db_map.parameter_definition_sq).all()
        self.assertEqual(len(pdefs), 1)
        self.assertEqual(pdefs[0].name, "parameter2")

    def test_update_parameter_definition_value_list(self):
        import_functions.import_parameter_value_lists(self._db_map, (("my_list", 99.0),))
        import_functions.import_object_classes(self._db_map, ("object_class",))
        import_functions.import_object_parameters(self._db_map, (("object_class", "my_parameter"),))
        self._db_map.commit_session("Populate with initial data.")
        updated_ids, errors = self._db_map.update_parameter_definitions(
            {"id": 1, "name": "my_parameter", "parameter_value_list_id": 1}
        )
        self.assertEqual(errors, [])
        self.assertEqual(updated_ids, {1})
        self._db_map.commit_session("Update data.")
        pdefs = self._db_map.query(self._db_map.parameter_definition_sq).all()
        self.assertEqual(len(pdefs), 1)
        self.assertEqual(
            pdefs[0]._asdict(),
            {
                "commit_id": 3,
                "default_type": None,
                "default_value": None,
                "description": None,
                "entity_class_id": 1,
                "id": 1,
                "list_value_id": None,
                "name": "my_parameter",
                "object_class_id": 1,
                "parameter_value_list_id": 1,
                "relationship_class_id": None,
            },
        )

    def test_update_parameter_definition_value_list_when_values_exist_gives_error(self):
        import_functions.import_parameter_value_lists(self._db_map, (("my_list", 99.0),))
        import_functions.import_object_classes(self._db_map, ("object_class",))
        import_functions.import_objects(self._db_map, (("object_class", "my_object"),))
        import_functions.import_object_parameters(self._db_map, (("object_class", "my_parameter"),))
        import_functions.import_object_parameter_values(
            self._db_map, (("object_class", "my_object", "my_parameter", 23.0),)
        )
        self._db_map.commit_session("Populate with initial data.")
        updated_ids, errors = self._db_map.update_parameter_definitions(
            {"id": 1, "name": "my_parameter", "parameter_value_list_id": 1}
        )
        self.assertEqual(
            list(map(str, errors)),
            ["Can't change value list on parameter my_parameter because it has parameter values."],
        )
        self.assertEqual(updated_ids, set())

    def test_update_parameter_definitions_default_value_that_is_not_on_value_list_gives_error(self):
        import_functions.import_parameter_value_lists(self._db_map, (("my_list", 99.0),))
        import_functions.import_object_classes(self._db_map, ("object_class",))
        import_functions.import_objects(self._db_map, (("object_class", "my_object"),))
        import_functions.import_object_parameters(self._db_map, (("object_class", "my_parameter", None, "my_list"),))
        self._db_map.commit_session("Populate with initial data.")
        updated_ids, errors = self._db_map.update_parameter_definitions(
            {"id": 1, "name": "my_parameter", "default_value": to_database(23.0)[0]}
        )
        self.assertEqual(
            list(map(str, errors)),
            ["Invalid default_value '23.0' - it should be one from the parameter value list: '99.0'."],
        )
        self.assertEqual(updated_ids, set())

    def test_update_parameter_definition_value_list_when_default_value_not_on_the_list_exists_gives_error(self):
        import_functions.import_parameter_value_lists(self._db_map, (("my_list", 99.0),))
        import_functions.import_object_classes(self._db_map, ("object_class",))
        import_functions.import_objects(self._db_map, (("object_class", "my_object"),))
        import_functions.import_object_parameters(self._db_map, (("object_class", "my_parameter", 23.0),))
        self._db_map.commit_session("Populate with initial data.")
        updated_ids, errors = self._db_map.update_parameter_definitions(
            {"id": 1, "name": "my_parameter", "parameter_value_list_id": 1}
        )
        self.assertEqual(
            list(map(str, errors)),
            ["Invalid default_value '23.0' - it should be one from the parameter value list: '99.0'."],
        )
        self.assertEqual(updated_ids, set())

    def test_update_object_metadata(self):
        import_functions.import_object_classes(self._db_map, ("my_class",))
        import_functions.import_objects(self._db_map, (("my_class", "my_object"),))
        import_functions.import_metadata(self._db_map, ('{"title": "My metadata."}',))
        import_functions.import_object_metadata(self._db_map, (("my_class", "my_object", '{"title": "My metadata."}'),))
        self._db_map.commit_session("Add test data")
        ids, errors = self._db_map.update_ext_entity_metadata(
            *[{"id": 1, "metadata_name": "key_2", "metadata_value": "new value"}]
        )
        self.assertEqual(errors, [])
        self.assertEqual(ids, {1})
        metadata_entries = self._db_map.query(self._db_map.metadata_sq).all()
        self.assertEqual(len(metadata_entries), 1)
        self.assertEqual(
            metadata_entries[0]._asdict(), {"id": 1, "name": "key_2", "value": "new value", "commit_id": 3}
        )
        entity_metadata_entries = self._db_map.query(self._db_map.entity_metadata_sq).all()
        self.assertEqual(len(entity_metadata_entries), 1)
        self.assertEqual(
            entity_metadata_entries[0]._asdict(), {"id": 1, "entity_id": 1, "metadata_id": 1, "commit_id": 2}
        )

    def test_update_object_metadata_reuses_existing_metadata(self):
        import_functions.import_object_classes(self._db_map, ("my_class",))
        import_functions.import_objects(self._db_map, (("my_class", "my_object"), ("my_class", "extra_object")))
        import_functions.import_metadata(self._db_map, ('{"title": "My metadata."}', '{"key 2": "metadata value 2"}'))
        import_functions.import_object_metadata(
            self._db_map,
            (
                ("my_class", "my_object", '{"title": "My metadata."}'),
                ("my_class", "extra_object", '{"key 2": "metadata value 2"}'),
            ),
        )
        self._db_map.commit_session("Add test data")
        ids, errors = self._db_map.update_ext_entity_metadata(
            *[{"id": 1, "metadata_name": "key 2", "metadata_value": "metadata value 2"}]
        )
        self.assertEqual(errors, [])
        self.assertEqual(ids, {1})
        metadata_entries = self._db_map.query(self._db_map.metadata_sq).all()
        self.assertEqual(len(metadata_entries), 2)
        self.assertEqual(
            metadata_entries[0]._asdict(), {"id": 1, "name": "title", "value": "My metadata.", "commit_id": 2}
        )
        self.assertEqual(
            metadata_entries[1]._asdict(), {"id": 2, "name": "key 2", "value": "metadata value 2", "commit_id": 2}
        )
        entity_metadata_entries = self._db_map.query(self._db_map.entity_metadata_sq).all()
        self.assertEqual(len(entity_metadata_entries), 2)
        self.assertEqual(
            entity_metadata_entries[0]._asdict(), {"id": 1, "entity_id": 1, "metadata_id": 2, "commit_id": 3}
        )
        self.assertEqual(
            entity_metadata_entries[1]._asdict(), {"id": 2, "entity_id": 2, "metadata_id": 2, "commit_id": 2}
        )

    def test_update_object_metadata_keeps_metadata_still_in_use(self):
        import_functions.import_object_classes(self._db_map, ("my_class",))
        import_functions.import_objects(self._db_map, (("my_class", "object_1"), ("my_class", "object_2")))
        import_functions.import_metadata(self._db_map, ('{"title": "My metadata."}',))
        import_functions.import_object_metadata(
            self._db_map,
            (
                ("my_class", "object_1", '{"title": "My metadata."}'),
                ("my_class", "object_2", '{"title": "My metadata."}'),
            ),
        )
        self._db_map.commit_session("Add test data")
        ids, errors = self._db_map.update_ext_entity_metadata(
            *[{"id": 1, "metadata_name": "new key", "metadata_value": "new value"}]
        )
        self.assertEqual(errors, [])
        self.assertEqual(ids, {1, 2})
        metadata_entries = self._db_map.query(self._db_map.metadata_sq).all()
        self.assertEqual(len(metadata_entries), 2)
        self.assertEqual(
            metadata_entries[0]._asdict(), {"id": 1, "name": "title", "value": "My metadata.", "commit_id": 2}
        )
        self.assertEqual(
            metadata_entries[1]._asdict(), {"id": 2, "name": "new key", "value": "new value", "commit_id": 3}
        )
        entity_metadata_entries = self._db_map.query(self._db_map.entity_metadata_sq).all()
        self.assertEqual(len(entity_metadata_entries), 2)
        self.assertEqual(
            entity_metadata_entries[0]._asdict(), {"id": 1, "entity_id": 1, "metadata_id": 2, "commit_id": 3}
        )
        self.assertEqual(
            entity_metadata_entries[1]._asdict(), {"id": 2, "entity_id": 2, "metadata_id": 1, "commit_id": 2}
        )

    def test_update_parameter_value_metadata(self):
        import_functions.import_object_classes(self._db_map, ("my_class",))
        import_functions.import_object_parameters(self._db_map, (("my_class", "my_parameter"),))
        import_functions.import_objects(self._db_map, (("my_class", "my_object"),))
        import_functions.import_object_parameter_values(
            self._db_map, (("my_class", "my_object", "my_parameter", 99.0),)
        )
        import_functions.import_metadata(self._db_map, ('{"title": "My metadata."}',))
        import_functions.import_object_parameter_value_metadata(
            self._db_map, (("my_class", "my_object", "my_parameter", '{"title": "My metadata."}'),)
        )
        self._db_map.commit_session("Add test data")
        ids, errors = self._db_map.update_ext_parameter_value_metadata(
            *[{"id": 1, "metadata_name": "key_2", "metadata_value": "new value"}]
        )
        self.assertEqual(errors, [])
        self.assertEqual(ids, {1})
        metadata_entries = self._db_map.query(self._db_map.metadata_sq).all()
        self.assertEqual(len(metadata_entries), 1)
        self.assertEqual(
            metadata_entries[0]._asdict(), {"id": 1, "name": "key_2", "value": "new value", "commit_id": 3}
        )
        value_metadata_entries = self._db_map.query(self._db_map.parameter_value_metadata_sq).all()
        self.assertEqual(len(value_metadata_entries), 1)
        self.assertEqual(
            value_metadata_entries[0]._asdict(), {"id": 1, "parameter_value_id": 1, "metadata_id": 1, "commit_id": 2}
        )

    def test_update_parameter_value_metadata_will_not_delete_shared_entity_metadata(self):
        import_functions.import_object_classes(self._db_map, ("my_class",))
        import_functions.import_object_parameters(self._db_map, (("my_class", "my_parameter"),))
        import_functions.import_objects(self._db_map, (("my_class", "my_object"),))
        import_functions.import_object_parameter_values(
            self._db_map, (("my_class", "my_object", "my_parameter", 99.0),)
        )
        import_functions.import_metadata(self._db_map, ('{"title": "My metadata."}',))
        import_functions.import_object_metadata(self._db_map, (("my_class", "my_object", '{"title": "My metadata."}'),))
        import_functions.import_object_parameter_value_metadata(
            self._db_map, (("my_class", "my_object", "my_parameter", '{"title": "My metadata."}'),)
        )
        self._db_map.commit_session("Add test data")
        ids, errors = self._db_map.update_ext_parameter_value_metadata(
            *[{"id": 1, "metadata_name": "key_2", "metadata_value": "new value"}]
        )
        self.assertEqual(errors, [])
        self.assertEqual(ids, {1, 2})
        metadata_entries = self._db_map.query(self._db_map.metadata_sq).all()
        self.assertEqual(len(metadata_entries), 2)
        self.assertEqual(
            metadata_entries[0]._asdict(), {"id": 1, "name": "title", "value": "My metadata.", "commit_id": 2}
        )
        self.assertEqual(
            metadata_entries[1]._asdict(), {"id": 2, "name": "key_2", "value": "new value", "commit_id": 3}
        )
        value_metadata_entries = self._db_map.query(self._db_map.parameter_value_metadata_sq).all()
        self.assertEqual(len(value_metadata_entries), 1)
        self.assertEqual(
            value_metadata_entries[0]._asdict(), {"id": 1, "parameter_value_id": 1, "metadata_id": 2, "commit_id": 3}
        )
        entity_metadata_entries = self._db_map.query(self._db_map.entity_metadata_sq).all()
        self.assertEqual(len(entity_metadata_entries), 1)
        self.assertEqual(
            entity_metadata_entries[0]._asdict(), {"id": 1, "entity_id": 1, "metadata_id": 1, "commit_id": 2}
        )

    def test_update_metadata(self):
        import_functions.import_metadata(self._db_map, ('{"title": "My metadata."}',))
        self._db_map.commit_session("Add test data.")
        ids, errors = self._db_map.update_metadata(*({"id": 1, "name": "author", "value": "Prof. T. Est"},))
        self.assertEqual(errors, [])
        self.assertEqual(ids, {1})
        metadata_records = self._db_map.query(self._db_map.metadata_sq).all()
        self.assertEqual(len(metadata_records), 1)
        self.assertEqual(
            metadata_records[0]._asdict(), {"id": 1, "name": "author", "value": "Prof. T. Est", "commit_id": 3}
        )


class TestDatabaseMappingRemoveMixin(unittest.TestCase):
    def setUp(self):
        self._db_map = DatabaseMapping(IN_MEMORY_DB_URL, create=True)

    def tearDown(self):
        self._db_map.connection.close()

    def test_remove_works_when_entity_groups_are_present(self):
        import_functions.import_object_classes(self._db_map, ("my_class",))
        import_functions.import_objects(self._db_map, (("my_class", "my_object"),))
        import_functions.import_objects(self._db_map, (("my_class", "my_group"),))
        import_functions.import_object_groups(self._db_map, (("my_class", "my_group", "my_object"),))
        self._db_map.commit_session("Add test data.")
        self._db_map.cascade_remove_items(object={1})  # This shouldn't raise an exception
        self._db_map.commit_session("Remove object.")
        objects = self._db_map.query(self._db_map.object_sq).all()
        self.assertEqual(len(objects), 1)
        self.assertEqual(objects[0].name, "my_group")

    def test_remove_object_class(self):
        import_functions.import_object_classes(self._db_map, ("my_class",))
        self._db_map.commit_session("Add test data.")
        my_class = self._db_map.query(self._db_map.object_class_sq).one_or_none()
        self.assertIsNotNone(my_class)
        self._db_map.cascade_remove_items(**{"object_class": {my_class.id}})
        self._db_map.commit_session("Remove object class.")
        my_class = self._db_map.query(self._db_map.object_class_sq).one_or_none()
        self.assertIsNone(my_class)

    def test_remove_relationship_class(self):
        import_functions.import_object_classes(self._db_map, ("my_class",))
        import_functions.import_relationship_classes(self._db_map, (("my_relationship_class", ("my_class",)),))
        self._db_map.commit_session("Add test data.")
        my_class = self._db_map.query(self._db_map.relationship_class_sq).one_or_none()
        self.assertIsNotNone(my_class)
        self._db_map.cascade_remove_items(**{"relationship_class": {my_class.id}})
        self._db_map.commit_session("Remove relationship class.")
        my_class = self._db_map.query(self._db_map.relationship_class_sq).one_or_none()
        self.assertIsNone(my_class)

    def test_remove_object(self):
        import_functions.import_object_classes(self._db_map, ("my_class",))
        import_functions.import_objects(self._db_map, (("my_class", "my_object"),))
        self._db_map.commit_session("Add test data.")
        my_object = self._db_map.query(self._db_map.object_sq).one_or_none()
        self.assertIsNotNone(my_object)
        self._db_map.cascade_remove_items(**{"object": {my_object.id}})
        self._db_map.commit_session("Remove object.")
        my_object = self._db_map.query(self._db_map.object_sq).one_or_none()
        self.assertIsNone(my_object)

    def test_remove_relationship(self):
        import_functions.import_object_classes(self._db_map, ("my_class",))
        import_functions.import_objects(self._db_map, (("my_class", "my_object"),))
        import_functions.import_relationship_classes(self._db_map, (("my_relationship_class", ("my_class",)),))
        import_functions.import_relationships(self._db_map, (("my_relationship_class", ("my_object",)),))
        self._db_map.commit_session("Add test data.")
        my_relationship = self._db_map.query(self._db_map.relationship_sq).one_or_none()
        self.assertIsNotNone(my_relationship)
        self._db_map.cascade_remove_items(**{"relationship": {2}})
        self._db_map.commit_session("Remove relationship.")
        my_relationship = self._db_map.query(self._db_map.relationship_sq).one_or_none()
        self.assertIsNone(my_relationship)

    def test_remove_parameter_value(self):
        import_functions.import_object_classes(self._db_map, ("my_class",))
        import_functions.import_objects(self._db_map, (("my_class", "my_object"),))
        import_functions.import_object_parameters(self._db_map, (("my_class", "my_parameter"),))
        import_functions.import_object_parameter_values(
            self._db_map, (("my_class", "my_object", "my_parameter", 23.0),)
        )
        self._db_map.commit_session("Add test data.")
        my_value = self._db_map.query(self._db_map.object_parameter_value_sq).one_or_none()
        self.assertIsNotNone(my_value)
        self._db_map.cascade_remove_items(**{"parameter_value": {my_value.id}})
        self._db_map.commit_session("Remove parameter value.")
        my_parameter = self._db_map.query(self._db_map.object_parameter_value_sq).one_or_none()
        self.assertIsNone(my_parameter)


class TestDatabaseMappingCommitMixin(unittest.TestCase):
    def setUp(self):
        self._db_map = DatabaseMapping(IN_MEMORY_DB_URL, create=True)

    def tearDown(self):
        self._db_map.connection.close()

    def test_commit_message(self):
        """Tests that commit comment ends up in the database."""
        self._db_map.add_object_classes({"name": "testclass"})
        self._db_map.commit_session("test commit")
        self.assertEqual(self._db_map.query(self._db_map.commit_sq).all()[-1].comment, "test commit")
        self._db_map.connection.close()

    def test_commit_session_raise_with_empty_comment(self):
        import_functions.import_object_classes(self._db_map, ("my_class",))
        self.assertRaisesRegex(SpineDBAPIError, "Commit message cannot be empty.", self._db_map.commit_session, "")

    def test_commit_session_raise_when_nothing_to_commit(self):
        self.assertRaisesRegex(SpineDBAPIError, "Nothing to commit.", self._db_map.commit_session, "No changes.")


if __name__ == "__main__":
    unittest.main()
