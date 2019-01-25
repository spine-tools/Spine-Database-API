######################################################################################################################
# Copyright (C) 2017 - 2018 Spine project consortium
# This file is part of Spine Toolbox.
# Spine Toolbox is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option)
# any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

"""
Unit tests for DiffDatabaseMapping class.

:author: F. Pallonetto (UCD)
:date:   29.11.2018
"""

from spinedatabase_api.diff_database_mapping import DiffDatabaseMapping
from sqlalchemy import inspect, MetaData
from spinedatabase_api.helpers import create_new_spine_database
from spinedatabase_api import DatabaseMapping
import unittest
from faker import Faker
import random

import os

from sqlalchemy.orm import Session


class TestDatabaseAPI(unittest.TestCase):


    def setUp(self):
        if os.path.exists('TestDatabaseAPI.sqlite'):
            os.remove('TestDatabaseAPI.sqlite')
        self.object_number = 100
        self.object_class_number = 100
        self.number_wide_relationship = 100
        self.number_of_parameter = 100
        self.number_of_parameter_value = 100
        self.db = create_new_spine_database('sqlite:///TestDatabaseAPI.sqlite')
        self.db_map = DatabaseMapping('sqlite:///TestDatabaseAPI.sqlite')


    def test_create_db(self):
        # create a in memory database
        m = MetaData()
        db = create_new_spine_database('sqlite://')
        m.reflect(db.engine)
        assert len(m.tables.values()) == 9

    def test_create_engine_and_session(self):

        db = create_new_spine_database('sqlite:///test_create_engine_and_session.sqlite')
        db.connect()

        m = DatabaseMapping('sqlite:///test_create_engine_and_session.sqlite',create_all=False)

        assert isinstance(m,DatabaseMapping)

        assert not isinstance(m.session, Session)

        m.create_engine_and_session()

        assert isinstance(m.session, Session)


    def test_add_object_class_and_object(self):

        objects_before_insert = self.db_map.session.query(self.db_map.Object).count()
        objectclasses_before_insert = self.db_map.session.query(self.db_map.ObjectClass).count()
        fake = Faker()
        obj_class_ids = list()
        [obj_class_ids.append(self.db_map.add_object_class(**{'name': fake.pystr(min_chars=None, max_chars=40)}).id) for i in range(self.object_class_number)]
        [self.db_map.add_object(**{'name': fake.pystr(min_chars=None, max_chars=40),'class_id':random.choice(obj_class_ids)}) for i in range(self.object_number)]

        assert self.db_map.session.query(self.db_map.Object).count() == self.object_number + objects_before_insert
        assert self.db_map.session.query(self.db_map.ObjectClass).count() == self.object_class_number + objectclasses_before_insert

    def test_single_object(self):

        assert self.db_map.single_object(1)
        assert self.db_map.single_object_class(1)

    def test_add_wide_relationship(self):
        fake = Faker()
        relationship_before_insert = self.db_map.session.query(self.db_map.Relationship).count()

        obj_ids_list = list()
        obj_class_ids = list()

        [obj_class_ids.append(self.db_map.add_object_class(**{'name': fake.pystr(min_chars=None, max_chars=40)}).id) for i in range(self.object_class_number)]
        [obj_ids_list.append(self.db_map.add_object(**{'name': fake.pystr(min_chars=None, max_chars=40),'class_id':random.choice(obj_class_ids)}).id) for i in range(self.object_number)]


        [self.db_map.add_wide_relationship(**{
            'object_id_list': [obj_ids_list[i]],
            'dimension': 1,
            'class_id': random.choice(obj_class_ids),
            'name': fake.pystr(min_chars=None, max_chars=10),
        }) for i in range(self.number_wide_relationship)]


        assert self.db_map.session.query(self.db_map.Relationship).count() == self.number_wide_relationship + relationship_before_insert

    def test_add_wide_relationship_class(self):
        fake = Faker()

        relationship_class_before_insert = self.db_map.session.query(self.db_map.RelationshipClass).count()

        obj_ids_list = list()
        obj_class_ids = list()

        [obj_class_ids.append(self.db_map.add_object_class(**{'name': fake.pystr(min_chars=None, max_chars=40)}).id) for i in range(self.object_class_number)]
        [obj_ids_list.append(self.db_map.add_object(**{'name': fake.pystr(min_chars=None, max_chars=40),'class_id':random.choice(obj_class_ids)}).id) for i in range(self.object_number)]


        [self.db_map.add_wide_relationship_class(**{
            'object_class_id_list': [obj_class_ids[i]],
            'dimension': 1,
            'object_class_id': random.choice(obj_class_ids),
            'name': fake.pystr(min_chars=None, max_chars=10),
        }) for i in range(self.number_wide_relationship)]


        assert self.db_map.session.query(self.db_map.RelationshipClass).count() == self.number_wide_relationship + relationship_class_before_insert


    def test_add_parameter(self):
        fake = Faker()

        obj_ids_list = list()
        obj_class_ids = list()
        relationship_list_ids = list()

        parameters_before_insert = self.db_map.session.query(self.db_map.Parameter).count()


        [obj_class_ids.append(self.db_map.add_object_class(**{'name': fake.pystr(min_chars=None, max_chars=40)}).id) for
         i in range(self.object_class_number)]
        [obj_ids_list.append(self.db_map.add_object(
            **{'name': fake.pystr(min_chars=None, max_chars=40), 'class_id': random.choice(obj_class_ids)}).id) for i in
         range(self.object_number)]

        [relationship_list_ids.append(self.db_map.add_wide_relationship(**{
            'object_id_list': [random.choice(obj_ids_list) for i in range(random.randint(1, len(obj_ids_list)))],
            'dimension': 4,
            'class_id': random.choice(obj_class_ids),
            'name': fake.pystr(min_chars=None, max_chars=10)
        }).id) for i in range(self.number_wide_relationship)]

        [self.db_map.add_parameter(**{
            'name': fake.pystr(min_chars=None, max_chars=40),
            'relationship_class_id':random.choice(relationship_list_ids),
            'object_class_id':random.choice(obj_ids_list),
            'can_have_time_series': fake.boolean(chance_of_getting_true=50),
            'can_have_time_pattern': fake.boolean(chance_of_getting_true=50),
            'can_be_stochastic': fake.boolean(chance_of_getting_true=50)
        }) for i in range(self.number_of_parameter)]

        assert self.db_map.session.query(self.db_map.Parameter).count() == self.number_of_parameter + parameters_before_insert


    def test_add_parameter_value(self):
        fake = Faker()

        obj_ids_list = list()
        obj_class_ids = list()
        relationship_list_ids = list()
        parameter_list_ids = list()

        parameters_before_insert = self.db_map.session.query(self.db_map.Parameter).count()

        [obj_class_ids.append(self.db_map.add_object_class(**{'name': fake.pystr(min_chars=None, max_chars=40)}).id) for
         i in range(self.object_class_number)]
        [obj_ids_list.append(self.db_map.add_object(
            **{'name': fake.pystr(min_chars=None, max_chars=40), 'class_id': random.choice(obj_class_ids)}).id) for i in
         range(self.object_number)]

        [relationship_list_ids.append(self.db_map.add_wide_relationship(**{
            'object_id_list': [random.choice(obj_ids_list) for i in range(random.randint(1, len(obj_ids_list)))],
            'dimension': 4,
            'class_id': random.choice(obj_class_ids),
            'name': fake.pystr(min_chars=None, max_chars=10)
        }).id) for i in range(self.number_wide_relationship)]

        [parameter_list_ids.append(self.db_map.add_parameter(**{
            'name': fake.pystr(min_chars=None, max_chars=40),
            'relationship_class_id': random.choice(relationship_list_ids),
            'object_class_id': random.choice(obj_ids_list),
            'can_have_time_series': fake.boolean(chance_of_getting_true=50),
            'can_have_time_pattern': fake.boolean(chance_of_getting_true=50),
            'can_be_stochastic': fake.boolean(chance_of_getting_true=50)
        }).id) for i in range(self.number_of_parameter)]

        [self.db_map.add_parameter_value(**{
            'json': str(fake.pydict(nb_elements=3, variable_nb_elements=True)),
            'parameter_definition_id': parameter_list_ids[i],
            'object_id': obj_ids_list[i],
            'value': fake.pyfloat(left_digits=None, right_digits=None, positive=False),
            'expression': str(fake.pydict(nb_elements=3, variable_nb_elements=True))
        }) for i in range(self.number_of_parameter_value)]

    def test_get_or_add_object_class(self):
        obj_class_ids = list()
        fake = Faker()
        object_before_insert = self.db_map.session.query(self.db_map.ObjectClass).count()
        [obj_class_ids.append(self.db_map.get_or_add_object_class(**{'name': fake.pystr(min_chars=None, max_chars=40)}).id) for
         i in range(self.object_class_number)]
        assert self.db_map.session.query(self.db_map.ObjectClass).count() == object_before_insert+self.object_class_number

    def test_get_or_add_wide_relationship_class(self):
         fake = Faker()

         relationship_class_before_insert = self.db_map.session.query(self.db_map.RelationshipClass).count()

         obj_ids_list = list()
         obj_class_ids = list()

         [obj_class_ids.append(self.db_map.add_object_class(**{'name': fake.pystr(min_chars=None, max_chars=40)}).id)
          for i in range(self.object_class_number)]
         [obj_ids_list.append(self.db_map.add_object(
             **{'name': fake.pystr(min_chars=None, max_chars=40), 'class_id': random.choice(obj_class_ids)}).id) for i
          in range(self.object_number)]

         [self.db_map.get_or_add_wide_relationship_class(**{
             'object_class_id_list': [obj_class_ids[i]],
             'dimension': 1,
             'object_class_id': random.choice(obj_class_ids),
             'name': fake.pystr(min_chars=None, max_chars=10),
         }) for i in range(self.number_wide_relationship)]

         assert self.db_map.session.query(
             self.db_map.RelationshipClass).count() == self.number_wide_relationship + relationship_class_before_insert


    def test_get_or_add_parameter(self):
        fake = Faker()

        obj_ids_list = list()
        obj_class_ids = list()
        relationship_list_ids = list()

        parameters_before_insert = self.db_map.session.query(self.db_map.Parameter).count()

        [obj_class_ids.append(self.db_map.add_object_class(**{'name': fake.pystr(min_chars=None, max_chars=40)}).id) for
         i in range(self.object_class_number)]
        [obj_ids_list.append(self.db_map.add_object(
            **{'name': fake.pystr(min_chars=None, max_chars=40), 'class_id': random.choice(obj_class_ids)}).id) for i in
         range(self.object_number)]

        [relationship_list_ids.append(self.db_map.add_wide_relationship(**{
            'object_id_list': [random.choice(obj_ids_list) for i in range(random.randint(1, len(obj_ids_list)))],
            'dimension': 4,
            'class_id': random.choice(obj_class_ids),
            'name': fake.pystr(min_chars=None, max_chars=10)
        }).id) for i in range(self.number_wide_relationship)]

        [self.db_map.get_or_add_parameter(**{
            'name': fake.pystr(min_chars=None, max_chars=40),
            'relationship_class_id': random.choice(relationship_list_ids),
            'object_class_id': random.choice(obj_ids_list),
            'can_have_time_series': fake.boolean(chance_of_getting_true=50),
            'can_have_time_pattern': fake.boolean(chance_of_getting_true=50),
            'can_be_stochastic': fake.boolean(chance_of_getting_true=50)
        }) for i in range(self.number_of_parameter)]

        assert self.db_map.session.query(
            self.db_map.Parameter).count() == self.number_of_parameter + parameters_before_insert

    def test_rename_object_class(self):

        fake = Faker()
        obj_class_ids = list()
        [obj_class_ids.append(self.db_map.add_object_class(**{'name': fake.name()}).id) for i in range(self.object_class_number)]

        new_elem = self.db_map.get_or_add_object_class(**{'name': fake.name()})

        self.db_map.rename_object_class(new_elem.id,"TEST_PASSED")

        renamed_element = self.db_map.get_or_add_object_class(**{'name': "TEST_PASSED"})

        assert new_elem.id == renamed_element.id
        assert renamed_element.name == "TEST_PASSED"

    def test_rename_object(self):
        obj_ids_list = list()
        obj_class_ids = list()

        fake = Faker()

        [obj_class_ids.append(self.db_map.add_object_class(**{'name': fake.pystr(min_chars=None, max_chars=40)}).id) for
         i in range(self.object_class_number)]
        [obj_ids_list.append(self.db_map.add_object(
            **{'name': fake.pystr(min_chars=None, max_chars=40), 'class_id': random.choice(obj_class_ids)}).id) for i in
         range(self.object_number)]

        test_id =  random.choice(obj_ids_list)

        self.db_map.rename_object(test_id, "TEST_PASSED")

        renamed_element=self.db_map.single_object(name="TEST_PASSED").one_or_none()

        assert renamed_element.id == test_id

    def test_rename_relationship_class(self):
        fake = Faker()
        obj_ids_list = list()
        obj_class_ids = list()
        obj_relationship_class_ids = list()

        [obj_class_ids.append(self.db_map.add_object_class(**{'name': fake.pystr(min_chars=None, max_chars=40)}).id) for
         i in range(self.object_class_number)]
        [obj_ids_list.append(self.db_map.add_object(
            **{'name': fake.pystr(min_chars=None, max_chars=40), 'class_id': random.choice(obj_class_ids)}).id) for i in
         range(self.object_number)]

        [obj_relationship_class_ids.append(self.db_map.add_wide_relationship_class(**{
            'object_class_id_list': [obj_class_ids[i]],
            'dimension': 1,
            'object_class_id': random.choice(obj_class_ids),
            'name': fake.pystr(min_chars=None, max_chars=10),
        })) for i in range(self.number_wide_relationship)]

        test_id = random.choice(obj_relationship_class_ids).id

        self.db_map.rename_relationship_class(test_id, "TEST_PASSED_CORRECTLY")

        renamed_element = self.db_map.single_wide_relationship_class(name="TEST_PASSED_CORRECTLY").one_or_none()

        assert renamed_element.id == test_id

    def test_rename_relationship(self):
        fake = Faker()

        relationship_before_insert = self.db_map.session.query(self.db_map.Relationship).count()

        obj_ids_list = list()
        obj_class_ids = list()
        obj_relationship_ids = list()

        [obj_class_ids.append(self.db_map.add_object_class(**{'name': fake.pystr(min_chars=None, max_chars=40)}).id) for
         i in range(self.object_class_number)]
        [obj_ids_list.append(self.db_map.add_object(
            **{'name': fake.pystr(min_chars=None, max_chars=40), 'class_id': random.choice(obj_class_ids)}).id) for i in
         range(self.object_number)]

        [obj_relationship_ids.append(self.db_map.add_wide_relationship(**{
            'object_id_list': [obj_ids_list[i]],
            'dimension': 1,
            'class_id': random.choice(obj_class_ids),
            'name': fake.pystr(min_chars=None, max_chars=10),
        })) for i in range(self.number_wide_relationship)]

        test_id = random.choice(obj_relationship_ids).id

        self.db_map.rename_relationship(test_id, "TEST_PASSED_CORRECTLY")

        renamed_element = self.db_map.single_wide_relationship(name="TEST_PASSED_CORRECTLY").one_or_none()

        assert renamed_element.id == test_id


    def test_update_parameter(self):
        fake = Faker()

        obj_ids_list = list()
        obj_class_ids = list()
        relationship_list_ids = list()
        obj_parameter_ids = list()

        [obj_class_ids.append(self.db_map.add_object_class(**{'name': fake.pystr(min_chars=None, max_chars=40)}).id) for
         i in range(self.object_class_number)]
        [obj_ids_list.append(self.db_map.add_object(
            **{'name': fake.pystr(min_chars=None, max_chars=40), 'class_id': random.choice(obj_class_ids)}).id) for i in
         range(self.object_number)]

        [relationship_list_ids.append(self.db_map.add_wide_relationship(**{
            'object_id_list': [random.choice(obj_ids_list) for i in range(random.randint(1, len(obj_ids_list)))],
            'dimension': 4,
            'class_id': random.choice(obj_class_ids),
            'name': fake.pystr(min_chars=None, max_chars=10)
        }).id) for i in range(self.number_wide_relationship)]

        [obj_parameter_ids.append(self.db_map.add_parameter(**{
            'name': fake.pystr(min_chars=None, max_chars=40),
            'relationship_class_id': random.choice(relationship_list_ids),
            'object_class_id': random.choice(obj_ids_list),
            'can_have_time_series': fake.boolean(chance_of_getting_true=50),
            'can_have_time_pattern': fake.boolean(chance_of_getting_true=50),
            'can_be_stochastic': fake.boolean(chance_of_getting_true=50)
        })) for i in range(self.number_of_parameter)]

        test_id = random.choice(obj_parameter_ids).id

        self.db_map.update_parameter(test_id, "name","PARAMETER_UPDATED_CORRECTLY")

        updated_parameter = self.db_map.single_parameter(test_id).one_or_none()

        assert updated_parameter.name == "PARAMETER_UPDATED_CORRECTLY"

    def test_update_parameter_value(self):
        fake = Faker()

        obj_ids_list = list()
        obj_class_ids = list()
        relationship_list_ids = list()
        parameter_list_ids = list()
        parameter_value_ids = list()

        [obj_class_ids.append(self.db_map.add_object_class(**{'name': fake.pystr(min_chars=None, max_chars=40)}).id) for
         i in range(self.object_class_number)]
        [obj_ids_list.append(self.db_map.add_object(
            **{'name': fake.pystr(min_chars=None, max_chars=40), 'class_id': random.choice(obj_class_ids)}).id) for i in
         range(self.object_number)]

        [relationship_list_ids.append(self.db_map.add_wide_relationship(**{
            'object_id_list': [random.choice(obj_ids_list) for i in range(random.randint(1, len(obj_ids_list)))],
            'dimension': 4,
            'class_id': random.choice(obj_class_ids),
            'name': fake.pystr(min_chars=None, max_chars=10)
        }).id) for i in range(self.number_wide_relationship)]

        [parameter_list_ids.append(self.db_map.add_parameter(**{
            'name': fake.pystr(min_chars=None, max_chars=40),
            'relationship_class_id': random.choice(relationship_list_ids),
            'object_class_id': random.choice(obj_ids_list),
            'can_have_time_series': fake.boolean(chance_of_getting_true=50),
            'can_have_time_pattern': fake.boolean(chance_of_getting_true=50),
            'can_be_stochastic': fake.boolean(chance_of_getting_true=50)
        }).id) for i in range(self.number_of_parameter)]

        [parameter_value_ids.append(self.db_map.add_parameter_value(**{
            'json': str(fake.pydict(nb_elements=3, variable_nb_elements=True)),
            'parameter_definition_id': parameter_list_ids[i],
            'object_id': obj_ids_list[i],
            'value': fake.pyfloat(left_digits=None, right_digits=None, positive=False),
            'expression': str(fake.pydict(nb_elements=3, variable_nb_elements=True))
        })) for i in range(self.number_of_parameter_value)]


        test_id = random.choice(parameter_value_ids).id

        self.db_map.update_parameter_value(test_id, "expression", "PARAMETER_UPDATED_CORRECTLY")

        updated_parameter = self.db_map.single_parameter_value(test_id).one_or_none()

        assert updated_parameter.expression == "PARAMETER_UPDATED_CORRECTLY"

    def test_remove_object_class(self):
        fake = Faker()
        obj_class_ids = list()
        [obj_class_ids.append(self.db_map.add_object_class(**{'name': fake.pystr(min_chars=None, max_chars=40)}).id) for
         i in range(self.object_class_number)]
        [self.db_map.add_object(
            **{'name': fake.pystr(min_chars=None, max_chars=40), 'class_id': random.choice(obj_class_ids)}) for i in
         range(self.object_number)]

        objects_before_deletion = self.db_map.session.query(self.db_map.Object).count()
        objectclasses_before_deletion = self.db_map.session.query(self.db_map.ObjectClass).count()

        remove_object_class_candidate = random.choice(obj_class_ids)

        self.db_map.remove_object_class(remove_object_class_candidate)

        assert self.db_map.session.query(
            self.db_map.ObjectClass).count() == objectclasses_before_deletion -1

    def test_remove_object(self):
        fake = Faker()
        obj_class_ids = list()
        obj_ids = list()
        [obj_class_ids.append(self.db_map.add_object_class(**{'name': fake.pystr(min_chars=None, max_chars=40)}).id) for
         i in range(self.object_class_number)]
        [obj_ids.append(self.db_map.add_object(
            **{'name': fake.pystr(min_chars=None, max_chars=40), 'class_id': random.choice(obj_class_ids)})) for i in
            range(self.object_number)]

        objects_before_deletion = self.db_map.session.query(self.db_map.Object).count()

        remove_object_candidate = random.choice(obj_ids).id

        self.db_map.remove_object(remove_object_candidate)

        assert self.db_map.session.query(
            self.db_map.Object).count() == objects_before_deletion - 1

    def test_remove_relationship_class(self):
        fake = Faker()
        obj_ids_list = list()
        obj_class_ids = list()
        obj_relationship_class_ids = list()

        [obj_class_ids.append(self.db_map.add_object_class(**{'name': fake.pystr(min_chars=None, max_chars=40)}).id) for
         i in range(self.object_class_number)]
        [obj_ids_list.append(self.db_map.add_object(
            **{'name': fake.pystr(min_chars=None, max_chars=40), 'class_id': random.choice(obj_class_ids)}).id) for i in
         range(self.object_number)]

        [obj_relationship_class_ids.append(self.db_map.add_wide_relationship_class(**{
            'object_class_id_list': [obj_class_ids[i]],
            'dimension': 1,
            'object_class_id': random.choice(obj_class_ids),
            'name': fake.pystr(min_chars=None, max_chars=10),
        })) for i in range(self.number_wide_relationship)]

        test_id = random.choice(obj_relationship_class_ids).id

        number_of_relationship_classes_before_deletion = self.db_map.session.query(self.db_map.RelationshipClass).count()

        self.db_map.remove_relationship_class(test_id)

        assert self.db_map.session.query(self.db_map.RelationshipClass).count() == number_of_relationship_classes_before_deletion -1

    def test_remove_relationship(self):
        fake = Faker()

        relationship_before_insert = self.db_map.session.query(self.db_map.Relationship).count()

        obj_ids_list = list()
        obj_class_ids = list()
        obj_relationship_ids = list()

        [obj_class_ids.append(self.db_map.add_object_class(**{'name': fake.pystr(min_chars=None, max_chars=40)}).id) for
         i in range(self.object_class_number)]
        [obj_ids_list.append(self.db_map.add_object(
            **{'name': fake.pystr(min_chars=None, max_chars=40), 'class_id': random.choice(obj_class_ids)}).id) for i in
         range(self.object_number)]

        [obj_relationship_ids.append(self.db_map.add_wide_relationship(**{
            'object_id_list': [obj_ids_list[i]],
            'dimension': 1,
            'class_id': random.choice(obj_class_ids),
            'name': fake.pystr(min_chars=None, max_chars=10),
        })) for i in range(self.number_wide_relationship)]

        test_id = random.choice(obj_relationship_ids).id

        number_of_relationship_before_deletion = self.db_map.session.query(
            self.db_map.Relationship).count()

        self.db_map.remove_relationship(test_id)

        assert self.db_map.session.query(
            self.db_map.Relationship).count() == number_of_relationship_before_deletion - 1

    def test_remove_parameter(self):
        fake = Faker()

        obj_ids_list = list()
        obj_class_ids = list()
        relationship_list_ids = list()
        obj_parameter_ids = list()

        [obj_class_ids.append(self.db_map.add_object_class(**{'name': fake.pystr(min_chars=None, max_chars=40)}).id) for
         i in range(self.object_class_number)]
        [obj_ids_list.append(self.db_map.add_object(
            **{'name': fake.pystr(min_chars=None, max_chars=40), 'class_id': random.choice(obj_class_ids)}).id) for i in
         range(self.object_number)]

        [relationship_list_ids.append(self.db_map.add_wide_relationship(**{
            'object_id_list': [random.choice(obj_ids_list) for i in range(random.randint(1, len(obj_ids_list)))],
            'dimension': 4,
            'class_id': random.choice(obj_class_ids),
            'name': fake.pystr(min_chars=None, max_chars=10)
        }).id) for i in range(self.number_wide_relationship)]

        [obj_parameter_ids.append(self.db_map.add_parameter(**{
            'name': fake.pystr(min_chars=None, max_chars=40),
            'relationship_class_id': random.choice(relationship_list_ids),
            'object_class_id': random.choice(obj_ids_list),
            'can_have_time_series': fake.boolean(chance_of_getting_true=50),
            'can_have_time_pattern': fake.boolean(chance_of_getting_true=50),
            'can_be_stochastic': fake.boolean(chance_of_getting_true=50)
        })) for i in range(self.number_of_parameter)]

        test_id = random.choice(obj_parameter_ids).id

        number_of_parameter_before_deletion = self.db_map.session.query(
            self.db_map.Parameter).count()

        self.db_map.remove_parameter(test_id)

        assert self.db_map.session.query(
            self.db_map.Parameter).count() == number_of_parameter_before_deletion - 1

    def test_remove_parameter_value(self):
        fake = Faker()

        obj_ids_list = list()
        obj_class_ids = list()
        relationship_list_ids = list()
        parameter_list_ids = list()
        parameter_value_ids = list()

        [obj_class_ids.append(self.db_map.add_object_class(**{'name': fake.pystr(min_chars=None, max_chars=40)}).id) for
         i in range(self.object_class_number)]
        [obj_ids_list.append(self.db_map.add_object(
            **{'name': fake.pystr(min_chars=None, max_chars=40), 'class_id': random.choice(obj_class_ids)}).id) for i in
         range(self.object_number)]

        [relationship_list_ids.append(self.db_map.add_wide_relationship(**{
            'object_id_list': [random.choice(obj_ids_list) for i in range(random.randint(1, len(obj_ids_list)))],
            'dimension': 4,
            'class_id': random.choice(obj_class_ids),
            'name': fake.pystr(min_chars=None, max_chars=10)
        }).id) for i in range(self.number_wide_relationship)]

        [parameter_list_ids.append(self.db_map.add_parameter(**{
            'name': fake.pystr(min_chars=None, max_chars=40),
            'relationship_class_id': random.choice(relationship_list_ids),
            'object_class_id': random.choice(obj_ids_list),
            'can_have_time_series': fake.boolean(chance_of_getting_true=50),
            'can_have_time_pattern': fake.boolean(chance_of_getting_true=50),
            'can_be_stochastic': fake.boolean(chance_of_getting_true=50)
        }).id) for i in range(self.number_of_parameter)]

        [parameter_value_ids.append(self.db_map.add_parameter_value(**{
            'json': str(fake.pydict(nb_elements=3, variable_nb_elements=True)),
            'parameter_definition_id': parameter_list_ids[i],
            'object_id': obj_ids_list[i],
            'value': fake.pyfloat(left_digits=None, right_digits=None, positive=False),
            'expression': str(fake.pydict(nb_elements=3, variable_nb_elements=True))
        })) for i in range(self.number_of_parameter_value)]

        test_id = random.choice(parameter_value_ids).id

        number_of_parameter_value_before_deletion = self.db_map.session.query(
            self.db_map.ParameterValue).count()

        self.db_map.remove_parameter_value(test_id)

        assert self.db_map.session.query(
            self.db_map.ParameterValue).count() == number_of_parameter_value_before_deletion - 1

    def test_reset_mapping(self):
        fake = Faker()

        obj_ids_list = list()
        obj_class_ids = list()
        relationship_list_ids = list()
        parameter_list_ids = list()
        parameter_value_ids = list()

        [obj_class_ids.append(self.db_map.add_object_class(**{'name': fake.pystr(min_chars=None, max_chars=40)}).id) for
         i in range(self.object_class_number)]
        [obj_ids_list.append(self.db_map.add_object(
            **{'name': fake.pystr(min_chars=None, max_chars=40), 'class_id': random.choice(obj_class_ids)}).id) for i in
         range(self.object_number)]

        [relationship_list_ids.append(self.db_map.add_wide_relationship(**{
            'object_id_list': [random.choice(obj_ids_list) for i in range(random.randint(1, len(obj_ids_list)))],
            'dimension': 4,
            'class_id': random.choice(obj_class_ids),
            'name': fake.pystr(min_chars=None, max_chars=10)
        }).id) for i in range(self.number_wide_relationship)]

        [parameter_list_ids.append(self.db_map.add_parameter(**{
            'name': fake.pystr(min_chars=None, max_chars=40),
            'relationship_class_id': random.choice(relationship_list_ids),
            'object_class_id': random.choice(obj_ids_list),
            'can_have_time_series': fake.boolean(chance_of_getting_true=50),
            'can_have_time_pattern': fake.boolean(chance_of_getting_true=50),
            'can_be_stochastic': fake.boolean(chance_of_getting_true=50)
        }).id) for i in range(self.number_of_parameter)]

        [parameter_value_ids.append(self.db_map.add_parameter_value(**{
            'json': str(fake.pydict(nb_elements=3, variable_nb_elements=True)),
            'parameter_definition_id': parameter_list_ids[i],
            'object_id': obj_ids_list[i],
            'value': fake.pyfloat(left_digits=None, right_digits=None, positive=False),
            'expression': str(fake.pydict(nb_elements=3, variable_nb_elements=True))
        })) for i in range(self.number_of_parameter_value)]

        self.db_map.reset_mapping()

        assert self.db_map.session.query(
            self.db_map.ParameterValue).count() == 0
        assert self.db_map.session.query(
            self.db_map.Parameter).count() == 0
        assert self.db_map.session.query(
            self.db_map.Object).count() == 0
        assert self.db_map.session.query(
            self.db_map.RelationshipClass).count() == 0
        assert self.db_map.session.query(
            self.db_map.Relationship).count() == 0

    def tearDown(self):
        """Overridden method. Runs after each test.
        Use this to free resources after a test if needed.
        """
        # delete temp excel file if it exists

        self.db_map.close()

        try:
            os.remove("TestDatabaseAPI.sqlite")
        except OSError as e:
            pass

        try:
            os.remove("test_create_engine_and_session.sqlite")
        except OSError as e:
            pass
