######################################################################################################################
# Copyright (C) 2017 - 2019 Spine project consortium
# This file is part of Spine Database API.
# Spine Database API is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser
# General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

"""Provides :class:`.DiffDatabaseMappingRemoveMixin`.

:author: Manuel Marin (KTH)
:date:   11.8.2018
"""

from sqlalchemy.exc import DBAPIError
from .exception import SpineDBAPIError

# TODO: improve docstrings


class DiffDatabaseMappingRemoveMixin:
    """Provides the :meth:`remove_items` method to stage ``REMOVE`` operations over a Spine db.
    """

    def remove_items(
        self,
        object_class_ids=(),
        object_ids=(),
        relationship_class_ids=(),
        relationship_ids=(),
        parameter_definition_ids=(),
        parameter_value_ids=(),
        parameter_tag_ids=(),
        parameter_definition_tag_ids=(),
        parameter_value_list_ids=(),
    ):
        """Removes items by id."""
        item_id, diff_item_id = self.cascading_ids(
            object_class_ids=object_class_ids,
            object_ids=object_ids,
            relationship_class_ids=relationship_class_ids,
            relationship_ids=relationship_ids,
            parameter_definition_ids=parameter_definition_ids,
            parameter_value_ids=parameter_value_ids,
            parameter_tag_ids=parameter_tag_ids,
            parameter_definition_tag_ids=parameter_definition_tag_ids,
            parameter_value_list_ids=parameter_value_list_ids,
        )
        self.do_remove_items(item_id, diff_item_id)

    def do_remove_items(self, item_id, diff_item_id):
        try:
            for tablename, ids in diff_item_id.items():
                id_col = self.table_ids.get(tablename, "id")
                classname = self.table_to_class[tablename]
                diff_class = getattr(self, "Diff" + classname)
                self.query(diff_class).filter(getattr(diff_class, id_col).in_(ids)).delete(synchronize_session=False)
            self.session.commit()
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while removing items: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)
        for tablename, ids in diff_item_id.items():
            self.added_item_id[tablename].difference_update(ids)
        for tablename, ids in item_id.items():
            self.removed_item_id[tablename].update(ids)
            self._mark_as_dirty(tablename, ids)

    def cascading_ids(
        self,
        object_class_ids=(),
        object_ids=(),
        relationship_class_ids=(),
        relationship_ids=(),
        parameter_definition_ids=(),
        parameter_value_ids=(),
        parameter_tag_ids=(),
        parameter_definition_tag_ids=(),
        parameter_value_list_ids=(),
    ):
        """Returns cascading ids.

        Returns:
            item_id (dict): cascading ids in the original tables
            diff_item_id (dict): cascading ids in the difference tables
        """
        item_id = {}
        diff_item_id = {}
        # object_class
        item_list = self.query(self.ObjectClass.entity_class_id).filter(
            self.ObjectClass.entity_class_id.in_(object_class_ids)
        )
        diff_item_list = self.query(self.DiffObjectClass.entity_class_id).filter(
            self.DiffObjectClass.entity_class_id.in_(object_class_ids)
        )
        self._collect_object_class_cascading_ids(
            [x.entity_class_id for x in item_list], [x.entity_class_id for x in diff_item_list], item_id, diff_item_id
        )
        # object
        item_list = self.query(self.Object.entity_id).filter(self.Object.entity_id.in_(object_ids))
        diff_item_list = self.query(self.DiffObject.entity_id).filter(self.DiffObject.entity_id.in_(object_ids))
        self._collect_object_cascading_ids(
            [x.entity_id for x in item_list], [x.entity_id for x in diff_item_list], item_id, diff_item_id
        )
        # relationship_class
        item_list = self.query(self.RelationshipClass.entity_class_id).filter(
            self.RelationshipClass.entity_class_id.in_(relationship_class_ids)
        )
        diff_item_list = self.query(self.DiffRelationshipClass.entity_class_id).filter(
            self.DiffRelationshipClass.entity_class_id.in_(relationship_class_ids)
        )
        self._collect_relationship_class_cascading_ids(
            [x.entity_class_id for x in item_list], [x.entity_class_id for x in diff_item_list], item_id, diff_item_id
        )
        # relationship
        item_list = self.query(self.Relationship.entity_id).filter(self.Relationship.entity_id.in_(relationship_ids))
        diff_item_list = self.query(self.DiffRelationship.entity_id).filter(
            self.DiffRelationship.entity_id.in_(relationship_ids)
        )
        self._collect_relationship_cascading_ids(
            [x.entity_id for x in item_list], [x.entity_id for x in diff_item_list], item_id, diff_item_id
        )
        # parameter
        item_list = self.query(self.ParameterDefinition.id).filter(
            self.ParameterDefinition.id.in_(parameter_definition_ids)
        )
        diff_item_list = self.query(self.DiffParameterDefinition.id).filter(
            self.DiffParameterDefinition.id.in_(parameter_definition_ids)
        )
        self._collect_parameter_definition_cascading_ids(
            [x.id for x in item_list], [x.id for x in diff_item_list], item_id, diff_item_id
        )
        # parameter_value
        item_list = self.query(self.ParameterValue.id).filter(self.ParameterValue.id.in_(parameter_value_ids))
        diff_item_list = self.query(self.DiffParameterValue.id).filter(
            self.DiffParameterValue.id.in_(parameter_value_ids)
        )
        self._collect_parameter_value_cascading_ids(
            [x.id for x in item_list], [x.id for x in diff_item_list], item_id, diff_item_id
        )
        # parameter_tag
        item_list = self.query(self.ParameterTag.id).filter(self.ParameterTag.id.in_(parameter_tag_ids))
        diff_item_list = self.query(self.DiffParameterTag.id).filter(self.DiffParameterTag.id.in_(parameter_tag_ids))
        self._collect_parameter_tag_cascading_ids(
            [x.id for x in item_list], [x.id for x in diff_item_list], item_id, diff_item_id
        )
        # parameter_definition_tag
        item_list = self.query(self.ParameterDefinitionTag.id).filter(
            self.ParameterDefinitionTag.id.in_(parameter_definition_tag_ids)
        )
        diff_item_list = self.query(self.DiffParameterDefinitionTag.id).filter(
            self.DiffParameterDefinitionTag.id.in_(parameter_definition_tag_ids)
        )
        self._collect_parameter_definition_tag_cascading_ids(
            [x.id for x in item_list], [x.id for x in diff_item_list], item_id, diff_item_id
        )
        # parameter_value_list
        item_list = self.query(self.ParameterValueList.id).filter(
            self.ParameterValueList.id.in_(parameter_value_list_ids)
        )
        diff_item_list = self.query(self.DiffParameterValueList.id).filter(
            self.DiffParameterValueList.id.in_(parameter_value_list_ids)
        )
        self._collect_parameter_value_list_cascading_ids(
            [x.id for x in item_list], [x.id for x in diff_item_list], item_id, diff_item_id
        )
        return item_id, diff_item_id

    def _collect_object_class_cascading_ids(self, ids, diff_ids, item_id, diff_item_id):
        """Finds object class cascading ids and adds them to the given dictionaries."""
        # Touch
        item_id.setdefault("entity_class", set()).update(ids)
        diff_item_id.setdefault("entity_class", set()).update(diff_ids)
        item_id.setdefault("object_class", set()).update(ids)
        diff_item_id.setdefault("object_class", set()).update(diff_ids)
        # object
        item_list = self.query(self.Entity.id).filter(self.Entity.class_id.in_(ids))
        diff_item_list = self.query(self.DiffEntity.id).filter(self.DiffEntity.class_id.in_(ids + diff_ids))
        self._collect_object_cascading_ids(
            [x.id for x in item_list], [x.id for x in diff_item_list], item_id, diff_item_id
        )
        # relationship_class
        item_list = self.query(self.RelationshipEntityClass.entity_class_id).filter(
            self.RelationshipEntityClass.member_class_id.in_(ids)
        )
        diff_item_list = self.query(self.DiffRelationshipEntityClass.entity_class_id).filter(
            self.DiffRelationshipEntityClass.member_class_id.in_(ids + diff_ids)
        )
        self._collect_relationship_class_cascading_ids(
            [x.entity_class_id for x in item_list], [x.entity_class_id for x in diff_item_list], item_id, diff_item_id
        )
        # parameter
        item_list = self.query(self.ParameterDefinition.id).filter(self.ParameterDefinition.entity_class_id.in_(ids))
        diff_item_list = self.query(self.DiffParameterDefinition.id).filter(
            self.DiffParameterDefinition.entity_class_id.in_(ids + diff_ids)
        )
        self._collect_parameter_definition_cascading_ids(
            [x.id for x in item_list], [x.id for x in diff_item_list], item_id, diff_item_id
        )

    def _collect_object_cascading_ids(self, ids, diff_ids, item_id, diff_item_id):
        """Finds object cascading ids and adds them to the given dictionaries."""
        # Touch
        item_id.setdefault("entity", set()).update(ids)
        diff_item_id.setdefault("entity", set()).update(diff_ids)
        item_id.setdefault("object", set()).update(ids)
        diff_item_id.setdefault("object", set()).update(diff_ids)
        # relationship
        item_list = self.query(self.RelationshipEntity.entity_id).filter(self.RelationshipEntity.member_id.in_(ids))
        diff_item_list = self.query(self.DiffRelationshipEntity.entity_id).filter(
            self.DiffRelationshipEntity.member_id.in_(ids + diff_ids)
        )
        self._collect_relationship_cascading_ids(
            [x.entity_id for x in item_list], [x.entity_id for x in diff_item_list], item_id, diff_item_id
        )
        # parameter_value
        item_list = self.query(self.ParameterValue.id).filter(self.ParameterValue.entity_id.in_(ids))
        diff_item_list = self.query(self.DiffParameterValue.id).filter(
            self.DiffParameterValue.entity_id.in_(ids + diff_ids)
        )
        self._collect_parameter_value_cascading_ids(
            [x.id for x in item_list], [x.id for x in diff_item_list], item_id, diff_item_id
        )

    def _collect_relationship_class_cascading_ids(self, ids, diff_ids, item_id, diff_item_id):
        """Find out which items need to be removed due to the removal of relationship classes
        given by `ids` and `diff_ids`,
        and add their ids to `item_id` and `diff_item_id`."""
        # Touch
        item_id.setdefault("relationship_class", set()).update(ids)
        diff_item_id.setdefault("relationship_class", set()).update(diff_ids)
        item_id.setdefault("relationship_entity_class", set()).update(ids)
        diff_item_id.setdefault("relationship_entity_class", set()).update(diff_ids)
        item_id.setdefault("entity_class", set()).update(ids)
        diff_item_id.setdefault("entity_class", set()).update(diff_ids)
        # relationship
        item_list = self.query(self.Relationship.entity_id).filter(self.Relationship.entity_class_id.in_(ids))
        diff_item_list = self.query(self.DiffRelationship.entity_id).filter(
            self.DiffRelationship.entity_class_id.in_(ids + diff_ids)
        )
        self._collect_relationship_cascading_ids(
            [x.entity_id for x in item_list], [x.entity_id for x in diff_item_list], item_id, diff_item_id
        )
        # parameter
        item_list = self.query(self.ParameterDefinition.id).filter(self.ParameterDefinition.entity_class_id.in_(ids))
        diff_item_list = self.query(self.DiffParameterDefinition.id).filter(
            self.DiffParameterDefinition.entity_class_id.in_(ids + diff_ids)
        )
        self._collect_parameter_definition_cascading_ids(
            [x.id for x in item_list], [x.id for x in diff_item_list], item_id, diff_item_id
        )

    def _collect_relationship_cascading_ids(self, ids, diff_ids, item_id, diff_item_id):
        """Find out which items need to be removed due to the removal of relationships
        given by `ids` and `diff_ids`,
        and add their ids to `item_id` and `diff_item_id`."""
        # Touch
        item_id.setdefault("relationship", set()).update(ids)
        diff_item_id.setdefault("relationship", set()).update(diff_ids)
        item_id.setdefault("entity", set()).update(ids)
        diff_item_id.setdefault("entity", set()).update(diff_ids)
        item_id.setdefault("relationship_entity", set()).update(ids)
        diff_item_id.setdefault("relationship_entity", set()).update(diff_ids)
        # parameter_value
        item_list = self.query(self.ParameterValue.id).filter(self.ParameterValue.entity_id.in_(ids))
        diff_item_list = self.query(self.DiffParameterValue.id).filter(
            self.DiffParameterValue.entity_id.in_(ids + diff_ids)
        )
        self._collect_parameter_value_cascading_ids(
            [x.id for x in item_list], [x.id for x in diff_item_list], item_id, diff_item_id
        )

    def _collect_parameter_definition_cascading_ids(self, ids, diff_ids, item_id, diff_item_id):
        """Find out which items need to be removed due to the removal of parameter definitions
        given by `ids` and `diff_ids`,
        and add their ids to `item_id` and `diff_item_id`."""
        # Touch
        item_id.setdefault("parameter_definition", set()).update(ids)
        diff_item_id.setdefault("parameter_definition", set()).update(diff_ids)
        # parameter_value
        item_list = self.query(self.ParameterValue.id).filter(self.ParameterValue.parameter_definition_id.in_(ids))
        diff_item_list = self.query(self.DiffParameterValue.id).filter(
            self.DiffParameterValue.parameter_definition_id.in_(ids + diff_ids)
        )
        self._collect_parameter_value_cascading_ids(
            [x.id for x in item_list], [x.id for x in diff_item_list], item_id, diff_item_id
        )
        # parameter_definition_tag
        item_list = self.query(self.ParameterDefinitionTag.id).filter(
            self.ParameterDefinitionTag.parameter_definition_id.in_(ids)
        )
        diff_item_list = self.query(self.DiffParameterDefinitionTag.id).filter(
            self.DiffParameterDefinitionTag.parameter_definition_id.in_(ids + diff_ids)
        )
        self._collect_parameter_definition_tag_cascading_ids(
            [x.id for x in item_list], [x.id for x in diff_item_list], item_id, diff_item_id
        )

    def _collect_parameter_value_cascading_ids(self, ids, diff_ids, item_id, diff_item_id):
        """Find out which items need to be removed due to the removal of parameter values
        given by `ids` and `diff_ids`,
        and add their ids to `item_id` and `diff_item_id`."""
        item_id.setdefault("parameter_value", set()).update(ids)
        diff_item_id.setdefault("parameter_value", set()).update(diff_ids)

    def _collect_parameter_tag_cascading_ids(self, ids, diff_ids, item_id, diff_item_id):
        """Find out which items need to be removed due to the removal of parameter tags
        given by `ids` and `diff_ids`,
        and add their ids to `item_id` and `diff_item_id`."""
        # Touch
        item_id.setdefault("parameter_tag", set()).update(ids)
        diff_item_id.setdefault("parameter_tag", set()).update(diff_ids)
        # parameter_definition_tag
        item_list = self.query(self.ParameterDefinitionTag.id).filter(
            self.ParameterDefinitionTag.parameter_tag_id.in_(ids)
        )
        diff_item_list = self.query(self.DiffParameterDefinitionTag.id).filter(
            self.DiffParameterDefinitionTag.parameter_tag_id.in_(ids + diff_ids)
        )
        self._collect_parameter_definition_tag_cascading_ids(
            [x.id for x in item_list], [x.id for x in diff_item_list], item_id, diff_item_id
        )

    def _collect_parameter_definition_tag_cascading_ids(self, ids, diff_ids, item_id, diff_item_id):
        """Find out which items need to be removed due to the removal of parameter definition tag pairs
        given by `ids` and `diff_ids`,
        and add their ids to `item_id` and `diff_item_id`."""
        item_id.setdefault("parameter_definition_tag", set()).update(ids)
        diff_item_id.setdefault("parameter_definition_tag", set()).update(diff_ids)

    def _collect_parameter_value_list_cascading_ids(self, ids, diff_ids, item_id, diff_item_id):
        """Find out which items need to be removed due to the removal of parameter value lists
        given by `ids` and `diff_ids`,
        and add their ids to `item_id` and `diff_item_id`.
        TODO: Should we remove parameter definitions here? Set their parameter_value_list_id to NULL?
        """
        item_id.setdefault("parameter_value_list", set()).update(ids)
        diff_item_id.setdefault("parameter_value_list", set()).update(diff_ids)
