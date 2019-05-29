#############################################################################
# Copyright (C) 2017 - 2018 VTT Technical Research Centre of Finland
#
# This file is part of Spine Database API.
#
# Spine Spine Database API is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#############################################################################

"""
A class to handle DELETE operations onto a Spine db 'diff' ORM.

:author: Manuel Marin (KTH)
:date:   11.8.2018
"""

from sqlalchemy.exc import DBAPIError
from .exception import SpineDBAPIError


# TODO: improve docstrings


class DiffDatabaseMappingRemoveMixin:
    """A mixin to handle DELETE operations onto a Spine db 'diff' ORM."""

    def __init__(self, *args, **kwargs):
        """Initialize class."""
        super().__init__(*args, **kwargs)

    def remove_items(
        self,
        object_class_ids=set(),
        object_ids=set(),
        relationship_class_ids=set(),
        relationship_ids=set(),
        parameter_ids=set(),
        parameter_value_ids=set(),
        parameter_tag_ids=set(),
        parameter_definition_tag_ids=set(),
        parameter_value_list_ids=set(),
    ):
        """Remove items by id."""
        removed_item_id, removed_diff_item_id = self._removed_items(
            object_class_ids=object_class_ids,
            object_ids=object_ids,
            relationship_class_ids=relationship_class_ids,
            relationship_ids=relationship_ids,
            parameter_definition_ids=parameter_ids,
            parameter_value_ids=parameter_value_ids,
            parameter_tag_ids=parameter_tag_ids,
            parameter_definition_tag_ids=parameter_definition_tag_ids,
            parameter_value_list_ids=parameter_value_list_ids,
        )
        try:
            for tablename, ids in removed_diff_item_id.items():
                classname = self.table_to_class[tablename]
                diff_class = getattr(self, "Diff" + classname)
                self.query(diff_class).filter(diff_class.id.in_(ids)).delete(
                    synchronize_session=False
                )
            self.session.commit()
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while removing items: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)
        for tablename, ids in removed_item_id.items():
            self.removed_item_id[tablename].update(ids)
            self.mark_as_dirty(tablename, ids)

    def _removed_items(
        self,
        object_class_ids=set(),
        object_ids=set(),
        relationship_class_ids=set(),
        relationship_ids=set(),
        parameter_definition_ids=set(),
        parameter_value_ids=set(),
        parameter_tag_ids=set(),
        parameter_definition_tag_ids=set(),
        parameter_value_list_ids=set(),
    ):
        """Find out which items need to be removed by the removal of items corresponding to given ids.

        Returns:
            removed_item_id (dict): removed items in the original tables
            removed_diff_item_id (dict): removed items in the difference tables
        """
        removed_item_id = {}
        removed_diff_item_id = {}
        # object_class
        item_list = self.query(self.ObjectClass.id).filter(
            self.ObjectClass.id.in_(object_class_ids)
        )
        diff_item_list = self.query(self.DiffObjectClass.id).filter(
            self.DiffObjectClass.id.in_(object_class_ids)
        )
        self._remove_cascade_object_classes(
            [x.id for x in item_list],
            [x.id for x in diff_item_list],
            removed_item_id,
            removed_diff_item_id,
        )
        # object
        item_list = self.query(self.Object.id).filter(self.Object.id.in_(object_ids))
        diff_item_list = self.query(self.DiffObject.id).filter(
            self.DiffObject.id.in_(object_ids)
        )
        self._remove_cascade_objects(
            [x.id for x in item_list],
            [x.id for x in diff_item_list],
            removed_item_id,
            removed_diff_item_id,
        )
        # relationship_class
        item_list = self.query(self.RelationshipClass.id).filter(
            self.RelationshipClass.id.in_(relationship_class_ids)
        )
        diff_item_list = self.query(self.DiffRelationshipClass.id).filter(
            self.DiffRelationshipClass.id.in_(relationship_class_ids)
        )
        self._remove_cascade_relationship_classes(
            [x.id for x in item_list],
            [x.id for x in diff_item_list],
            removed_item_id,
            removed_diff_item_id,
        )
        # relationship
        item_list = self.query(self.Relationship.id).filter(
            self.Relationship.id.in_(relationship_ids)
        )
        diff_item_list = self.query(self.DiffRelationship.id).filter(
            self.DiffRelationship.id.in_(relationship_ids)
        )
        self._remove_cascade_relationships(
            [x.id for x in item_list],
            [x.id for x in diff_item_list],
            removed_item_id,
            removed_diff_item_id,
        )
        # parameter
        item_list = self.query(self.ParameterDefinition.id).filter(
            self.ParameterDefinition.id.in_(parameter_definition_ids)
        )
        diff_item_list = self.query(self.DiffParameterDefinition.id).filter(
            self.DiffParameterDefinition.id.in_(parameter_definition_ids)
        )
        self._remove_cascade_parameter_definitions(
            [x.id for x in item_list],
            [x.id for x in diff_item_list],
            removed_item_id,
            removed_diff_item_id,
        )
        # parameter_value
        item_list = self.query(self.ParameterValue.id).filter(
            self.ParameterValue.id.in_(parameter_value_ids)
        )
        diff_item_list = self.query(self.DiffParameterValue.id).filter(
            self.DiffParameterValue.id.in_(parameter_value_ids)
        )
        self._remove_cascade_parameter_values(
            [x.id for x in item_list],
            [x.id for x in diff_item_list],
            removed_item_id,
            removed_diff_item_id,
        )
        # parameter_tag
        item_list = self.query(self.ParameterTag.id).filter(
            self.ParameterTag.id.in_(parameter_tag_ids)
        )
        diff_item_list = self.query(self.DiffParameterTag.id).filter(
            self.DiffParameterTag.id.in_(parameter_tag_ids)
        )
        self._remove_cascade_parameter_tags(
            [x.id for x in item_list],
            [x.id for x in diff_item_list],
            removed_item_id,
            removed_diff_item_id,
        )
        # parameter_definition_tag
        item_list = self.query(self.ParameterDefinitionTag.id).filter(
            self.ParameterDefinitionTag.id.in_(parameter_definition_tag_ids)
        )
        diff_item_list = self.query(self.DiffParameterDefinitionTag.id).filter(
            self.DiffParameterDefinitionTag.id.in_(parameter_definition_tag_ids)
        )
        self._remove_cascade_parameter_definition_tags(
            [x.id for x in item_list],
            [x.id for x in diff_item_list],
            removed_item_id,
            removed_diff_item_id,
        )
        # parameter_value_list
        item_list = self.query(self.ParameterValueList.id).filter(
            self.ParameterValueList.id.in_(parameter_value_list_ids)
        )
        diff_item_list = self.query(self.DiffParameterValueList.id).filter(
            self.DiffParameterValueList.id.in_(parameter_value_list_ids)
        )
        self._remove_cascade_parameter_value_lists(
            [x.id for x in item_list],
            [x.id for x in diff_item_list],
            removed_item_id,
            removed_diff_item_id,
        )
        return removed_item_id, removed_diff_item_id

    def _remove_cascade_object_classes(
        self, ids, diff_ids, removed_item_id, removed_diff_item_id
    ):
        """Find out which items need to be removed by the removal of object classes
        given by `ids` and `diff_ids`,
        and add their ids to `removed_item_id` and `removed_diff_item_id`."""
        # Touch
        removed_item_id.setdefault("object_class", set()).update(ids)
        removed_diff_item_id.setdefault("object_class", set()).update(diff_ids)
        # object
        item_list = self.query(self.Object.id).filter(self.Object.class_id.in_(ids))
        diff_item_list = self.query(self.DiffObject.id).filter(
            self.DiffObject.class_id.in_(ids + diff_ids)
        )
        self._remove_cascade_objects(
            [x.id for x in item_list],
            [x.id for x in diff_item_list],
            removed_item_id,
            removed_diff_item_id,
        )
        # relationship_class
        item_list = self.query(self.RelationshipClass.id).filter(
            self.RelationshipClass.object_class_id.in_(ids)
        )
        diff_item_list = self.query(self.DiffRelationshipClass.id).filter(
            self.DiffRelationshipClass.object_class_id.in_(ids + diff_ids)
        )
        self._remove_cascade_relationship_classes(
            [x.id for x in item_list],
            [x.id for x in diff_item_list],
            removed_item_id,
            removed_diff_item_id,
        )
        # parameter
        item_list = self.query(self.ParameterDefinition.id).filter(
            self.ParameterDefinition.object_class_id.in_(ids)
        )
        diff_item_list = self.query(self.DiffParameterDefinition.id).filter(
            self.DiffParameterDefinition.object_class_id.in_(ids + diff_ids)
        )
        self._remove_cascade_parameter_definitions(
            [x.id for x in item_list],
            [x.id for x in diff_item_list],
            removed_item_id,
            removed_diff_item_id,
        )

    def _remove_cascade_objects(
        self, ids, diff_ids, removed_item_id, removed_diff_item_id
    ):
        """Find out which items need to be removed by the removal of objects
        given by `ids` and `diff_ids`,
        and add their ids to `removed_item_id` and `removed_diff_item_id`."""
        # Touch
        removed_item_id.setdefault("object", set()).update(ids)
        removed_diff_item_id.setdefault("object", set()).update(diff_ids)
        # relationship
        item_list = self.query(self.Relationship.id).filter(
            self.Relationship.object_id.in_(ids)
        )
        diff_item_list = self.query(self.DiffRelationship.id).filter(
            self.DiffRelationship.object_id.in_(ids + diff_ids)
        )
        self._remove_cascade_relationships(
            [x.id for x in item_list],
            [x.id for x in diff_item_list],
            removed_item_id,
            removed_diff_item_id,
        )
        # parameter_value
        item_list = self.query(self.ParameterValue.id).filter(
            self.ParameterValue.object_id.in_(ids)
        )
        diff_item_list = self.query(self.DiffParameterValue.id).filter(
            self.DiffParameterValue.object_id.in_(ids + diff_ids)
        )
        self._remove_cascade_parameter_values(
            [x.id for x in item_list],
            [x.id for x in diff_item_list],
            removed_item_id,
            removed_diff_item_id,
        )

    def _remove_cascade_relationship_classes(
        self, ids, diff_ids, removed_item_id, removed_diff_item_id
    ):
        """Find out which items need to be removed by the removal of relationship classes
        given by `ids` and `diff_ids`,
        and add their ids to `removed_item_id` and `removed_diff_item_id`."""
        # Touch
        removed_item_id.setdefault("relationship_class", set()).update(ids)
        removed_diff_item_id.setdefault("relationship_class", set()).update(diff_ids)
        # relationship
        item_list = self.query(self.Relationship.id).filter(
            self.Relationship.class_id.in_(ids)
        )
        diff_item_list = self.query(self.DiffRelationship.id).filter(
            self.DiffRelationship.class_id.in_(ids + diff_ids)
        )
        self._remove_cascade_relationships(
            [x.id for x in item_list],
            [x.id for x in diff_item_list],
            removed_item_id,
            removed_diff_item_id,
        )
        # parameter
        item_list = self.query(self.ParameterDefinition.id).filter(
            self.ParameterDefinition.relationship_class_id.in_(ids)
        )
        diff_item_list = self.query(self.DiffParameterDefinition.id).filter(
            self.DiffParameterDefinition.relationship_class_id.in_(ids + diff_ids)
        )
        self._remove_cascade_parameter_definitions(
            [x.id for x in item_list],
            [x.id for x in diff_item_list],
            removed_item_id,
            removed_diff_item_id,
        )

    def _remove_cascade_relationships(
        self, ids, diff_ids, removed_item_id, removed_diff_item_id
    ):
        """Find out which items need to be removed by the removal of relationships
        given by `ids` and `diff_ids`,
        and add their ids to `removed_item_id` and `removed_diff_item_id`."""
        # Touch
        removed_item_id.setdefault("relationship", set()).update(ids)
        removed_diff_item_id.setdefault("relationship", set()).update(diff_ids)
        # parameter_value
        item_list = self.query(self.ParameterValue.id).filter(
            self.ParameterValue.relationship_id.in_(ids)
        )
        diff_item_list = self.query(self.DiffParameterValue.id).filter(
            self.DiffParameterValue.relationship_id.in_(ids + diff_ids)
        )
        self._remove_cascade_parameter_values(
            [x.id for x in item_list],
            [x.id for x in diff_item_list],
            removed_item_id,
            removed_diff_item_id,
        )

    def _remove_cascade_parameter_definitions(
        self, ids, diff_ids, removed_item_id, removed_diff_item_id
    ):
        """Find out which items need to be removed by the removal of parameter definitions
        given by `ids` and `diff_ids`,
        and add their ids to `removed_item_id` and `removed_diff_item_id`."""
        # Touch
        removed_item_id.setdefault("parameter_definition", set()).update(ids)
        removed_diff_item_id.setdefault("parameter_definition", set()).update(diff_ids)
        # parameter_value
        item_list = self.query(self.ParameterValue.id).filter(
            self.ParameterValue.parameter_definition_id.in_(ids)
        )
        diff_item_list = self.query(self.DiffParameterValue.id).filter(
            self.DiffParameterValue.parameter_definition_id.in_(ids + diff_ids)
        )
        self._remove_cascade_parameter_values(
            [x.id for x in item_list],
            [x.id for x in diff_item_list],
            removed_item_id,
            removed_diff_item_id,
        )
        # parameter_definition_tag
        item_list = self.query(self.ParameterDefinitionTag.id).filter(
            self.ParameterDefinitionTag.parameter_definition_id.in_(ids)
        )
        diff_item_list = self.query(self.DiffParameterDefinitionTag.id).filter(
            self.DiffParameterDefinitionTag.parameter_definition_id.in_(ids + diff_ids)
        )
        self._remove_cascade_parameter_definition_tags(
            [x.id for x in item_list],
            [x.id for x in diff_item_list],
            removed_item_id,
            removed_diff_item_id,
        )

    def _remove_cascade_parameter_values(
        self, ids, diff_ids, removed_item_id, removed_diff_item_id
    ):
        """Find out which items need to be removed by the removal of parameter values
        given by `ids` and `diff_ids`,
        and add their ids to `removed_item_id` and `removed_diff_item_id`."""
        removed_item_id.setdefault("parameter_value", set()).update(ids)
        removed_diff_item_id.setdefault("parameter_value", set()).update(diff_ids)

    def _remove_cascade_parameter_tags(
        self, ids, diff_ids, removed_item_id, removed_diff_item_id
    ):
        """Find out which items need to be removed by the removal of parameter tags
        given by `ids` and `diff_ids`,
        and add their ids to `removed_item_id` and `removed_diff_item_id`."""
        # Touch
        removed_item_id.setdefault("parameter_tag", set()).update(ids)
        removed_diff_item_id.setdefault("parameter_tag", set()).update(diff_ids)
        # parameter_definition_tag
        item_list = self.query(self.ParameterDefinitionTag.id).filter(
            self.ParameterDefinitionTag.parameter_tag_id.in_(ids)
        )
        diff_item_list = self.query(self.DiffParameterDefinitionTag.id).filter(
            self.DiffParameterDefinitionTag.parameter_tag_id.in_(ids + diff_ids)
        )
        self._remove_cascade_parameter_definition_tags(
            [x.id for x in item_list],
            [x.id for x in diff_item_list],
            removed_item_id,
            removed_diff_item_id,
        )

    def _remove_cascade_parameter_definition_tags(
        self, ids, diff_ids, removed_item_id, removed_diff_item_id
    ):
        """Find out which items need to be removed by the removal of parameter definition tag pairs
        given by `ids` and `diff_ids`,
        and add their ids to `removed_item_id` and `removed_diff_item_id`."""
        removed_item_id.setdefault("parameter_definition_tag", set()).update(ids)
        removed_diff_item_id.setdefault("parameter_definition_tag", set()).update(
            diff_ids
        )

    def _remove_cascade_parameter_value_lists(
        self, ids, diff_ids, removed_item_id, removed_diff_item_id
    ):
        """Find out which items need to be removed by the removal of parameter value lists
        given by `ids` and `diff_ids`,
        and add their ids to `removed_item_id` and `removed_diff_item_id`.
        TODO: Should we remove parameter definitions here? Set their parameter_value_list_id to NULL?
        """
        removed_item_id.setdefault("parameter_value_list", set()).update(ids)
        removed_diff_item_id.setdefault("parameter_value_list", set()).update(diff_ids)
