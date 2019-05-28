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
A class to handle COMMIT and ROLLBACK operations onto a Spine db 'diff' ORM.

:author: Manuel Marin (KTH)
:date:   11.8.2018
"""

from sqlalchemy.exc import DBAPIError
from .exception import SpineDBAPIError
from .helpers import attr_dict
from datetime import datetime, timezone


# TODO: improve docstrings


class _DiffDatabaseMappingCommit:
    """A class to handle COMMIT and ROLLBACK operations onto a Spine db 'diff' ORM."""

    def __init__(self):
        """Initialize class."""
        super().__init__()

    def commit_session(self, comment):
        """Make differences into original tables and commit."""
        try:
            user = self.username
            date = datetime.now(timezone.utc)
            commit = self.Commit(comment=comment, date=date, user=user)
            self.session.add(commit)
            self.session.flush()
            n = 499  # Maximum number of sql variables
            # Remove removed
            removed_object_class_id = list(self.removed_item_id["object_class"])
            removed_object_id = list(self.removed_item_id["object"])
            removed_relationship_class_id = list(
                self.removed_item_id["relationship_class"]
            )
            removed_relationship_id = list(self.removed_item_id["relationship"])
            removed_parameter_definition_id = list(
                self.removed_item_id["parameter_definition"]
            )
            removed_parameter_value_id = list(self.removed_item_id["parameter_value"])
            removed_parameter_tag_id = list(self.removed_item_id["parameter_tag"])
            removed_parameter_definition_tag_id = list(
                self.removed_item_id["parameter_definition_tag"]
            )
            removed_parameter_value_list_id = list(
                self.removed_item_id["parameter_value_list"]
            )
            for i in range(0, len(removed_object_class_id), n):
                self.session.query(self.ObjectClass).filter(
                    self.ObjectClass.id.in_(removed_object_class_id[i : i + n])
                ).delete(synchronize_session=False)
            for i in range(0, len(removed_object_id), n):
                self.session.query(self.Object).filter(
                    self.Object.id.in_(removed_object_id[i : i + n])
                ).delete(synchronize_session=False)
            for i in range(0, len(removed_relationship_class_id), n):
                self.session.query(self.RelationshipClass).filter(
                    self.RelationshipClass.id.in_(
                        removed_relationship_class_id[i : i + n]
                    )
                ).delete(synchronize_session=False)
            for i in range(0, len(removed_relationship_id), n):
                self.session.query(self.Relationship).filter(
                    self.Relationship.id.in_(removed_relationship_id[i : i + n])
                ).delete(synchronize_session=False)
            for i in range(0, len(removed_parameter_definition_id), n):
                self.session.query(self.ParameterDefinition).filter(
                    self.ParameterDefinition.id.in_(
                        removed_parameter_definition_id[i : i + n]
                    )
                ).delete(synchronize_session=False)
            for i in range(0, len(removed_parameter_value_id), n):
                self.session.query(self.ParameterValue).filter(
                    self.ParameterValue.id.in_(removed_parameter_value_id[i : i + n])
                ).delete(synchronize_session=False)
            for i in range(0, len(removed_parameter_tag_id), n):
                self.session.query(self.ParameterTag).filter(
                    self.ParameterTag.id.in_(removed_parameter_tag_id[i : i + n])
                ).delete(synchronize_session=False)
            for i in range(0, len(removed_parameter_definition_tag_id), n):
                self.session.query(self.ParameterDefinitionTag).filter(
                    self.ParameterDefinitionTag.id.in_(
                        removed_parameter_definition_tag_id[i : i + n]
                    )
                ).delete(synchronize_session=False)
            for i in range(0, len(removed_parameter_value_list_id), n):
                self.session.query(self.ParameterValueList).filter(
                    self.ParameterValueList.id.in_(
                        removed_parameter_value_list_id[i : i + n]
                    )
                ).delete(synchronize_session=False)
            # Merge dirty
            dirty_object_class_id = list(self.dirty_item_id["object_class"])
            dirty_object_id = list(self.dirty_item_id["object"])
            dirty_relationship_class_id = list(self.dirty_item_id["relationship_class"])
            dirty_relationship_id = list(self.dirty_item_id["relationship"])
            dirty_parameter_id = list(self.dirty_item_id["parameter_definition"])
            dirty_parameter_value_id = list(self.dirty_item_id["parameter_value"])
            dirty_parameter_tag_id = list(self.dirty_item_id["parameter_tag"])
            dirty_parameter_definition_tag_id = list(
                self.dirty_item_id["parameter_definition_tag"]
            )
            dirty_parameter_value_list_id = list(
                self.dirty_item_id["parameter_value_list"]
            )
            dirty_items = {}
            for i in range(0, len(dirty_object_class_id), n):
                for item in self.session.query(self.DiffObjectClass).filter(
                    self.DiffObjectClass.id.in_(dirty_object_class_id[i : i + n])
                ):
                    kwargs = attr_dict(item)
                    kwargs["commit_id"] = commit.id
                    dirty_items.setdefault(self.ObjectClass, []).append(kwargs)
            for i in range(0, len(dirty_object_id), n):
                for item in self.session.query(self.DiffObject).filter(
                    self.DiffObject.id.in_(dirty_object_id[i : i + n])
                ):
                    kwargs = attr_dict(item)
                    kwargs["commit_id"] = commit.id
                    dirty_items.setdefault(self.Object, []).append(kwargs)
            for i in range(0, len(dirty_relationship_class_id), n):
                for item in self.session.query(self.DiffRelationshipClass).filter(
                    self.DiffRelationshipClass.id.in_(
                        dirty_relationship_class_id[i : i + n]
                    )
                ):
                    kwargs = attr_dict(item)
                    kwargs["commit_id"] = commit.id
                    dirty_items.setdefault(self.RelationshipClass, []).append(kwargs)
            for i in range(0, len(dirty_relationship_id), n):
                for item in self.session.query(self.DiffRelationship).filter(
                    self.DiffRelationship.id.in_(dirty_relationship_id[i : i + n])
                ):
                    kwargs = attr_dict(item)
                    kwargs["commit_id"] = commit.id
                    dirty_items.setdefault(self.Relationship, []).append(kwargs)
            for i in range(0, len(dirty_parameter_id), n):
                for item in self.session.query(self.DiffParameterDefinition).filter(
                    self.DiffParameterDefinition.id.in_(dirty_parameter_id[i : i + n])
                ):
                    kwargs = attr_dict(item)
                    kwargs["commit_id"] = commit.id
                    dirty_items.setdefault(self.ParameterDefinition, []).append(kwargs)
            for i in range(0, len(dirty_parameter_value_id), n):
                for item in self.session.query(self.DiffParameterValue).filter(
                    self.DiffParameterValue.id.in_(dirty_parameter_value_id[i : i + n])
                ):
                    kwargs = attr_dict(item)
                    kwargs["commit_id"] = commit.id
                    dirty_items.setdefault(self.ParameterValue, []).append(kwargs)
            for i in range(0, len(dirty_parameter_tag_id), n):
                for item in self.session.query(self.DiffParameterTag).filter(
                    self.DiffParameterTag.id.in_(dirty_parameter_tag_id[i : i + n])
                ):
                    kwargs = attr_dict(item)
                    kwargs["commit_id"] = commit.id
                    dirty_items.setdefault(self.ParameterTag, []).append(kwargs)
            for i in range(0, len(dirty_parameter_definition_tag_id), n):
                for item in self.session.query(self.DiffParameterDefinitionTag).filter(
                    self.DiffParameterDefinitionTag.id.in_(
                        dirty_parameter_definition_tag_id[i : i + n]
                    )
                ):
                    kwargs = attr_dict(item)
                    kwargs["commit_id"] = commit.id
                    dirty_items.setdefault(self.ParameterDefinitionTag, []).append(
                        kwargs
                    )
            for i in range(0, len(dirty_parameter_value_list_id), n):
                for item in self.session.query(self.DiffParameterValueList).filter(
                    self.DiffParameterValueList.id.in_(
                        dirty_parameter_value_list_id[i : i + n]
                    )
                ):
                    kwargs = attr_dict(item)
                    kwargs["commit_id"] = commit.id
                    dirty_items.setdefault(self.ParameterValueList, []).append(kwargs)
            self.session.flush()  # TODO: Check if this is needed
            # Bulk update
            for k, v in dirty_items.items():
                self.session.bulk_update_mappings(k, v)
            # Add new
            new_object_class_id = list(self.new_item_id["object_class"])
            new_object_id = list(self.new_item_id["object"])
            new_relationship_class_id = list(self.new_item_id["relationship_class"])
            new_relationship_id = list(self.new_item_id["relationship"])
            new_parameter_id = list(self.new_item_id["parameter_definition"])
            new_parameter_value_id = list(self.new_item_id["parameter_value"])
            new_parameter_tag_id = list(self.new_item_id["parameter_tag"])
            new_parameter_definition_tag_id = list(
                self.new_item_id["parameter_definition_tag"]
            )
            new_parameter_value_list_id = list(self.new_item_id["parameter_value_list"])
            new_items = {}
            for i in range(0, len(new_object_class_id), n):
                for item in self.session.query(self.DiffObjectClass).filter(
                    self.DiffObjectClass.id.in_(new_object_class_id[i : i + n])
                ):
                    kwargs = attr_dict(item)
                    kwargs["commit_id"] = commit.id
                    new_items.setdefault(self.ObjectClass, []).append(kwargs)
            for i in range(0, len(new_object_id), n):
                for item in self.session.query(self.DiffObject).filter(
                    self.DiffObject.id.in_(new_object_id[i : i + n])
                ):
                    kwargs = attr_dict(item)
                    kwargs["commit_id"] = commit.id
                    new_items.setdefault(self.Object, []).append(kwargs)
            for i in range(0, len(new_relationship_class_id), n):
                for item in self.session.query(self.DiffRelationshipClass).filter(
                    self.DiffRelationshipClass.id.in_(
                        new_relationship_class_id[i : i + n]
                    )
                ):
                    kwargs = attr_dict(item)
                    kwargs["commit_id"] = commit.id
                    new_items.setdefault(self.RelationshipClass, []).append(kwargs)
            for i in range(0, len(new_relationship_id), n):
                for item in self.session.query(self.DiffRelationship).filter(
                    self.DiffRelationship.id.in_(new_relationship_id[i : i + n])
                ):
                    kwargs = attr_dict(item)
                    kwargs["commit_id"] = commit.id
                    new_items.setdefault(self.Relationship, []).append(kwargs)
            for i in range(0, len(new_parameter_id), n):
                for item in self.session.query(self.DiffParameterDefinition).filter(
                    self.DiffParameterDefinition.id.in_(new_parameter_id[i : i + n])
                ):
                    kwargs = attr_dict(item)
                    kwargs["commit_id"] = commit.id
                    new_items.setdefault(self.ParameterDefinition, []).append(kwargs)
            for i in range(0, len(new_parameter_value_id), n):
                for item in self.session.query(self.DiffParameterValue).filter(
                    self.DiffParameterValue.id.in_(new_parameter_value_id[i : i + n])
                ):
                    kwargs = attr_dict(item)
                    kwargs["commit_id"] = commit.id
                    new_items.setdefault(self.ParameterValue, []).append(kwargs)
            for i in range(0, len(new_parameter_tag_id), n):
                for item in self.session.query(self.DiffParameterTag).filter(
                    self.DiffParameterTag.id.in_(new_parameter_tag_id[i : i + n])
                ):
                    kwargs = attr_dict(item)
                    kwargs["commit_id"] = commit.id
                    new_items.setdefault(self.ParameterTag, []).append(kwargs)
            for i in range(0, len(new_parameter_definition_tag_id), n):
                for item in self.session.query(self.DiffParameterDefinitionTag).filter(
                    self.DiffParameterDefinitionTag.id.in_(
                        new_parameter_definition_tag_id[i : i + n]
                    )
                ):
                    kwargs = attr_dict(item)
                    kwargs["commit_id"] = commit.id
                    new_items.setdefault(self.ParameterDefinitionTag, []).append(kwargs)
            for i in range(0, len(new_parameter_value_list_id), n):
                for item in self.session.query(self.DiffParameterValueList).filter(
                    self.DiffParameterValueList.id.in_(
                        new_parameter_value_list_id[i : i + n]
                    )
                ):
                    kwargs = attr_dict(item)
                    kwargs["commit_id"] = commit.id
                    new_items.setdefault(self.ParameterValueList, []).append(kwargs)
            # Bulk insert
            for k, v in new_items.items():
                self.session.bulk_insert_mappings(k, v)
            self.reset_diff_mapping()
            self.session.commit()
            self.init_diff_dicts()
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while commiting changes: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def rollback_session(self):
        """Clear all differences."""
        try:
            self.reset_diff_mapping()
            self.session.commit()
            self.init_diff_dicts()
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while rolling back changes: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)
