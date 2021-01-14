######################################################################################################################
# Copyright (C) 2017-2021 Spine project consortium
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

    # pylint: disable=redefined-builtin
    def cascade_remove_items(self, **kwargs):
        """Removes items by id in cascade.

        Args:
            **kwargs: keyword is table name, argument is list of ids to remove
        """
        cascading_ids = self.cascading_ids(**kwargs)
        self.remove_items(**cascading_ids)

    def remove_items(self, **kwargs):
        """Removes items by id, *not in cascade*.

        Args:
            **kwargs: keyword is table name, argument is list of ids to remove
        """
        for tablename, ids in kwargs.items():
            table_id = self.table_ids.get(tablename, "id")
            diff_table = self._diff_table(tablename)
            delete = diff_table.delete().where(self.in_(getattr(diff_table.c, table_id), ids))
            try:
                self.connection.execute(delete)
            except DBAPIError as e:
                msg = f"DBAPIError while removing {tablename} items: {e.orig.args}"
                raise SpineDBAPIError(msg)
        for tablename, ids in kwargs.items():
            self.added_item_id[tablename].difference_update(ids)
            self.updated_item_id[tablename].difference_update(ids)
            self.removed_item_id[tablename].update(ids)
            self._mark_as_dirty(tablename, ids)

    # pylint: disable=redefined-builtin
    def cascading_ids(self, **kwargs):
        """Returns cascading ids.

        Keyword args:
            <tablename> (set): set of ids to be removed for table

        Returns:
            cascading_ids (dict): cascading ids keyed by table name
        """
        ids = {}
        self._merge(ids, self._object_class_cascading_ids(kwargs.get("object_class", set())))
        self._merge(ids, self._object_cascading_ids(kwargs.get("object", set())))
        self._merge(ids, self._relationship_class_cascading_ids(kwargs.get("relationship_class", set())))
        self._merge(ids, self._relationship_cascading_ids(kwargs.get("relationship", set())))
        self._merge(ids, self._entity_group_cascading_ids(kwargs.get("entity_group", set())))
        self._merge(ids, self._parameter_definition_cascading_ids(kwargs.get("parameter_definition", set())))
        self._merge(ids, self._parameter_value_cascading_ids(kwargs.get("parameter_value", set())))
        self._merge(ids, self._parameter_tag_cascading_ids(kwargs.get("parameter_tag", set())))
        self._merge(ids, self._parameter_definition_tag_cascading_ids(kwargs.get("parameter_definition_tag", set())))
        self._merge(ids, self._parameter_value_list_cascading_ids(kwargs.get("parameter_value_list", set())))
        self._merge(ids, self._alternative_cascading_ids(kwargs.get("alternative", set())))
        self._merge(ids, self._scenario_cascading_ids(kwargs.get("scenario", set())))
        self._merge(ids, self._scenario_alternatives_cascading_ids(kwargs.get("scenario_alternative", set())))
        self._merge(ids, self._feature_cascading_ids(kwargs.get("feature", set())))
        self._merge(ids, self._tool_cascading_ids(kwargs.get("tool", set())))
        self._merge(ids, self._tool_feature_cascading_ids(kwargs.get("tool_feature", set())))
        self._merge(ids, self._tool_feature_method_cascading_ids(kwargs.get("tool_feature_method", set())))
        return {key: value for key, value in ids.items() if value}

    @staticmethod
    def _merge(left, right):
        for tablename, ids in right.items():
            left.setdefault(tablename, set()).update(ids)

    def _alternative_cascading_ids(self, ids):
        """Returns alternative cascading ids."""
        cascading_ids = {"alternative": ids.copy()}
        parameter_values = self.query(self.parameter_value_sq.c.id).filter(
            self.in_(self.parameter_value_sq.c.alternative_id, ids)
        )
        scenario_alternatives = self.query(self.scenario_alternative_sq.c.id).filter(
            self.in_(self.scenario_alternative_sq.c.alternative_id, ids)
        )
        self._merge(cascading_ids, self._parameter_value_cascading_ids({x.id for x in parameter_values}))
        self._merge(cascading_ids, self._scenario_alternatives_cascading_ids({x.id for x in scenario_alternatives}))
        return cascading_ids

    def _scenario_cascading_ids(self, ids):
        cascading_ids = {"scenario": ids.copy()}
        scenario_alternatives = self.query(self.scenario_alternative_sq.c.id).filter(
            self.in_(self.scenario_alternative_sq.c.scenario_id, ids)
        )
        self._merge(cascading_ids, self._scenario_alternatives_cascading_ids({x.id for x in scenario_alternatives}))
        return cascading_ids

    def _object_class_cascading_ids(self, ids):
        """Returns object class cascading ids."""
        cascading_ids = {"entity_class": ids.copy(), "object_class": ids.copy()}
        objects = self.query(self.object_sq.c.id).filter(self.in_(self.object_sq.c.class_id, ids))
        relationship_classes = self.query(self.relationship_class_sq.c.id).filter(
            self.in_(self.relationship_class_sq.c.object_class_id, ids)
        )
        paramerer_definitions = self.query(self.parameter_definition_sq.c.id).filter(
            self.in_(self.parameter_definition_sq.c.object_class_id, ids)
        )
        self._merge(cascading_ids, self._object_cascading_ids({x.id for x in objects}))
        self._merge(cascading_ids, self._relationship_class_cascading_ids({x.id for x in relationship_classes}))
        self._merge(cascading_ids, self._parameter_definition_cascading_ids({x.id for x in paramerer_definitions}))
        return cascading_ids

    def _object_cascading_ids(self, ids):
        """Returns object cascading ids."""
        cascading_ids = {"entity": ids.copy(), "object": ids.copy()}
        relationships = self.query(self.relationship_sq.c.id).filter(self.in_(self.relationship_sq.c.object_id, ids))
        parameter_values = self.query(self.parameter_value_sq.c.id).filter(
            self.in_(self.parameter_value_sq.c.object_id, ids)
        )
        # TODO: try to use `or_` here
        group_entities = self.query(self.entity_group_sq.c.id).filter(self.in_(self.entity_group_sq.c.entity_id, ids))
        member_entities = self.query(self.entity_group_sq.c.id).filter(self.in_(self.entity_group_sq.c.member_id, ids))
        self._merge(cascading_ids, self._relationship_cascading_ids({x.id for x in relationships}))
        self._merge(cascading_ids, self._parameter_value_cascading_ids({x.id for x in parameter_values}))
        self._merge(cascading_ids, self._entity_group_cascading_ids({x.id for x in group_entities}))
        self._merge(cascading_ids, self._entity_group_cascading_ids({x.id for x in member_entities}))
        return cascading_ids

    def _relationship_class_cascading_ids(self, ids):
        """Returns relationship class cascading ids."""
        cascading_ids = {
            "relationship_class": ids.copy(),
            "relationship_entity_class": ids.copy(),
            "entity_class": ids.copy(),
        }
        relationships = self.query(self.relationship_sq.c.id).filter(self.in_(self.relationship_sq.c.class_id, ids))
        paramerer_definitions = self.query(self.parameter_definition_sq.c.id).filter(
            self.in_(self.parameter_definition_sq.c.relationship_class_id, ids)
        )
        self._merge(cascading_ids, self._relationship_cascading_ids({x.id for x in relationships}))
        self._merge(cascading_ids, self._parameter_definition_cascading_ids({x.id for x in paramerer_definitions}))
        return cascading_ids

    def _relationship_cascading_ids(self, ids):
        """Returns relationship cascading ids."""
        cascading_ids = {"relationship": ids.copy(), "entity": ids.copy(), "relationship_entity": ids.copy()}
        parameter_values = self.query(self.parameter_value_sq.c.id).filter(
            self.in_(self.parameter_value_sq.c.relationship_id, ids)
        )
        # TODO: try to use `or_` here
        group_entities = self.query(self.entity_group_sq.c.id).filter(self.in_(self.entity_group_sq.c.entity_id, ids))
        member_entities = self.query(self.entity_group_sq.c.id).filter(self.in_(self.entity_group_sq.c.member_id, ids))
        self._merge(cascading_ids, self._parameter_value_cascading_ids({x.id for x in parameter_values}))
        self._merge(cascading_ids, self._entity_group_cascading_ids({x.id for x in group_entities}))
        self._merge(cascading_ids, self._entity_group_cascading_ids({x.id for x in member_entities}))
        return cascading_ids

    def _entity_group_cascading_ids(self, ids):  # pylint: disable=no-self-use
        """Returns entity group cascading ids."""
        return {"entity_group": ids.copy()}

    def _parameter_definition_cascading_ids(self, ids):
        """Returns parameter definition cascading ids."""
        cascading_ids = {"parameter_definition": ids.copy()}
        parameter_values = self.query(self.parameter_value_sq.c.id).filter(
            self.in_(self.parameter_value_sq.c.parameter_definition_id, ids)
        )
        param_def_tags = self.query(self.parameter_definition_tag_sq.c.id).filter(
            self.in_(self.parameter_definition_tag_sq.c.parameter_definition_id, ids)
        )
        features = self.query(self.feature_sq.c.id).filter(self.in_(self.feature_sq.c.parameter_definition_id, ids))
        self._merge(cascading_ids, self._parameter_value_cascading_ids({x.id for x in parameter_values}))
        self._merge(cascading_ids, self._parameter_definition_tag_cascading_ids({x.id for x in param_def_tags}))
        self._merge(cascading_ids, self._feature_cascading_ids({x.id for x in features}))
        return cascading_ids

    def _parameter_value_cascading_ids(self, ids):  # pylint: disable=no-self-use
        """Returns parameter value cascading ids."""
        return {"parameter_value": ids.copy()}

    def _parameter_tag_cascading_ids(self, ids):
        """Returns parameter tag cascading ids."""
        cascading_ids = {"parameter_tag": ids.copy()}
        # parameter_definition_tag
        param_def_tags = self.query(self.parameter_definition_tag_sq.c.id).filter(
            self.in_(self.parameter_definition_tag_sq.c.parameter_tag_id, ids)
        )
        self._merge(cascading_ids, self._parameter_definition_tag_cascading_ids({x.id for x in param_def_tags}))
        return cascading_ids

    def _parameter_definition_tag_cascading_ids(self, ids):  # pylint: disable=no-self-use
        """Returns parameter definition tag cascading ids."""
        return {"parameter_definition_tag": ids.copy()}

    def _parameter_value_list_cascading_ids(self, ids):  # pylint: disable=no-self-use
        """Returns parameter value list cascading ids and adds them to the given dictionaries.
        """
        cascading_ids = {"parameter_value_list": ids.copy()}
        features = self.query(self.feature_sq.c.id).filter(self.in_(self.feature_sq.c.parameter_value_list_id, ids))
        self._merge(cascading_ids, self._feature_cascading_ids({x.id for x in features}))
        return cascading_ids

    def _scenario_alternatives_cascading_ids(self, ids):
        return {"scenario_alternative": ids.copy()}

    def _feature_cascading_ids(self, ids):
        cascading_ids = {"feature": ids.copy()}
        tool_features = self.query(self.tool_feature_sq.c.id).filter(self.in_(self.tool_feature_sq.c.feature_id, ids))
        self._merge(cascading_ids, self._tool_feature_cascading_ids({x.id for x in tool_features}))
        return cascading_ids

    def _tool_cascading_ids(self, ids):
        cascading_ids = {"tool": ids.copy()}
        tool_features = self.query(self.tool_feature_sq.c.id).filter(self.in_(self.tool_feature_sq.c.tool_id, ids))
        self._merge(cascading_ids, self._tool_feature_cascading_ids({x.id for x in tool_features}))
        return cascading_ids

    def _tool_feature_cascading_ids(self, ids):
        cascading_ids = {"tool_feature": ids.copy()}
        tool_feature_methods = self.query(self.tool_feature_method_sq.c.id).filter(
            self.in_(self.tool_feature_method_sq.c.tool_feature_id, ids)
        )
        self._merge(cascading_ids, self._tool_feature_method_cascading_ids({x.id for x in tool_feature_methods}))
        return cascading_ids

    def _tool_feature_method_cascading_ids(self, ids):
        return {"tool_feature_method": ids.copy()}
