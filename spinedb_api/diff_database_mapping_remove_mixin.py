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
        alternative_ids=(),
        scenario_ids=(),
        scenario_alternative_ids=(),
    ):
        """Removes items by id."""
        cascading_ids = self._cascading_ids(
            object_class_ids=object_class_ids,
            object_ids=object_ids,
            relationship_class_ids=relationship_class_ids,
            relationship_ids=relationship_ids,
            parameter_definition_ids=parameter_definition_ids,
            parameter_value_ids=parameter_value_ids,
            parameter_tag_ids=parameter_tag_ids,
            parameter_definition_tag_ids=parameter_definition_tag_ids,
            parameter_value_list_ids=parameter_value_list_ids,
            alternative_ids=alternative_ids,
            scenario_ids=scenario_ids,
            scenario_alternative_ids=scenario_alternative_ids,
        )
        try:
            for tablename, ids in cascading_ids.items():
                table_id = self.table_ids.get(tablename, "id")
                classname = self.table_to_class[tablename]
                diff_class = getattr(self, "Diff" + classname)
                self.query(diff_class).filter(self.in_(getattr(diff_class, table_id), ids)).delete(
                    synchronize_session=False
                )
            self.session.commit()
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while removing items: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)
        for tablename, ids in cascading_ids.items():
            self.added_item_id[tablename].difference_update(ids)
            self.updated_item_id[tablename].difference_update(ids)
            self.removed_item_id[tablename].update(ids)
            self._mark_as_dirty(tablename, ids)

    def _cascading_ids(
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
        alternative_ids=(),
        scenario_ids=(),
        scenario_alternative_ids=(),
    ):
        """Returns cascading ids.

        Returns:
            cascading_ids (dict): cascading ids keyed by table name
        """
        cascading_ids = {}
        self._merge(cascading_ids, self._object_class_cascading_ids(set(object_class_ids)))
        self._merge(cascading_ids, self._object_cascading_ids(set(object_ids)))
        self._merge(cascading_ids, self._relationship_class_cascading_ids(set(relationship_class_ids)))
        self._merge(cascading_ids, self._relationship_cascading_ids(set(relationship_ids)))
        self._merge(cascading_ids, self._parameter_definition_cascading_ids(set(parameter_definition_ids)))
        self._merge(cascading_ids, self._parameter_value_cascading_ids(set(parameter_value_ids)))
        self._merge(cascading_ids, self._parameter_tag_cascading_ids(set(parameter_tag_ids)))
        self._merge(cascading_ids, self._parameter_definition_tag_cascading_ids(set(parameter_definition_tag_ids)))
        self._merge(cascading_ids, self._parameter_value_list_cascading_ids(set(parameter_value_list_ids)))
        self._merge(cascading_ids, self._alternative_cascading_ids(set(alternative_ids)))
        self._merge(cascading_ids, self._scenario_cascading_ids(set(scenario_ids)))
        self._merge(cascading_ids, self._scenario_alternatives_cascading_ids(set(scenario_alternative_ids)))
        return cascading_ids

    @staticmethod
    def _merge(left, right):
        for tablename, ids in right.items():
            left.setdefault(tablename, set()).update(ids)

    def _alternative_cascading_ids(self, ids):
        """Finds object class cascading ids and adds them to the given dictionaries."""
        cascading_ids = {"alternative": ids}
        parameter_values = self.query(self.parameter_value_sq.c.id).filter(self.in_(self.parameter_value_sq.c.id, ids))
        scenario_alternatives = self.query(self.scenario_alternatives_sq.c.id).filter(
            self.in_(self.scenario_alternatives_sq.c.alternative_id, ids)
        )
        self._merge(cascading_ids, self._parameter_value_cascading_ids({x.id for x in parameter_values}))
        self._merge(cascading_ids, self._scenario_alternatives_cascading_ids({x.id for x in scenario_alternatives}))
        return cascading_ids

    def _scenario_cascading_ids(self, ids):
        cascading_ids = {"scenario": ids}
        scenario_alternatives = self.query(self.scenario_alternatives_sq.c.id).filter(
            self.in_(self.scenario_alternatives_sq.c.scenario_id, ids)
        )
        self._merge(cascading_ids, self._scenario_alternatives_cascading_ids({x.id for x in scenario_alternatives}))
        return cascading_ids

    def _object_class_cascading_ids(self, ids):
        """Returns object class cascading ids."""
        cascading_ids = {"entity_class": ids, "object_class": ids}
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
        cascading_ids = {"entity": ids, "object": ids}
        relationships = self.query(self.relationship_sq.c.id).filter(self.in_(self.relationship_sq.c.object_id, ids))
        parameter_values = self.query(self.parameter_value_sq.c.id).filter(
            self.in_(self.parameter_value_sq.c.object_id, ids)
        )
        self._merge(cascading_ids, self._relationship_cascading_ids({x.id for x in relationships}))
        self._merge(cascading_ids, self._parameter_value_cascading_ids({x.id for x in parameter_values}))
        return cascading_ids

    def _relationship_class_cascading_ids(self, ids):
        """Returns relationship class cascading ids."""
        cascading_ids = {"relationship_class": ids, "relationship_entity_class": ids, "entity_class": ids}
        relationships = self.query(self.relationship_sq.c.id).filter(self.in_(self.relationship_sq.c.class_id, ids))
        paramerer_definitions = self.query(self.parameter_definition_sq.c.id).filter(
            self.in_(self.parameter_definition_sq.c.relationship_class_id, ids)
        )
        self._merge(cascading_ids, self._relationship_cascading_ids({x.id for x in relationships}))
        self._merge(cascading_ids, self._parameter_definition_cascading_ids({x.id for x in paramerer_definitions}))
        return cascading_ids

    def _relationship_cascading_ids(self, ids):
        """Returns relationship cascading ids."""
        cascading_ids = {"relationship": ids, "entity": ids, "relationship_entity": ids}
        parameter_values = self.query(self.parameter_value_sq.c.id).filter(
            self.in_(self.parameter_value_sq.c.relationship_id, ids)
        )
        self._merge(cascading_ids, self._parameter_value_cascading_ids({x.id for x in parameter_values}))
        return cascading_ids

    def _parameter_definition_cascading_ids(self, ids):
        """Returns parameter definition cascading ids."""
        cascading_ids = {"parameter_definition": ids}
        parameter_values = self.query(self.parameter_value_sq.c.id).filter(
            self.in_(self.parameter_value_sq.c.parameter_definition_id, ids)
        )
        param_def_tags = self.query(self.parameter_definition_tag_sq.c.id).filter(
            self.in_(self.parameter_definition_tag_sq.c.parameter_definition_id, ids)
        )
        self._merge(cascading_ids, self._parameter_value_cascading_ids({x.id for x in parameter_values}))
        self._merge(cascading_ids, self._parameter_definition_tag_cascading_ids({x.id for x in param_def_tags}))
        return cascading_ids

    def _parameter_value_cascading_ids(self, ids):  # pylint: disable=no-self-use
        """Returns parameter value cascading ids."""
        return {"parameter_value": ids}

    def _parameter_tag_cascading_ids(self, ids):
        """Returns parameter tag cascading ids."""
        cascading_ids = {"parameter_tag": ids}
        # parameter_definition_tag
        param_def_tags = self.query(self.parameter_definition_tag_sq.c.id).filter(
            self.in_(self.parameter_definition_tag_sq.c.parameter_tag_id, ids)
        )
        self._merge(cascading_ids, self._parameter_definition_tag_cascading_ids({x.id for x in param_def_tags}))
        return cascading_ids

    def _parameter_definition_tag_cascading_ids(self, ids):  # pylint: disable=no-self-use
        """Returns parameter definition tag cascading ids."""
        return {"parameter_definition_tag": ids}

    def _parameter_value_list_cascading_ids(self, ids):  # pylint: disable=no-self-use
        """Returns parameter value list cascading ids and adds them to the given dictionaries.
        TODO: Should we remove parameter definitions here? Set their parameter_value_list_id to NULL?
        """
        return {"parameter_value_list": ids}

    def _scenario_alternatives_cascading_ids(self, ids):
        return {"scenario_alternative": ids}
