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
"""
Contains export mappings for database items such as entities, entity classes and parameter values.

:author: A. Soininen (VTT)
:date:   10.12.2020
"""

from copy import copy
from enum import auto, Enum, unique
from itertools import cycle, dropwhile, chain
import re
import json
from spinedb_api.parameter_value import convert_containers_to_maps, convert_map_to_dict, from_database, IndexedValue
from spinedb_api.export_mapping.group_functions import NoGroup
from spinedb_api.mapping import Mapping, Position, is_pivoted, unflatten


@unique
class ExportKey(Enum):
    ALTERNATIVE_LIST_INDEX = auto()
    ALTERNATIVE_NAME_LIST = auto()
    ALTERNATIVE_ROW_CACHE = auto()
    CLASS_ROW_CACHE = auto()
    ENTITY_ROW_CACHE = auto()
    FEATURE_ROW_CACHE = auto()
    OBJECT_CLASS_LIST_INDEX = auto()
    OBJECT_CLASS_NAME_LIST = auto()
    OBJECT_LIST_INDEX = auto()
    OBJECT_NAME_LIST = auto()
    PARAMETER_DEFINITION_ID = auto()
    PARAMETER_DEFINITION_ROW_CACHE = auto()
    PARAMETER_ROW_CACHE = auto()
    PARAMETER_VALUE_LIST_ROW_CACHE = auto()
    PARAMETER_VALUE_ROW_CACHE = auto()
    PARAMETER_VALUE_LOOKUP_CACHE = auto()
    PARAMETER_VALUE_TYPE = auto()
    EXPANDED_PARAMETER_CACHE = auto()
    SCENARIO_ID = auto()
    SCENARIO_ROW_CACHE = auto()
    TOOL_FEATURE_ROW_CACHE = auto()
    TOOL_FEATURE_METHOD_ROW_CACHE = auto()
    TOOL_ID = auto()


_ignored = object()
"""A sentinel object to tag ignored database rows."""


def check_validity(root_mapping):
    """Checks validity of a mapping hierarchy.

    To check validity of individual mappings withing the hierarchy, use :func:`Mapping.check_validity`.

    Args:
        root_mapping (Mapping): root mapping

    Returns:
        list of str: a list of issue descriptions
    """
    issues = list()
    flattened = root_mapping.flatten()
    non_title_mappings = [m for m in flattened if m.position != Position.table_name]
    if len(non_title_mappings) == 2 and is_pivoted(non_title_mappings[0].position):
        issues.append("First mapping cannot be pivoted")
    title_mappings = [m for m in flattened if m.position == Position.table_name]
    if len(title_mappings) > 1:
        issues.append("Only a single mapping can be the table name.")
    return issues


class ExportMapping(Mapping):

    _TITLE_SEP = ","

    def __init__(self, position, value=None, header="", filter_re="", group_fn=None):
        """
        Args:
            position (int or Position): column index or Position
            header (str, optional); A string column header that's yielt as 'first row', if not empty.
                The default is an empty string (so it's not yielt).
            filter_re (str, optional): A regular expression to filter the mapped values by
            group_fn (str, Optional): Only for topmost mappings. The name of one of our supported group functions,
                for aggregating values over repeated 'headers' (in tables with hidden elements).
                If None (the default), then no such aggregation is performed and 'headers' are just repeated as needed.
        """
        super().__init__(position, value=value)
        self._filter_re = ""
        self._group_fn = None
        self._ignorable = False
        self._unignorable_update_state = self._update_state
        self._unignorable_query = self._query
        self._unignorable_data = self._data
        self._unfiltered_query = self._query
        self.header = header
        self.filter_re = filter_re
        self.group_fn = group_fn

    @property
    def filter_re(self):
        return self._filter_re

    @filter_re.setter
    def filter_re(self, filter_re):
        self._filter_re = filter_re
        self._set_query_filtered()

    def _set_query_filtered(self):
        """
        Overrides ``self._query()`` so the output is filtered according to ``self.filter_re``
        """
        if not self._filter_re:
            self._query = self._unfiltered_query
            return
        self._query = self._filtered_query

    def _filtered_query(self, *args, **kwargs):
        for db_row in self._unfiltered_query(*args, **kwargs):
            if re.search(self._filter_re, self._data(db_row)):
                yield db_row

    def check_validity(self):
        """Checks if mapping is valid.

        Returns:
            list: a list of issues
        """
        issues = list()
        if self.child is None:
            is_effective_leaf = True
        else:
            is_effective_leaf = any(
                child.position in (Position.hidden, Position.table_name) for child in self.child.flatten()
            )
        if is_effective_leaf and is_pivoted(self.position):
            issues.append("Cannot be pivoted.")
        return issues

    def _update_state(self, state, db_row):
        """
        Modifies current sate.

        Args:
            state (dict): state instance to modify
            db_row (namedtuple): a database row
        """
        raise NotImplementedError()

    def _expand_state(self, state, db_map, fixed_state):
        """
        Expands a pivoted state.

        Args:
            state (dict): a state to expand
            db_map (DatabaseMappingBase): a database map
            fixed_state (dict): state for fixed items

        Returns:
            list of dict: expanded states
        """
        expanded = list()
        for db_row in self._query(db_map, state, fixed_state):
            expanded_state = copy(state)
            self._update_state(expanded_state, db_row)
            expanded.append(expanded_state)
        return expanded

    def has_title(self):
        """
        Returns True if this mapping or one of its children generates titles.

        Returns:
            bool: True if mappings generate titles, False otherwise
        """
        if self.position == Position.table_name:
            return True
        if self.child is not None:
            return self.child.has_title()
        return False

    def _data(self, db_row):
        """
        Extracts item's cell data from database row.

        Args:
            db_row (namedtuple): database row

        Returns:
            str: cell data
        """
        raise NotImplementedError()

    def _query(self, db_map, state, fixed_state):
        """
        Creates a database subquery.

        Args:
            db_map (DatabaseMappingBase): a database map
            state (dict): state for filtering
            fixed_state (dict): state that fixes current item

        Returns:
            Alias: a subquery
        """
        raise NotImplementedError()

    def replace_data(self, data):
        """
        Replaces the data generated by this item by user given data.

        If data is exhausted, it gets cycled again from the beginning.

        Args:
            data (Iterable): user data
        """
        data_iterator = cycle(data)
        self._data = lambda _: next(data_iterator)

    def rows(self, db_map, state, fixed_state):
        """
        Generates a row of non-pivoted data.

        Args:
            db_map (DatabaseMappingBase): a database map
            state (dict): state
            fixed_state (dict): state for fixed items

        Yields:
            dict: a mapping from column index to cell data
        """
        # Yield header if topmost mapping
        if self.parent is None:
            header = self.make_header()
            yield header
        if self.child is None:
            # Non-recursive case
            if self.position == Position.hidden:
                yield {}
                return
            for db_row in self._query(db_map, state, fixed_state):
                yield {self.position: self._data(db_row)}
            return
        # Recursive case
        for db_row in self._query(db_map, state, fixed_state):
            self._update_state(state, db_row)
            data = self._data(db_row)
            for child_row in self.child.rows(db_map, state, fixed_state):
                row = {self.position: data}
                row.update(child_row)
                yield row

    def make_header(self):
        """
        Generates the header recursively.

        Returns
            dict: a mapping from column index to string header
        """
        header = {self.position: self.header}
        if self.child is not None:
            child_header = self.child.make_header()
            header.update(child_header)
        return header

    def _ignorable_update_state(self, state, db_row):
        if db_row is _ignored:
            return
        self._unignorable_update_state(state, db_row)

    def _ignorable_data(self, db_row):
        if db_row is _ignored:
            return None
        return self._unignorable_data(db_row)

    def _ignorable_query(self, db_map, state, fixed_state):
        yielded = False
        for db_row in self._unignorable_query(db_map, state, fixed_state):
            yielded = True
            yield db_row
        if not yielded:
            yield _ignored

    def set_ignorable(self):
        """
        Sets mapping as ignorable.

        Mappings that are ignorable map to None if there is no other data to yield.
        This allows 'incomplete' rows if child mappings do not depend on the ignored mapping.
        """
        self._ignorable = True
        self._data = self._ignorable_data
        self._query = self._ignorable_query
        self._update_state = self._ignorable_update_state

    def title(self, db_map, state, fixed_state=None):
        """
        Generates title data.

        Args:
            db_map (DatabaseMappingBase): a database map
            state (dict): state
            fixed_state (dict, optional): state for fixed items

        Yields:
            tuple(str,dict): unique title, and associated 'title' state dictionary
        """
        if fixed_state is None:
            fixed_state = dict()
        titles = {}
        for title, title_state in self.make_titles(db_map, state, fixed_state):
            titles.setdefault(title, {}).update(title_state)
        yield from titles.items()

    def make_titles(self, db_map, state, fixed_state):
        """
        Generates titles recursively.

        Yields
            tuple(str,dict): title string and associated state dictionary
        """
        if self.child is None:
            # Non-recursive case
            if self.position is not Position.table_name:
                return ()
            for db_row in self._query(db_map, state, fixed_state):
                title = self._data(db_row)
                title_state = dict()
                self._update_state(title_state, db_row)
                yield title, title_state
            return
        # Recursive case
        for db_row in self._query(db_map, state, fixed_state):
            self._update_state(state, db_row)
            fixed_state = copy(fixed_state)
            self._update_state(fixed_state, db_row)
            child_titles = self.child.make_titles(db_map, state, fixed_state)
            if self.position is not Position.table_name:
                yield from child_titles
                continue
            title = self._data(db_row)
            title_state = dict()
            self._update_state(title_state, db_row)
            first_child_title = next(child_titles, None)
            if first_child_title is None:
                yield title, title_state
                continue
            for child_title, child_title_state in chain((first_child_title,), child_titles):
                final_title = title + self._TITLE_SEP + child_title
                final_title_state = copy(title_state)
                final_title_state.update(child_title_state)
                yield final_title, final_title_state

    def to_dict(self):
        """
        Serializes mapping into dict.

        Returns:
            dict: serialized mapping
        """
        mapping_dict = super().to_dict()
        if self._ignorable:
            mapping_dict["ignorable"] = True
        if self.header:
            mapping_dict["header"] = self.header
        if self.filter_re:
            mapping_dict["filter_re"] = self.filter_re
        if self.group_fn and self.group_fn != NoGroup.NAME:
            mapping_dict["group_fn"] = self.group_fn
        return mapping_dict

    @classmethod
    def reconstruct(cls, position, ignorable, mapping_dict):
        """
        Reconstructs mapping.

        Args:
            position (int or Position, optional): mapping's position
            ignorable (bool): ignorable flag
            mapping_dict (dict): serialized mapping

        Returns:
            Mapping: reconstructed mapping
        """
        value = mapping_dict.get("value")
        header = mapping_dict.get("header", "")
        filter_re = mapping_dict.get("filter_re", "")
        group_fn = mapping_dict.get("group_fn")
        mapping = cls(position, value=value, header=header, filter_re=filter_re, group_fn=group_fn)
        if ignorable:
            mapping.set_ignorable()
        return mapping


def drop_non_positioned_tail(root_mapping):
    """Makes a modified mapping hierarchy without hidden tail mappings.

    This enable pivot tables to work correctly in certain situations.

    Args:
        root_mapping (Mapping): root mapping

    Returns:
        Mapping: modified mapping hierarchy
    """
    mappings = root_mapping.flatten()
    return unflatten(reversed(list(dropwhile(lambda m: m.position == Position.hidden, reversed(mappings)))))


class FixedValueMapping(ExportMapping):
    """Always yields a fixed value.

    Can be used as the topmost mapping.

    """

    MAP_TYPE = "FixedValue"

    def __init__(self, position, value, header="", filter_re="", group_fn=None):
        """
        Args:
            position (int or Position, optional): mapping's position
            value (Any): value to yield
            header (str, optional); A string column header that's yielt as 'first row', if not empty.
                The default is an empty string (so it's not yielt).
            filter_re (str, optional): A regular expression to filter the mapped values by
            group_fn (str, Optional): Only for topmost mappings. The name of one of our supported group functions,
                for aggregating values over repeated 'headers' (in tables with hidden elements).
                If None (the default), then no such aggregation is performed and 'headers' are just repeated as needed.
        """
        super().__init__(position, value, header, filter_re, group_fn)

    def _data(self, db_row):
        # Will be replaced by base class constructor.
        raise NotImplementedError()

    def _update_state(self, state, db_row):
        return

    def _query(self, db_map, state, fixed_state):
        yield None


class ObjectClassMapping(ExportMapping):
    """Maps object classes.

    Can be used as the topmost mapping.
    """

    MAP_TYPE = "ObjectClass"

    def _update_state(self, state, db_row):
        state[ExportKey.CLASS_ROW_CACHE] = db_row

    def _data(self, db_row):
        return db_row.name

    def _query(self, db_map, state, fixed_state):
        if ExportKey.CLASS_ROW_CACHE not in fixed_state:
            return db_map.query(db_map.object_class_sq)
        return [fixed_state[ExportKey.CLASS_ROW_CACHE]]


class ObjectMapping(ExportMapping):
    """Maps objects.

    Cannot be used as the topmost mapping; one of the parents must be :class:`ObjectClassMapping`.
    """

    MAP_TYPE = "Object"

    def _data(self, db_row):
        return db_row.name

    def _update_state(self, state, db_row):
        state[ExportKey.ENTITY_ROW_CACHE] = db_row

    def _query(self, db_map, state, fixed_state):
        if ExportKey.ENTITY_ROW_CACHE not in fixed_state:
            return db_map.query(db_map.object_sq).filter_by(class_id=state[ExportKey.CLASS_ROW_CACHE].id)
        return [fixed_state[ExportKey.ENTITY_ROW_CACHE]]


class ObjectGroupMapping(ExportMapping):
    """Maps object groups.

    Cannot be used as the topmost mapping; must have :class:`ObjectClassMapping` and :class:`ObjectMapping` as parents.
    """

    MAP_TYPE = "ObjectGroup"

    def _data(self, db_row):
        return db_row.group_name

    def _update_state(self, state, db_row):
        return

    def _query(self, db_map, state, fixed_state):
        return db_map.query(db_map.ext_object_group_sq).filter_by(
            class_id=state[ExportKey.CLASS_ROW_CACHE].id, member_id=state[ExportKey.ENTITY_ROW_CACHE].id
        )


class RelationshipClassMapping(ExportMapping):
    """Maps relationships classes.

    Can be used as the topmost mapping.
    """

    MAP_TYPE = "RelationshipClass"

    def _update_state(self, state, db_row):
        state[ExportKey.CLASS_ROW_CACHE] = db_row
        state[ExportKey.OBJECT_CLASS_LIST_INDEX] = 0
        state[ExportKey.OBJECT_CLASS_NAME_LIST] = db_row.object_class_name_list.split(",")

    def _data(self, db_row):
        return db_row.name

    def _query(self, db_map, state, fixed_state):
        if ExportKey.CLASS_ROW_CACHE not in fixed_state:
            return db_map.query(db_map.wide_relationship_class_sq)
        return [fixed_state[ExportKey.CLASS_ROW_CACHE]]


class RelationshipClassObjectClassMapping(ExportMapping):
    """Maps relationship class object classes.

    Cannot be used as the topmost mapping; one of the parents must be :class:`RelationshipClassMapping`.
    """

    MAP_TYPE = "RelationshipClassObjectClass"

    def _update_state(self, state, db_row):
        try:
            state[ExportKey.OBJECT_CLASS_LIST_INDEX] += 1
        except KeyError:
            # Could happen when forming title states.
            # Since only the mappings that go into Position.table_name update such state, this key might be missing.
            # E.g. relationship class mapping goes to some row, member object class mapping goes to table_name.
            state[ExportKey.OBJECT_CLASS_LIST_INDEX] = 1

    def _data(self, db_row):
        return db_row

    def _query(self, db_map, state, fixed_state):
        name_list = state[ExportKey.OBJECT_CLASS_NAME_LIST]
        try:
            name = name_list[state[ExportKey.OBJECT_CLASS_LIST_INDEX]]
        except IndexError:
            name = None
        yield name


class RelationshipMapping(ExportMapping):
    """Maps relationships.

    Cannot be used as the topmost mapping; one of the parents must be :class:`RelationshipClassMapping`.
    """

    MAP_TYPE = "Relationship"

    def _update_state(self, state, db_row):
        state[ExportKey.ENTITY_ROW_CACHE] = db_row
        state[ExportKey.OBJECT_LIST_INDEX] = 0
        state[ExportKey.OBJECT_NAME_LIST] = db_row.object_name_list.split(",")

    def _data(self, db_row):
        return db_row.name

    def _query(self, db_map, state, fixed_state):
        if ExportKey.ENTITY_ROW_CACHE not in fixed_state:
            return db_map.query(db_map.wide_relationship_sq).filter_by(class_id=state[ExportKey.CLASS_ROW_CACHE].id)
        return [fixed_state[ExportKey.ENTITY_ROW_CACHE]]


class RelationshipObjectMapping(ExportMapping):
    """Maps relationship's objects.

    Cannot be used as the topmost mapping; must have :class:`RelationshipClassMapping` and :class:`RelationshipMapping`
    as parents.
    """

    MAP_TYPE = "RelationshipObject"

    def _update_state(self, state, db_row):
        try:
            state[ExportKey.OBJECT_LIST_INDEX] += 1
        except KeyError:
            # Could happen when forming title states.
            # Since only the mappings that go into Position.table_name update such state, this key might be missing.
            # E.g. relationship mapping goes to some row, member object mapping goes to table_name.
            state[ExportKey.OBJECT_LIST_INDEX] = 1

    def _data(self, db_row):
        return db_row

    def _query(self, db_map, state, fixed_state):
        name_list = state[ExportKey.OBJECT_NAME_LIST]
        try:
            name = name_list[state[ExportKey.OBJECT_LIST_INDEX]]
        except IndexError:
            name = None
        yield name


class ParameterDefinitionMapping(ExportMapping):
    """Maps parameter definitions.

    Cannot be used as the topmost mapping; must have an entity class mapping as one of parents.
    """

    MAP_TYPE = "ParameterDefinition"

    def _update_state(self, state, db_row):
        state[ExportKey.PARAMETER_DEFINITION_ID] = db_row.id
        state[ExportKey.PARAMETER_DEFINITION_ROW_CACHE] = db_row

    def _data(self, db_row):
        return db_row.name

    def _query(self, db_map, state, fixed_state):
        if ExportKey.PARAMETER_DEFINITION_ROW_CACHE not in fixed_state:
            return db_map.query(db_map.parameter_definition_sq).filter_by(
                entity_class_id=state[ExportKey.CLASS_ROW_CACHE].id
            )
        return [fixed_state[ExportKey.PARAMETER_DEFINITION_ROW_CACHE]]


class ParameterDefaultValueMapping(ExportMapping):
    """Maps scalar (non-indexed) default values

    Cannot be used as the topmost mapping; must have a :class:`ParameterDefinitionMapping` as parent.
    """

    MAP_TYPE = "ParameterDefaultValue"

    def _update_state(self, state, db_row):
        return

    def _data(self, db_row):
        default_value = from_database(db_row.default_value)
        return default_value if not isinstance(default_value, IndexedValue) else type(default_value).__name__

    def _query(self, db_map, state, fixed_state):
        yield state[ExportKey.PARAMETER_DEFINITION_ROW_CACHE]


class ParameterDefaultValueIndexMapping(ExportMapping):
    """Maps default value indexes.

    Cannot be used as the topmost mapping; must have a :class:`ParameterDefinitionMapping` as parent.
    """

    MAP_TYPE = "ParameterDefaultValueIndex"

    def _update_state(self, state, db_row):
        state[ExportKey.EXPANDED_PARAMETER_CACHE] = db_row

    def _data(self, db_row):
        return db_row.index

    def _query(self, db_map, state, fixed_state):
        cached_parameter = state.get(ExportKey.EXPANDED_PARAMETER_CACHE)
        if cached_parameter is None or not cached_parameter.same(state):
            yield from _expand_default_value_from_state(state)
        else:
            yield from _expand_values_from_parameter(cached_parameter)


class ExpandedParameterDefaultValueMapping(ExportMapping):
    """Maps indexed default values.

    Whenever this mapping is a child of :class:`ParameterDefaultValueIndexMapping`, it maps individual values of
    indexed parameters.

    Cannot be used as the topmost mapping; must have a :class:`ParameterDefinitionMapping` as parent.
    """

    MAP_TYPE = "ExpandedDefaultValue"

    def _update_state(self, state, db_row):
        return

    def _data(self, db_row):
        return db_row.value

    def _query(self, db_map, state, fixed_state):
        cached_parameter = state.get(ExportKey.EXPANDED_PARAMETER_CACHE)
        if cached_parameter is None or not cached_parameter.same(state):
            yield from _expand_default_value_from_state(state)
        else:
            yield cached_parameter


class ParameterValueMapping(ExportMapping):
    """Maps scalar (non-indexed) parameter values.

    Cannot be used as the topmost mapping; must have a :class:`ParameterDefinitionMapping`, an entity mapping and
    an :class:`AlternativeMapping` as parents.
    """

    MAP_TYPE = "ParameterValue"

    def _update_state(self, state, db_row):
        state[ExportKey.PARAMETER_VALUE_ROW_CACHE] = db_row

    def _data(self, db_row):
        value = from_database(db_row.value)
        return value if not isinstance(value, IndexedValue) else type(value).__name__

    def _query(self, db_map, state, fixed_state):
        if ExportKey.PARAMETER_VALUE_ROW_CACHE in fixed_state:
            return [fixed_state[ExportKey.PARAMETER_VALUE_ROW_CACHE]]
        if ExportKey.PARAMETER_VALUE_LOOKUP_CACHE not in state:
            state[ExportKey.PARAMETER_VALUE_LOOKUP_CACHE] = _make_parameter_value_lookup(db_map)
        definition_id = state[ExportKey.PARAMETER_DEFINITION_ID]
        entity_id = state[ExportKey.ENTITY_ROW_CACHE].id
        alternative_id = state[ExportKey.ALTERNATIVE_ROW_CACHE].id
        value_row = state[ExportKey.PARAMETER_VALUE_LOOKUP_CACHE].get((definition_id, entity_id, alternative_id))
        if value_row is None:
            return []
        return [value_row]


class ParameterValueTypeMapping(ParameterValueMapping):
    MAP_TYPE = "ParameterValueType"

    def _update_state(self, state, db_row):
        state[ExportKey.PARAMETER_VALUE_TYPE] = db_row

    def _data(self, db_row):
        return db_row

    def _query(self, db_map, state, fixed_state):
        qry = super()._query(db_map, state, fixed_state)
        return [_type_from_value(db_row.value) for db_row in qry]


class ParameterValueIndexMapping(ExportMapping):
    """Maps parameter value indexes.

    Cannot be used as the topmost mapping; must have a :class:`ParameterDefinitionMapping`, an entity mapping and
    an :class:`AlternativeMapping` as parents.
    """

    MAP_TYPE = "ParameterValueIndex"

    def _update_state(self, state, db_row):
        state[ExportKey.EXPANDED_PARAMETER_CACHE] = db_row

    def _data(self, db_row):
        return db_row.index

    def _query(self, db_map, state, fixed_state):
        cached_parameter = state.get(ExportKey.EXPANDED_PARAMETER_CACHE)
        if cached_parameter is None or not cached_parameter.same(state):
            if ExportKey.PARAMETER_VALUE_LOOKUP_CACHE not in state:
                state[ExportKey.PARAMETER_VALUE_LOOKUP_CACHE] = _make_parameter_value_lookup(db_map)
            yield from _expand_parameter_value_from_state(state, fixed_state)
        else:
            yield from _expand_values_from_parameter(cached_parameter)


class ExpandedParameterValueMapping(ExportMapping):
    """Maps parameter values.

    Whenever this mapping is a child of :class:`ParameterValueIndexMapping`, it maps individual values of indexed
    parameters.

    Cannot be used as the topmost mapping; must have a :class:`ParameterDefinitionMapping`, an entity mapping and
    an :class:`AlternativeMapping` as parents.
    """

    MAP_TYPE = "ExpandedValue"

    def _update_state(self, state, db_row):
        return

    def _data(self, db_row):
        return db_row.value

    def _query(self, db_map, state, fixed_state):
        cached_parameter = state.get(ExportKey.EXPANDED_PARAMETER_CACHE)
        if cached_parameter is None or not cached_parameter.same(state):
            if ExportKey.PARAMETER_VALUE_LOOKUP_CACHE not in state:
                state[ExportKey.PARAMETER_VALUE_LOOKUP_CACHE] = _make_parameter_value_lookup(db_map)
            yield from _expand_parameter_value_from_state(state, fixed_state)
        else:
            yield cached_parameter


class ParameterValueListMapping(ExportMapping):
    """Maps parameter value list names.

    Can be used as the topmost mapping; in case the mapping has a :class:`ParameterDefinitionMapping` as parent,
    yields value list name for that parameter definition.
    """

    MAP_TYPE = "ParameterValueList"

    def _update_state(self, state, db_row):
        state[ExportKey.PARAMETER_VALUE_LIST_ROW_CACHE] = db_row

    def _data(self, db_row):
        return db_row.name

    def _query(self, db_map, state, fixed_state):
        if ExportKey.PARAMETER_VALUE_LIST_ROW_CACHE in fixed_state:
            return [fixed_state[ExportKey.PARAMETER_VALUE_LIST_ROW_CACHE]]
        qry = db_map.query(db_map.wide_parameter_value_list_sq)
        if ExportKey.PARAMETER_DEFINITION_ROW_CACHE in state:
            qry = qry.filter_by(id=state[ExportKey.PARAMETER_DEFINITION_ROW_CACHE].parameter_value_list_id)
        return qry


class ParameterValueListValueMapping(ExportMapping):
    """Maps parameter value list values.

    Cannot be used as the topmost mapping; must have a :class:`ParameterValueListMapping` as parent.

    """

    MAP_TYPE = "ParameterValueListValue"

    def _update_state(self, state, db_row):
        return

    def _data(self, db_row):
        return from_database(db_row)

    def _query(self, db_map, state, fixed_state):
        return state[ExportKey.PARAMETER_VALUE_LIST_ROW_CACHE].value_list.split(";")


class AlternativeMapping(ExportMapping):
    """Maps alternatives.

    Can be used as the topmost mapping.
    """

    MAP_TYPE = "Alternative"

    def _update_state(self, state, db_row):
        state[ExportKey.ALTERNATIVE_ROW_CACHE] = db_row

    def _data(self, db_row):
        return db_row.name

    def _query(self, db_map, state, fixed_state):
        if ExportKey.ALTERNATIVE_ROW_CACHE not in fixed_state:
            return db_map.query(db_map.alternative_sq)
        return [fixed_state[ExportKey.ALTERNATIVE_ROW_CACHE]]


class ScenarioMapping(ExportMapping):
    """Maps scenarios.

    Can be used as the topmost mapping.
    """

    MAP_TYPE = "Scenario"

    def _update_state(self, state, db_row):
        state[ExportKey.SCENARIO_ID] = db_row.id
        alternative_name_list = db_row.alternative_name_list
        if alternative_name_list is not None:
            state[ExportKey.ALTERNATIVE_NAME_LIST] = alternative_name_list.split(",")
        else:
            state[ExportKey.ALTERNATIVE_NAME_LIST] = None
        state[ExportKey.SCENARIO_ROW_CACHE] = db_row

    def _data(self, db_row):
        return db_row.name

    def _query(self, db_map, state, fixed_state):
        if ExportKey.SCENARIO_ROW_CACHE not in fixed_state:
            return db_map.query(db_map.wide_scenario_sq)
        return [fixed_state[ExportKey.SCENARIO_ROW_CACHE]]


class ScenarioActiveFlagMapping(ExportMapping):
    """Maps scenario active flags.

    Cannot be used as the topmost mapping; must have a :class:`ScenarioMapping` as parent.
    """

    MAP_TYPE = "ScenarioActiveFlag"

    def _update_state(self, state, db_row):
        return

    def _data(self, db_row):
        return db_row

    def _query(self, db_map, state, fixed_state):
        yield state[ExportKey.SCENARIO_ROW_CACHE].active


class ScenarioAlternativeMapping(ExportMapping):
    """Maps scenario alternatives.

    Cannot be used as the topmost mapping; must have a :class:`ScenarioMapping` as parent.
    """

    MAP_TYPE = "ScenarioAlternative"

    def _update_state(self, state, db_row):
        state[ExportKey.ALTERNATIVE_LIST_INDEX] += 1

    def _data(self, db_row):
        return db_row

    def _query(self, db_map, state, fixed_state):
        state[ExportKey.ALTERNATIVE_LIST_INDEX] = 0
        alternative_name_list = state[ExportKey.ALTERNATIVE_NAME_LIST]
        if alternative_name_list is None:
            return []
        return alternative_name_list


class ScenarioBeforeAlternativeMapping(ExportMapping):
    """Maps scenario 'before' alternatives.

    Cannot be used as the topmost mapping; must have a :class:`ScenarioAlternativeMapping` as parent.
    """

    MAP_TYPE = "ScenarioBeforeAlternative"

    def _update_state(self, state, db_row):
        pass

    def _data(self, db_row):
        return db_row

    def _query(self, db_map, state, fixed_state):
        alternative_name_list = state[ExportKey.ALTERNATIVE_NAME_LIST]
        if alternative_name_list is None:
            return []
        i = state[ExportKey.ALTERNATIVE_LIST_INDEX]
        try:
            return [alternative_name_list[i]]
        except IndexError:
            return [""]


class ToolMapping(ExportMapping):
    """Maps tools.

    Can be used as the topmost mapping.
    """

    MAP_TYPE = "Tool"

    def _update_state(self, state, db_row):
        state[ExportKey.TOOL_ID] = db_row.id

    def _data(self, db_row):
        return db_row.name

    def _query(self, db_map, state, fixed_state):
        if ExportKey.TOOL_ID not in fixed_state:
            return db_map.query(db_map.tool_sq)
        return db_map.query(db_map.tool_sq).filter_by(id=fixed_state[ExportKey.TOOL_ID])


class FeatureEntityClassMapping(ExportMapping):
    """Maps feature entity classes.

    Can be used as the topmost mapping.
    """

    MAP_TYPE = "FeatureEntityClass"

    def _update_state(self, state, db_row):
        state[ExportKey.FEATURE_ROW_CACHE] = db_row

    def _data(self, db_row):
        return db_row.entity_class_name

    def _query(self, db_map, state, fixed_state):
        if ExportKey.FEATURE_ROW_CACHE not in fixed_state:
            return db_map.query(db_map.ext_feature_sq)
        return [fixed_state[ExportKey.FEATURE_ROW_CACHE]]


class FeatureParameterDefinitionMapping(ExportMapping):
    """Maps feature parameter definitions.

    Cannot be used as the topmost mapping; must have a :class:`FeatureEntityClassMapping` as parent.
    """

    MAP_TYPE = "FeatureParameterDefinition"

    def _update_state(self, state, db_row):
        return

    def _data(self, db_row):
        return db_row.parameter_definition_name

    def _query(self, db_map, state, fixed_state):
        yield state[ExportKey.FEATURE_ROW_CACHE]


class ToolFeatureEntityClassMapping(ExportMapping):
    """Maps tool feature entity classes.

    Cannot be used as the topmost mapping; must have :class:`ToolMapping` as parent.
    """

    MAP_TYPE = "ToolFeatureEntityClass"

    def _update_state(self, state, db_row):
        state[ExportKey.TOOL_FEATURE_ROW_CACHE] = db_row

    def _data(self, db_row):
        return db_row.entity_class_name

    def _query(self, db_map, state, fixed_state):
        return db_map.query(db_map.ext_tool_feature_sq).filter_by(tool_id=state[ExportKey.TOOL_ID])


class ToolFeatureParameterDefinitionMapping(ExportMapping):
    """Maps tool feature parameter definitions.

    Cannot be used as the topmost mapping; must have :class:`ToolFeatureEntityClassMapping` as parent.
    """

    MAP_TYPE = "ToolFeatureParameterDefinition"

    def _update_state(self, state, db_row):
        return

    def _data(self, db_row):
        return db_row.parameter_definition_name

    def _query(self, db_map, state, fixed_state):
        yield state[ExportKey.TOOL_FEATURE_ROW_CACHE]


class ToolFeatureRequiredFlagMapping(ExportMapping):
    """Maps tool feature required flags.

    Cannot be used as the topmost mapping; must have :class:`ToolFeatureEntityClassMapping` as parent.
    """

    MAP_TYPE = "ToolFeatureRequiredFlag"

    def _update_state(self, state, db_row):
        return

    def _data(self, db_row):
        return db_row.required

    def _query(self, db_map, state, fixed_state):
        yield state[ExportKey.TOOL_FEATURE_ROW_CACHE]


class ToolFeatureMethodEntityClassMapping(ExportMapping):
    """Maps tool feature method entity classes.

    Cannot be used as the topmost mapping; must have :class:`ToolMapping` as parent.
    """

    MAP_TYPE = "ToolFeatureMethodEntityClass"

    def _update_state(self, state, db_row):
        state[ExportKey.TOOL_FEATURE_METHOD_ROW_CACHE] = db_row

    def _data(self, db_row):
        return db_row.entity_class_name

    def _query(self, db_map, state, fixed_state):
        return db_map.query(db_map.ext_tool_feature_method_sq).filter_by(tool_id=state[ExportKey.TOOL_ID])


class ToolFeatureMethodParameterDefinitionMapping(ExportMapping):
    """Maps tool feature method parameter definitions.

    Cannot be used as the topmost mapping; must have :class:`ToolFeatureMethodEntityClassMapping` as parent.
    """

    MAP_TYPE = "ToolFeatureMethodParameterDefinition"

    def _update_state(self, state, db_row):
        return

    def _data(self, db_row):
        return db_row.parameter_definition_name

    def _query(self, db_map, state, fixed_state):
        yield state[ExportKey.TOOL_FEATURE_METHOD_ROW_CACHE]


class ToolFeatureMethodMethodMapping(ExportMapping):
    """Maps tool feature method methods.

    Cannot be used as the topmost mapping; must have :class:`ToolFeatureMethodEntityClassMapping` as parent.
    """

    MAP_TYPE = "ToolFeatureMethodMethod"

    def _update_state(self, state, db_row):
        return

    def _data(self, db_row):
        return from_database(db_row.method)

    def _query(self, db_map, state, fixed_state):
        yield state[ExportKey.TOOL_FEATURE_METHOD_ROW_CACHE]


class _DescriptionMappingBase(ExportMapping):
    """Maps descriptions."""

    MAP_TYPE = "Description"
    _key = NotImplemented

    def _update_state(self, state, db_row):
        return

    def _data(self, db_row):
        return db_row

    def _query(self, db_map, state, fixed_state):
        yield state[self._key].description


class AlternativeDescriptionMapping(_DescriptionMappingBase):
    """Maps alternative descriptions.

    Cannot be used as the topmost mapping; must have :class:`AlternativeMapping` as parent.
    """

    MAP_TYPE = "AlternativeDescription"
    _key = ExportKey.ALTERNATIVE_ROW_CACHE


class ScenarioDescriptionMapping(_DescriptionMappingBase):
    """Maps scenario descriptions.

    Cannot be used as the topmost mapping; must have :class:`ScenarioMapping` as parent.
    """

    MAP_TYPE = "ScenarioDescription"
    _key = ExportKey.SCENARIO_ROW_CACHE


class ExpandedParameter:
    """
    Replaces ``db_row`` for mappings that deal with indexed parameters.

    Attributes:
        index (object): current index
        value (object): current value
    """

    def __init__(self, index, value, definition_id, entity_id=None, alternative_id=None):
        """
        Args:
            index (object): current index
            value (object): current parameter value
            definition_id (int): parameter definition id
            entity_id (int, optional): entity id
            alternative_id (int, optional): alternative_id
        """
        self.index = index
        self.value = value
        self._definition_id = definition_id
        self._entity_id = entity_id
        self._alternative_id = alternative_id

    def same(self, state):
        """
        Checks if state refers to the same parameter.

        Args:
            state (dict): a state

        Returns:
            bool: True if the parameter is the same, False otherwise
        """
        entity_row_cache = state.get(ExportKey.ENTITY_ROW_CACHE)
        alternative_row_cache = state.get(ExportKey.ALTERNATIVE_ROW_CACHE)
        entity_id = entity_row_cache.id if entity_row_cache is not None else None
        alternative_id = alternative_row_cache.id if alternative_row_cache is not None else None
        return (
            self._definition_id == state.get(ExportKey.PARAMETER_DEFINITION_ID)
            and self._entity_id == entity_id
            and self._alternative_id == alternative_id
        )


def _make_parameter_value_lookup(db_map):
    """
    Returns a dictionary mapping triplets (parameter_definition_id, entity_id, alternative_id)
    to the corresponding parameter value row from the given db.

    Args:
        db_map (DatabaseMappingBase): a database map

    Returns:
        dict
    """
    return {
        (db_row.parameter_definition_id, db_row.entity_id, db_row.alternative_id): db_row
        for db_row in db_map.query(db_map.parameter_value_sq)
    }


def _expand_default_value_from_state(state):
    """
    Expands the default value from the given state into ExpandedParameter instances.

    Args:
        state (dict): a state with parameter definition data

    Yields:
        ExpandedParameter
    """
    definition_id = state[ExportKey.PARAMETER_DEFINITION_ID]
    default_value = from_database(state[ExportKey.PARAMETER_DEFINITION_ROW_CACHE].default_value)
    expanded_value = _expand_value(default_value)
    for index, x in expanded_value.items():
        yield ExpandedParameter(index, x, definition_id)


def _expand_parameter_value_from_state(state, fixed_state):
    """
    Expands the value from the given state into ExpandedParameter instances.

    Args:
        state (dict): a state with parameter value data

    Yields:
        ExpandedParameter
    """
    definition_id = state[ExportKey.PARAMETER_DEFINITION_ID]
    entity_id = state[ExportKey.ENTITY_ROW_CACHE].id
    alternative_id = state[ExportKey.ALTERNATIVE_ROW_CACHE].id
    for expanded_value in _load_and_expand(state, definition_id, entity_id, alternative_id, fixed_state):
        for index, x in expanded_value.items():
            yield ExpandedParameter(index, x, definition_id, entity_id, alternative_id)


def _expand_values_from_parameter(cached_parameter):
    """Expands a cached parameter into indexed values.

    Args:
        cached_parameter (ExpandedParameter)

    Yields:
        ExpandedParameter
    """
    if isinstance(cached_parameter.value, dict):
        for index, x in cached_parameter.value.items():
            db_row = copy(cached_parameter)
            db_row.index = index
            db_row.value = x
            yield db_row
    else:
        db_row = copy(cached_parameter)
        db_row.index = None
        db_row.value = cached_parameter.value
        yield db_row


def _type_from_value(db_value):
    """

    Args:
        str (db_value): Value in the database
    Returns:
        str: The type key in case of indexed parameter value, 'single_value' otherwise
    """
    value = json.loads(db_value)
    if isinstance(value, dict):
        type_ = value["type"]
        if type_ == "map":
            inner_value = value["data"]
            k = 1
            while isinstance(inner_value, dict) and inner_value["type"] == "map":
                inner_value = inner_value["data"]
                k += 1
            return f"{k}d_map"
        if type_ in ("array", "time_series", "time_pattern"):
            return type_
    return "single_value"


def _load_and_expand(state, definition_id, entity_id, alternative_id, fixed_state):
    """
    Loads and parses parameter values from database and expands them into a dict.

    Args:
        state (dict): a state with parameter value data, notably ``ExportKey.PARAMETER_VALUE_LOOKUP_CACHE``
            holding the output of ``_make_parameter_value_lookup``
        definition_id (int): parameter definition id
        entity_id (int): entity id
        alternative_id (int): alternative id

    Yields:
        dict: a (nested) dictionary mapping parameter index (or None in case of scalar) to value
    """
    value_row = state[ExportKey.PARAMETER_VALUE_LOOKUP_CACHE].get((definition_id, entity_id, alternative_id))
    if value_row is None:
        return []
    value_type = fixed_state.get(ExportKey.PARAMETER_VALUE_TYPE)
    if value_type is not None and _type_from_value(value_row.value) != value_type:
        return []
    yield _expand_value(from_database(value_row.value))


def _expand_value(value):
    """
    Expands parsed values into a dict.

    Args:
        value (Any): parameter's default value

    Returns:
        dict: a (nested) dictionary mapping parameter index (or None in case of scalar) to value
    """
    if isinstance(value, IndexedValue):
        expanded_value = convert_map_to_dict(convert_containers_to_maps(value))
    else:
        expanded_value = {None: value}
    return expanded_value


def from_dict(serialized):
    """
    Deserializes mappings.

    Args:
        serialized (list): serialize mappings

    Returns:
        Mapping: root mapping
    """
    mappings = {
        klass.MAP_TYPE: klass
        for klass in (
            AlternativeDescriptionMapping,
            AlternativeMapping,
            ExpandedParameterValueMapping,
            FeatureEntityClassMapping,
            FeatureParameterDefinitionMapping,
            FixedValueMapping,
            ObjectClassMapping,
            ObjectGroupMapping,
            ObjectMapping,
            ParameterDefinitionMapping,
            ParameterValueIndexMapping,
            ParameterValueListMapping,
            ParameterValueListValueMapping,
            ParameterValueMapping,
            RelationshipMapping,
            RelationshipClassMapping,
            RelationshipClassObjectClassMapping,
            RelationshipObjectMapping,
            ScenarioActiveFlagMapping,
            ScenarioAlternativeMapping,
            ScenarioBeforeAlternativeMapping,
            ScenarioDescriptionMapping,
            ScenarioMapping,
            ToolMapping,
            ToolFeatureEntityClassMapping,
            ToolFeatureParameterDefinitionMapping,
            ToolFeatureRequiredFlagMapping,
            ToolFeatureMethodEntityClassMapping,
            ToolFeatureMethodParameterDefinitionMapping,
        )
    }
    # Legacy
    mappings["ParameterIndex"] = ParameterValueIndexMapping
    flattened = list()
    for mapping_dict in serialized:
        position = mapping_dict["position"]
        if isinstance(position, str):
            position = Position(position)
        ignorable = mapping_dict.get("ignorable", False)
        flattened.append(mappings[mapping_dict["map_type"]].reconstruct(position, ignorable, mapping_dict))
    return unflatten(flattened)
