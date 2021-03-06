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
from itertools import cycle
from types import MethodType
from sqlalchemy.util import KeyedTuple
from ..parameter_value import convert_containers_to_maps, convert_map_to_dict, from_database, IndexedValue


@unique
class Position(Enum):
    """Export item positions when they are not in columns."""

    hidden = "hidden"
    single_row = "single_row"
    table_name = "table_name"


def is_pivoted(position):
    """Checks if position is pivoted.

    Args:
        position (Position or int): position

    Returns:
        bool: True if position is pivoted, False otherwise
    """
    return isinstance(position, int) and position < 0


def is_regular(position):
    """Checks if position is column index.

    Args:
        position (Position or int): position

    Returns:
        bool: True if position is a column index, False otherwise
    """
    return isinstance(position, int) and position >= 0


@unique
class Key(Enum):
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
    PARAMETER_VALUE_ID = auto()
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


class Mapping:
    """Base class for export item mappings.

    Attributes:
        position (int or Position): defines where the data is placed in the output table.
            Nonnegative numbers are columns, negative numbers are pivot rows, and then there are some special cases
            in the Position enum.
        parent (Mapping or None): Another mapping that's the 'parent' of this one.
            Used to determine if a mapping is root, in which case it needs to yield the header.

    """

    MAP_TYPE = None
    """Mapping type identifier for serialization."""

    def __init__(self, position, header=""):
        """
        Args:
            position (int or Position): column index or Position
            header (str, optional); A string column header that's yielt as 'first row', if not empty.
                The default is an empty string (so it's not yielt).
        """
        self._child = None
        self.parent = None
        self.position = position
        self.header = header
        self._ignorable = False
        self._original_update_state = None
        self._original_data = None
        self._original_query = None

    @property
    def child(self):
        return self._child

    @child.setter
    def child(self, child):
        self._child = child
        if isinstance(child, Mapping):
            child.parent = self

    def __eq__(self, other):
        if not isinstance(other, Mapping):
            return NotImplemented
        return self.MAP_TYPE == other.MAP_TYPE and self.position == other.position and self.child == other.child

    def __getstate__(self):
        state = self.__dict__.copy()
        if self._ignorable:
            # Delete unpicklable entries.
            del state["_update_state"]
            del state["_data"]
            del state["_query"]
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        if self._ignorable:
            # Restore unpicklable entries.
            self._update_state = self._original_update_state
            self._data = self._original_data
            self._query = self._original_query
            self.set_ignorable()

    def can_drop(self):
        """
        Returns True if mapping is just dead weight and can be removed.

        Returns:
            bool: True if mapping is leaf mapping and has no position set
        """
        return self.child is None and self.position == Position.hidden

    def check_validity(self):
        """Checks if mapping is valid.

        Returns:
            list: a list of issues
        """
        issues = list()
        is_effective_leaf = self.child is None
        if self.child is not None:
            is_effective_leaf = any(
                child.position in (Position.hidden, Position.table_name) for child in self.child.flatten()
            )
        if not is_effective_leaf:
            if self.position == Position.single_row:
                issues.append("Cannot span multiple columns.")
        else:
            if is_pivoted(self.position):
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

    def count_mappings(self):
        """
        Counts this and child mappings.

        Returns:
            int: number of mappings
        """
        return 1 + (self.child.count_mappings() if self.child is not None else 0)

    def _data(self, db_row):
        """
        Extracts item's cell data from database row.

        Args:
            db_row (namedtuple): database row

        Returns:
            str: cell data
        """
        raise NotImplementedError()

    def drop_non_positioned_tail(self):
        """Removes children from the end of the hierarchy that don't have a position set."""
        if self.child is not None:
            self.child.drop_non_positioned_tail()
            if self.child.can_drop():
                self.child = None

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

    def flatten(self):
        """
        Flattens the mapping tree.

        Returns:
            list of Mapping: mappings in parent-child-grand child-etc order
        """
        return [self] + (self.child.flatten() if self.child is not None else [])

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

    def is_pivoted(self):
        """
        Queries recursively if export items are pivoted.

        Returns:
            bool: True if any of the items is pivoted, False otherwise
        """
        if self.child is not None:
            if is_pivoted(self.position):
                return True
            return self.child.is_pivoted()
        return False

    def non_pivoted_width(self, parent_is_pivoted=False):
        """
        Calculates columnar width of non-pivoted data.

        Args:
            parent_is_pivoted (bool): True if a parent item is pivoted, False otherwise

        Returns:
            int: non-pivoted data width
        """
        if self.child is None:
            if is_regular(self.position) and not parent_is_pivoted:
                return self.position + 1
            return 0
        width = self.position + 1 if is_regular(self.position) else 0
        return max(width, self.child.non_pivoted_width(parent_is_pivoted or is_pivoted(self.position)))

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
        if self.child is None:
            if self.position == Position.hidden:
                yield {}
            elif self.position == Position.single_row:
                row = [self._data(db_row) for db_row in self._query(db_map, state, fixed_state)]
                if row:
                    yield {self.position: row}
            else:
                for db_row in self._query(db_map, state, fixed_state):
                    yield {self.position: self._data(db_row)}
        else:
            # Yield header if top-most mapping
            if self.parent is None:
                header = self.make_header()
                if any(header.values()):
                    yield header
            # Yield normal rows
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

    def set_ignorable(self):
        """
        Sets mapping as ignorable.

        Mappings that are ignorable map to None if there is no other data to yield.
        This allows 'incomplete' rows if child mappings do not depend on the ignored mapping.
        """
        self._ignorable = True
        self._original_update_state = self._update_state
        self._original_data = self._data
        self._original_query = self._query

        def _update_state(_, state, db_row):
            if db_row is _ignored:
                return
            self._original_update_state(state, db_row)

        def _data(_, db_row):
            if db_row is _ignored:
                return None
            return self._original_data(db_row)

        def _query(_, db_map, state, fixed_state):
            yielded = False
            for db_row in self._original_query(db_map, state, fixed_state):
                yielded = True
                yield db_row
            if not yielded:
                yield _ignored

        self._update_state = MethodType(_update_state, self)
        self._data = MethodType(_data, self)
        self._query = MethodType(_query, self)

    def title(self, db_map, state, fixed_state=None):
        """
        Generates title data.

        Args:
            db_map (DatabaseMappingBase): a database map
            state (dict): state
            fixed_state (dict, optional): state for fixed items

        Yields:
            dict: a mapping from column index to cell data
        """
        if fixed_state is None:
            fixed_state = dict()
        if self.position is Position.table_name:
            for db_row in self._query(db_map, state, fixed_state):
                fixed_state = copy(fixed_state)
                self._update_state(fixed_state, db_row)
                yield self._data(db_row), fixed_state
        elif self.child is None:
            yield None, fixed_state
        else:
            for db_row in self._query(db_map, state, fixed_state):
                self._update_state(state, db_row)
                self._update_state(fixed_state, db_row)
                for title_data in self.child.title(db_map, state, fixed_state):
                    yield title_data

    def to_dict(self):
        """
        Serializes mapping into dict.

        Returns:
            dict: serialized mapping
        """
        position = self.position.value if isinstance(self.position, Position) else self.position
        mapping_dict = {"map_type": self.MAP_TYPE, "position": position}
        if self._ignorable:
            mapping_dict["ignorable"] = True
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
        mapping = cls(position)
        if ignorable:
            mapping.set_ignorable()
        return mapping


class FixedValueMapping(Mapping):
    """Always yields a fixed value.

    Can be used as the topmost mapping.

    Attributes:
        value (any): fixed value
    """

    MAP_TYPE = "FixedValue"

    def __init__(self, position, value):
        """
        Args:
            position (int or Position, optional): mapping's position
            value (Any): value to yield
        """
        super().__init__(position)
        self.value = value

    def _update_state(self, state, db_row):
        return

    def _data(self, db_row):
        return self.value

    def _query(self, db_map, state, fixed_state):
        yield None

    def to_dict(self):
        serialized = super().to_dict()
        serialized["value"] = self.value
        return serialized

    @classmethod
    def reconstruct(cls, position, ignorable, mapping_dict):
        mapping = cls(position, mapping_dict["value"])
        if ignorable:
            mapping.set_ignorable()
        return mapping


class ObjectClassMapping(Mapping):
    """Maps object classes.

    Can be used as the topmost mapping.
    """

    MAP_TYPE = "ObjectClass"

    def _update_state(self, state, db_row):
        state[Key.CLASS_ROW_CACHE] = db_row

    def _data(self, db_row):
        return db_row.name

    def _query(self, db_map, state, fixed_state):
        if Key.CLASS_ROW_CACHE not in fixed_state:
            return db_map.query(db_map.object_class_sq)
        return [fixed_state[Key.CLASS_ROW_CACHE]]


class ObjectMapping(Mapping):
    """Maps objects.

    Cannot be used as the topmost mapping; one of the parents must be :class:`ObjectClassMapping`.
    """

    MAP_TYPE = "Object"

    def _data(self, db_row):
        return db_row.name

    def _update_state(self, state, db_row):
        state[Key.ENTITY_ROW_CACHE] = db_row

    def _query(self, db_map, state, fixed_state):
        if Key.ENTITY_ROW_CACHE not in fixed_state:
            return db_map.query(db_map.object_sq).filter_by(class_id=state[Key.CLASS_ROW_CACHE].id)
        return [fixed_state[Key.ENTITY_ROW_CACHE]]


class ObjectGroupMapping(Mapping):
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
            class_id=state[Key.CLASS_ROW_CACHE].id, member_id=state[Key.ENTITY_ROW_CACHE].id
        )


class RelationshipClassMapping(Mapping):
    """Maps relationships classes.

    Can be used as the topmost mapping.
    """

    MAP_TYPE = "RelationshipClass"

    def _update_state(self, state, db_row):
        state[Key.CLASS_ROW_CACHE] = db_row
        state[Key.OBJECT_CLASS_LIST_INDEX] = 0
        state[Key.OBJECT_CLASS_NAME_LIST] = db_row.object_class_name_list.split(",")

    def _data(self, db_row):
        return db_row.name

    def _query(self, db_map, state, fixed_state):
        if Key.CLASS_ROW_CACHE not in fixed_state:
            return db_map.query(db_map.wide_relationship_class_sq)
        return [fixed_state[Key.CLASS_ROW_CACHE]]


class RelationshipClassObjectClassMapping(Mapping):
    """Maps relationship class object classes.

    Cannot be used as the topmost mapping; one of the parents must be :class:`RelationshipClassMapping`.
    """

    MAP_TYPE = "RelationshipClassObjectClass"

    def _update_state(self, state, db_row):
        state[Key.OBJECT_CLASS_LIST_INDEX] += 1

    def _data(self, db_row):
        return db_row

    def _query(self, db_map, state, fixed_state):
        name_list = state[Key.OBJECT_CLASS_NAME_LIST]
        try:
            name = name_list[state[Key.OBJECT_CLASS_LIST_INDEX]]
        except IndexError:
            name = None
        yield name


class RelationshipMapping(Mapping):
    """Maps relationships.

    Cannot be used as the topmost mapping; one of the parents must be :class:`RelationshipClassMapping`.
    """

    MAP_TYPE = "Relationship"

    def _update_state(self, state, db_row):
        state[Key.ENTITY_ROW_CACHE] = db_row
        state[Key.OBJECT_LIST_INDEX] = 0
        state[Key.OBJECT_NAME_LIST] = db_row.object_name_list.split(",")

    def _data(self, db_row):
        return db_row.name

    def _query(self, db_map, state, fixed_state):
        if Key.ENTITY_ROW_CACHE not in fixed_state:
            return db_map.query(db_map.wide_relationship_sq).filter_by(class_id=state[Key.CLASS_ROW_CACHE].id)
        return [fixed_state[Key.ENTITY_ROW_CACHE]]


class RelationshipObjectMapping(Mapping):
    """Maps relationship's objects.

    Cannot be used as the topmost mapping; must have :class:`RelationshipClassMapping` and :class:`RelationshipMapping`
    as parents.
    """

    MAP_TYPE = "RelationshipObject"

    def _update_state(self, state, db_row):
        state[Key.OBJECT_LIST_INDEX] += 1

    def _data(self, db_row):
        return db_row

    def _query(self, db_map, state, fixed_state):
        name_list = state[Key.OBJECT_NAME_LIST]
        try:
            name = name_list[state[Key.OBJECT_LIST_INDEX]]
        except IndexError:
            name = None
        yield name


class ParameterDefinitionMapping(Mapping):
    """Maps parameter definitions.

    Cannot be used as the topmost mapping; must have an entity class mapping as one of parents.
    """

    MAP_TYPE = "ParameterDefinition"

    def _update_state(self, state, db_row):
        state[Key.PARAMETER_DEFINITION_ID] = db_row.id
        state[Key.PARAMETER_DEFINITION_ROW_CACHE] = db_row

    def _data(self, db_row):
        return db_row.name

    def _query(self, db_map, state, fixed_state):
        if Key.PARAMETER_DEFINITION_ROW_CACHE not in fixed_state:
            return db_map.query(db_map.parameter_definition_sq).filter_by(entity_class_id=state[Key.CLASS_ROW_CACHE].id)
        return [fixed_state[Key.PARAMETER_DEFINITION_ROW_CACHE]]


class ParameterDefaultValueMapping(Mapping):
    """Maps scalar (non-indexed) default values

    Cannot be used as the topmost mapping; must have a :class:`ParameterDefinitionMapping` as parent.
    """

    MAP_TYPE = "ParameterDefaultValue"

    def _update_state(self, state, db_row):
        return

    def _data(self, db_row):
        return from_database(db_row.default_value)

    def _query(self, db_map, state, fixed_state):
        yield state[Key.PARAMETER_DEFINITION_ROW_CACHE]


class ParameterDefaultValueIndexMapping(Mapping):
    """Maps default value indexes.

    Cannot be used as the topmost mapping; must have a :class:`ParameterDefinitionMapping` as parent.
    """

    MAP_TYPE = "ParameterDefaultValueIndex"

    def _update_state(self, state, db_row):
        state[Key.EXPANDED_PARAMETER_CACHE] = db_row

    def _data(self, db_row):
        return db_row.index

    def _query(self, db_map, state, fixed_state):
        cached_parameter = state.get(Key.EXPANDED_PARAMETER_CACHE)
        if cached_parameter is None or not cached_parameter.same(state):
            yield from _expand_default_value_from_state(state)
        else:
            yield from _expand_values_from_parameter(cached_parameter)


class ExpandedParameterDefaultValueMapping(Mapping):
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
        cached_parameter = state.get(Key.EXPANDED_PARAMETER_CACHE)
        if cached_parameter is None or not cached_parameter.same(state):
            yield from _expand_default_value_from_state(state)
        else:
            yield cached_parameter


class ParameterValueMapping(Mapping):
    """Maps scalar (non-indexed) parameter values.

    Cannot be used as the topmost mapping; must have a :class:`ParameterDefinitionMapping`, an entity mapping and
    an :class:`AlternativeMapping` as parents.
    """

    MAP_TYPE = "ParameterValue"

    def _update_state(self, state, db_row):
        state[Key.PARAMETER_VALUE_ID] = db_row.id

    def _data(self, db_row):
        value = from_database(db_row.value)
        return value if not isinstance(value, IndexedValue) else type(value).__name__

    def _query(self, db_map, state, fixed_state):
        definition_id = state[Key.PARAMETER_DEFINITION_ID]
        entity_id = state[Key.ENTITY_ROW_CACHE].id
        alternative_id = state[Key.ALTERNATIVE_ROW_CACHE].id
        if Key.PARAMETER_VALUE_ID not in fixed_state:
            return db_map.query(db_map.parameter_value_sq).filter_by(
                parameter_definition_id=definition_id, entity_id=entity_id, alternative_id=alternative_id
            )
        return db_map.query(db_map.parameter_value_sq).filter_by(
            parameter_definition_id=definition_id,
            entity_id=entity_id,
            alternative_id=alternative_id,
            id=state[Key.PARAMETER_VALUE_ID],
        )


class ParameterValueIndexMapping(Mapping):
    """Maps parameter value indexes.

    Cannot be used as the topmost mapping; must have a :class:`ParameterDefinitionMapping`, an entity mapping and
    an :class:`AlternativeMapping` as parents.
    """

    MAP_TYPE = "ParameterValueIndex"

    def _update_state(self, state, db_row):
        state[Key.EXPANDED_PARAMETER_CACHE] = db_row

    def _data(self, db_row):
        return db_row.index

    def _query(self, db_map, state, fixed_state):
        cached_parameter = state.get(Key.EXPANDED_PARAMETER_CACHE)
        if cached_parameter is None or not cached_parameter.same(state):
            yield from _expanded_parameter_value_from_state(db_map, state)
        else:
            yield from _expand_values_from_parameter(cached_parameter)


class ExpandedParameterValueMapping(Mapping):
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
        cached_parameter = state.get(Key.EXPANDED_PARAMETER_CACHE)
        if cached_parameter is None or not cached_parameter.same(state):
            yield from _expanded_parameter_value_from_state(db_map, state)
        else:
            yield cached_parameter


class ParameterValueListMapping(Mapping):
    """Maps parameter value list names.

    Can be used as the topmost mapping; in case the mapping has a :class:`ParameterDefinitionMapping` as parent,
    yields value list name for that parameter definition.
    """

    MAP_TYPE = "ParameterValueList"

    def _update_state(self, state, db_row):
        state[Key.PARAMETER_VALUE_LIST_ROW_CACHE] = db_row

    def _data(self, db_row):
        return db_row.name

    def _query(self, db_map, state, fixed_state):
        if Key.PARAMETER_VALUE_LIST_ROW_CACHE in fixed_state:
            return [fixed_state[Key.PARAMETER_VALUE_LIST_ROW_CACHE]]
        qry = db_map.query(db_map.wide_parameter_value_list_sq)
        if Key.PARAMETER_DEFINITION_ROW_CACHE in state:
            qry = qry.filter_by(id=state[Key.PARAMETER_DEFINITION_ROW_CACHE].parameter_value_list_id)
        return qry


class ParameterValueListValueMapping(Mapping):
    """Maps parameter value list values.

    Cannot be used as the topmost mapping; must have a :class:`ParameterValueListMapping` as parent.

    """

    MAP_TYPE = "ParameterValueListValue"

    def _update_state(self, state, db_row):
        return

    def _data(self, db_row):
        return from_database(db_row)

    def _query(self, db_map, state, fixed_state):
        return state[Key.PARAMETER_VALUE_LIST_ROW_CACHE].value_list.split(";")


class AlternativeMapping(Mapping):
    """Maps alternatives.

    Can be used as the topmost mapping.
    """

    MAP_TYPE = "Alternative"

    def _update_state(self, state, db_row):
        state[Key.ALTERNATIVE_ROW_CACHE] = db_row

    def _data(self, db_row):
        return db_row.name

    def _query(self, db_map, state, fixed_state):
        if Key.ALTERNATIVE_ROW_CACHE not in fixed_state:
            return db_map.query(db_map.alternative_sq)
        return [fixed_state[Key.ALTERNATIVE_ROW_CACHE]]


class ScenarioMapping(Mapping):
    """Maps scenarios.

    Can be used as the topmost mapping.
    """

    MAP_TYPE = "Scenario"

    def _update_state(self, state, db_row):
        state[Key.SCENARIO_ID] = db_row.id
        alternative_name_list = db_row.alternative_name_list
        if alternative_name_list is not None:
            state[Key.ALTERNATIVE_NAME_LIST] = alternative_name_list.split(",")
        else:
            state[Key.ALTERNATIVE_NAME_LIST] = None
        state[Key.SCENARIO_ROW_CACHE] = db_row

    def _data(self, db_row):
        return db_row.name

    def _query(self, db_map, state, fixed_state):
        if Key.SCENARIO_ROW_CACHE not in fixed_state:
            return db_map.query(db_map.wide_scenario_sq)
        return [fixed_state[Key.SCENARIO_ROW_CACHE]]


class ScenarioActiveFlagMapping(Mapping):
    """Maps scenario active flags.

    Cannot be used as the topmost mapping; must have a :class:`ScenarioMapping` as parent.
    """

    MAP_TYPE = "ScenarioActiveFlag"

    def _update_state(self, state, db_row):
        return

    def _data(self, db_row):
        return db_row

    def _query(self, db_map, state, fixed_state):
        yield state[Key.SCENARIO_ROW_CACHE].active


class ScenarioAlternativeMapping(Mapping):
    """Maps scenario alternatives.

    Cannot be used as the topmost mapping; must have a :class:`ScenarioMapping` as parent.
    """

    MAP_TYPE = "ScenarioAlternative"

    def _update_state(self, state, db_row):
        state[Key.ALTERNATIVE_LIST_INDEX] += 1

    def _data(self, db_row):
        return db_row

    def _query(self, db_map, state, fixed_state):
        state[Key.ALTERNATIVE_LIST_INDEX] = 0
        alternative_name_list = state[Key.ALTERNATIVE_NAME_LIST]
        if alternative_name_list is None:
            return []
        return alternative_name_list


class ScenarioBeforeAlternativeMapping(Mapping):
    """Maps scenario 'before' alternatives.

    Cannot be used as the topmost mapping; must have a :class:`ScenarioAlternativeMapping` as parent.
    """

    MAP_TYPE = "ScenarioBeforeAlternative"

    def _update_state(self, state, db_row):
        pass

    def _data(self, db_row):
        return db_row

    def _query(self, db_map, state, fixed_state):
        alternative_name_list = state[Key.ALTERNATIVE_NAME_LIST]
        if alternative_name_list is None:
            return []
        i = state[Key.ALTERNATIVE_LIST_INDEX]
        try:
            return [alternative_name_list[i]]
        except IndexError:
            return [""]


class ToolMapping(Mapping):
    """Maps tools.

    Can be used as the topmost mapping.
    """

    MAP_TYPE = "Tool"

    def _update_state(self, state, db_row):
        state[Key.TOOL_ID] = db_row.id

    def _data(self, db_row):
        return db_row.name

    def _query(self, db_map, state, fixed_state):
        if Key.TOOL_ID not in fixed_state:
            return db_map.query(db_map.tool_sq)
        return db_map.query(db_map.tool_sq).filter_by(id=fixed_state[Key.TOOL_ID])


class FeatureEntityClassMapping(Mapping):
    """Maps feature entity classes.

    Can be used as the topmost mapping.
    """

    MAP_TYPE = "FeatureEntityClass"

    def _update_state(self, state, db_row):
        state[Key.FEATURE_ROW_CACHE] = db_row

    def _data(self, db_row):
        return db_row.entity_class_name

    def _query(self, db_map, state, fixed_state):
        if Key.FEATURE_ROW_CACHE not in fixed_state:
            return db_map.query(db_map.ext_feature_sq)
        return [fixed_state[Key.FEATURE_ROW_CACHE]]


class FeatureParameterDefinitionMapping(Mapping):
    """Maps feature parameter definitions.

    Cannot be used as the topmost mapping; must have a :class:`FeatureEntityClassMapping` as parent.
    """

    MAP_TYPE = "FeatureParameterDefinition"

    def _update_state(self, state, db_row):
        return

    def _data(self, db_row):
        return db_row.parameter_definition_name

    def _query(self, db_map, state, fixed_state):
        yield state[Key.FEATURE_ROW_CACHE]


class ToolFeatureEntityClassMapping(Mapping):
    """Maps tool feature entity classes.

    Cannot be used as the topmost mapping; must have :class:`ToolMapping` as parent.
    """

    MAP_TYPE = "ToolFeatureEntityClass"

    def _update_state(self, state, db_row):
        state[Key.TOOL_FEATURE_ROW_CACHE] = db_row

    def _data(self, db_row):
        return db_row.entity_class_name

    def _query(self, db_map, state, fixed_state):
        return db_map.query(db_map.ext_tool_feature_sq).filter_by(tool_id=state[Key.TOOL_ID])


class ToolFeatureParameterDefinitionMapping(Mapping):
    """Maps tool feature parameter definitions.

    Cannot be used as the topmost mapping; must have :class:`ToolFeatureEntityClassMapping` as parent.
    """

    MAP_TYPE = "ToolFeatureParameterDefinition"

    def _update_state(self, state, db_row):
        return

    def _data(self, db_row):
        return db_row.parameter_definition_name

    def _query(self, db_map, state, fixed_state):
        yield state[Key.TOOL_FEATURE_ROW_CACHE]


class ToolFeatureRequiredFlagMapping(Mapping):
    """Maps tool feature required flags.

    Cannot be used as the topmost mapping; must have :class:`ToolFeatureEntityClassMapping` as parent.
    """

    MAP_TYPE = "ToolFeatureRequiredFlag"

    def _update_state(self, state, db_row):
        return

    def _data(self, db_row):
        return db_row.required

    def _query(self, db_map, state, fixed_state):
        yield state[Key.TOOL_FEATURE_ROW_CACHE]


class ToolFeatureMethodEntityClassMapping(Mapping):
    """Maps tool feature method entity classes.

    Cannot be used as the topmost mapping; must have :class:`ToolMapping` as parent.
    """

    MAP_TYPE = "ToolFeatureMethodEntityClass"

    def _update_state(self, state, db_row):
        state[Key.TOOL_FEATURE_METHOD_ROW_CACHE] = db_row

    def _data(self, db_row):
        return db_row.entity_class_name

    def _query(self, db_map, state, fixed_state):
        return db_map.query(db_map.ext_tool_feature_method_sq).filter_by(tool_id=state[Key.TOOL_ID])


class ToolFeatureMethodParameterDefinitionMapping(Mapping):
    """Maps tool feature method parameter definitions.

    Cannot be used as the topmost mapping; must have :class:`ToolFeatureMethodEntityClassMapping` as parent.
    """

    MAP_TYPE = "ToolFeatureMethodParameterDefinition"

    def _update_state(self, state, db_row):
        return

    def _data(self, db_row):
        return db_row.parameter_definition_name

    def _query(self, db_map, state, fixed_state):
        yield state[Key.TOOL_FEATURE_METHOD_ROW_CACHE]


class ToolFeatureMethodMethodMapping(Mapping):
    """Maps tool feature method methods.

    Cannot be used as the topmost mapping; must have :class:`ToolFeatureMethodEntityClassMapping` as parent.
    """

    MAP_TYPE = "ToolFeatureMethodMethod"

    def _update_state(self, state, db_row):
        return

    def _data(self, db_row):
        return from_database(db_row.method)

    def _query(self, db_map, state, fixed_state):
        yield state[Key.TOOL_FEATURE_METHOD_ROW_CACHE]


class _DescriptionMappingBase(Mapping):
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
    _key = Key.ALTERNATIVE_ROW_CACHE


class ScenarioDescriptionMapping(_DescriptionMappingBase):
    """Maps scenario descriptions.

    Cannot be used as the topmost mapping; must have :class:`ScenarioMapping` as parent.
    """

    MAP_TYPE = "ScenarioDescription"
    _key = Key.SCENARIO_ROW_CACHE


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
        entity_row_cache = state.get(Key.ENTITY_ROW_CACHE)
        alternative_row_cache = state.get(Key.ALTERNATIVE_ROW_CACHE)
        entity_id = entity_row_cache.id if entity_row_cache is not None else None
        alternative_id = alternative_row_cache.id if alternative_row_cache is not None else None
        return (
            self._definition_id == state.get(Key.PARAMETER_DEFINITION_ID)
            and self._entity_id == entity_id
            and self._alternative_id == alternative_id
        )


def _expand_default_value_from_state(state):
    """
    Expands the default value from the given state into ExpandedParameter instances.

    Args:
        state (dict): a state with parameter definition data

    Yields:
        ExpandedParameter
    """
    definition_id = state[Key.PARAMETER_DEFINITION_ID]
    default_value = from_database(state[Key.PARAMETER_DEFINITION_ROW_CACHE].default_value)
    expanded_value = _expand_value(default_value)
    for index, x in expanded_value.items():
        yield ExpandedParameter(index, x, definition_id)


def _expanded_parameter_value_from_state(db_map, state):
    """
    Expands the value from the given state into ExpandedParameter instances.

    Args:
        state (dict): a state with parameter value data

    Yields:
        ExpandedParameter
    """
    definition_id = state[Key.PARAMETER_DEFINITION_ID]
    entity_id = state[Key.ENTITY_ROW_CACHE].id
    alternative_id = state[Key.ALTERNATIVE_ROW_CACHE].id
    for expanded_value in _load_and_expand(db_map, definition_id, entity_id, alternative_id):
        for index, x in expanded_value.items():
            yield ExpandedParameter(index, x, definition_id, entity_id, alternative_id)


def _expand_values_from_parameter(cached_parameter):
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


def _load_and_expand(db_map, definition_id, entity_id, alternative_id):
    """
    Loads and parses parameter values from database and expands them into a dict.

    Args:
        db_map (DatabaseMappingBase): a database map
        definition_id (int): parameter definition id
        entity_id (int): entity id
        alternative_id (int): alternative id

    Yields:
        dict: a (nested) dictionary mapping parameter index (or None in case of scalar) to value
    """
    for value_row in db_map.query(db_map.parameter_value_sq).filter_by(
        parameter_definition_id=definition_id, entity_id=entity_id, alternative_id=alternative_id
    ):
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


def unflatten(mappings):
    """
    Builds a mapping hierarchy from flattened mappings.

    Args:
        mappings (Iterable of Mapping): flattened mappings

    Returns:
        Mapping: root mapping
    """
    root = None
    current = None
    for mapping in mappings:
        if root is None:
            root = mapping
        else:
            current.child = mapping
        current = mapping
    current.child = None
    return root


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
            ScenarioMapping,
            ToolMapping,
            ToolFeatureEntityClassMapping,
            ToolFeatureParameterDefinitionMapping,
            ToolFeatureRequiredFlagMapping,
            ToolFeatureMethodEntityClassMapping,
            ToolFeatureMethodParameterDefinitionMapping,
        )
    }
    flattened = list()
    for mapping_dict in serialized:
        position = mapping_dict["position"]
        if isinstance(position, str):
            position = Position(position)
        ignorable = mapping_dict.get("ignorable", False)
        flattened.append(mappings[mapping_dict["map_type"]].reconstruct(position, ignorable, mapping_dict))
    return unflatten(flattened)


def to_dict(root_mapping):
    """
    Serializes mappings into JSON compatible data structure.

    Args:
        root_mapping (Mapping): root mapping

    Returns:
        list: serialized mappings
    """
    return list(mapping.to_dict() for mapping in root_mapping.flatten())
