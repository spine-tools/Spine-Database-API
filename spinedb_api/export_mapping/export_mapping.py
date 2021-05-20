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

from dataclasses import dataclass
from enum import auto, Enum, unique
from itertools import cycle, dropwhile
import re
from sqlalchemy import and_
from ..parameter_value import convert_containers_to_maps, convert_map_to_dict, from_database, IndexedValue
from ..mapping import Mapping, Position, is_pivoted, is_regular, unflatten
from ..helpers import type_from_value
from .group_functions import NoGroup


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
    OBJECT_GROUP_ROW_CACHE = auto()
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


class _IndexMappingMixin:
    _current_leaf = None

    def current_leaf(self):
        return self._current_leaf

    @staticmethod
    def _expand_value(value):
        """
        Expands given parsed parameter value into a dict.

        Args:
            value (Any): parsed parameter value

        Returns:
            dict: a (nested) dictionary mapping parameter index (or None in case of scalar) to value
        """
        if isinstance(value, IndexedValue):
            return convert_map_to_dict(convert_containers_to_maps(value))
        return {None: value}

    def _expand_data(self, data):
        if isinstance(self.parent, _IndexMappingMixin):
            # Get leaf from parent
            current_leaf = self.parent.current_leaf()
        else:
            # Start the whole thing
            current_leaf = self._expand_value(data)
        if not isinstance(current_leaf, dict):
            # Nothing to expand. Project the leaf forward so the child can find it
            self._current_leaf = current_leaf
            yield None
            return
        # Expand and set the current leaf so the child can find it
        for index, value in current_leaf.items():
            self._current_leaf = value
            yield index


class ExportMapping(Mapping):

    _TITLE_SEP = ","

    def __init__(self, position, value=None, header="", filter_re="", group_fn=None):
        """
        Args:
            position (int or Position): column index or Position
            value (Any, optional): A fixed value
            header (str, optional); A string column header that's yielded as 'first row', if not empty.
                The default is an empty string (so it's not yielded).
            filter_re (str, optional): A regular expression to filter the mapped values by
            group_fn (str, Optional): Only for topmost mappings. The name of one of our supported group functions,
                for aggregating values over repeated 'headers' (in tables with hidden elements).
                If None (the default), then no such aggregation is performed and 'headers' are just repeated as needed.
        """
        super().__init__(position, value=value)
        self._filter_re = ""
        self._group_fn = None
        self._ignorable = False
        self.header = header
        self.filter_re = filter_re
        self.group_fn = group_fn

    def __eq__(self, other):
        if not isinstance(other, ExportMapping):
            return NotImplemented
        if not super().__eq__(other):
            return False
        return (
            self._filter_re == other._filter_re
            and self._group_fn == other._group_fn
            and self._ignorable == other._ignorable
            and self.header == other.header
        )

    @property
    def filter_re(self):
        return self._filter_re

    @filter_re.setter
    def filter_re(self, filter_re):
        self._filter_re = filter_re

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

    def replace_data(self, data):
        """
        Replaces the data generated by this item by user given data.

        If data is exhausted, it gets cycled again from the beginning.

        Args:
            data (Iterable): user data
        """
        data_iterator = cycle(data)
        self._data = lambda _: next(data_iterator)

    @staticmethod
    def is_buddy(parent):
        """Checks if mapping uses a parent's state for its data.

        Args:
            parent (ExportMapping): a parent mapping

        Returns:
            bool: True if parent's state affects what a mapping yields
        """
        return False

    def is_ignorable(self):
        """Returns True if the mapping is ignorable, False otherwise.

        Returns:
            bool: True if mapping is ignorable, False otherwise
        """
        return self._ignorable

    def set_ignorable(self, ignorable):
        """
        Sets mapping as ignorable.

        Mappings that are ignorable map to None if there is no other data to yield.
        This allows 'incomplete' rows if child mappings do not depend on the ignored mapping.

        Args:
            ignorable (bool): True to set mapping ignorable, False to unset
        """
        if ignorable == self._ignorable:
            return
        self._ignorable = ignorable

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
        mapping.set_ignorable(ignorable)
        return mapping

    def add_query_columns(self, db_map, query):
        """Adds columns to the mapping query if needed, and returns the new query.

        The base class implementation just returns the same query without adding any new columns.

        Args:
            db_map (DatabaseMappingBase)
            query (dict)

        Returns:
            Query: expanded query, or the same if nothing to add.
        """
        return query

    def filter_query(self, db_map, query):
        """Filters the mapping query if needed, and returns the new query.

        The base class implementation just returns the same query without applying any new filters.

        Args:
            db_map (DatabaseMappingBase)
            query (dict)

        Returns:
            Query: expanded query, or the same if nothing to add.
        """
        return query

    def _build_query(self, db_map, title_state):
        """Builds and returns the query to run for this mapping hierarchy.

        Args:
            db_map (DatabaseMappingBase)
            title_state (dict)

        Returns:
            Query
        """
        # Start with empty query
        qry = db_map.query()
        # Add columns
        for m in self.flatten():
            qry = m.add_query_columns(db_map, qry)
        # Apply filters
        for m in self.flatten():
            qry = m.filter_query(db_map, qry)
        # Apply title filters
        sq = qry.subquery()
        qry = db_map.query(sq)
        for key, value in title_state.items():
            qry = qry.filter(getattr(sq.c, key) == value)
        return qry

    @staticmethod
    def name_field():
        """Returns the 'name' field associated to this mapping within the query.
        Used to obtain the relevant data from a db row.

        Returns:
            str
        """
        raise NotImplementedError()

    @staticmethod
    def id_field():
        """Returns the 'id' field associated to this mapping within the query.
        Used to obtain the title state.

        Returns:
            str
        """
        raise NotImplementedError()

    def _data(self, db_row):  # pylint: disable=arguments-differ
        """Returns the data relevant to this mapping for a given database row.

        The base class implementation returns the field given by ``name_field()``.

        Args:
            db_row (KeyedTuple)

        Returns:
            any
        """
        return getattr(db_row, self.name_field())

    def _expand_data(self, data):
        """Takes data from an individual field in the db and yields all data generated by this mapping.

        The base class implementation simply yields the given data.

        Reimplement in subclasses that need to expand the data into multiple elements (e.g., indexed value mappings).

        Args:
            data (any)

        Returns:
            generator(any)
        """
        yield data

    def _get_rows(self, db_row):
        """Yields rows issued by this mapping for given database row.

        Args:
            db_row (KeyedTuple)

        Returns:
            generator(dict)
        """
        if self.position == Position.table_name:
            yield {}
            return
        data = self._data(db_row)
        if data is None and not self._ignorable:
            return ()
        for data in self._expand_data(data):
            yield {self.position: data}

    def get_rows_recursive(self, db_row):
        """Takes a database row and yields rows issued by this mapping and its children combined.

        Args:
            db_row (KeyedTuple)

        Returns:
            generator(dict)
        """
        if self.child is None:
            yield from self._get_rows(db_row)
            return
        for row in self._get_rows(db_row):
            for child_row in self.child.get_rows_recursive(db_row):
                row = row.copy()
                row.update(child_row)
                yield row

    def rows(self, db_map, title_state):
        """Yields rows issued by this mapping and its children combined.

        Args:
            db_map (DatabaseMappingBase)
            title_state (dict)

        Returns:
            generator(dict)
        """
        qry = self._build_query(db_map, title_state)
        for db_row in qry.yield_per(1000):
            yield from self.get_rows_recursive(db_row)

    def has_titles(self):
        """Returns True if this mapping or one of its children generates titles.

        Returns:
            bool: True if mappings generate titles, False otherwise
        """
        if self.position == Position.table_name:
            return True
        if self.child is not None:
            return self.child.has_titles()
        return False

    def _get_titles(self, db_row):
        """Yields pairs (title, title state) issued by this mapping for given database row.

        Args:
            db_row (KeyedTuple)

        Returns:
            generator(str,dict)
        """
        if self.position != Position.table_name:
            yield "", {}
            return
        data = self._data(db_row)
        data_iterator = self._expand_data(data)
        id_field = self.id_field()
        title_state = {} if id_field is None else {id_field: getattr(db_row, id_field)}
        for data in data_iterator:
            if data is None:
                data = ""
            yield data, title_state

    def get_titles_recursive(self, db_row):
        """Takes a database row and yields pairs (title, title state) issued by this mapping and its children combined.

        Args:
            db_row (KeyedTuple)

        Returns:
            generator(str,dict)
        """
        if self.child is None:
            yield from self._get_titles(db_row)
            return
        for title, title_state in self._get_titles(db_row):
            for child_title, child_title_state in self.child.get_titles_recursive(db_row):
                title_sep = self._TITLE_SEP if title and child_title else ""
                final_title = title + title_sep + child_title
                yield final_title, {**title_state, **child_title_state}

    def _non_unique_titles(self, db_map):
        """Yields all titles, not necessarily unique, and associated state dictionaries.

        Args:
            db_map (DatabaseMappingBase): a database map

        Yields:
            tuple(str,dict): title, and associated title state dictionary
        """
        qry = self._build_query(db_map, dict())
        for db_row in qry.yield_per(1000):
            yield from self.get_titles_recursive(db_row)

    def titles(self, db_map):
        """Yields unique titles and associated state dictionaries.

        Args:
            db_map (DatabaseMappingBase): a database map

        Yields:
            tuple(str,dict): unique title, and associated title state dictionary
        """
        titles = {}
        for title, title_state in self._non_unique_titles(db_map):
            titles.setdefault(title, {}).update(title_state)
        yield from titles.items()

    def has_header(self):
        """Recursively checks if mapping would create a header row.

        Returns:
            bool: True if make_header() would return something useful
        """
        if self.header or self.position == Position.header:
            return True
        if self.child is None:
            return False
        return self.child.has_header()

    def make_header_recursive(self, first_row, title_state, buddies):
        """Builds the header recursively.

        Args:
            db_map (DatabaseMappingBase): database map
            title_state (dict): title state
            buddies (list of tuple): buddy mappings

        Returns
            dict: a mapping from column index to string header
        """
        if self.child is None:
            if not is_regular(self.position):
                return {}
            return {self.position: self.header}
        header = self.child.make_header_recursive(first_row, title_state, buddies)
        if self.position == Position.header:
            buddy = find_my_buddy(self, buddies)
            if buddy is not None:
                header[buddy.position] = self._data(first_row)
        else:
            header[self.position] = self.header
        return header

    def make_header(self, db_map, title_state, buddies):
        """Returns the header for this mapping.

        Args:
            db_map (DatabaseMappingBase): database map
            title_state (dict): title state
            buddies (list of tuple): buddy mappings

        Returns
            dict: a mapping from column index to string header
        """
        qry = self._build_query(db_map, title_state)
        first_row = qry.first()
        return self.make_header_recursive(first_row, title_state, buddies)


def drop_non_positioned_tail(root_mapping):
    """Makes a modified mapping hierarchy without hidden tail mappings.

    This enables pivot tables to work correctly in certain situations.

    Args:
        root_mapping (Mapping): root mapping

    Returns:
        Mapping: modified mapping hierarchy
    """
    mappings = root_mapping.flatten()
    return unflatten(
        reversed(list(dropwhile(lambda m: m.position == Position.hidden and not m.filter_re, reversed(mappings))))
    )


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

    @staticmethod
    def name_field():
        return None

    @staticmethod
    def id_field():
        return None


class ObjectClassMapping(ExportMapping):
    """Maps object classes.

    Can be used as the topmost mapping.
    """

    MAP_TYPE = "ObjectClass"

    def add_query_columns(self, db_map, query):
        return query.add_columns(
            db_map.entity_class_sq.c.id.label("object_class_id"),
            db_map.entity_class_sq.c.name.label("object_class_name"),
        )

    def filter_query(self, db_map, query):
        object_class_sq = db_map._subquery("object_class")
        return query.filter(db_map.entity_class_sq.c.id == object_class_sq.c.entity_class_id)

    @staticmethod
    def name_field():
        return "object_class_name"

    @staticmethod
    def id_field():
        return "object_class_id"


class ObjectMapping(ExportMapping):
    """Maps objects.

    Cannot be used as the topmost mapping; one of the parents must be :class:`ObjectClassMapping`.
    """

    MAP_TYPE = "Object"

    def add_query_columns(self, db_map, query):
        return query.add_columns(db_map.entity_sq.c.id.label("object_id"), db_map.entity_sq.c.name.label("object_name"))

    def filter_query(self, db_map, query):
        object_sq = db_map._subquery("object")
        return query.outerjoin(db_map.entity_sq, db_map.entity_sq.c.class_id == db_map.entity_class_sq.c.id).outerjoin(
            object_sq, object_sq.c.entity_id == db_map.entity_sq.c.id
        )

    @staticmethod
    def name_field():
        return "object_name"

    @staticmethod
    def id_field():
        return "object_id"

    @staticmethod
    def is_buddy(parent):
        return isinstance(parent, ObjectClassMapping)


class ObjectGroupMapping(ExportMapping):
    """Maps object groups.

    Cannot be used as the topmost mapping; one of the parents must be :class:`ObjectClassMapping`.
    """

    MAP_TYPE = "ObjectGroup"

    def add_query_columns(self, db_map, query):
        return query.add_columns(db_map.ext_entity_group_sq.c.group_id, db_map.ext_entity_group_sq.c.group_name)

    def filter_query(self, db_map, query):
        return query.outerjoin(
            db_map.ext_entity_group_sq, db_map.ext_entity_group_sq.c.class_id == db_map.entity_class_sq.c.id
        ).distinct()

    @staticmethod
    def name_field():
        return "group_name"

    @staticmethod
    def id_field():
        return "group_id"

    @staticmethod
    def is_buddy(parent):
        return isinstance(parent, ObjectClassMapping)


class ObjectGroupObjectMapping(ExportMapping):
    """Maps objects in object groups.

    Cannot be used as the topmost mapping; one of the parents must be :class:`ObjectGroupMapping`.
    """

    MAP_TYPE = "ObjectGroupObject"

    def add_query_columns(self, db_map, query):
        return query.add_columns(db_map.entity_sq.c.id.label("object_id"), db_map.entity_sq.c.name.label("object_name"))

    def filter_query(self, db_map, query):
        return query.filter(db_map.ext_entity_group_sq.c.member_id == db_map.entity_sq.c.id)

    @staticmethod
    def name_field():
        return "object_name"

    @staticmethod
    def id_field():
        return "object_id"

    @staticmethod
    def is_buddy(parent):
        return isinstance(parent, ObjectGroupMapping)


class RelationshipClassMapping(ExportMapping):
    """Maps relationships classes.

    Can be used as the topmost mapping.
    """

    MAP_TYPE = "RelationshipClass"

    def add_query_columns(self, db_map, query):
        return query.add_columns(
            db_map.entity_class_sq.c.id.label("relationship_class_id"),
            db_map.entity_class_sq.c.name.label("relationship_class_name"),
        )

    def filter_query(self, db_map, query):
        relationship_class_sq = db_map._subquery("relationship_class")
        return query.filter(db_map.entity_class_sq.c.id == relationship_class_sq.c.entity_class_id)

    @staticmethod
    def name_field():
        return "relationship_class_name"

    @staticmethod
    def id_field():
        return "relationship_class_id"

    def index(self):
        return -1


class RelationshipClassObjectClassMapping(ExportMapping):
    """Maps relationship class object classes.

    Cannot be used as the topmost mapping; one of the parents must be :class:`RelationshipClassMapping`.
    """

    MAP_TYPE = "RelationshipClassObjectClass"

    def add_query_columns(self, db_map, query):
        return query.add_columns(
            db_map.wide_relationship_class_sq.c.object_class_id_list,
            db_map.wide_relationship_class_sq.c.object_class_name_list,
        )

    def filter_query(self, db_map, query):
        return query.filter(db_map.entity_class_sq.c.id == db_map.wide_relationship_class_sq.c.id)

    @staticmethod
    def name_field():
        return "object_class_name_list"

    @staticmethod
    def id_field():
        return "object_class_id_list"

    def _title_state(self, db_row):
        object_class_id_list = db_row.object_class_id_list.split(",")
        index = self.index()
        try:
            return {"object_class_id": int(object_class_id_list[index])}
        except IndexError:
            return {}

    def _data(self, db_row):
        data = super()._data(db_row).split(",")
        index = self.index()
        try:
            return data[index]
        except IndexError:
            return ""

    def index(self):
        return self.parent.index() + 1

    @staticmethod
    def is_buddy(parent):
        return isinstance(parent, RelationshipClassMapping)


class RelationshipMapping(ExportMapping):
    """Maps relationships.

    Cannot be used as the topmost mapping; one of the parents must be :class:`RelationshipClassMapping`.
    """

    MAP_TYPE = "Relationship"

    def add_query_columns(self, db_map, query):
        return query.add_columns(
            db_map.entity_sq.c.id.label("relationship_id"), db_map.entity_sq.c.name.label("relationship_name")
        )

    def filter_query(self, db_map, query):
        relationship_sq = db_map._subquery("relationship")
        return query.outerjoin(db_map.entity_sq, db_map.entity_sq.c.class_id == db_map.entity_class_sq.c.id).outerjoin(
            relationship_sq, relationship_sq.c.entity_id == db_map.entity_sq.c.id
        )

    @staticmethod
    def name_field():
        return "relationship_name"

    @staticmethod
    def id_field():
        return "relationship_id"

    @staticmethod
    def is_buddy(parent):
        return isinstance(parent, RelationshipClassMapping)

    def index(self):
        return -1


class RelationshipObjectMapping(ExportMapping):
    """Maps relationship's objects.

    Cannot be used as the topmost mapping; must have :class:`RelationshipClassMapping` and :class:`RelationshipMapping`
    as parents.
    """

    MAP_TYPE = "RelationshipObject"

    def add_query_columns(self, db_map, query):
        return query.add_columns(
            db_map.wide_relationship_sq.c.object_id_list, db_map.wide_relationship_sq.c.object_name_list
        )

    def filter_query(self, db_map, query):
        return query.filter(db_map.entity_sq.c.id == db_map.wide_relationship_sq.c.id)

    @staticmethod
    def name_field():
        return "object_name_list"

    @staticmethod
    def id_field():
        return "object_id_list"

    def _title_state(self, db_row):
        object_id_list = db_row.object_id_list.split(",")
        index = self.index()
        try:
            return {"object_id": int(object_id_list[index])}
        except IndexError:
            return {}

    def _data(self, db_row):
        data = super()._data(db_row).split(",")
        index = self.index()
        try:
            return data[index]
        except IndexError:
            return ""

    def index(self):
        return self.parent.index() + 1

    @staticmethod
    def is_buddy(parent):
        return isinstance(parent, RelationshipClassObjectClassMapping)


class ParameterDefinitionMapping(ExportMapping):
    """Maps parameter definitions.

    Cannot be used as the topmost mapping; must have an entity class mapping as one of parents.
    """

    MAP_TYPE = "ParameterDefinition"

    def add_query_columns(self, db_map, query):
        return query.add_columns(
            db_map.parameter_definition_sq.c.id.label("parameter_definition_id"),
            db_map.parameter_definition_sq.c.name.label("parameter_definition_name"),
        )

    def filter_query(self, db_map, query):
        return query.outerjoin(
            db_map.parameter_definition_sq,
            db_map.parameter_definition_sq.c.entity_class_id == db_map.entity_class_sq.c.id,
        )

    @staticmethod
    def name_field():
        return "parameter_definition_name"

    @staticmethod
    def id_field():
        return "parameter_definition_id"


class ParameterDefaultValueMapping(ExportMapping):
    """Maps scalar (non-indexed) default values

    Cannot be used as the topmost mapping; must have a :class:`ParameterDefinitionMapping` as parent.
    """

    MAP_TYPE = "ParameterDefaultValue"

    def add_query_columns(self, db_map, query):
        return query.add_columns(db_map.parameter_definition_sq.c.default_value)

    @staticmethod
    def name_field():
        return "default_value"

    @staticmethod
    def id_field():
        return "default_value"

    def _data(self, db_row):
        data = super()._data(db_row)
        data = from_database(data)
        return data if not isinstance(data, IndexedValue) else type(data).__name__

    @staticmethod
    def is_buddy(parent):
        return isinstance(parent, ParameterDefinitionMapping)


class ParameterDefaultValueIndexMapping(_IndexMappingMixin, ExportMapping):
    """Maps default value indexes.

    Cannot be used as the topmost mapping; must have a :class:`ParameterDefinitionMapping` as parent.
    """

    MAP_TYPE = "ParameterDefaultValueIndex"
    _expanded_value = None

    def add_query_columns(self, db_map, query):
        if "default_value" in {c["name"] for c in query.column_descriptions}:
            return query
        return query.add_columns(db_map.parameter_definition_sq.c.default_value)

    @staticmethod
    def name_field():
        return "default_value"

    @staticmethod
    def id_field():
        return "default_value"

    def _data(self, db_row):
        data = super()._data(db_row)
        return from_database(data)


class ExpandedParameterDefaultValueMapping(ExportMapping):
    """Maps indexed default values.

    Whenever this mapping is a child of :class:`ParameterDefaultValueIndexMapping`, it maps individual values of
    indexed parameters.

    Cannot be used as the topmost mapping; must have a :class:`ParameterDefinitionMapping` as parent.
    """

    MAP_TYPE = "ExpandedDefaultValue"

    @staticmethod
    def name_field():
        return "default_value"

    @staticmethod
    def id_field():
        return "default_value"

    def _data(self, db_row):
        return self.parent.current_leaf()


class ParameterValueMapping(ExportMapping):
    """Maps scalar (non-indexed) parameter values.

    Cannot be used as the topmost mapping; must have a :class:`ParameterDefinitionMapping`, an entity mapping and
    an :class:`AlternativeMapping` as parents.
    """

    MAP_TYPE = "ParameterValue"
    _selects_value = False

    def add_query_columns(self, db_map, query):
        if "value" in {c["name"] for c in query.column_descriptions}:
            return query
        self._selects_value = True
        return query.add_columns(db_map.parameter_value_sq.c.value)

    def filter_query(self, db_map, query):
        if not self._selects_value:
            return query
        return query.outerjoin(
            db_map.parameter_value_sq,
            and_(
                db_map.parameter_value_sq.c.entity_id == db_map.entity_sq.c.id,
                db_map.parameter_value_sq.c.parameter_definition_id == db_map.parameter_definition_sq.c.id,
            ),
        )

    @staticmethod
    def name_field():
        return "value"

    @staticmethod
    def id_field():
        return "value"

    def _data(self, db_row):
        data = super()._data(db_row)
        data = from_database(data)
        return data if not isinstance(data, IndexedValue) else type(data).__name__

    @staticmethod
    def is_buddy(parent):
        return isinstance(parent, (ParameterDefinitionMapping, ObjectMapping, RelationshipMapping, AlternativeMapping))


class ParameterValueTypeMapping(ParameterValueMapping):
    """Maps parameter value types.

    Cannot be used as the topmost mapping; must have a :class:`ParameterDefinitionMapping`, an entity mapping and
    an :class:`AlternativeMapping` as parents.
    """

    MAP_TYPE = "ParameterValueType"

    def _data(self, db_row):
        data = type_from_value(db_row.value)
        if data.type_ != "map":
            return data.type_
        return f"{data.dimension_count}d_{data.type_}"


class ParameterValueIndexMapping(_IndexMappingMixin, ParameterValueMapping):
    """Maps parameter value indexes.

    Cannot be used as the topmost mapping; must have a :class:`ParameterDefinitionMapping`, an entity mapping and
    an :class:`AlternativeMapping` as parents.
    """

    MAP_TYPE = "ParameterValueIndex"

    def _data(self, db_row):
        # pylint: disable=bad-super-call
        data = super(ParameterValueMapping, self)._data(db_row)
        return from_database(data)


class ExpandedParameterValueMapping(ExportMapping):
    """Maps parameter values.

    Whenever this mapping is a child of :class:`ParameterValueIndexMapping`, it maps individual values of indexed
    parameters.

    Cannot be used as the topmost mapping; must have a :class:`ParameterDefinitionMapping`, an entity mapping and
    an :class:`AlternativeMapping` as parents.
    """

    MAP_TYPE = "ExpandedValue"

    @staticmethod
    def name_field():
        return "value"

    @staticmethod
    def id_field():
        return "value"

    def _data(self, db_row):
        return self.parent.current_leaf()


class ParameterValueListMapping(ExportMapping):
    """Maps parameter value list names.

    Can be used as the topmost mapping; in case the mapping has a :class:`ParameterDefinitionMapping` as parent,
    yields value list name for that parameter definition.
    """

    MAP_TYPE = "ParameterValueList"

    def add_query_columns(self, db_map, query):
        return query.add_columns(
            db_map.parameter_value_list_sq.c.id.label("parameter_value_list_id"),
            db_map.parameter_value_list_sq.c.name.label("parameter_value_list_name"),
        )

    def filter_query(self, db_map, query):
        if self.parent is None:
            return query
        return query.outerjoin(
            db_map.parameter_value_list_sq,
            db_map.parameter_value_list_sq.c.id == db_map.parameter_definition_sq.c.parameter_value_list_id,
        )

    @staticmethod
    def name_field():
        return "parameter_value_list_name"

    @staticmethod
    def id_field():
        return "parameter_value_list_id"


class ParameterValueListValueMapping(ExportMapping):
    """Maps parameter value list values.

    Cannot be used as the topmost mapping; must have a :class:`ParameterValueListMapping` as parent.

    """

    MAP_TYPE = "ParameterValueListValue"

    def add_query_columns(self, db_map, query):
        return query.add_columns(db_map.parameter_value_list_sq.c.value)

    @staticmethod
    def name_field():
        return "value"

    @staticmethod
    def id_field():
        return "value"

    def _data(self, db_row):
        data = super()._data(db_row)
        return from_database(data)

    @staticmethod
    def is_buddy(parent):
        return isinstance(parent, ParameterValueListMapping)


class AlternativeMapping(ExportMapping):
    """Maps alternatives.

    Can be used as the topmost mapping.
    """

    MAP_TYPE = "Alternative"

    def add_query_columns(self, db_map, query):
        return query.add_columns(
            db_map.alternative_sq.c.id.label("alternative_id"),
            db_map.alternative_sq.c.name.label("alternative_name"),
            db_map.alternative_sq.c.description.label("description"),
        )

    def filter_query(self, db_map, query):
        if self.parent is None:
            return query
        return query.filter(db_map.alternative_sq.c.id == db_map.parameter_value_sq.c.alternative_id)

    @staticmethod
    def name_field():
        return "alternative_name"

    @staticmethod
    def id_field():
        return "alternative_id"


class ScenarioMapping(ExportMapping):
    """Maps scenarios.

    Can be used as the topmost mapping.
    """

    MAP_TYPE = "Scenario"

    def add_query_columns(self, db_map, query):
        return query.add_columns(
            db_map.scenario_sq.c.id.label("scenario_id"),
            db_map.scenario_sq.c.name.label("scenario_name"),
            db_map.scenario_sq.c.description.label("description"),
        )

    @staticmethod
    def name_field():
        return "scenario_name"

    @staticmethod
    def id_field():
        return "scenario_id"


class ScenarioActiveFlagMapping(ExportMapping):
    """Maps scenario active flags.

    Cannot be used as the topmost mapping; must have a :class:`ScenarioMapping` as parent.
    """

    MAP_TYPE = "ScenarioActiveFlag"

    def add_query_columns(self, db_map, query):
        return query.add_columns(db_map.scenario_sq.c.active)

    @staticmethod
    def name_field():
        return "active"

    @staticmethod
    def id_field():
        return "active"


class ScenarioAlternativeMapping(ExportMapping):
    """Maps scenario alternatives.

    Cannot be used as the topmost mapping; must have a :class:`ScenarioMapping` as parent.
    """

    MAP_TYPE = "ScenarioAlternative"

    def add_query_columns(self, db_map, query):
        return query.add_columns(
            db_map.ext_linked_scenario_alternative_sq.c.alternative_id,
            db_map.ext_linked_scenario_alternative_sq.c.alternative_name,
        )

    def filter_query(self, db_map, query):
        return query.outerjoin(
            db_map.ext_linked_scenario_alternative_sq,
            db_map.ext_linked_scenario_alternative_sq.c.scenario_id == db_map.scenario_sq.c.id,
        )

    @staticmethod
    def name_field():
        return "alternative_name"

    @staticmethod
    def id_field():
        return "alternative_id"

    @staticmethod
    def is_buddy(parent):
        return isinstance(parent, ScenarioMapping)


class ScenarioBeforeAlternativeMapping(ExportMapping):
    """Maps scenario 'before' alternatives.

    Cannot be used as the topmost mapping; must have a :class:`ScenarioAlternativeMapping` as parent.
    """

    MAP_TYPE = "ScenarioBeforeAlternative"

    def add_query_columns(self, db_map, query):
        return query.add_columns(
            db_map.ext_linked_scenario_alternative_sq.c.before_alternative_id,
            db_map.ext_linked_scenario_alternative_sq.c.before_alternative_name,
        )

    @staticmethod
    def name_field():
        return "before_alternative_name"

    @staticmethod
    def id_field():
        return "before_alternative_id"

    @staticmethod
    def is_buddy(parent):
        return isinstance(parent, ScenarioAlternativeMapping)


class FeatureEntityClassMapping(ExportMapping):
    """Maps feature entity classes.

    Can be used as the topmost mapping.
    """

    MAP_TYPE = "FeatureEntityClass"

    def add_query_columns(self, db_map, query):
        return query.add_columns(db_map.ext_feature_sq.c.entity_class_id, db_map.ext_feature_sq.c.entity_class_name)

    @staticmethod
    def name_field():
        return "entity_class_name"

    @staticmethod
    def id_field():
        return "entity_class_id"


class FeatureParameterDefinitionMapping(ExportMapping):
    """Maps feature parameter definitions.

    Cannot be used as the topmost mapping; must have a :class:`FeatureEntityClassMapping` as parent.
    """

    MAP_TYPE = "FeatureParameterDefinition"

    def add_query_columns(self, db_map, query):
        return query.add_columns(
            db_map.ext_feature_sq.c.parameter_definition_id, db_map.ext_feature_sq.c.parameter_definition_name
        )

    @staticmethod
    def name_field():
        return "parameter_definition_name"

    @staticmethod
    def id_field():
        return "parameter_definition_id"

    @staticmethod
    def is_buddy(parent):
        return isinstance(parent, FeatureEntityClassMapping)


class ToolMapping(ExportMapping):
    """Maps tools.

    Can be used as the topmost mapping.
    """

    MAP_TYPE = "Tool"

    def add_query_columns(self, db_map, query):
        return query.add_columns(db_map.tool_sq.c.id.label("tool_id"), db_map.tool_sq.c.name.label("tool_name"))

    @staticmethod
    def name_field():
        return "tool_name"

    @staticmethod
    def id_field():
        return "tool_id"


class ToolFeatureEntityClassMapping(ExportMapping):
    """Maps tool feature entity classes.

    Cannot be used as the topmost mapping; must have :class:`ToolMapping` as parent.
    """

    MAP_TYPE = "ToolFeatureEntityClass"

    def add_query_columns(self, db_map, query):
        return query.add_columns(
            db_map.ext_tool_feature_sq.c.entity_class_id, db_map.ext_tool_feature_sq.c.entity_class_name
        )

    def filter_query(self, db_map, query):
        return query.outerjoin(db_map.ext_tool_feature_sq, db_map.ext_tool_feature_sq.c.tool_id == db_map.tool_sq.c.id)

    @staticmethod
    def name_field():
        return "entity_class_name"

    @staticmethod
    def id_field():
        return "entity_class_id"

    @staticmethod
    def is_buddy(parent):
        return isinstance(parent, ToolMapping)


class ToolFeatureParameterDefinitionMapping(ExportMapping):
    """Maps tool feature parameter definitions.

    Cannot be used as the topmost mapping; must have :class:`ToolFeatureEntityClassMapping` as parent.
    """

    MAP_TYPE = "ToolFeatureParameterDefinition"

    def add_query_columns(self, db_map, query):
        return query.add_columns(
            db_map.ext_tool_feature_sq.c.parameter_definition_id, db_map.ext_tool_feature_sq.c.parameter_definition_name
        )

    @staticmethod
    def name_field():
        return "parameter_definition_name"

    @staticmethod
    def id_field():
        return "parameter_definition_id"

    @staticmethod
    def is_buddy(parent):
        return isinstance(parent, ToolFeatureEntityClassMapping)


class ToolFeatureRequiredFlagMapping(ExportMapping):
    """Maps tool feature required flags.

    Cannot be used as the topmost mapping; must have :class:`ToolFeatureEntityClassMapping` as parent.
    """

    MAP_TYPE = "ToolFeatureRequiredFlag"

    def add_query_columns(self, db_map, query):
        return query.add_columns(db_map.ext_tool_feature_sq.c.required)

    @staticmethod
    def name_field():
        return "required"

    @staticmethod
    def id_field():
        return "required"


class ToolFeatureMethodEntityClassMapping(ExportMapping):
    """Maps tool feature method entity classes.

    Cannot be used as the topmost mapping; must have :class:`ToolMapping` as parent.
    """

    MAP_TYPE = "ToolFeatureMethodEntityClass"

    def add_query_columns(self, db_map, query):
        return query.add_columns(
            db_map.ext_tool_feature_sq.c.entity_class_id, db_map.ext_tool_feature_sq.c.entity_class_name
        )

    def filter_query(self, db_map, query):
        return query.outerjoin(db_map.ext_tool_feature_sq, db_map.ext_tool_feature_sq.c.tool_id == db_map.tool_sq.c.id)

    @staticmethod
    def name_field():
        return "entity_class_name"

    @staticmethod
    def id_field():
        return "entity_class_id"


class ToolFeatureMethodParameterDefinitionMapping(ExportMapping):
    """Maps tool feature method parameter definitions.

    Cannot be used as the topmost mapping; must have :class:`ToolFeatureMethodEntityClassMapping` as parent.
    """

    MAP_TYPE = "ToolFeatureMethodParameterDefinition"

    def add_query_columns(self, db_map, query):
        return query.add_columns(
            db_map.ext_tool_feature_sq.c.parameter_definition_id, db_map.ext_tool_feature_sq.c.parameter_definition_name
        )

    @staticmethod
    def name_field():
        return "parameter_definition_name"

    @staticmethod
    def id_field():
        return "parameter_definition_id"


class ToolFeatureMethodMethodMapping(ExportMapping):
    """Maps tool feature method methods.

    Cannot be used as the topmost mapping; must have :class:`ToolFeatureMethodEntityClassMapping` as parent.
    """

    MAP_TYPE = "ToolFeatureMethodMethod"

    def add_query_columns(self, db_map, query):
        return query.add_columns(db_map.ext_tool_feature_method_sq.c.method)

    def filter_query(self, db_map, query):
        return query.outerjoin(
            db_map.ext_tool_feature_method_sq,
            and_(
                db_map.ext_tool_feature_method_sq.c.tool_id == db_map.ext_tool_feature_sq.c.tool_id,
                db_map.ext_tool_feature_method_sq.c.feature_id == db_map.ext_tool_feature_sq.c.feature_id,
            ),
        )

    @staticmethod
    def name_field():
        return "method"

    @staticmethod
    def id_field():
        return "method"

    def _data(self, db_row):
        data = super()._data(db_row)
        return from_database(data)


class _DescriptionMappingBase(ExportMapping):
    """Maps descriptions."""

    MAP_TYPE = "Description"

    @staticmethod
    def name_field():
        return "description"

    @staticmethod
    def id_field():
        return "description"


class AlternativeDescriptionMapping(_DescriptionMappingBase):
    """Maps alternative descriptions.

    Cannot be used as the topmost mapping; must have :class:`AlternativeMapping` as parent.
    """

    MAP_TYPE = "AlternativeDescription"


class ScenarioDescriptionMapping(_DescriptionMappingBase):
    """Maps scenario descriptions.

    Cannot be used as the topmost mapping; must have :class:`ScenarioMapping` as parent.
    """

    MAP_TYPE = "ScenarioDescription"


def pair_header_buddies(root_mapping):
    """Pairs mappings that have Position.header to their 'buddy' child mappings.

    Args:
        root_mapping (ExportMapping): root mapping

    Returns:
        list of tuple: pairs of parent mapping - buddy child mapping
    """

    @dataclass
    class Pairable:
        mapping: ExportMapping
        paired: bool

    pairables = [Pairable(m, False) for m in root_mapping.flatten()]
    buddies = list()
    for i, parent in enumerate(pairables):
        if parent.mapping.position != Position.header:
            continue
        for child in pairables[i + 1 :]:
            if child.mapping.is_buddy(parent.mapping) and not child.paired:
                buddies.append((parent.mapping, child.mapping))
                child.paired = True
                break
    return buddies


def find_my_buddy(mapping, buddies):
    """Finds mapping's buddy.

    Args:
        mapping (ExportMapping): a mapping
        buddies (list of tuple): list of mapping - buddy mapping pairs

    Returns:
        ExportMapping: buddy mapping or None if not found
    """
    for parent, buddy in buddies:
        if mapping is parent:
            return buddy
    return None


def from_dict(serialized):
    """
    Deserializes mappings.

    Args:
        serialized (list): serialize mappings

    Returns:
        ExportMapping: root mapping
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
            ObjectGroupObjectMapping,
            ObjectMapping,
            ParameterDefinitionMapping,
            ParameterValueIndexMapping,
            ParameterValueListMapping,
            ParameterValueListValueMapping,
            ParameterValueMapping,
            ParameterValueTypeMapping,
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
