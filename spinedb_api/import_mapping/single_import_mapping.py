######################################################################################################################
# Copyright (C) 2017 - 2020 Spine project consortium
# This file is part of Spine Database API.
# Spine Database API is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser
# General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

"""
Classes for single import mappings.

:author: P. Vennström (VTT)
:date:   22.02.2018
"""
import itertools
from operator import itemgetter
from ..exception import InvalidMapping


class SingleMappingBase:
    """
    Base class for single mappings:

        Mapping {
            map_type: 'column' | 'row'
            value_reference: str | int
        }
    """

    MAP_TYPE = None

    def __init__(self, reference=None):

        # this needs to be before value_reference because value_reference uses
        # self.map_type
        self._reference = None
        self.reference = reference

    @property
    def reference(self):
        return self._reference

    @reference.setter
    def reference(self, reference):
        """Setter method for reference, should be implemented in subclasses
        """
        raise NotImplementedError()

    def is_pivoted(self):
        """Should return True if Mapping type is reading columns in a row, pivoted."""
        raise NotImplementedError()

    def last_pivot_row(self):
        """Returns the last row that is pivoted"""
        return -1

    def to_dict(self):
        """Creates a dict representation of mapping, should be compatible with json.dumps and json.loads"""
        map_dict = {"map_type": self.MAP_TYPE}
        if self._reference is not None:
            map_dict["reference"] = self._reference
        return map_dict

    @classmethod
    def from_dict(cls, map_dict):
        """Creates a mapping object from dict representation of mapping

        Should return an instance of the subclass
        """
        raise NotImplementedError()

    def is_valid(self):
        """Should return True or False if mapping is ready to read data.
        """
        return self.reference is not None

    def returns_value(self):
        return self.is_valid()

    def create_getter_function(self, pivoted_columns, pivoted_data, data_header):
        """
        Creates a callable that maps a reference to a value.

        Args:
            pivoted_columns
            pivoted_data
            data_header (list): a list of header names
        Returns:
            function: the getter callable or None; 
            int: total reference count or None;
            bool: True if the getter read data, False or None otherwise
        """
        raise NotImplementedError()


class NoneMapping(SingleMappingBase):
    """A reference to a None value."""

    MAP_TYPE = "None"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @SingleMappingBase.reference.setter
    def reference(self, reference):
        """Setter method for reference, ignored by NoneMapping.
        """

    def is_pivoted(self):
        """Should return True if Mapping type is reading columns in a row, pivoted."""
        return False

    @classmethod
    def from_dict(cls, map_dict):
        """Creates a mapping object from dict representation of mapping

        Should return an instance of the subclass
        """
        if not isinstance(map_dict, dict):
            raise TypeError(f"map_dict must be a dict, instead got {type(map_dict).__name__}")
        map_type = map_dict.get("map_type", None)
        if map_type is not None and map_type != cls.MAP_TYPE:
            raise ValueError(f"If field 'map_type' is specified, it must be {cls.MAP_TYPE}, instead got {map_type}")
        return NoneMapping()

    def to_dict(self):
        return {"map_type": self.MAP_TYPE}

    def returns_value(self):
        return False

    def is_valid(self):
        return True

    def create_getter_function(self, pivoted_columns, pivoted_data, data_header):
        return None, None, None


class ConstantMapping(SingleMappingBase):
    """A reference to a string.
    """

    MAP_TYPE = "constant"

    @SingleMappingBase.reference.setter
    def reference(self, reference):
        if reference is not None and not isinstance(reference, str):
            raise TypeError(f"reference must be str or None, instead got: {type(reference).__name__}")
        if not reference:
            reference = None
        self._reference = reference

    def is_pivoted(self):
        """Should return True if Mapping type is reading columns in a row, pivoted."""
        return False

    @classmethod
    def from_dict(cls, map_dict):
        """Creates a mapping object from dict representation of mapping
        
        Should return an instance of the subclass
        """
        if not isinstance(map_dict, dict):
            raise TypeError(f"map_dict must be a dict, instead got {type(map_dict).__name__}")
        map_type = map_dict.get("map_type", None)
        if map_type is not None and map_type != cls.MAP_TYPE:
            raise ValueError(f"If field 'map_type' is specified, it must be {cls.MAP_TYPE}, instead got {map_type}")
        reference = map_dict.get("reference", None)
        if reference is None:
            reference = map_dict.get("value_reference", None)
        return cls(reference)

    def create_getter_function(self, pivoted_columns, pivoted_data, data_header):
        constant = str(self.reference)

        def getter(_):
            return constant

        return getter, 1, False


class ColumnMapping(SingleMappingBase):
    """A reference to a column by number or header string
    """

    MAP_TYPE = "column"
    """Type of ``ColumnMapping``."""

    @SingleMappingBase.reference.setter
    def reference(self, reference):
        """Setter method for reference, should be implemented in subclasses
        """
        if reference is not None and not isinstance(reference, (str, int)):
            raise TypeError(f"reference must be int, str or None, instead got: {type(reference).__name__}")
        if isinstance(reference, str):
            if not reference:
                reference = None
            else:
                try:
                    reference = int(reference)
                except ValueError:
                    pass
        if isinstance(reference, int) and reference < 0:
            reference = 0
        self._reference = reference

    def is_pivoted(self):
        """Should return True if Mapping type is reading columns in a row, pivoted."""
        return False

    @classmethod
    def from_dict(cls, map_dict):
        """Creates a mapping object from dict representation of mapping
        
        Should return an instance of the subclass
        """
        if not isinstance(map_dict, dict):
            raise TypeError(f"map_dict must be a dict, instead got {type(map_dict).__name__}")
        map_type = map_dict.get("map_type", None)
        if map_type is not None and map_type != cls.MAP_TYPE:
            raise ValueError(f"If field 'map_type' is specified, it must be {cls.MAP_TYPE}, instead got {map_type}")
        reference = map_dict.get("reference", None)
        if reference is None:
            reference = map_dict.get("value_reference", None)
        return cls(reference)

    def create_getter_function(self, pivoted_columns, pivoted_data, data_header):
        ref = self.reference
        if isinstance(ref, str):
            try:
                ref = data_header.index(ref)
            except ValueError:
                raise InvalidMapping(f"Column header '{ref}' not found")
        getter = itemgetter(ref)
        num = 1
        reads_data = True
        return getter, num, reads_data


class ColumnHeaderMapping(ColumnMapping):
    """A reference to a column header by number or header string
    """

    MAP_TYPE = "column_header"

    @SingleMappingBase.reference.setter
    def reference(self, reference):
        if reference is not None and not isinstance(reference, (str, int)):
            raise TypeError(f"reference must be int, str or None, instead got: {type(reference).__name__}")
        if isinstance(reference, str) and not reference:
            reference = None
        if isinstance(reference, int) and reference < 0:
            reference = 0
        self._reference = reference

    def create_getter_function(self, pivoted_columns, pivoted_data, data_header):
        ref = self.reference
        if isinstance(ref, str):
            try:
                ref = data_header.index(ref)
            except ValueError:
                if not ref.isdigit():
                    raise IndexError(
                        f'mapping contains string reference to data header but reference "{ref}"'
                        ' could not be found in header.'
                    )
                try:
                    ref = int(ref)
                except ValueError:
                    raise IndexError(
                        f'mapping contains string reference to data header but reference "{ref}"'
                        ' could not be found in header.'
                    )
        if not 0 <= ref < len(data_header):
            raise IndexError(f'Reference index to column header should be between 0 and {len(data_header)}, got {ref}.')
        constant = data_header[ref]

        def getter(_):
            return constant

        num = 1
        reads_data = False
        return getter, num, reads_data


class RowMapping(SingleMappingBase):
    """A reference to a row number, where -1 refers to the header row.
    """

    MAP_TYPE = "row"
    """The type of ``RowMapping``."""

    @SingleMappingBase.reference.setter
    def reference(self, reference):
        """Setter method for reference, should be implemented in subclasses
        """
        if reference is not None and not isinstance(reference, (int, str)):
            raise TypeError(f"reference must be int or None, instead got: {type(reference).__name__}")
        if isinstance(reference, str):
            if not reference:
                reference = None
            elif reference.isdigit():
                reference = int(reference)
            else:
                if reference.lower() != "header":
                    raise ValueError(f"If reference is a string, it must be 'header'. Instead got '{reference}'")
                reference = -1
        if reference is not None and reference < -1:
            reference = 0
        self._reference = reference

    def is_pivoted(self):
        """Should return True if Mapping type is reading columns in a row, pivoted."""
        return True

    def last_pivot_row(self):
        """Returns the last row that is pivoted"""
        if self._reference is None:
            return -1
        return self._reference

    @classmethod
    def from_dict(cls, map_dict):
        """Creates a mapping object from dict representation of mapping
        
        Should return an instance of the subclass
        """
        if not isinstance(map_dict, dict):
            raise TypeError(f"map_dict must be a dict, instead got {type(map_dict).__name__}")
        map_type = map_dict.get("map_type", None)
        if map_type is not None and map_type != cls.MAP_TYPE:
            raise ValueError(f"If field 'map_type' is specified, it must be {cls.MAP_TYPE}, instead got {map_type}")
        reference = map_dict.get("reference", None)
        return RowMapping(reference)

    def create_getter_function(self, pivoted_columns, pivoted_data, data_header):
        if self.reference == -1:
            # special case, read pivoted rows from data header instead
            # of pivoted data.
            read_from = data_header
        else:
            read_from = pivoted_data[self.reference]
        if pivoted_columns:
            piv_values = [read_from[i] for i in pivoted_columns]
            num = len(piv_values)
            if len(piv_values) == 1:
                piv_values = piv_values[0]

            def getter(_):
                return piv_values

            reads_data = False
        else:
            # no data
            getter = None
            num = None
            reads_data = None
        return getter, num, reads_data


class TableNameMapping(SingleMappingBase):
    """A reference to a table name."""

    MAP_TYPE = "table_name"

    def __init__(self, table_name):
        super().__init__(table_name)

    @SingleMappingBase.reference.setter
    def reference(self, reference):
        if reference is not None and not isinstance(reference, str):
            raise TypeError(f"reference must be a string or None, instead got {type(reference).__name__}")
        if not reference:
            reference = None
        self._reference = reference

    def is_pivoted(self):
        """Returns False."""
        return False

    def to_dict(self):
        """Creates a dict representation of mapping, should be compatible with json.dumps and json.loads"""
        map_dict = {"map_type": self.MAP_TYPE}
        if self._reference is not None:
            map_dict.update({"reference": self._reference})
        return map_dict

    @classmethod
    def from_dict(cls, map_dict):
        """Creates a mapping object from dict representation of mapping."""
        if not isinstance(map_dict, dict):
            raise TypeError(f"map_dict must be a dict, instead got {type(map_dict).__name__}")
        map_type = map_dict.get("map_type", None)
        if map_type is not None and map_type != cls.MAP_TYPE:
            raise ValueError(f"If field 'map_type' is specified, it must be {cls.MAP_TYPE}, instead got {map_type}")
        reference = map_dict.get("reference", None)
        return cls(reference)

    def is_valid(self):
        return True

    def returns_value(self):
        return True

    def create_getter_function(self, pivoted_columns, pivoted_data, data_header):
        table_name = str(self._reference)

        def getter(_):
            return table_name

        return getter, 1, False


def single_mapping_from_int_str(value):
    if isinstance(value, int):
        try:
            return ColumnMapping(value)
        except ValueError:
            pass
    if isinstance(value, str):
        return ConstantMapping(value)
    raise TypeError(f"Can't convert {type(value).__name__} to SingleMappingBase")


def single_mapping_from_dict(map_dict):
    type_str_to_class = {
        class_.MAP_TYPE: class_
        for class_ in (RowMapping, ColumnMapping, ColumnHeaderMapping, ConstantMapping, TableNameMapping, NoneMapping,)
    }
    map_type_str = map_dict.get("map_type", None)
    if map_type_str == "column_name":
        map_type_str = map_dict["map_type"] = ColumnHeaderMapping.MAP_TYPE
    if "value_reference" in map_dict and "reference" not in map_dict:
        map_dict["reference"] = map_dict["value_reference"]
    map_class = type_str_to_class.get(map_type_str, NoneMapping)
    return map_class.from_dict(map_dict)


def single_mapping_from_value(value):
    """Creates SingleMappingBase derived object from given value.
    """
    if value is None:
        return NoneMapping()
    if isinstance(value, SingleMappingBase):
        return value
    if isinstance(value, dict):
        return single_mapping_from_dict(value)
    if isinstance(value, (int, str)):
        return single_mapping_from_int_str(value)
    raise TypeError(f"value must be dict, int or str, instead got {type(value)}")


def create_getter_list(mapping, pivoted_columns, pivoted_data, data_header):
    """Creates a list of getter functions from a list of Mappings"""
    getter_list = []
    num_list = []
    reads_list = []
    for map_ in mapping:
        o, num, reads = map_.create_getter_function(pivoted_columns, pivoted_data, data_header)
        getter_list.append(o)
        num_list.append(num)
        reads_list.append(reads)
    return getter_list, num_list, reads_list


def create_final_getter_function(function_list, function_output_len_list, reads_data_list):
    """Creates a single function that will return a list of items
    If there are any None in the function_list then return an empty list
    """
    if None in function_list:
        # invalid data return empty list
        def getter(row):
            return []

        reads_data = False
    else:
        # valid return list
        getter, _, reads_data = create_getter_function_from_function_list(
            function_list, function_output_len_list, reads_data_list, True
        )
    return getter, reads_data


def create_getter_function_from_function_list(function_list, len_output_list, reads_data_list, list_wrap=False):
    """Function that takes a list of getter functions and returns one function
    that zips together the result into a list,

    Example::

        row = (1,2,3)
        function_list = [lambda row: row[2], lambda row: 'constant', itemgetter(0,1)]
        row_getter, output_len, reads_data = create_getter_function_from_function_list(function_list, [1,1,2], [True, False, True])
        list(row_getter(row)) == [(3, 'constant', 1), (3, 'constant', 2)]


        # nested
        row = (1,2,3)
        function_list_inner = [lambda row: 'inner', itemgetter(0,1)]
        inner_function, inner_len, reads_data = create_getter_function_from_function_list(function_list_inner, [1,2], [False, True])

        function_list = [lambda row: 'outer', inner_function]
        row_getter, output_len, reads_data = create_getter_function_from_function_list(function_list, [1,2], [False, reads_data])
        list(row_getter(row)) == [('outer', ('inner', 1)), ('outer', ('inner', 2))]
    """
    if not function_list or None in function_list:
        # empty or incomplete list
        return None, None, None

    if not all(l in (l, max(len_output_list)) for l in len_output_list):
        raise ValueError("each element in len_output_list must be 1 or max(len_output_list)")

    if any(n > 1 for n in len_output_list):
        # not all getters return one value, some will return more. repeat the
        # ones that return only one value, the getters should only return
        # one value or the same number if n > 1
        def create_repeat_function(f):
            def func(row):
                return itertools.repeat(f(row))

            return func

        for i, (g, num) in enumerate(zip(function_list, len_output_list)):
            if num == 1:
                function_list[i] = create_repeat_function(g)
        return_len = max(len_output_list)
        if len(function_list) == 1:
            f = function_list[0]

            def getter(row):
                return list(f(row))

        else:

            def getter(row):
                return zip(*[g(row) for g in function_list])

    elif all(n == 1 for n in len_output_list):
        # all getters return one element, just make a tuple
        return_len = 1
        if len(function_list) == 1:
            f = function_list[0]
            if list_wrap:

                def getter(row):
                    return [f(row)]

            else:
                getter = f
        else:
            if list_wrap:

                def getter(row):
                    return [tuple(f(row) for f in function_list)]

            else:

                def getter(row):
                    return tuple(f(row) for f in function_list)

    if not any(reads_data_list):
        # since none of the functions are actually reading new data we
        # can simplify the function to just return the value.
        value = getter(None)
        if return_len > 1:
            value = list(value)

        def getter(row):
            return value

    reads_data = any(reads_data_list)
    return getter, return_len, reads_data
