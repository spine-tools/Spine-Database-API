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
Classes for reading data with json mapping specifications

:author: P. Vennström (VTT)
:date:   22.02.2018
"""

import itertools
import math
from collections.abc import Iterable
from operator import itemgetter
from .parameter_value import Array, Map, TimeSeriesVariableResolution, TimePattern, ParameterValueFormatError
from .exception import InvalidMapping, TypeConversionError


class MappingBase:
    """
    Class for holding and validating Mapping specification:
    
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
            tuple: the getter callable or None; total reference count or None;
                True if the getter read data, False or None otherwise
        """
        raise NotImplementedError()


class NoneMapping(MappingBase):
    """Class for holding a reference to a column by number or header string
    """

    MAP_TYPE = "None"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @MappingBase.reference.setter
    def reference(self, reference):
        """Setter method for reference, ignored by NoneMapping.
        """

    def is_pivoted(self):
        """Should return True if Mapping type is reading columns in a row, pivoted."""
        return False

    def last_pivot_row(self):
        """Returns the last row that is pivoted"""
        return -1

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


class ConstantMapping(MappingBase):
    """Class for holding a reference to a string.
    """

    MAP_TYPE = "constant"

    @MappingBase.reference.setter
    def reference(self, reference):
        if reference is not None and not isinstance(reference, str):
            raise TypeError(f"reference must be str or None, instead got: {type(reference).__name__}")
        if not reference:
            reference = None
        self._reference = reference

    def is_pivoted(self):
        """Should return True if Mapping type is reading columns in a row, pivoted."""
        return False

    def last_pivot_row(self):
        """Returns the last row that is pivoted"""
        return -1

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


class ColumnMapping(ConstantMapping):
    """Class for holding a reference to a column by number or header string
    """

    MAP_TYPE = "column"
    """Type of ``ColumnMapping``."""

    @MappingBase.reference.setter
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
    """Class for holding a reference to a column header by number or header string
    """

    MAP_TYPE = "column_header"

    @MappingBase.reference.setter
    def reference(self, reference):
        if reference is not None and not isinstance(reference, (str, int)):
            raise TypeError(f"reference must be str or None, instead got: {type(reference).__name__}")
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


class RowMapping(MappingBase):
    """Class for holding a reference to a row number or headers
    """

    MAP_TYPE = "row"
    """The type of ``RowMapping``."""

    @MappingBase.reference.setter
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


class TableNameMapping(MappingBase):
    """A mapping for table names."""

    MAP_TYPE = "table_name"

    def __init__(self, table_name):
        super().__init__(table_name)

    @MappingBase.reference.setter
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


class TimeSeriesOptions:
    """
    Class for holding parameter type-specific options for time series parameter values.

    Attributes:
        repeat (bool): time series repeat flag
        ignore_year (bool): time series ignore year flag
        fixed_resolution (bool): True for fixed resolution time series, False for variable resolution
    """

    def __init__(self, repeat=False, ignore_year=False, fixed_resolution=False):
        self.repeat = repeat
        self.ignore_year = ignore_year
        self.fixed_resolution = fixed_resolution

    @staticmethod
    def from_dict(options_dict):
        """Restores TimeSeriesOptions from a dictionary."""
        repeat = options_dict.get("repeat", False)
        ignore_year = options_dict.get("ignore_year", False)
        fixed_resolution = options_dict.get("fixed_resolution", False)
        return TimeSeriesOptions(repeat, ignore_year, fixed_resolution)

    def to_dict(self):
        """Saves the options to a dictionary."""
        return {"repeat": self.repeat, "ignore_year": self.ignore_year, "fixed_resolution": self.fixed_resolution}


class ParameterDefinitionMapping:
    PARAMETER_TYPE = "definition"
    MAP_TYPE = "parameter"

    def __init__(self, name=None):
        self._name = ColumnMapping(None)
        self.name = name

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = mappingbase_from_dict_int_str(name)

    def non_pivoted_columns(self):
        non_pivoted_columns = []
        if isinstance(self.name, ColumnMapping) and self.name.returns_value():
            non_pivoted_columns.append(self.name.reference)
        return non_pivoted_columns

    def last_pivot_row(self):
        return self.name.last_pivot_row()

    def is_pivoted(self):
        return self.name.is_pivoted()

    @classmethod
    def from_dict(cls, map_dict):
        if not isinstance(map_dict, dict):
            raise ValueError("map_dict must be a dict")
        map_type = map_dict.get("map_type", None)
        if map_type is not None and map_type != cls.MAP_TYPE:
            raise ValueError(f"If field 'map_type' is specified, it must be {cls.MAP_TYPE}, instead got {map_type}")
        name = map_dict.get("name", None)
        return ParameterDefinitionMapping(name)

    def to_dict(self):
        map_dict = {"map_type": self.MAP_TYPE}
        map_dict.update({"name": self.name.to_dict()})
        map_dict.update({"parameter_type": self.PARAMETER_TYPE})
        return map_dict

    def is_valid(self, parent_pivot: bool):
        # check that parameter mapping has a valid name mapping
        issue = self.names_issues()
        if issue:
            return False, issue
        return True, ""

    def names_issues(self):
        if isinstance(self._name, NoneMapping):
            return "The source type for parameter names cannot be None."
        if self._name.reference != 0 and not self._name.reference:
            return "No reference set for parameter names."
        return ""

    def create_getter_list(self, is_pivoted, pivoted_columns, pivoted_data, data_header):
        if self.name.returns_value():
            getter, num, reads = self.name.create_getter_function(pivoted_columns, pivoted_data, data_header)
        else:
            getter, num, reads = (None, None, None)
        return {"name": (getter, num, reads)}


class ParameterValueMapping(ParameterDefinitionMapping):
    PARAMETER_TYPE = "single value"

    def __init__(self, name=None, value=None):
        super().__init__(name)
        self._value = ColumnMapping(None)
        self.value = value

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        self._value = mappingbase_from_dict_int_str(value)

    def non_pivoted_columns(self):
        non_pivoted_columns = super().non_pivoted_columns()
        if isinstance(self.value, ColumnMapping) and self.value.returns_value():
            non_pivoted_columns.append(self.value.reference)
        return non_pivoted_columns

    def last_pivot_row(self):
        return max(super().last_pivot_row(), self.value.last_pivot_row())

    def is_pivoted(self):
        return super().is_pivoted() or self.value.is_pivoted()

    @classmethod
    def from_dict(cls, map_dict):
        if not isinstance(map_dict, dict):
            raise ValueError("map_dict must be a dict")
        map_type = map_dict.get("map_type", None)
        if map_type is not None and map_type != cls.MAP_TYPE:
            raise ValueError(f"If field 'map_type' is specified, it must be {cls.MAP_TYPE}, instead got {map_type}")
        name = map_dict.get("name", None)
        value = map_dict.get("value", None)
        return ParameterValueMapping(name, value)

    def to_dict(self):
        map_dict = super().to_dict()
        map_dict["map_type"] = self.MAP_TYPE
        map_dict.update({"parameter_type": self.PARAMETER_TYPE})
        map_dict.update({"value": self.value.to_dict()})
        return map_dict

    def is_valid(self, parent_pivot: bool):
        # check that parameter mapping has a valid name mapping
        name_valid, msg = super().is_valid(parent_pivot)
        if not name_valid:
            return False, msg
        issue = self.values_issues(parent_pivot)
        if issue:
            return False, issue
        return True, ""

    def values_issues(self, parent_pivot):
        if not (self.is_pivoted() or parent_pivot):
            if isinstance(self._value, NoneMapping):
                return "The source type for parameter values cannot be None."
            if self._value.reference != 0 and not self._value.reference:
                return "No reference set for parameter values."
        return ""

    def create_getter_list(self, is_pivoted, pivoted_columns, pivoted_data, data_header):
        getters = super().create_getter_list(is_pivoted, pivoted_columns, pivoted_data, data_header)
        num, getter, reads = (None, None, None)
        if (is_pivoted or self.is_pivoted()) and not self.value.is_pivoted():
            # if mapping is pivoted values for parameters are read from pivoted data
            if pivoted_columns:
                num = len(pivoted_columns)
                getter = itemgetter(*pivoted_columns)
                reads = True
        elif self.value.returns_value():
            getter, num, reads = self.value.create_getter_function(pivoted_columns, pivoted_data, data_header)
        getters["value"] = (getter, num, reads)
        return getters

    def raw_data_to_type(self, data):
        return data


class ParameterArrayMapping(ParameterValueMapping):
    ALLOW_EXTRA_DIMENSION_NO_RETURN = True
    PARAMETER_TYPE = "array"

    def __init__(self, name=None, value=None, extra_dimension=None):
        super().__init__(name, value)
        self._extra_dimensions = None
        self.extra_dimensions = extra_dimension

    @property
    def extra_dimensions(self):
        return self._extra_dimensions

    @extra_dimensions.setter
    def extra_dimensions(self, extra_dimensions):
        if extra_dimensions is None:
            self._extra_dimensions = [mappingbase_from_dict_int_str(None)]
            return
        if not isinstance(extra_dimensions, (list, tuple)):
            raise TypeError(
                f"extra_dimensions must be a list or tuple of MappingBase, int, str, dict, instead got: {type(extra_dimensions).__name__}"
            )
        if len(extra_dimensions) != 1:
            raise ValueError(f"extra_dimensions must be of length 1 instead got len: {len(extra_dimensions)}")
        self._extra_dimensions = [mappingbase_from_dict_int_str(extra_dimensions[0])]

    def non_pivoted_columns(self):
        non_pivoted_columns = super().non_pivoted_columns()
        non_pivoted_columns.extend(
            ed.reference for ed in self.extra_dimensions if isinstance(ed, ColumnMapping) and ed.returns_value()
        )
        return non_pivoted_columns

    def last_pivot_row(self):
        return max(super().last_pivot_row(), max(ed.last_pivot_row() for ed in self.extra_dimensions))

    def is_pivoted(self):
        return super().is_pivoted() or any(ed.is_pivoted() for ed in self.extra_dimensions)

    @classmethod
    def from_dict(cls, map_dict):
        if not isinstance(map_dict, dict):
            raise ValueError("map_dict must be a dict")
        map_type = map_dict.get("map_type", None)
        parameter_type = map_dict.get("parameter_type", None)
        if map_type is not None and map_type != cls.MAP_TYPE:
            raise ValueError(f"If field 'map_type' is specified, it must be {cls.MAP_TYPE}, instead got {map_type}")
        if parameter_type is not None and parameter_type != cls.PARAMETER_TYPE:
            raise ValueError(
                f"If field 'parameter_type' is specified, it must be {cls.PARAMETER_TYPE}, instead got {parameter_type}"
            )

        name = map_dict.get("name", None)
        value = map_dict.get("value", None)
        extra_dimensions = map_dict.get("extra_dimensions", None)
        return cls(name, value, extra_dimensions)

    def to_dict(self):
        map_dict = super().to_dict()
        map_dict["map_type"] = self.MAP_TYPE
        map_dict["parameter_type"] = self.PARAMETER_TYPE
        map_dict["extra_dimensions"] = [ed.to_dict() for ed in self.extra_dimensions]
        return map_dict

    def create_getter_list(self, is_pivoted, pivoted_columns, pivoted_data, data_header):
        getters = super().create_getter_list(is_pivoted, pivoted_columns, pivoted_data, data_header)
        value_getter, value_num, value_reads = getters["value"]
        if value_getter is None:
            return getters
        has_ed = False
        if all(ed.returns_value() for ed in self.extra_dimensions):
            # create functions to get extra_dimensions if there is a value getter
            ed_getters, ed_num, ed_reads_data = create_getter_list(
                self.extra_dimensions, pivoted_columns, pivoted_data, data_header
            )
            value_getter = ed_getters + [value_getter]
            value_num = ed_num + [value_num]
            value_reads = ed_reads_data + [value_reads]
            # create a function that returns a tuple with extra dimensions and value
            value_getter, value_num, value_reads = create_getter_function_from_function_list(
                value_getter, value_num, value_reads
            )
            has_ed = True
        elif not self.ALLOW_EXTRA_DIMENSION_NO_RETURN:
            # extra dimensions doesn't return anything so don't read anything from the data source
            value_getter, value_num, value_reads = (None, None, None)

        getters["value"] = (value_getter, value_num, value_reads)
        getters["has_extra_dimensions"]: has_ed

        return getters

    def raw_data_to_type(self, data):
        out = []
        data = sorted(data, key=lambda x: x[:-1])
        if self.extra_dimensions[0].returns_value():
            for keys, values in itertools.groupby(data, key=lambda x: x[:-1]):
                values = [value[-1][-1] for value in values if value[-1][-1] is not None]
                if values:
                    out.append(keys + (values,))
        else:
            for keys, values in itertools.groupby(data, key=lambda x: x[:-1]):
                values = [value[-1] for value in values if value[-1] is not None]
                if values:
                    out.append(keys + (Array(values),))
        return out


class ParameterIndexedMapping(ParameterArrayMapping):
    def is_valid(self, parent_pivot: bool):
        valid, msg = super().is_valid(parent_pivot)
        if not valid:
            return False, msg
        for i in range(len(self.extra_dimensions)):
            issue = self.indexes_issues(i)
            if issue:
                return False, f"Extra dimension {i + 1}: {issue}"
        return True, ""

    def indexes_issues(self, dimension_index):
        dimension = self._extra_dimensions[dimension_index]
        if isinstance(dimension, NoneMapping):
            return "The source type for parameter indexes cannot be None."
        if dimension.reference != 0 and not dimension.reference:
            return "No reference set for parameter indexes."
        return ""


class ParameterMapMapping(ParameterIndexedMapping):
    PARAMETER_TYPE = "map"
    ALLOW_EXTRA_DIMENSION_NO_RETURN = False

    @ParameterIndexedMapping.extra_dimensions.setter
    def extra_dimensions(self, extra_dimensions):
        if extra_dimensions is None:
            self._extra_dimensions = [mappingbase_from_dict_int_str(None)]
            return
        if not isinstance(extra_dimensions, (list, tuple)):
            raise TypeError(
                f"extra_dimensions must be a list or tuple of MappingBase, int, str, dict, instead got: {type(extra_dimensions).__name__}"
            )
        self._extra_dimensions = [mappingbase_from_dict_int_str(ed) for ed in extra_dimensions]

    def set_number_of_extra_dimensions(self, dimension_count):
        """
        Changes the number of dimensions in the map.

        Args:
            dimension_count (int): number of map's dimensions
        """
        if dimension_count < 1:
            raise InvalidMapping("Map cannot have less than one dimension.")
        if dimension_count <= len(self._extra_dimensions):
            self._extra_dimensions = self._extra_dimensions[:dimension_count]
        else:
            diff = dimension_count - len(self._extra_dimensions)
            self._extra_dimensions += [mappingbase_from_dict_int_str(None) for _ in range(diff)]

    def raw_data_to_type(self, data):
        out = []
        data = sorted(data, key=lambda x: x[:-1])
        for keys, values in itertools.groupby(data, key=lambda x: x[:-1]):
            values = [items[-1] for items in values if all(i is not None for i in items[-1])]
            if values:
                map_as_dict = self._raw_data_to_dict(values)
                map_ = self._convert_dict_to_map(map_as_dict)
                out.append(keys + (map_,))
        return out

    @staticmethod
    def _raw_data_to_dict(values):
        row_length = len(values[0])
        map_as_dict = dict()
        for row in values:
            current_dict = map_as_dict
            for i, cell in enumerate(row):
                future = i + 2
                if future == row_length or row[future] is None or row[future] == "":
                    current_dict[cell] = row[i + 1]
                    break
                new_dict = current_dict.setdefault(cell, dict())
                current_dict = new_dict
        return map_as_dict

    @staticmethod
    def _convert_dict_to_map(map_as_dict):
        indexes = list()
        values = list()
        for key, value in map_as_dict.items():
            if isinstance(value, dict):
                value = ParameterMapMapping._convert_dict_to_map(value)
            indexes.append(key)
            values.append(value)
        return Map(indexes, values)


class ParameterTimeSeriesMapping(ParameterIndexedMapping):
    PARAMETER_TYPE = "time series"
    ALLOW_EXTRA_DIMENSION_NO_RETURN = False

    def __init__(self, name=None, value=None, extra_dimension=None, options=None):
        super().__init__(name, value, extra_dimension)
        self._options = None
        self.options = options

    @property
    def options(self):
        return self._options

    @options.setter
    def options(self, options):
        if options is None:
            options = TimeSeriesOptions()
        if not isinstance(options, TimeSeriesOptions):
            raise TypeError(f"options must be a TimeSeriesOptions, instead got: {type(options).__name__}")
        self._options = options

    @classmethod
    def from_dict(cls, map_dict):
        if not isinstance(map_dict, dict):
            raise ValueError("map_dict must be a dict")
        map_type = map_dict.get("map_type", None)
        parameter_type = map_dict.get("parameter_type", None)
        if map_type is not None and map_type != cls.MAP_TYPE:
            raise ValueError(f"If field 'map_type' is specified, it must be {cls.MAP_TYPE}, instead got {map_type}")
        if parameter_type is not None and parameter_type != cls.PARAMETER_TYPE:
            raise ValueError(
                f"If field 'parameter_type' is specified, it must be {cls.PARAMETER_TYPE}, instead got {parameter_type}"
            )

        name = map_dict.get("name", None)
        value = map_dict.get("value", None)
        extra_dimensions = map_dict.get("extra_dimensions", None)
        options = map_dict.get("options", None)
        if options:
            options = TimeSeriesOptions.from_dict(options)
        return cls(name, value, extra_dimensions, options)

    def to_dict(self):
        map_dict = super().to_dict()
        map_dict["options"] = self.options.to_dict()
        return map_dict

    def raw_data_to_type(self, data):
        out = []
        data = sorted(data, key=lambda x: x[:-1])
        for keys, values in itertools.groupby(data, key=lambda x: x[:-1]):
            values = [items[-1] for items in values if all(i is not None for i in items[-1])]
            if values:
                indexes = [items[0] for items in values]
                values = [items[1] for items in values]
                out.append(
                    keys
                    + (TimeSeriesVariableResolution(indexes, values, self.options.ignore_year, self.options.repeat),)
                )
        return out


class ParameterTimePatternMapping(ParameterIndexedMapping):
    PARAMETER_TYPE = "time pattern"
    ALLOW_EXTRA_DIMENSION_NO_RETURN = False

    def raw_data_to_type(self, data):
        out = []
        data = sorted(data, key=lambda x: x[:-1])
        for keys, values in itertools.groupby(data, key=lambda x: x[:-1]):
            values = [items[-1] for items in values if all(i is not None for i in items[-1])]
            if values:
                indexes = [items[0] for items in values]
                values = [items[1] for items in values]
                out.append(keys + (TimePattern(indexes, values),))
        return out


class ItemMappingBase:
    """A base class for top level item mappings."""

    MAP_TYPE = None
    """Mapping's name in JSON. Should be specified by subclasses"""

    def __init__(self, skip_columns, read_start_row):
        """
        Args:
            skip_columns (list, optional): a list of columns to skip while mapping
            read_start_row (int): skip this many rows while mapping
        """
        self._skip_columns = []
        self._read_start_row = 0
        self.skip_columns = skip_columns
        self.read_start_row = read_start_row

    @property
    def skip_columns(self):
        return self._skip_columns

    @skip_columns.setter
    def skip_columns(self, skip_columns=None):
        if skip_columns is None:
            self._skip_columns = []
        else:
            if isinstance(skip_columns, (str, int)):
                skip_columns = [skip_columns]
            if isinstance(skip_columns, list):
                for i, column in enumerate(skip_columns):
                    if not isinstance(column, (str, int)):
                        raise TypeError(
                            f"""skip_columns must be str, int or
                                        list of str, int, instead got list
                                        with {type(column).__name__} on index {i}"""
                        )
            else:
                raise TypeError(
                    f"""skip_columns must be str, int or list of
                                str, int, instead {type(skip_columns).__name__}"""
                )
            self._skip_columns = skip_columns

    @property
    def read_start_row(self):
        return self._read_start_row

    @read_start_row.setter
    def read_start_row(self, row):
        if not isinstance(row, int):
            raise TypeError(f"row must be int, instead got {type(row).__name__}")
        if row < 0:
            raise ValueError(f"row must be >= 0, instead was: {row}")
        self._read_start_row = row

    def has_parameters(self):
        """Returns True if this mapping has parameters, otherwise returns False."""
        return False

    def is_valid(self):
        raise NotImplementedError()

    def is_pivoted(self):
        raise NotImplementedError()

    def non_pivoted_columns(self):
        raise NotImplementedError()

    def pivoted_columns(self, data_header, num_cols):
        if not self.is_pivoted():
            return []
        # make sure all column references are found
        non_pivoted_columns = self.non_pivoted_columns()
        int_non_piv_cols = []
        for pc in non_pivoted_columns:
            if isinstance(pc, str):
                try:
                    pc = data_header.index(pc)
                except ValueError:
                    if not pc.isdigit():
                        raise IndexError(
                            f'mapping contains string reference to data header but reference "{pc}"'
                            ' could not be found in header.'
                        )
                    try:
                        pc = int(pc)
                    except ValueError:
                        raise IndexError(
                            f'mapping contains string reference to data header but reference "{pc}"'
                            ' could not be found in header.'
                        )
            if not 0 <= pc < num_cols:
                raise IndexError(f'mapping contains invalid index: {pc}, data column number: {num_cols}')
            int_non_piv_cols.append(pc)
        # parameter column mapping is not in use and we have a pivoted mapping
        pivoted_cols = set(range(num_cols)).difference(set(int_non_piv_cols))
        # remove skipped columns
        for skip_c in self.skip_columns:
            if isinstance(skip_c, str):
                if skip_c in data_header:
                    skip_c = data_header.index(skip_c)
            pivoted_cols.discard(skip_c)
        return pivoted_cols

    def last_pivot_row(self):
        raise NotImplementedError()

    def create_mapping_readers(self, num_columns, pivoted_data, data_header):
        raise NotImplementedError()

    def to_dict(self):
        map_dict = {
            "map_type": self.MAP_TYPE,
            "skip_columns": self._skip_columns,
            "read_start_row": self._read_start_row,
        }
        return map_dict

    @classmethod
    def from_dict(cls, map_dict):
        raise NotImplementedError()

    @classmethod
    def from_instance(cls, instance):
        """Constructs a new object based on an existing object of possibly another subclass of ItemMappingBase."""
        raise NotImplementedError()


class NamedItemMapping(ItemMappingBase):
    """A base class for top level named item mappings such as entity classes, alternatives and scenarios."""

    def __init__(self, name, skip_columns, read_start_row):
        """
        Args:
            name (str or MappingBase, optional): mapping for the item name
            skip_columns (list, optional): a list of columns to skip while mapping
            read_start_row (int): skip this many rows while mapping
        """
        super().__init__(skip_columns, read_start_row)
        self._name = None
        self.name = name

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = mappingbase_from_dict_int_str(name)

    def is_pivoted(self):
        return self._name.is_pivoted()

    def last_pivot_row(self):
        last_pivot_row = max(self._name.last_pivot_row(), -1)
        return last_pivot_row

    def is_valid(self):
        raise NotImplementedError()

    def non_pivoted_columns(self):
        if isinstance(self._name, ColumnMapping) and self._name.returns_value():
            return [self._name.reference]
        return []

    def _create_getters(self, pivoted_columns, pivoted_data, data_header):
        if self.name.returns_value():
            return {"item_name": self.name.create_getter_function(pivoted_columns, pivoted_data, data_header)}
        return {"item_name": (None, None, None)}

    def create_mapping_readers(self, num_columns, pivoted_data, data_header):
        raise NotImplementedError()

    def to_dict(self):
        map_dict = super().to_dict()
        map_dict["name"] = self._name.to_dict()
        return map_dict

    @classmethod
    def from_dict(cls, map_dict):
        raise NotImplementedError()

    @classmethod
    def from_instance(cls, instance):
        """See base class."""
        raise NotImplementedError()


class EntityClassMapping(NamedItemMapping):
    """
    Class for holding and validating Mappings for entity classes.
    """

    MAP_TYPE = None

    def __init__(self, name, parameters, import_objects, skip_columns, read_start_row):
        """
        Args:
            name (str or spinedb_api.MappingBase, optional): mapping for the class name
            parameters (str or spinedb_api.ParameterDefinitionMapping, optional): mapping for the parameters of the class
            import_objects (bool): True if this mapping imports additional objects, False otherwise
            skip_columns (list, optional): a list of columns to skip while mapping
            read_start_row (int): skip this many rows while mapping
        """
        super().__init__(name, skip_columns, read_start_row)
        self._parameters = None
        self.parameters = parameters
        self._import_objects = import_objects

    @property
    def dimensions(self):
        """Number of dimensions in this entity class."""
        return 1

    def has_fixed_dimensions(self):
        """Returns True if the dimensions of this mapping cannot be set."""
        raise NotImplementedError()

    @property
    def import_objects(self):
        """True if this entity class also imports object entities, False otherwise."""
        return self._import_objects

    @import_objects.setter
    def import_objects(self, import_objects):
        raise NotImplementedError()

    def non_pivoted_columns(self):
        non_pivoted_columns = super().non_pivoted_columns()
        if isinstance(self.parameters, ParameterDefinitionMapping):
            non_pivoted_columns.extend(self.parameters.non_pivoted_columns())
        return non_pivoted_columns

    def last_pivot_row(self):
        last_pivot_row = super().last_pivot_row()
        last_pivot_row = max(self._parameters.last_pivot_row(), last_pivot_row)
        return last_pivot_row

    def is_pivoted(self):
        return super().is_pivoted() or self._parameters.is_pivoted()

    def has_parameters(self):
        """Returns True if this mapping has parameters, otherwise returns False."""
        return True

    @property
    def parameters(self):
        return self._parameters

    @parameters.setter
    def parameters(self, parameters=None):
        if parameters is None:
            parameters = NoneMapping()
        if not isinstance(parameters, (ParameterDefinitionMapping, NoneMapping)):
            raise ValueError(
                f"""parameters must be a None, ParameterDefinition or
                             NoneMapping, instead got
                             {type(parameters).__name__}"""
            )
        self._parameters = parameters

    def to_dict(self):
        map_dict = super().to_dict()
        map_dict["parameters"] = self.parameters.to_dict()
        map_dict["import_objects"] = self._import_objects
        return map_dict

    def is_valid(self):
        issue = self.class_names_issues()
        if issue:
            return False, issue
        if not isinstance(self._parameters, NoneMapping):
            parameter_valid, msg = self._parameters.is_valid(self.is_pivoted())
            if not parameter_valid:
                return False, msg
        return True, ""

    def class_names_issues(self):
        """Returns a non-empty message string if the entity class name is invalid."""
        if isinstance(self._name, NoneMapping):
            return "The source type for class names cannot be None."
        if self._name.reference != 0 and not self._name.reference:
            return "No reference set for class names."
        return ""

    def _create_getters(self, pivoted_columns, pivoted_data, data_header):
        """Creates a dict of getter functions."""
        getters = super()._create_getters(pivoted_columns, pivoted_data, data_header)
        if isinstance(self._parameters, ParameterDefinitionMapping):
            parameter_getters = self._parameters.create_getter_list(
                self.is_pivoted(), pivoted_columns, pivoted_data, data_header
            )
            if "name" in parameter_getters:
                parameter_getters["parameter_name"] = parameter_getters.pop("name")
            if "value" in parameter_getters:
                parameter_getters["parameter_value"] = parameter_getters.pop("value")
            getters.update(**parameter_getters)
        return getters

    def create_mapping_readers(self, num_columns, pivoted_data, data_header):
        raise NotImplementedError()

    @classmethod
    def from_dict(cls, map_dict):
        raise NotImplementedError()

    @classmethod
    def from_instance(cls, instance):
        """See base class."""
        raise NotImplementedError()


class ObjectClassMapping(EntityClassMapping):
    """
    Class for holding and validating Mapping specification::

        ObjectClassMapping {
            map_type: 'object'
            name: str | Mapping
            objects: Mapping | str | None
            parameters: ParameterMapping | ParameterColumnCollectionMapping | None
        }
    """

    MAP_TYPE = "ObjectClass"

    def __init__(self, name=None, objects=None, parameters=None, skip_columns=None, read_start_row=0):
        super().__init__(name, parameters, True, skip_columns, read_start_row)
        self._objects = NoneMapping()
        self.objects = objects

    def non_pivoted_columns(self):
        non_pivoted_columns = super().non_pivoted_columns()
        if isinstance(self.objects, ColumnMapping) and self.objects.returns_value():
            non_pivoted_columns.append(self.objects.reference)
        return non_pivoted_columns

    def last_pivot_row(self):
        return max(super().last_pivot_row(), self.objects.last_pivot_row())

    def is_pivoted(self):
        return super().is_pivoted() or self.objects.is_pivoted()

    @property
    def dimensions(self):
        return 1

    def has_fixed_dimensions(self):
        """See base class."""
        return True

    @EntityClassMapping.import_objects.setter
    def import_objects(self, import_objects):
        raise NotImplementedError()

    @property
    def objects(self):
        return self._objects

    @objects.setter
    def objects(self, objects):
        self._objects = mappingbase_from_dict_int_str(objects)

    @classmethod
    def from_dict(cls, map_dict):
        if not isinstance(map_dict, dict):
            raise TypeError(f"map_dict must be a dict, instead got {type(map_dict).__name__}")

        map_type = map_dict.get("map_type", None)
        if map_type is not None and map_type != cls.MAP_TYPE:
            raise ValueError(f"If field 'map_type' is specified, it must be {cls.MAP_TYPE}, instead got {map_type}")
        name = map_dict.get("name", None)
        objects = map_dict.get("objects", None)
        if objects is None:
            # previous versions saved "object" instead of "objects"
            objects = map_dict.get("object", None)
        parameters = parameter_mapping_from_dict(map_dict.get("parameters", None))
        skip_columns = map_dict.get("skip_columns", [])
        read_start_row = map_dict.get("read_start_row", 0)
        return ObjectClassMapping(
            name=name, objects=objects, parameters=parameters, skip_columns=skip_columns, read_start_row=read_start_row
        )

    def to_dict(self):
        map_dict = super().to_dict()
        map_dict.update(objects=self.objects.to_dict())
        return map_dict

    def is_valid(self):
        valid, msg = super().is_valid()
        if not valid:
            return valid, msg
        issue = self.object_names_issues()
        if issue:
            return False, issue
        return True, ""

    def object_names_issues(self):
        if isinstance(self._parameters, ParameterValueMapping) and isinstance(self._objects, NoneMapping):
            return "The source type for object names cannot be None."
        if not isinstance(self._objects, NoneMapping) and self._objects.reference != 0 and not self._objects.reference:
            return "No reference set for object names."
        return ""

    def _create_getters(self, pivoted_columns, pivoted_data, data_header):
        """See base class."""
        readers = super()._create_getters(pivoted_columns, pivoted_data, data_header)
        if self._objects.returns_value():
            readers["objects"] = self._objects.create_getter_function(pivoted_columns, pivoted_data, data_header)
        else:
            readers["objects"] = (None, None, None)
        return readers

    def create_mapping_readers(self, num_columns, pivoted_data, data_header):
        pivoted_columns = self.pivoted_columns(data_header, num_columns)
        readers = list()
        component_readers = self._create_getters(pivoted_columns, pivoted_data, data_header)
        name_getter, name_num, name_reads = component_readers["item_name"]
        o_getter, o_num, o_reads = component_readers["objects"]
        readers.append(("object_classes",) + create_final_getter_function([name_getter], [name_num], [name_reads]))
        readers.append(
            ("objects",)
            + create_final_getter_function([name_getter, o_getter], [name_num, o_num], [name_reads, o_reads])
        )
        readers += _parameter_readers(
            "object",
            self.parameters,
            (name_getter, name_num, name_reads),
            (o_getter, o_num, o_reads),
            component_readers,
        )
        return readers

    @classmethod
    def from_instance(cls, instance):
        """See base class."""
        if isinstance(instance, ObjectClassMapping):
            return ObjectClassMapping(
                instance.name, instance.objects, instance.parameters, instance.skip_columns, instance.read_start_row
            )
        if isinstance(instance, ObjectGroupMapping):
            return ObjectClassMapping(
                instance.name, instance.members, instance.parameters, instance.skip_columns, instance.read_start_row
            )
        if isinstance(instance, RelationshipClassMapping):
            return ObjectClassMapping(
                instance.object_classes[0],
                instance.objects[0],
                instance.parameters,
                instance.skip_columns,
                instance.read_start_row,
            )
        if isinstance(instance, NamedItemMapping):
            return ObjectClassMapping(
                instance.name, skip_columns=instance.skip_columns, read_start_row=instance.read_start_row
            )
        return ObjectClassMapping(skip_columns=instance.skip_columns, read_start_row=instance.read_start_row)


class ObjectGroupMapping(EntityClassMapping):
    """
    Class for holding and validating Mapping specification::

        ObjectGroupMapping {
            map_type: 'ObjectGroup'
            name: Mapping | str | None
            groups: Mapping | str | None
            members: Mapping | str | None
            parameters: Mapping | str | None
        }
    """

    MAP_TYPE = "ObjectGroup"

    def __init__(
        self,
        name=None,
        groups=None,
        members=None,
        parameters=None,
        import_objects=False,
        skip_columns=None,
        read_start_row=0,
    ):
        super().__init__(name, parameters, import_objects, skip_columns, read_start_row)
        self._groups = NoneMapping()
        self._members = NoneMapping()
        self.groups = groups
        self.members = members
  
    def non_pivoted_columns(self):
        non_pivoted_columns = super().non_pivoted_columns()
        if isinstance(self._groups, ColumnMapping) and self._groups.returns_value():
            non_pivoted_columns.append(self._groups.reference)
        if isinstance(self._members, ColumnMapping) and self._members.returns_value():
            non_pivoted_columns.append(self._members.reference)
        return non_pivoted_columns

    def last_pivot_row(self):
        return max(super().last_pivot_row(), self._groups.last_pivot_row(), self._members.last_pivot_row())

    def is_pivoted(self):
        return super().is_pivoted() or self._groups.is_pivoted() or self._members.is_pivoted()

    def has_fixed_dimensions(self):
        """Returns True if the dimensions of this mapping cannot be set."""
        return True

    @EntityClassMapping.import_objects.setter
    def import_objects(self, import_objects):
        if not isinstance(import_objects, bool):
            raise TypeError(f"import_objects must be a bool, instead got: {type(import_objects).__name__}")
        self._import_objects = import_objects

    @property
    def groups(self):
        return self._groups

    @groups.setter
    def groups(self, groups):
        self._groups = mappingbase_from_dict_int_str(groups)

    @property
    def members(self):
        return self._members

    @members.setter
    def members(self, members):
        self._members = mappingbase_from_dict_int_str(members)

    @classmethod
    def from_dict(cls, map_dict):
        if not isinstance(map_dict, dict):
            raise TypeError(f"map_dict must be a dict, instead got {type(map_dict).__name__}")

        map_type = map_dict.get("map_type", None)
        if map_type is not None and map_type != cls.MAP_TYPE:
            raise ValueError(f"If field 'map_type' is specified, it must be {cls.MAP_TYPE}, instead got {map_type}")
        name = map_dict.get("name", None)
        groups = map_dict.get("groups", None)
        members = map_dict.get("members", None)
        parameters = parameter_mapping_from_dict(map_dict.get("parameters", None))
        skip_columns = map_dict.get("skip_columns", [])
        import_objects = map_dict.get("import_objects", False)
        read_start_row = map_dict.get("read_start_row", 0)
        return ObjectGroupMapping(
            name=name,
            groups=groups,
            members=members,
            parameters=parameters,
            skip_columns=skip_columns,
            import_objects=import_objects,
            read_start_row=read_start_row,
        )

    def to_dict(self):
        map_dict = super().to_dict()
        map_dict["groups"] = self._groups.to_dict()
        map_dict["members"] = self._members.to_dict()
        return map_dict

    def is_valid(self):
        # check that parameter mapping has a valid name mapping
        issue = self.group_names_issues()
        if issue:
            return False, issue
        issue = self.member_names_issues()
        if issue:
            return False, issue
        return True, ""

    def group_names_issues(self):
        if isinstance(self._groups, NoneMapping):
            return "The source type for group names cannot be None."
        if self._name.reference != 0 and not self._name.reference:
            return "No reference set for group names."
        return ""

    def member_names_issues(self):
        if isinstance(self._members, NoneMapping):
            return "The source type for member names cannot be None."
        if self._members.reference != 0 and not self._members.reference:
            return "No reference set for member names."
        return ""

    def _create_getters(self, pivoted_columns, pivoted_data, data_header):
        """See base class."""
        getters = super()._create_getters(pivoted_columns, pivoted_data, data_header)
        g_getter, g_num, g_reads = (None, None, None)
        if self._groups.returns_value():
            g_getter, g_num, g_reads = self._groups.create_getter_function(pivoted_columns, pivoted_data, data_header)
        getters["groups"] = (g_getter, g_num, g_reads)
        m_getter, m_num, m_reads = (None, None, None)
        if self._members.returns_value():
            m_getter, m_num, m_reads = self._members.create_getter_function(pivoted_columns, pivoted_data, data_header)
        getters["members"] = (m_getter, m_num, m_reads)
        return getters

    def create_mapping_readers(self, num_columns, pivoted_data, data_header):
        pivoted_columns = self.pivoted_columns(data_header, num_columns)
        readers = list()
        component_readers = self._create_getters(pivoted_columns, pivoted_data, data_header)
        name_getter, name_num, name_reads = component_readers["item_name"]
        g_getter, g_num, g_reads = component_readers["groups"]
        m_getter, m_num, m_reads = component_readers["members"]
        readers.append(("object_classes",) + create_final_getter_function([name_getter], [name_num], [name_reads]))
        readers.append(
            ("object_groups",)
            + create_final_getter_function(
                [name_getter, g_getter, m_getter], [name_num, g_num, m_num], [name_reads, g_reads, m_reads]
            )
        )
        if self._import_objects:
            readers.append(
                ("objects",)
                + create_final_getter_function([name_getter, g_getter], [name_num, g_num], [name_reads, g_reads])
            )
            readers.append(
                ("objects",)
                + create_final_getter_function([name_getter, m_getter], [name_num, m_num], [name_reads, m_reads])
            )
        readers += _parameter_readers(
            "object",
            self.parameters,
            (name_getter, name_num, name_reads),
            (g_getter, g_num, g_reads),
            component_readers,
        )
        return readers

    @classmethod
    def from_instance(cls, instance):
        """See base class."""
        if isinstance(instance, ObjectClassMapping):
            return ObjectGroupMapping(
                name=instance.name,
                members=instance.objects,
                parameters=instance.parameters,
                skip_columns=instance.skip_columns,
                read_start_row=instance.read_start_row,
            )
        if isinstance(instance, RelationshipClassMapping):
            return ObjectGroupMapping(
                name=instance.object_classes[0],
                members=instance.objects[0],
                import_objects=instance.import_objects,
                skip_columns=instance.skip_columns,
                read_start_row=instance.read_start_row,
            )
        if isinstance(instance, ObjectGroupMapping):
            return ObjectGroupMapping(
                instance.name,
                instance.groups,
                instance.members,
                instance.parameters,
                instance.import_objects,
                instance.skip_columns,
                instance.read_start_row,
            )
        if isinstance(instance, NamedItemMapping):
            return ObjectGroupMapping(
                instance.name, skip_columns=instance.skip_columns, read_start_row=instance.read_start_row
            )
        return ObjectGroupMapping(skip_columns=instance.skip_columns, read_start_row=instance.read_start_row)


class RelationshipClassMapping(EntityClassMapping):
    """
    Class for holding and validating Mapping specification::

        ObjectClassMapping {
            map_type: 'object'
            name: str | Mapping
            objects: Mapping | str | None
            parameters: ParameterMapping | ParameterColumnCollectionMapping | None
        }
    """

    MAP_TYPE = "RelationshipClass"

    def __init__(
        self,
        name=None,
        object_classes=None,
        objects=None,
        parameters=None,
        import_objects=False,
        skip_columns=None,
        read_start_row=0,
    ):
        super().__init__(name, parameters, import_objects, skip_columns, read_start_row)
        self._objects = None
        self._object_classes = None
        self.object_classes = object_classes
        self.objects = objects

    def non_pivoted_columns(self):
        non_pivoted_columns = super().non_pivoted_columns()
        non_pivoted_columns.extend(
            o.reference for o in self.objects if isinstance(o, ColumnMapping) and o.returns_value()
        )
        non_pivoted_columns.extend(
            oc.reference for oc in self.object_classes if isinstance(oc, ColumnMapping) and oc.returns_value()
        )
        return non_pivoted_columns

    def last_pivot_row(self):
        o_last_pivot = max(o.last_pivot_row() for o in self.objects)
        oc_last_pivot = max(oc.last_pivot_row() for oc in self.object_classes)
        return max(super().last_pivot_row(), o_last_pivot, oc_last_pivot)

    def is_pivoted(self):
        o_pivoted = any(o.is_pivoted() for o in self.objects)
        oc_pivoted = any(oc.is_pivoted() for oc in self.object_classes)
        return super().is_pivoted() or o_pivoted or oc_pivoted

    @property
    def objects(self):
        return self._objects

    @property
    def dimensions(self):
        return len(self._objects)

    def has_fixed_dimensions(self):
        """Returns True if the dimensions of this mapping cannot be set."""
        return False

    @objects.setter
    def objects(self, objects):
        if objects is None:
            objects = [NoneMapping()]
        if not isinstance(objects, (list, tuple)):
            raise TypeError(
                f"objects must be a list or tuple of MappingBase, int, str, dict, instead got: {type(objects).__name__}"
            )
        if len(objects) != len(self.object_classes):
            raise ValueError(
                f"objects must be of same length as object_classes: {len(self.object_classes)} "
                f"instead got length: {len(objects)}"
            )
        self._objects = [mappingbase_from_dict_int_str(o) for o in objects]

    @EntityClassMapping.import_objects.setter
    def import_objects(self, import_objects):
        if not isinstance(import_objects, bool):
            raise TypeError(f"import_objects must be a bool, instead got: {type(import_objects).__name__}")
        self._import_objects = import_objects

    @property
    def object_classes(self):
        return self._object_classes

    @object_classes.setter
    def object_classes(self, object_classes):
        if object_classes is None:
            object_classes = [NoneMapping()]
        if not isinstance(object_classes, (list, tuple)):
            raise TypeError(
                f"object_classes must be a list or tuple of MappingBase, int, str, dict, instead got: {type(object_classes).__name__}"
            )
        self._object_classes = [mappingbase_from_dict_int_str(oc) for oc in object_classes]

    @classmethod
    def from_dict(cls, map_dict):
        if not isinstance(map_dict, dict):
            raise TypeError(f"map_dict must be a dict, instead got {type(map_dict).__name__}")
        map_type = map_dict.get("map_type", None)
        if map_type is not None and map_type != cls.MAP_TYPE:
            raise ValueError(f"If field 'map_type' is specified, it must be {cls.MAP_TYPE}, instead got {map_type}")
        name = map_dict.get("name", None)
        object_classes = map_dict.get("object_classes", None)
        objects = map_dict.get("objects", None)
        parameters = parameter_mapping_from_dict(map_dict.get("parameters", None))
        skip_columns = map_dict.get("skip_columns", [])
        import_objects = map_dict.get("import_objects", False)
        read_start_row = map_dict.get("read_start_row", 0)
        return RelationshipClassMapping(
            name=name,
            object_classes=object_classes,
            objects=objects,
            parameters=parameters,
            import_objects=import_objects,
            skip_columns=skip_columns,
            read_start_row=read_start_row,
        )

    def to_dict(self):
        map_dict = super().to_dict()
        map_dict.update(objects=[o.to_dict() for o in self.objects])
        map_dict.update(object_classes=[oc.to_dict() for oc in self.object_classes])
        return map_dict

    def is_valid(self):
        # check that parameter mapping has a valid name mapping
        valid, msg = super().is_valid()
        if not valid:
            return valid, msg
        for i in range(len(self._object_classes)):
            issue = self.object_class_names_issues(i)
            if issue:
                return False, issue
            issue = self.object_names_issues(i)
            if issue:
                return False, issue
        return True, ""

    def object_class_names_issues(self, class_index):
        mapping = self._object_classes[class_index]
        if isinstance(mapping, NoneMapping):
            return "The source type for object class names cannot be None."
        if not isinstance(mapping, NoneMapping) and mapping.reference != 0 and not mapping.reference:
            return "No reference set for object class names."
        return ""

    def object_names_issues(self, object_index):
        mapping = self._objects[object_index]
        if isinstance(self._parameters, ParameterValueMapping) and isinstance(mapping, NoneMapping):
            return "The source type for object names cannot be None."
        if not isinstance(mapping, NoneMapping) and mapping.reference != 0 and not mapping.reference:
            return "No reference set for object names."
        return ""

    def _create_getters(self, pivoted_columns, pivoted_data, data_header):
        """See base class."""
        getters = super()._create_getters(pivoted_columns, pivoted_data, data_header)
        oc_getter, oc_num, oc_reads = (None, None, None)
        if all(oc.returns_value() for oc in self.object_classes):
            # create functions to get object_classes
            oc_getter, oc_num, oc_reads = create_getter_function_from_function_list(
                *create_getter_list(self.object_classes, pivoted_columns, pivoted_data, data_header),
                list_wrap=len(self.object_classes) == 1,
            )
        o_getter, o_num, o_reads = (None, None, None)
        if all(o.returns_value() for o in self.objects):
            # create functions to get objects
            o_getter, o_num, o_reads = create_getter_function_from_function_list(
                *create_getter_list(self.objects, pivoted_columns, pivoted_data, data_header),
                list_wrap=len(self.objects) == 1,
            )
        getters["objects"] = (o_getter, o_num, o_reads)
        getters["object_classes"] = (oc_getter, oc_num, oc_reads)
        return getters

    def create_mapping_readers(self, num_columns, pivoted_data, data_header):
        pivoted_columns = self.pivoted_columns(data_header, num_columns)
        readers = list()
        component_readers = self._create_getters(pivoted_columns, pivoted_data, data_header)
        name_getter, name_num, name_reads = component_readers["item_name"]
        o_getter, o_num, o_reads = component_readers["objects"]
        oc_getter, oc_num, oc_reads = component_readers["object_classes"]
        readers.append(
            ("relationship_classes",)
            + create_final_getter_function([name_getter, oc_getter], [name_num, oc_num], [name_reads, oc_reads])
        )
        readers.append(
            ("relationships",)
            + create_final_getter_function([name_getter, o_getter], [name_num, o_num], [name_reads, o_reads])
        )
        if self.import_objects:
            for oc, o in zip(self.object_classes, self.objects):
                oc_getter, oc_num, oc_reads = oc.create_getter_function(pivoted_columns, pivoted_data, data_header)
                single_o_getter, single_o_num, single_o_reads = o.create_getter_function(
                    pivoted_columns, pivoted_data, data_header
                )
                readers.append(("object_classes",) + create_final_getter_function([oc_getter], [oc_num], [oc_reads]))
                readers.append(
                    ("objects",)
                    + create_final_getter_function(
                        [oc_getter, single_o_getter], [oc_num, single_o_num], [oc_reads, single_o_reads]
                    )
                )
        readers += _parameter_readers(
            "relationship",
            self.parameters,
            (name_getter, name_num, name_reads),
            (o_getter, o_num, o_reads),
            component_readers,
        )
        return readers

    @classmethod
    def from_instance(cls, instance):
        """See base class."""
        if isinstance(instance, ObjectClassMapping):
            return RelationshipClassMapping(
                object_classes=[instance.name],
                objects=[instance.objects],
                parameters=instance.parameters,
                skip_columns=instance.skip_columns,
                read_start_row=instance.read_start_row,
            )
        if isinstance(instance, ObjectGroupMapping):
            return RelationshipClassMapping(
                object_classes=[instance.name],
                objects=[instance.members],
                parameters=instance.parameters,
                import_objects=instance.import_objects,
                skip_columns=instance.skip_columns,
                read_start_row=instance.read_start_row,
            )
        if isinstance(instance, RelationshipClassMapping):
            return RelationshipClassMapping(
                instance.name,
                instance.object_classes,
                instance.objects,
                instance.import_objects,
                instance.parameters,
                instance.skip_columns,
                instance.read_start_row,
            )
        if isinstance(instance, NamedItemMapping):
            return RelationshipClassMapping(
                instance.name, skip_columns=instance.skip_columns, read_start_row=instance.read_start_row
            )
        return RelationshipClassMapping(skip_columns=instance.skip_columns, read_start_row=instance.read_start_row)


class AlternativeMapping(NamedItemMapping):
    """
        Holds mapping for alternatives.

        specification:

            AlternativeMapping {
                map_type: 'Alternative'
                name: str | Mapping
            }
    """

    MAP_TYPE = "Alternative"

    def __init__(self, name=None, skip_columns=None, read_start_row=0):
        """
        Args:
            name (str or MappingBase, optional): mapping for the item name
            skip_columns (list, optional): a list of columns to skip while mapping
            read_start_row (int): skip this many rows while mapping
        """
        super().__init__(name, skip_columns, read_start_row)

    def is_valid(self):
        issue = self.alternative_names_issues()
        if issue:
            return False, issue
        return True, ""

    def alternative_names_issues(self):
        """Returns a non-empty message string if the alternative name is invalid."""
        if isinstance(self._name, NoneMapping):
            return "The source type for alternative names cannot be None."
        if self._name.reference != 0 and not self._name.reference:
            return "No reference set for alternative names."
        return ""

    def create_mapping_readers(self, num_columns, pivoted_data, data_header):
        pivoted_columns = self.pivoted_columns(data_header, num_columns)
        getters = self._create_getters(pivoted_columns, pivoted_data, data_header)
        name_getter, name_num, name_reads = getters["item_name"]
        readers = [("alternatives",) + create_final_getter_function([name_getter], [name_num], [name_reads])]
        return readers

    @classmethod
    def from_dict(cls, map_dict):
        if not isinstance(map_dict, dict):
            raise TypeError(f"map_dict must be a dict, instead got {type(map_dict).__name__}")
        name = map_dict.get("name", None)
        skip_columns = map_dict.get("skip_columns", [])
        read_start_row = map_dict.get("read_start_row", 0)
        return AlternativeMapping(name, skip_columns, read_start_row)

    @classmethod
    def from_instance(cls, instance):
        """See base class."""
        if isinstance(instance, NamedItemMapping):
            return AlternativeMapping(
                instance.name, skip_columns=instance.skip_columns, read_start_row=instance.read_start_row
            )
        return AlternativeMapping(skip_columns=instance.skip_columns, read_start_row=instance.read_start_row)


class ScenarioMapping(NamedItemMapping):
    """
        Holds mapping for scenarios.

        specification:

            ScenarioMapping {
                map_type: 'Scenario'
                name: str | Mapping
                active: str | Mapping
            }
    """

    MAP_TYPE = "Scenario"

    def __init__(self, name=None, active=False, skip_columns=None, read_start_row=0):
        """
        Args:
            name (str or MappingBase, optional): mapping for the scenario name
            active (str or Mapping, optional): mapping for scenario's active flag
            skip_columns (list, optional): a list of columns to skip while mapping
            read_start_row (int): skip this many rows while mapping
        """
        super().__init__(name, skip_columns, read_start_row)
        if active is not None:
            self._active = mappingbase_from_dict_int_str(active)
        else:
            self._active = ConstantMapping("false")

    @property
    def active(self):
        return self._active

    @active.setter
    def active(self, active):
        self._active = mappingbase_from_dict_int_str(active)

    def is_valid(self):
        issue = self.scenario_names_issues()
        if issue:
            return False, issue
        return True, ""

    def scenario_names_issues(self):
        """Returns a non-empty message string if the scenario name is invalid."""
        if isinstance(self._name, NoneMapping):
            return "The source type for scenario names cannot be None."
        if self._name.reference != 0 and not self._name.reference:
            return "No reference set for scenario names."
        return ""

    def _create_getters(self, pivoted_columns, pivoted_data, data_header):
        getters = super()._create_getters(pivoted_columns, pivoted_data, data_header)
        if self.active.returns_value():
            getters["active"] = self.active.create_getter_function(pivoted_columns, pivoted_data, data_header)
        else:
            getters["active"] = (None, None, None)
        return getters

    def create_mapping_readers(self, num_columns, pivoted_data, data_header):
        pivoted_columns = self.pivoted_columns(data_header, num_columns)
        getters = self._create_getters(pivoted_columns, pivoted_data, data_header)
        name_getter, name_num, name_reads = getters["item_name"]
        active_getter, active_num, active_reads = getters["active"]
        readers = [
            ("scenarios",)
            + create_final_getter_function(
                [name_getter, active_getter], [name_num, active_num], [name_reads, active_reads]
            )
        ]
        return readers

    def to_dict(self):
        map_dict = super().to_dict()
        map_dict["active"] = self._active.to_dict()
        return map_dict

    @classmethod
    def from_dict(cls, map_dict):
        if not isinstance(map_dict, dict):
            raise TypeError(f"map_dict must be a dict, instead got {type(map_dict).__name__}")
        name = map_dict.get("name")
        active = map_dict.get("active")
        skip_columns = map_dict.get("skip_columns", [])
        read_start_row = map_dict.get("read_start_row", 0)
        return ScenarioMapping(name, active, skip_columns, read_start_row)

    @classmethod
    def from_instance(cls, instance):
        """See base class."""
        if isinstance(instance, ScenarioMapping):
            return ScenarioMapping(instance.name, instance._active, instance.skip_columns, instance.read_start_row)
        if isinstance(instance, NamedItemMapping):
            return ScenarioMapping(
                instance.name, skip_columns=instance.skip_columns, read_start_row=instance.read_start_row
            )
        return ScenarioMapping(skip_columns=instance.skip_columns, read_start_row=instance.read_start_row)


class ScenarioAlternativeMapping(ItemMappingBase):
    """
        Holds mapping for scenario alternatives.

        specification:

            ScenarioAlternativeMapping {
                map_type: 'ScenarioAlternative'
                scenario_name: str | Mapping
                alternatives: str | Mapping
                before_alternative_name: str | Mapping
            }
    """

    MAP_TYPE = "ScenarioAlternative"

    def __init__(
        self,
        scenario_name=None,
        alternative_name=None,
        before_alternative_name=None,
        skip_columns=None,
        read_start_row=0,
    ):
        super().__init__(skip_columns, read_start_row)
        self._scenario_name = mappingbase_from_dict_int_str(scenario_name)
        self._alternative_name = mappingbase_from_dict_int_str(alternative_name)
        self._before_alternative_name = mappingbase_from_dict_int_str(before_alternative_name)

    @property
    def scenario_name(self):
        return self._scenario_name

    @property
    def alternative_name(self):
        return self._alternative_name

    @property
    def before_alternative_name(self):
        return self._before_alternative_name

    @scenario_name.setter
    def scenario_name(self, scenario_name):
        self._scenario_name = mappingbase_from_dict_int_str(scenario_name)

    @alternative_name.setter
    def alternative_name(self, alternative_name):
        self._alternative_name = mappingbase_from_dict_int_str(alternative_name)

    @before_alternative_name.setter
    def before_alternative_name(self, before_alternative_name):
        self._before_alternative_name = mappingbase_from_dict_int_str(before_alternative_name)

    def is_valid(self):
        issue = self.scenario_names_issues()
        if issue:
            return False, issue
        return True, ""

    def is_pivoted(self):
        return self._scenario_name.is_pivoted()

    def non_pivoted_columns(self):
        if isinstance(self._scenario_name, ColumnMapping) and self._scenario_name.returns_value():
            return [self._scenario_name.reference]
        return []

    def last_pivot_row(self):
        return max(
            self._scenario_name.last_pivot_row(),
            self._alternative_name.last_pivot_row(),
            self._before_alternative_name.last_pivot_row(),
            -1,
        )

    def scenario_names_issues(self):
        """Returns a non-empty message string if the scenario name is invalid."""
        if isinstance(self._scenario_name, NoneMapping):
            return "The source type for scenario names cannot be None."
        if self._scenario_name.reference != 0 and not self._scenario_name.reference:
            return "No reference set for scenario names."
        return ""

    def create_mapping_readers(self, num_columns, pivoted_data, data_header):
        pivoted_columns = self.pivoted_columns(data_header, num_columns)
        if self._scenario_name.returns_value():
            scenario_name_getter, scenario_name_length, scenario_name_reads = self._scenario_name.create_getter_function(
                pivoted_columns, pivoted_data, data_header
            )
        else:
            scenario_name_getter, scenario_name_length, scenario_name_reads = None, None, None
        if self._alternative_name.returns_value():
            alt_getter, alt_length, alt_reads = self._alternative_name.create_getter_function(
                pivoted_columns, pivoted_data, data_header
            )
        else:
            alt_getter, alt_length, alt_reads = None, None, None
        if self._before_alternative_name.returns_value():
            before_name_getter, before_name_length, before_name_reads = self._before_alternative_name.create_getter_function(
                pivoted_columns, pivoted_data, data_header
            )
        else:
            before_name_getter, before_name_length, before_name_reads = None, None, None
        functions = [scenario_name_getter, alt_getter, before_name_getter]
        output_lengths = [scenario_name_length, alt_length, before_name_length]
        reads_data = [scenario_name_reads, alt_reads, before_name_reads]
        readers = [("scenario_alternatives",) + create_final_getter_function(functions, output_lengths, reads_data)]
        return readers

    @classmethod
    def from_dict(cls, map_dict):
        if not isinstance(map_dict, dict):
            raise TypeError(f"map_dict must be a dict, instead got {type(map_dict).__name__}")
        scenario_name = map_dict.get("scenario_name", None)
        alternative_name = map_dict.get("alternative_name", None)
        before_alternative_name = map_dict.get("before_alternative_name", None)
        skip_columns = map_dict.get("skip_columns", [])
        read_start_row = map_dict.get("read_start_row", 0)
        return ScenarioAlternativeMapping(
            scenario_name, alternative_name, before_alternative_name, skip_columns, read_start_row
        )

    def to_dict(self):
        map_dict = super().to_dict()
        map_dict["scenario_name"] = self._scenario_name.to_dict()
        map_dict["alternative_name"] = self._alternative_name.to_dict()
        map_dict["before_alternative_name"] = self._before_alternative_name.to_dict()
        return map_dict

    @classmethod
    def from_instance(cls, instance):
        """See base class."""
        if isinstance(instance, ScenarioAlternativeMapping):
            return ScenarioAlternativeMapping(
                instance._scenario_name,
                instance._alternative_name,
                instance._before_alternative_name,
                instance.skip_columns,
                instance.read_start_row,
            )
        if isinstance(instance, ObjectClassMapping):
            return ScenarioAlternativeMapping(
                instance.name,
                instance.objects,
                skip_columns=instance.skip_columns,
                read_start_row=instance.read_start_row,
            )
        if isinstance(instance, RelationshipClassMapping):
            return ScenarioAlternativeMapping(
                instance.name,
                instance.object_classes[0],
                skip_columns=instance.skip_columns,
                read_start_row=instance.read_start_row,
            )
        if isinstance(instance, NamedItemMapping):
            return ScenarioAlternativeMapping(
                instance.name, skip_columns=instance.skip_columns, read_start_row=instance.read_start_row
            )
        return ScenarioAlternativeMapping(skip_columns=instance.skip_columns, read_start_row=instance.read_start_row)


def mappingbase_from_value(value):
    if value is None:
        return ColumnMapping()
    if isinstance(value, MappingBase):
        return value
    if isinstance(value, int):
        try:
            return ColumnMapping(value)
        except ValueError:
            pass
    if isinstance(value, str):
        return ConstantMapping(value)
    raise TypeError(f"Can't convert {type(value).__name__} to MappingBase")


def mapping_from_dict(map_dict):
    type_str_to_class = {
        RowMapping.MAP_TYPE: RowMapping,
        ColumnMapping.MAP_TYPE: ColumnMapping,
        "column_name": ColumnHeaderMapping,
        ColumnHeaderMapping.MAP_TYPE: ColumnHeaderMapping,
        ConstantMapping.MAP_TYPE: ConstantMapping,
        TableNameMapping.MAP_TYPE: TableNameMapping,
        NoneMapping.MAP_TYPE: NoneMapping,
    }
    map_type_str = map_dict.get("map_type", None)
    if map_type_str == "column_name":
        map_dict["map_type"] = ColumnHeaderMapping.MAP_TYPE
    if "value_reference" in map_dict and "reference" not in map_dict:
        map_dict["reference"] = map_dict["value_reference"]
    map_class = type_str_to_class.get(map_type_str, NoneMapping)
    return map_class.from_dict(map_dict)


def mappingbase_from_dict_int_str(value):
    """Creates Mapping object if `value` is a `dict` or `int`;
    if `str` or `None` returns same value. If `int`, the Mapping is created
    with map_type == column (default) unless other type is specified
    """
    if value is None:
        return NoneMapping()
    if isinstance(value, MappingBase):
        return value
    if isinstance(value, dict):
        return mapping_from_dict(value)
    if isinstance(value, (int, str)):
        return mappingbase_from_value(value)
    raise TypeError(f"value must be dict, int or str, instead got {type(value)}")


def parameter_mapping_from_dict(map_dict):
    if map_dict is None:
        return NoneMapping()
    if map_dict.get("map_type", "") == NoneMapping.MAP_TYPE:
        return NoneMapping()

    parameter_type_to_class = {
        "definition": ParameterDefinitionMapping,
        "single value": ParameterValueMapping,
        "array": ParameterArrayMapping,
        "map": ParameterMapMapping,
        "time series": ParameterTimeSeriesMapping,
        "time pattern": ParameterTimePatternMapping,
    }
    parameter_type = map_dict.get("parameter_type", None)
    default_map = ParameterValueMapping
    map_class = parameter_type_to_class.get(parameter_type, default_map)
    if parameter_type is None:
        map_dict.update(parameter_type=map_class.PARAMETER_TYPE)

    return parameter_type_to_class.get(parameter_type, default_map).from_dict(map_dict)


def dict_to_map(map_dict):
    """Creates Mapping object from a dict"""
    if not isinstance(map_dict, dict):
        raise TypeError(f"map_dict must be a dict, instead it was: {type(map_dict)}")
    map_type = map_dict.get("map_type", None)
    mapping_classes = (
        RelationshipClassMapping,
        ObjectClassMapping,
        ObjectGroupMapping,
        AlternativeMapping,
        ScenarioMapping,
        ScenarioAlternativeMapping,
    )
    mapping_classes = {c.MAP_TYPE: c for c in mapping_classes}
    mapping_class = mapping_classes.get(map_type)
    if mapping_class is not None:
        return mapping_class.from_dict(map_dict)
    raise ValueError(f"""invalid "map_type" value, expected any of {", ".join(mapping_classes)}, got {map_type}""")


def type_class_list_from_spec(types, num_sections, skip_sections=None):
    if skip_sections is None:
        skip_sections = []
    type_conv_list = []
    for section in range(num_sections):
        type_class = types.get(section, None)
        if section in skip_sections or type_class is None:
            type_class = lambda x: x
        type_conv_list.append(type_class)
    return type_conv_list


def convert_value(value, type_converter):
    try:
        if isinstance(value, str) and not value:
            value = None
        if value is not None:
            value = type_converter(value)
        return value
    except (ValueError, ParameterValueFormatError):
        raise TypeConversionError(f"Could not convert value: '{value}' to type: '{type_converter.__name__}'")


def convert_function_from_spec(column_types, num_cols, skip_cols=None):
    """Creates a function that converts a list of data with length num_cols to the
    types in the column_types dict. If no type is given then the function returns the original value
    
    Arguments:
        column_types {dict} -- dict with column number as key and type constructor as value for each column
        num_cols {int} -- length of data that the function should convert
    
    Raises:
        ValueError: Raised if the column_types dict contains a unsupported class type
        TypeConversionError: [description]
    
    Returns:
        [function] -- A function that converts a row of data to the types given by column_types. 
    """
    if column_types:
        type_conv_list = type_class_list_from_spec(column_types, num_cols, skip_cols)

        def convert_row_data(row):
            row_list = []
            for row_item, col_type in zip(row, type_conv_list):
                row_list.append(convert_value(row_item, col_type))
            return row_list

    else:
        convert_row_data = lambda x: x
    return convert_row_data


def get_pivoted_data(data_source, mapping, num_cols, data_header, row_types):
    pivoted_data = []
    errors = []

    # find used columns
    map_list = [mapping]
    used_columns = set()
    for map_ in map_list:
        skip_columns = set(mapping_non_pivoted_columns(map_, num_cols, data_header))
        if map_.skip_columns is not None:
            skip_columns.update(set(map_.skip_columns))
        map_using_cols = set(range(num_cols)).difference(skip_columns)
        used_columns.update(map_using_cols)

    # get data from iterator and convert to correct type.
    if mapping.is_pivoted():
        do_nothing = lambda x: x
        for row_number in range(mapping.last_pivot_row() + 1):
            # TODO: if data_source iterator ends before all pivoted rows are collected.
            type_converter = row_types.get(row_number, do_nothing)
            row_data = next(data_source)
            typed_row = []
            for col, value in enumerate(row_data):
                if col in used_columns:
                    try:
                        typed_row.append(convert_value(value, type_converter))
                    except TypeConversionError as e:
                        errors.append((row_number, e))
                        typed_row.append(None)
                else:
                    typed_row.append(value)
            pivoted_data.append(typed_row)
    return pivoted_data, errors


def read_with_mapping(data_source, mapping, num_cols, data_header=None, column_types=None, row_types=None):
    """Reads data_source line by line with supplied Mapping object or dict
    that can be translated into a Mapping object"""
    if row_types is None:
        row_types = {}

    errors = []
    mappings = []
    if not isinstance(mapping, (list, tuple)):
        mapping = [mapping]
    for map_ in mapping:
        if isinstance(map_, dict):
            mappings.append(dict_to_map(map_))
        elif isinstance(map_, ItemMappingBase):
            mappings = [map_]
        else:
            raise TypeError(
                "mapping must be a dict, ItemMappingBase subclass, or list of those, "
                f"instead got: {type(map_).__name__}"
            )

    for map_ in mappings:
        valid, message = map_.is_valid()
        if not valid:
            raise InvalidMapping(message)

    # find max pivot row since mapping can have different number of pivoted rows.
    last_pivot_row = -1
    has_pivot = False
    for map_ in mappings:
        if map_.is_pivoted():
            has_pivot = True
            last_pivot_row = max(last_pivot_row, map_.last_pivot_row())

    # get pivoted rows of data.
    raw_pivoted_data = []
    if has_pivot:
        for row_number in range(last_pivot_row + 1):
            raw_pivoted_data.append(next(data_source))

    # get a list of reader functions
    readers = []
    min_read_data_from_row = math.inf
    for map_index, m in enumerate(mappings):
        pivoted_data, pivot_type_errors = get_pivoted_data(iter(raw_pivoted_data), m, num_cols, data_header, row_types)
        errors.extend(pivot_type_errors)
        read_data_from_row = max(m.last_pivot_row() + 1, m.read_start_row)
        r = m.create_mapping_readers(num_cols, pivoted_data, data_header)
        readers.extend([((map_index, key), reader, reads_row, read_data_from_row) for key, reader, reads_row in r])
        min_read_data_from_row = min(min_read_data_from_row, read_data_from_row)
    merged_data = {
        "object_classes": [],
        "objects": [],
        "object_parameters": [],
        "object_parameter_values": [],
        "relationship_classes": [],
        "relationships": [],
        "relationship_parameters": [],
        "relationship_parameter_values": [],
        "object_groups": [],
        "alternatives": [],
        "scenarios": [],
        "scenario_alternatives": [],
    }
    data = dict()
    # run functions that read from header or pivoted area first
    # select only readers that actually need to read row data
    row_readers = []
    for key, func, reads_rows, read_data_from_row in readers:
        if key not in data:
            data[key] = []
        if reads_rows:
            row_readers.append((key, func, read_data_from_row))
        else:
            data[key].extend(func(None))

    # function that converts column in the row data to the types specified in column_types
    convert_row_types = convert_function_from_spec(column_types, num_cols)

    if raw_pivoted_data:
        data_source = itertools.chain(raw_pivoted_data, data_source)

    data_source = itertools.islice(data_source, min_read_data_from_row, None)
    skipped_rows = min_read_data_from_row

    # read each row in data source
    if row_readers:
        for row_number, row_data in enumerate(data_source):
            row_number = row_number + skipped_rows
            if not row_data:
                continue
            try:
                row_data = convert_row_types(row_data)
            except TypeConversionError as e:
                errors.append((row_number, e))
                continue
            try:
                # read the row with each reader
                for key, reader, read_data_from_row in row_readers:
                    if row_number >= read_data_from_row:
                        data[key].extend(
                            [row_value for row_value in reader(row_data) if _is_row_value_valid(row_value)]
                        )
            except IndexError as e:
                errors.append((row_number, e))
    # convert parameter values to right class and put all data in one dict
    for key, v in data.items():
        map_i, k = key
        if "parameter_values" in k:
            current_mapping = mappings[map_i]
            merged_data[k].extend(current_mapping.parameters.raw_data_to_type(v))
        else:
            merged_data[k].extend(v)
    return merged_data, errors


def _is_row_value_valid(row_value):
    if row_value is None:
        return False
    if not isinstance(row_value, Iterable):
        return True
    return all(v is not None for v in row_value)


def mapping_non_pivoted_columns(mapping, num_cols, data_header=None):
    """Returns columns that are referenced but not pivoted given a header and number of columns
    
    Arguments:
        mapping {Mapping} -- mapping object
        num_cols {int} -- number of columns to check
    
    Keyword Arguments:
        data_header {list[str]} -- list of strings, headers (default: {None})
    
    Returns:
        [set] -- referenced columns in mapping that is not pivoted.
    """
    if data_header is None:
        data_header = []
    non_pivoted_columns = mapping.non_pivoted_columns()
    int_non_piv_cols = []
    for pc in non_pivoted_columns:
        if isinstance(pc, str):
            if pc not in data_header:
                # could not find reference
                continue
            pc = data_header.index(pc)
        if pc >= num_cols:
            continue
        int_non_piv_cols.append(pc)
    return set(int_non_piv_cols)


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


def _parameter_readers(object_or_relationship, parameters_mapping, class_getters, entity_getters, component_readers):
    """
    Creates a list of parameter readers.

    Args:
        object_or_relationship (str): either "object" or "relationship"
        parameters_mapping (ParameterDefinitionMapping): mapping for parameters
        class_getters (tuple): a tuple consisting of entity class name getters
        entity_getters (tuple): a tuple consisting of entity name getters
        component_readers (dict): a mapping from reader name to reader tuple

    Returns:
        list: readers for parameter definitions and (optionally) values, or empty list if not applicable
    """
    readers = list()
    if isinstance(parameters_mapping, ParameterDefinitionMapping):
        par_name_getter, par_name_num, par_name_reads = component_readers["parameter_name"]
        readers.append(
            (object_or_relationship + "_parameters",)
            + create_final_getter_function(
                [class_getters[0], par_name_getter],
                [class_getters[1], par_name_num],
                [class_getters[2], par_name_reads],
            )
        )
        if isinstance(parameters_mapping, ParameterValueMapping):
            par_val_name = object_or_relationship + "_parameter_values"
            par_value_getter, par_value_num, par_value_reads = component_readers["parameter_value"]
            readers.append(
                (par_val_name,)
                + create_final_getter_function(
                    [class_getters[0], entity_getters[0], par_name_getter, par_value_getter],
                    [class_getters[1], entity_getters[1], par_name_num, par_value_num],
                    [class_getters[2], entity_getters[2], par_name_reads, par_value_reads],
                )
            )
    return readers
