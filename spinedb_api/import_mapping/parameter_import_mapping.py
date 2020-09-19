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
Classes for parameter import mappings.

:author: P. Vennström (VTT)
:date:   22.02.2018
"""

import itertools
from operator import itemgetter
from ..parameter_value import (
    Array,
    convert_leaf_maps_to_specialized_containers,
    Map,
    TimeSeriesVariableResolution,
    TimePattern,
)
from ..exception import InvalidMapping
from .single_import_mapping import (
    NoneMapping,
    ColumnMapping,
    single_mapping_from_dict_int_str,
    create_getter_list,
    create_getter_function_from_function_list,
)


class TimeSeriesOptions:
    """
    Holds parameter type-specific options for time series parameter values.

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


class ParameterMappingBase:
    """Base class for parameter mappings."""

    MAP_TYPE = "parameter"

    def __init__(self, name=None):
        self._name = ColumnMapping(None)
        self.name = name

    def display_names(self):
        return ["Parameter names"]

    def component_mappings(self):
        return [self.name]

    def set_component_by_display_name(self, display_name, mapping):
        if display_name == "Parameter names":
            self.name = mapping
            return True
        return False

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = single_mapping_from_dict_int_str(name)

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
        raise NotImplementedError()

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

    def _component_issues_getters(self):
        return [self.names_issues]

    def component_issues(self, component_index):
        """Returns issues for given mapping component index.

        Args:
            component_index (int)

        Returns:
            str: issue string
        """
        component_issues_getters = self._component_issues_getters()
        try:
            return component_issues_getters[component_index]()
        except IndexError:
            return ""

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


class ParameterDefinitionMapping(ParameterMappingBase):

    PARAMETER_TYPE = "definition"

    def __init__(self, name=None, default_value=None, parameter_value_list_name=None):
        super().__init__(name)
        self._default_value = ColumnMapping(None)
        self._parameter_value_list_name = single_mapping_from_dict_int_str(parameter_value_list_name)
        self.default_value = default_value

    def display_names(self):
        return super().display_names() + ["Default values", "Parameter value list names"]

    def component_mappings(self):
        return super().component_mappings() + [self.default_value, self.parameter_value_list_name]

    def set_component_by_display_name(self, display_name, mapping):
        if display_name == "Default values":
            self.default_value = mapping
            return True
        if display_name == "Parameter value list names":
            self.parameter_value_list_name = mapping
            return True
        return super().set_component_by_display_name(display_name, mapping)

    @property
    def default_value(self):
        return self._default_value

    @property
    def parameter_value_list_name(self):
        return self._parameter_value_list_name

    @default_value.setter
    def default_value(self, default_value):
        self._default_value = single_mapping_from_dict_int_str(default_value)

    @parameter_value_list_name.setter
    def parameter_value_list_name(self, parameter_value_list_name):
        self._parameter_value_list_name = single_mapping_from_dict_int_str(parameter_value_list_name)

    @classmethod
    def from_dict(cls, map_dict):
        if not isinstance(map_dict, dict):
            raise ValueError("map_dict must be a dict")
        map_type = map_dict.get("map_type", None)
        if map_type is not None and map_type != cls.MAP_TYPE:
            raise ValueError(f"If field 'map_type' is specified, it must be {cls.MAP_TYPE}, instead got {map_type}")
        name = map_dict.get("name", None)
        return ParameterDefinitionMapping(name)


class ParameterValueMapping(ParameterMappingBase):
    PARAMETER_TYPE = "single value"

    def __init__(self, name=None, value=None, alternative_name=None):
        super().__init__(name)
        self._value = ColumnMapping(None)
        self._alternative_name = single_mapping_from_dict_int_str(alternative_name)
        self.value = value

    def display_names(self):
        return super().display_names() + ["Parameter values", "Alternative names"]

    def component_mappings(self):
        return super().component_mappings() + [self.value, self.alternative_name]

    def set_component_by_display_name(self, display_name, mapping):
        if display_name == "Parameter values":
            self.value = mapping
            return True
        if display_name == "Alternative names":
            self.alternative_name = mapping
            return True
        return super().set_component_by_display_name(display_name, mapping)

    @property
    def value(self):
        return self._value

    @property
    def alternative_name(self):
        return self._alternative_name

    @value.setter
    def value(self, value):
        self._value = single_mapping_from_dict_int_str(value)

    @alternative_name.setter
    def alternative_name(self, alternative_name):
        self._alternative_name = single_mapping_from_dict_int_str(alternative_name)

    def non_pivoted_columns(self):
        non_pivoted_columns = super().non_pivoted_columns()
        if isinstance(self.value, ColumnMapping) and self.value.returns_value():
            non_pivoted_columns.append(self.value.reference)
        if isinstance(self.alternative_name, ColumnMapping) and self.alternative_name.returns_value():
            non_pivoted_columns.append(self.alternative_name.reference)
        return non_pivoted_columns

    def last_pivot_row(self):
        return max(super().last_pivot_row(), self.value.last_pivot_row(), self.alternative_name.last_pivot_row())

    def is_pivoted(self):
        return super().is_pivoted() or self.value.is_pivoted() or self.alternative_name.is_pivoted()

    @classmethod
    def from_dict(cls, map_dict):
        if not isinstance(map_dict, dict):
            raise ValueError("map_dict must be a dict")
        map_type = map_dict.get("map_type", None)
        if map_type is not None and map_type != cls.MAP_TYPE:
            raise ValueError(f"If field 'map_type' is specified, it must be {cls.MAP_TYPE}, instead got {map_type}")
        name = map_dict.get("name", None)
        value = map_dict.get("value", None)
        alternative_name = map_dict.get("alternative_name", None)
        return ParameterValueMapping(name, value, alternative_name)

    def to_dict(self):
        map_dict = super().to_dict()
        map_dict["map_type"] = self.MAP_TYPE
        map_dict.update({"parameter_type": self.PARAMETER_TYPE})
        map_dict.update({"value": self.value.to_dict()})
        map_dict.update({"alternative_name": self.alternative_name.to_dict()})
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

    def _component_issues_getters(self):
        # NOTE: A bit crazy, but let alternative issues follow value issues for now
        return super()._component_issues_getters() + [self.values_issues, self.values_issues]

    def values_issues(self, parent_pivot=False):
        if not (self.is_pivoted() or parent_pivot):
            if isinstance(self._value, NoneMapping):
                return "The source type for parameter values cannot be None."
            if self._value.reference != 0 and not self._value.reference:
                return "No reference set for parameter values."
        return ""

    def create_getter_list(self, is_pivoted, pivoted_columns, pivoted_data, data_header):
        getters = super().create_getter_list(is_pivoted, pivoted_columns, pivoted_data, data_header)
        val_getter, val_num, val_reads = (None, None, None)
        if (is_pivoted or self.is_pivoted()) and not self.value.is_pivoted():
            # if mapping is pivoted values for parameters are read from pivoted data
            if pivoted_columns:
                val_getter = itemgetter(*pivoted_columns)
                val_num = len(pivoted_columns)
                val_reads = True
        elif self.value.returns_value():
            val_getter, val_num, val_reads = self.value.create_getter_function(
                pivoted_columns, pivoted_data, data_header
            )
        getters["value"] = (val_getter, val_num, val_reads)
        if (is_pivoted or self.is_pivoted()) and not self.alternative_name.is_pivoted():
            # if mapping is pivoted values for parameters are read from pivoted data
            if pivoted_columns and self._alternative_name.returns_value():
                getters["alternative_name"] = (itemgetter(*pivoted_columns), len(pivoted_columns), True)
        elif self.alternative_name.returns_value():
            getters["alternative_name"] = self.alternative_name.create_getter_function(
                pivoted_columns, pivoted_data, data_header
            )
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
            self._extra_dimensions = [single_mapping_from_dict_int_str(None)]
            return
        if not isinstance(extra_dimensions, (list, tuple)):
            raise TypeError(
                f"extra_dimensions must be a list or tuple of SingleMappingBase, int, str, dict, instead got: {type(extra_dimensions).__name__}"
            )
        if len(extra_dimensions) != 1:
            raise ValueError(f"extra_dimensions must be of length 1 instead got len: {len(extra_dimensions)}")
        self._extra_dimensions = [single_mapping_from_dict_int_str(extra_dimensions[0])]

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

    def _dimension_display_names(self):
        return [f"Parameter index {i + 1}" for i in range(len(self.extra_dimensions))]

    def display_names(self):
        return super().display_names() + self._dimension_display_names()

    def component_mappings(self):
        return super().component_mappings() + self.extra_dimensions

    def set_component_by_display_name(self, display_name, mapping):
        try:
            ind = self._dimension_display_names().index(display_name)
        except ValueError:
            return super().set_component_by_display_name(display_name, mapping)
        self.extra_dimensions[ind] = mapping
        return True

    def component_issues(self, component_index):
        """see base class"""
        component_issues_getters = self._component_issues_getters()
        if component_index < len(component_issues_getters):
            return component_issues_getters[component_index]()
        dimension_index = component_index - len(component_issues_getters)
        if dimension_index < len(self._extra_dimensions):
            return self.indexes_issues(dimension_index)
        return ""

    def indexes_issues(self, dimension_index):
        dimension = self._extra_dimensions[dimension_index]
        if isinstance(dimension, NoneMapping):
            return "The source type for parameter indexes cannot be None."
        if dimension.reference != 0 and not dimension.reference:
            return "No reference set for parameter indexes."
        return ""


class ParameterMapMapping(ParameterIndexedMapping):
    """
    Attributes:
        compress (bool): if True, compress leaf Maps when reading the data
    """

    PARAMETER_TYPE = "map"
    ALLOW_EXTRA_DIMENSION_NO_RETURN = False

    def __init__(self, name=None, value=None, extra_dimension=None):
        super().__init__(name, value, extra_dimension)
        self.compress = False

    @ParameterIndexedMapping.extra_dimensions.setter
    def extra_dimensions(self, extra_dimensions):
        if extra_dimensions is None:
            self._extra_dimensions = [single_mapping_from_dict_int_str(None)]
            return
        if not isinstance(extra_dimensions, (list, tuple)):
            raise TypeError(
                f"extra_dimensions must be a list or tuple of SingleMappingBase, int, str, dict, instead got: {type(extra_dimensions).__name__}"
            )
        self._extra_dimensions = [single_mapping_from_dict_int_str(ed) for ed in extra_dimensions]

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
            self._extra_dimensions += [single_mapping_from_dict_int_str(None) for _ in range(diff)]

    def raw_data_to_type(self, data):
        out = []
        data = sorted(data, key=lambda x: x[:-1])
        for keys, values in itertools.groupby(data, key=lambda x: x[:-1]):
            values = [items[-1] for items in values if all(i is not None for i in items[-1])]
            if values:
                map_as_dict = self._raw_data_to_dict(values)
                map_ = self._convert_dict_to_map(map_as_dict)
                if self.compress:
                    map_ = convert_leaf_maps_to_specialized_containers(map_)
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

    def to_dict(self):
        map_dict = super().to_dict()
        map_dict["compress"] = self.compress
        return map_dict

    @classmethod
    def from_dict(cls, map_dict):
        super_mapping = super().from_dict(map_dict)
        compress = map_dict.get("compress", False)
        map_ = cls(super_mapping.name, super_mapping.value, super_mapping.extra_dimensions)
        map_.compress = compress
        return map_


class ParameterTimeSeriesMapping(ParameterIndexedMapping):
    PARAMETER_TYPE = "time series"
    ALLOW_EXTRA_DIMENSION_NO_RETURN = False

    def __init__(self, name=None, value=None, extra_dimension=None, options=None):
        super().__init__(name, value, extra_dimension)
        self._options = None
        self.options = options

    def display_names(self):
        return super().display_names() + ["Parameter time index"]

    def component_mappings(self):
        return super().component_mappings() + [self.extra_dimensions[0]]

    def set_component_by_display_name(self, display_name, mapping):
        if display_name == "Parameter time index":
            self.extra_dimensions = [mapping]
            return True
        return super().set_component_by_display_name(display_name, mapping)

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

    def display_names(self):
        return super().display_names() + ["Parameter time pattern index"]

    def component_mappings(self):
        return super().component_mappings() + [self.extra_dimensions[0]]

    def set_component_by_display_name(self, display_name, mapping):
        if display_name == "Parameter time pattern index":
            self.extra_dimensions = [mapping]
            return True
        return super().set_component_by_display_name(display_name, mapping)

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
