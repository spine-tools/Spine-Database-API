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
    single_mapping_from_value,
    create_getter_list,
    create_getter_function_from_function_list,
    multiple_append,
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
    """Base class for ParameterDefinitionMapping and ParameterValueMapping."""

    MAP_TYPE = "Parameter"

    def __init__(self, name=None):
        self.parent = None
        self._name = None
        self.name = name

    def component_names(self):  # pylint: disable=no-self-use
        return ["Parameter names"]

    def component_mappings(self):
        return [self.name]

    def _optional_component_names(self):  # pylint: disable=no-self-use
        return []

    def set_component_by_name(self, name, mapping):
        if name == "Parameter names":
            self.name = mapping
            return True
        return False

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = single_mapping_from_value(name)

    def non_pivoted_columns(self):
        return [m.reference for m in self.component_mappings() if isinstance(m, ColumnMapping) and m.returns_value()]

    def last_pivot_row(self):
        return max(*(m.last_pivot_row() for m in self.component_mappings()), -1)

    def is_pivoted(self):
        return any(m.is_pivoted() for m in self.component_mappings())

    @classmethod
    def from_dict(cls, map_dict):
        raise NotImplementedError()

    def to_dict(self):
        map_dict = {"map_type": self.MAP_TYPE}
        map_dict.update({"name": self.name.to_dict()})
        return map_dict

    def is_valid(self):
        issues = [self.component_issues(k) for k in range(len(self.component_names()))]
        issues = [x for x in issues if x]
        if not issues:
            return True, ""
        return False, ", ".join(issues)

    def component_issues(self, component_index):
        """Returns issues for given mapping component index.

        Args:
            component_index (int)

        Returns:
            str: issue string
        """
        try:
            name = self.component_names()[component_index]
            mapping = self.component_mappings()[component_index]
        except IndexError:
            return ""
        return self._component_issues(name, mapping)

    def _component_issues(self, name, mapping):
        if name in self._optional_component_names():
            return ""
        if isinstance(mapping, NoneMapping):
            return f"The source type for {name} cannot be None."
        if not mapping.is_valid():
            return f"No reference set for {name}."
        return ""

    def create_getter_list(self, pivoted_columns, pivoted_data, data_header):
        if self.name.returns_value():
            return {"parameter_name": self.name.create_getter_function(pivoted_columns, pivoted_data, data_header)}
        return {"parameter_name": (None, None, None)}


class ParameterDefinitionMapping(ParameterMappingBase):

    MAP_TYPE = "ParameterDefinition"

    def __init__(self, name=None, default_value=None, parameter_value_list_name=None):
        super().__init__(name)
        self._default_value = None
        self.default_value = default_value
        self._parameter_value_list_name = single_mapping_from_value(parameter_value_list_name)
        self.main_value_name = "Default values"

    def component_names(self):
        return super().component_names() + self.default_value.component_names() + ["Parameter value list names"]

    def component_mappings(self):
        return super().component_mappings() + self.default_value.component_mappings() + [self.parameter_value_list_name]

    def _optional_component_names(self):
        return (
            super()._optional_component_names() + self.default_value.component_names() + ["Parameter value list names"]
        )

    def set_component_by_name(self, name, mapping):
        if name in self.default_value.component_names():
            return self.default_value.set_component_by_name(name, mapping)
        if name == "Parameter value list names":
            self.parameter_value_list_name = mapping
            return True
        return super().set_component_by_name(name, mapping)

    @property
    def default_value(self):
        return self._default_value

    @property
    def parameter_value_list_name(self):
        return self._parameter_value_list_name

    @default_value.setter
    def default_value(self, default_value):
        self._default_value = value_mapping_from_any(default_value)
        self._default_value.parent = self

    @parameter_value_list_name.setter
    def parameter_value_list_name(self, parameter_value_list_name):
        self._parameter_value_list_name = single_mapping_from_value(parameter_value_list_name)

    def _create_value_list_name_getter_list(self, pivoted_columns, pivoted_data, data_header):
        if (
            (self.parent.is_pivoted() or self.name.is_pivoted())
            and not self.alternative_name.is_pivoted()
            and pivoted_columns
            and self._parameter_value_list_name.returns_value()
        ):
            # if mapping is pivoted value list names are read from pivoted data
            return (itemgetter(*pivoted_columns), len(pivoted_columns), True)
        if self.parameter_value_list_name.returns_value():
            return self.parameter_value_list_name.create_getter_function(pivoted_columns, pivoted_data, data_header)
        return None

    def create_getter_list(self, pivoted_columns, pivoted_data, data_header):
        getters = super().create_getter_list(pivoted_columns, pivoted_data, data_header)
        default_value_getter_list = self.default_value.create_getter_function(
            pivoted_columns, pivoted_data, data_header
        )
        value_list_name_getter_list = self._create_value_list_name_getter_list(
            pivoted_columns, pivoted_data, data_header
        )
        if default_value_getter_list is not None:
            getters["default_value"] = default_value_getter_list
        if value_list_name_getter_list is not None:
            getters["parameter_value_list_name"] = value_list_name_getter_list
        return getters

    @classmethod
    def from_dict(cls, map_dict):
        if not isinstance(map_dict, dict):
            raise ValueError("map_dict must be a dict")
        map_type = map_dict.get("map_type", None)
        if map_type is not None and map_type != cls.MAP_TYPE:
            raise ValueError(f"If field 'map_type' is specified, it must be {cls.MAP_TYPE}, instead got {map_type}")
        name = map_dict.get("name", None)
        default_value = map_dict.get("default_value", None)
        parameter_value_list_name = map_dict.get("parameter_value_list_name", None)
        return ParameterDefinitionMapping(name, default_value, parameter_value_list_name)

    def to_dict(self):
        map_dict = super().to_dict()
        map_dict["default_value"] = self.default_value.to_dict()
        map_dict["parameter_value_list_name"] = self.parameter_value_list_name.to_dict()
        return map_dict


class ParameterValueMapping(ParameterMappingBase):
    MAP_TYPE = "ParameterValue"

    def __init__(self, name=None, value=None, alternative_name=None):
        super().__init__(name)
        self._value = None
        self.value = value
        self._alternative_name = single_mapping_from_value(alternative_name)
        self.main_value_name = "Parameter values"

    def component_names(self):
        return super().component_names() + self.value.component_names() + ["Alternative names"]

    def component_mappings(self):
        return super().component_mappings() + self.value.component_mappings() + [self.alternative_name]

    def _optional_component_names(self):
        return super()._optional_component_names() + ["Alternative names"]

    def set_component_by_name(self, name, mapping):
        if name in self.value.component_names():
            return self.value.set_component_by_name(name, mapping)
        if name == "Alternative names":
            self.alternative_name = mapping
            return True
        return super().set_component_by_name(name, mapping)

    @property
    def value(self):
        return self._value

    @property
    def alternative_name(self):
        return self._alternative_name

    @value.setter
    def value(self, value):
        self._value = value_mapping_from_any(value)
        self._value.parent = self

    @alternative_name.setter
    def alternative_name(self, alternative_name):
        self._alternative_name = single_mapping_from_value(alternative_name)

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
        map_dict.update({"value": self.value.to_dict()})
        map_dict.update({"alternative_name": self.alternative_name.to_dict()})
        return map_dict

    def _component_issues(self, name, mapping):
        if name == "Parameter values" and (self.is_pivoted() or self.parent.is_pivoted()):
            return ""
        return super()._component_issues(name, mapping)

    def _create_alternative_name_getter_list(self, pivoted_columns, pivoted_data, data_header):
        if (
            (self.parent.is_pivoted() or self.name.is_pivoted())
            and not self.alternative_name.is_pivoted()
            and pivoted_columns
            and self._alternative_name.returns_value()
        ):
            # if mapping is pivoted alternatives are read from pivoted data
            return (itemgetter(*pivoted_columns), len(pivoted_columns), True)
        if self.alternative_name.returns_value():
            return self.alternative_name.create_getter_function(pivoted_columns, pivoted_data, data_header)
        return (None, None, None)

    def create_getter_list(self, pivoted_columns, pivoted_data, data_header):
        getters = super().create_getter_list(pivoted_columns, pivoted_data, data_header)
        value_getter_list = self.value.create_getter_function(pivoted_columns, pivoted_data, data_header)
        alternative_name_getter_list = self._create_alternative_name_getter_list(
            pivoted_columns, pivoted_data, data_header
        )
        if value_getter_list[0] is not None:
            getters["value"] = value_getter_list
        if alternative_name_getter_list[0] is not None:
            getters["alternative_name"] = alternative_name_getter_list
        return getters

    def raw_data_to_type(self, data):
        return self.value.raw_data_to_type(data)


class SingleValueMapping:
    """Base class for value mappings, that go either in
    ParameterDefinitionMapping's default_value, or ParameterValueMapping's value.
    """

    VALUE_TYPE = "single value"

    def __init__(self, main_value=None):
        self._main_value = single_mapping_from_value(main_value)
        self.parent = None  # NOTE: This is set by ParameterDefinitionMapping or ParameterValueMapping

    def component_names(self):
        return [self.parent.main_value_name]

    def component_mappings(self):
        return [self.main_value]

    def set_component_by_name(self, name, mapping):
        if name == self.parent.main_value_name:
            self.main_value = mapping
            return True
        return False

    def is_pivoted(self):
        return any(m.is_pivoted() for m in self.component_mappings())

    @property
    def main_value(self):
        return self._main_value

    @main_value.setter
    def main_value(self, main_value):
        self._main_value = single_mapping_from_value(main_value)

    @classmethod
    def from_dict(cls, map_dict):
        if not isinstance(map_dict, dict):
            raise ValueError("map_dict must be a dict")
        value_type = map_dict.get("value_type", None)
        if value_type is not None and value_type != cls.VALUE_TYPE:
            raise ValueError(
                f"If field 'value_type' is specified, it must be {cls.VALUE_TYPE}, instead got {value_type}"
            )
        main_value = map_dict.get("main_value", None)
        return SingleValueMapping(main_value)

    def to_dict(self):
        return {
            "main_value": self.main_value.to_dict(),
            "value_type": self.VALUE_TYPE,
        }

    def create_getter_function(self, pivoted_columns, pivoted_data, data_header):
        if (
            (self.parent.parent.is_pivoted() or self.parent.name.is_pivoted())
            and not self.main_value.is_pivoted()
            and pivoted_columns
        ):
            # if mapping is pivoted values are read from pivoted data
            return (itemgetter(*pivoted_columns), len(pivoted_columns), True)
        if self.main_value.returns_value():
            return self.main_value.create_getter_function(pivoted_columns, pivoted_data, data_header)
        return (None, None, None)

    def raw_data_to_type(self, data):
        return data


class ArrayValueMapping(SingleValueMapping):
    ALLOW_EXTRA_DIMENSION_NO_RETURN = True
    VALUE_TYPE = "array"

    def __init__(self, main_value=None, extra_dimension=None):
        super().__init__(main_value)
        self._extra_dimensions = None
        self.extra_dimensions = extra_dimension

    @property
    def extra_dimensions(self):
        return self._extra_dimensions

    @extra_dimensions.setter
    def extra_dimensions(self, extra_dimensions):
        if extra_dimensions is None:
            self._extra_dimensions = [single_mapping_from_value(None)]
            return
        if not isinstance(extra_dimensions, (list, tuple)):
            raise TypeError(
                "extra_dimensions must be a list or tuple of SingleMappingBase, int, str, dict, "
                f"instead got: {type(extra_dimensions).__name__}"
            )
        if len(extra_dimensions) != 1:
            raise ValueError(f"extra_dimensions must be of length 1 instead got len: {len(extra_dimensions)}")
        self._extra_dimensions = [single_mapping_from_value(extra_dimensions[0])]

    @classmethod
    def from_dict(cls, map_dict):
        if not isinstance(map_dict, dict):
            raise ValueError("map_dict must be a dict")
        value_type = map_dict.get("value_type", None)
        if value_type is not None and value_type != cls.VALUE_TYPE:
            raise ValueError(
                f"If field 'parameter_type' is specified, it must be {cls.VALUE_TYPE}, instead got {value_type}"
            )
        main_value = map_dict.get("main_value", None)
        extra_dimensions = map_dict.get("extra_dimensions", None)
        return cls(main_value, extra_dimensions)

    def to_dict(self):
        map_dict = super().to_dict()
        map_dict["extra_dimensions"] = [ed.to_dict() for ed in self.extra_dimensions]
        return map_dict

    def create_getter_function(self, pivoted_columns, pivoted_data, data_header):
        val_getters = super().create_getter_function(pivoted_columns, pivoted_data, data_header)
        if val_getters[0] is None:
            return val_getters
        if all(ed.returns_value() for ed in self.extra_dimensions):
            # create functions to get extra_dimensions if there is a value getter
            ed_getters = create_getter_list(self.extra_dimensions, pivoted_columns, pivoted_data, data_header)
            multiple_append(ed_getters, val_getters)
            # create a function that returns a tuple with extra dimensions and value
            return create_getter_function_from_function_list(*ed_getters)
        if not self.ALLOW_EXTRA_DIMENSION_NO_RETURN:
            # extra dimensions doesn't return anything so don't read anything from the data source
            return None, None, None
        return val_getters

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


class MapValueMapping(ArrayValueMapping):
    """
    Attributes:
        compress (bool): if True, compress leaf Maps when reading the data
    """

    VALUE_TYPE = "map"
    ALLOW_EXTRA_DIMENSION_NO_RETURN = False

    def __init__(self, main_value=None, extra_dimension=None):
        super().__init__(main_value, extra_dimension)
        self.compress = False

    def _dimension_component_names(self):
        return [f"Parameter index {i + 1}" for i in range(len(self.extra_dimensions))]

    def component_names(self):
        return super().component_names() + self._dimension_component_names()

    def component_mappings(self):
        return super().component_mappings() + self.extra_dimensions

    def set_component_by_name(self, name, mapping):
        try:
            ind = self._dimension_component_names().index(name)
        except ValueError:
            return super().set_component_by_name(name, mapping)
        self.extra_dimensions[ind] = mapping
        return True

    @ArrayValueMapping.extra_dimensions.setter
    def extra_dimensions(self, extra_dimensions):
        if extra_dimensions is None:
            self._extra_dimensions = [single_mapping_from_value(None)]
            return
        if not isinstance(extra_dimensions, (list, tuple)):
            raise TypeError(
                "extra_dimensions must be a list or tuple of SingleMappingBase, int, str, dict, "
                f"instead got: {type(extra_dimensions).__name__}"
            )
        self._extra_dimensions = [single_mapping_from_value(ed) for ed in extra_dimensions]

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
            self._extra_dimensions += [single_mapping_from_value(None) for _ in range(diff)]

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
                value = MapValueMapping._convert_dict_to_map(value)
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
        map_ = cls(super_mapping.main_value, super_mapping.extra_dimensions)
        map_.compress = compress
        return map_


class TimeSeriesValueMapping(ArrayValueMapping):
    VALUE_TYPE = "time series"
    ALLOW_EXTRA_DIMENSION_NO_RETURN = False

    def __init__(self, main_value=None, extra_dimension=None, options=None):
        super().__init__(main_value, extra_dimension)
        self._options = None
        self.options = options

    def component_names(self):
        return super().component_names() + ["Parameter time index"]

    def component_mappings(self):
        return super().component_mappings() + [self.extra_dimensions[0]]

    def set_component_by_name(self, name, mapping):
        if name == "Parameter time index":
            self.extra_dimensions = [mapping]
            return True
        return super().set_component_by_name(name, mapping)

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
        value_type = map_dict.get("value_type", None)
        if value_type is not None and value_type != cls.VALUE_TYPE:
            raise ValueError(
                f"If field 'value_type' is specified, it must be {cls.VALUE_TYPE}, instead got {value_type}"
            )

        main_value = map_dict.get("main_value", None)
        extra_dimensions = map_dict.get("extra_dimensions", None)
        options = map_dict.get("options", None)
        if options:
            options = TimeSeriesOptions.from_dict(options)
        return cls(main_value, extra_dimensions, options)

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


class TimePatternValueMapping(ArrayValueMapping):
    VALUE_TYPE = "time pattern"
    ALLOW_EXTRA_DIMENSION_NO_RETURN = False

    def component_names(self):
        return super().component_names() + ["Parameter time pattern index"]

    def component_mappings(self):
        return super().component_mappings() + [self.extra_dimensions[0]]

    def set_component_by_name(self, name, mapping):
        if name == "Parameter time pattern index":
            self.extra_dimensions = [mapping]
            return True
        return super().set_component_by_name(name, mapping)

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
    map_type = map_dict.get("map_type", "")
    if map_type == NoneMapping.MAP_TYPE:
        return NoneMapping()
    if map_type == "parameter" or "parameter_type" in map_dict:
        return _legacy_parameter_mapping_from_dict(map_dict)
    map_type_to_class = {
        "ParameterDefinition": ParameterDefinitionMapping,
        "ParameterValue": ParameterValueMapping,
    }
    map_class = map_type_to_class.get(map_type, ParameterValueMapping)
    if map_type is None:
        map_dict.update(map_type=map_class.MAP_TYPE)
    return map_class.from_dict(map_dict)


def _legacy_parameter_mapping_from_dict(map_dict):
    parameter_type = map_dict.pop("parameter_type", None)
    if parameter_type == "definition":
        map_dict.update(map_type="ParameterDefinition")
        return ParameterDefinitionMapping.from_dict(map_dict)
    value_dict = map_dict.copy()
    value_dict.pop("name", None)
    value_dict["value_type"] = parameter_type
    value_dict["main_value"] = value_dict.pop("value", None)
    map_dict.update(map_type="ParameterValue", value=value_dict)
    return ParameterValueMapping.from_dict(map_dict)


def value_mapping_from_any(value):
    if isinstance(value, SingleValueMapping):
        return value
    if isinstance(value, dict):
        value_type = value.get("value_type")
        value_type_to_class = {
            "single value": SingleValueMapping,
            "array": ArrayValueMapping,
            "map": MapValueMapping,
            "time series": TimeSeriesValueMapping,
            "time pattern": TimePatternValueMapping,
        }
        map_class = value_type_to_class.get(value_type, SingleValueMapping)
        return map_class.from_dict(value)
    return SingleValueMapping()
