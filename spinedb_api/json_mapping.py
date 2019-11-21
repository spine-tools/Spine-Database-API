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

"""
Classes for reading data with json mapping specifications

:author: P. Vennström (VTT)
:date:   22.02.2018
"""

from operator import itemgetter
import itertools
import json
import math


from .parameter_value import TimeSeriesVariableResolution, TimePattern, ParameterValueFormatError, SUPPORTED_TYPES
from .exception import TypeConversionError


# Constants for json spec
ROW = "row"
COLUMN = "column"
COLUMN_NAME = "column_name"
OBJECTCLASS = "ObjectClass"
RELATIONSHIPCLASS = "RelationshipClass"
PARAMETER = "parameter"
PARAMETERCOLUMN = "parameter_column"
PARAMETERCOLUMNCOLLECTION = "parameter_column_collection"
MAPPINGCOLLECTION = "collection"
VALID_PARAMETER_TYPES = ['time series', 'time pattern', '1d array', 'single value', 'definition']


def valid_mapping_or_value(mapping):
    if isinstance(mapping, Mapping):
        valid, msg = mapping.is_valid()
        if not valid:
            return False, msg
    if mapping is None:
        return False, "No mapping specified."
    return True, ""


def none_is_minus_inf(value):
    """Returns minus infinity if value is None, otherwise returns the value.
    Used in the `key` argument to the `max` function.
    """
    if value is None:
        return float("-inf")
    return value


def mapping_from_dict_int_str(value, map_type=COLUMN):
    """Creates Mapping object if `value` is a `dict` or `int`;
    if `str` or `None` returns same value. If `int`, the Mapping is created
    with map_type == column (default) unless other type is specified
    """
    if value is None:
        return None
    if isinstance(value, dict):
        return Mapping.from_dict(value)
    elif isinstance(value, int):
        return Mapping(map_type=map_type, value_reference=value)
    elif isinstance(value, str):
        return value
    else:
        raise TypeError(f"value must be dict, int or str, instead got {type(value)}")


class Mapping:
    """
    Class for holding and validating Mapping specification::
    
        Mapping {
            map_type: 'column' | 'row' | 'column_name'
            value_reference: str | int
            append_str: str
            prepend_str: str
        }
    """

    _required_fields = ("map_type",)

    def __init__(self, map_type=COLUMN, value_reference=None, append_str=None, prepend_str=None):

        # this needs to be before value_reference because value_reference uses
        # self.map_type
        self._map_type = COLUMN
        self._value_reference = None
        self._append_str = None
        self._prepend_str = None
        self.map_type = map_type
        self.value_reference = value_reference
        self.append_str = append_str
        self.prepend_str = prepend_str

    @property
    def append_str(self):
        return self._append_str

    @property
    def prepend_str(self):
        return self._prepend_str

    @property
    def map_type(self):
        return self._map_type

    @property
    def value_reference(self):
        return self._value_reference

    @append_str.setter
    def append_str(self, append_str=None):
        if append_str is not None and not isinstance(append_str, str):
            raise ValueError(f"append_str must be a None or str, instead got {type(append_str)}")
        self._append_str = append_str

    @prepend_str.setter
    def prepend_str(self, prepend_str=None):
        if prepend_str is not None and not isinstance(prepend_str, str):
            raise ValueError(f"prepend_str must be None or str, instead got {type(prepend_str)}")
        self._prepend_str = prepend_str

    @map_type.setter
    def map_type(self, map_type=COLUMN):
        if map_type not in [ROW, COLUMN, COLUMN_NAME]:
            raise ValueError(f"map_type must be '{ROW}', '{COLUMN}' or '{COLUMN_NAME}', instead got '{map_type}'")
        self._map_type = map_type

    @value_reference.setter
    def value_reference(self, value_reference=None):
        if value_reference is not None and not isinstance(value_reference, (str, int)):
            raise ValueError(f"value_reference must be str or int, instead got {type(value_reference)}")
        if isinstance(value_reference, str) and self.map_type == ROW:
            raise ValueError(f'value_reference cannot be str if map_type = "{self.map_type}"')
        if isinstance(value_reference, int):
            if value_reference < 0 and self.map_type in (COLUMN, COLUMN_NAME):
                raise ValueError(f'value_reference must be >= 0 if map_type is "{COLUMN}" or "{COLUMN_NAME}"')
            if value_reference < -1 and self.map_type == ROW:
                raise ValueError(f'value_reference must be >= -1 if map_type is "{ROW}"')
        self._value_reference = value_reference

    def is_pivoted(self):
        """Returns True if Mapping type is ROW"""
        return self.map_type == ROW

    def last_pivot_row(self):
        if self.is_pivoted():
            return self.value_reference
        else:
            return -1

    def to_dict(self):
        map_dict = {"value_reference": self.value_reference, "map_type": self.map_type}
        if self.append_str is not None:
            map_dict.update({"append_str": self.append_str})
        if self.prepend_str is not None:
            map_dict.update({"prepend_str": self.prepend_str})
        return map_dict

    @classmethod
    def from_dict(cls, map_dict):
        if not isinstance(map_dict, dict):
            raise TypeError(f"map_dict must be a dict, instead got {type(map_dict)}")
        if not all(k in map_dict.keys() for k in cls._required_fields):
            raise KeyError("dict must contain keys: {}".format(cls._required_fields))
        map_type = map_dict["map_type"]
        append_str = map_dict.get("append_str", None)
        prepend_str = map_dict.get("prepend_str", None)
        value_reference = map_dict.get("value_reference", None)
        return Mapping(map_type, value_reference, append_str, prepend_str)
    
    def is_valid(self):
        if self.value_reference is None:
            return False, "Mapping is missing a reference"
        return True, ""


class ParameterMapping:
    """
    Class for holding and validating Mapping specification::

        ParameterMapping {
            map_type: 'parameter'
            name: Mapping | str
            value: Mapping | None
            extra_dimensions: [Mapping] | None
            parameter_type: 'time series' | 'time pattern' | '1d array' | 'single value'
        }
    """

    def __init__(self, name=None, value=None, extra_dimensions=None, parameter_type='single value'):

        self._name = None
        self._value = None
        self._extra_dimensions = None
        self._parameter_type = None
        self.name = name
        self.value = value
        self.extra_dimensions = extra_dimensions
        self.parameter_type = parameter_type
        self._map_type = PARAMETER

    def non_pivoted_columns(self):
        non_pivoted_columns = []
        if self.name is not None:
            if isinstance(self.name, Mapping) and self.name.map_type == COLUMN:
                non_pivoted_columns.append(self.name.value_reference)
        if self.value is not None and not self.is_pivoted():
            if isinstance(self.value, Mapping) and self.value.map_type == COLUMN:
                non_pivoted_columns.append(self.value.value_reference)
        if self.extra_dimensions is not None:
            for extra_dim in self.extra_dimensions:
                if isinstance(extra_dim, Mapping) and extra_dim.map_type == COLUMN:
                    non_pivoted_columns.append(extra_dim.value_reference)
        return non_pivoted_columns

    def last_pivot_row(self):
        last_pivot_rows = []
        if isinstance(self.name, Mapping):
            last_pivot_rows.append(self.name.last_pivot_row())
        if self.extra_dimensions is not None:
            last_pivot_rows += [m.last_pivot_row() for m in self.extra_dimensions if isinstance(m, Mapping)]
        return max(last_pivot_rows, default=-1)

    def is_pivoted(self):
        if isinstance(self.name, Mapping) and self.name.is_pivoted():
            return True
        if self.extra_dimensions is not None and any(ed.is_pivoted() for ed in self.extra_dimensions if isinstance(ed, Mapping)):
            return True
        return False

    @property
    def name(self):
        return self._name

    @property
    def value(self):
        return self._value

    @property
    def extra_dimensions(self):
        return self._extra_dimensions
    
    @property
    def parameter_type(self):
        return self._parameter_type
    
    @parameter_type.setter
    def parameter_type(self, parameter_type):
        if not isinstance(parameter_type, str):
            raise TypeError(f"parameter_type must be str, instead got: {type(parameter_type)}")
        if parameter_type.lower() not in VALID_PARAMETER_TYPES:
            raise ValueError(f"parameter_type must be one of the following: {VALID_PARAMETER_TYPES}, instead got {parameter_type}")
        self._parameter_type = parameter_type

    @name.setter
    def name(self, name=None):
        if name is not None and not isinstance(name, (str, Mapping)):
            raise ValueError(f"""name must be a None, str or Mapping, instead got {type(name)}""")
        self._name = name

    @value.setter
    def value(self, value=None):
        if value is not None and not isinstance(value, (str, Mapping)):
            raise ValueError(f"""value must be a None, Mapping or string, instead got {type(value)}""")
        self._value = value

    @extra_dimensions.setter
    def extra_dimensions(self, extra_dimensions=None):
        if extra_dimensions is not None and not all(
            isinstance(ed, (Mapping, str)) or ed is None for ed in extra_dimensions
        ):
            ed_types = [type(ed) for ed in extra_dimensions]
            raise TypeError(f"""extra_dimensions must be a list of Mapping or str, instead got {ed_types}""")
        self._extra_dimensions = extra_dimensions

    @classmethod
    def from_dict(cls, map_dict):
        if not isinstance(map_dict, dict):
            raise ValueError("map_dict must be a dict")
        name = mapping_from_dict_int_str(map_dict.get("name", None))
        value = mapping_from_dict_int_str(map_dict.get("value", None))
        extra_dimensions = map_dict.get("extra_dimensions", None)
        parameter_type = map_dict.get("parameter_type", 'single value')
        if isinstance(extra_dimensions, list):
            extra_dimensions = [mapping_from_dict_int_str(ed) for ed in extra_dimensions]
        return ParameterMapping(name, value, extra_dimensions, parameter_type)

    def to_dict(self):
        map_dict = {"map_type": self._map_type}
        if self.name is not None:
            if isinstance(self.name, Mapping):
                map_dict.update({"name": self.name.to_dict()})
            else:
                map_dict.update({"name": self.name})
        if self.value is not None:
            if isinstance(self.value, Mapping):
                map_dict.update({"value": self.value.to_dict()})
            else:
                map_dict.update({"value": self.value})
        if self.extra_dimensions is not None:
            extra_dim_list = []
            for extra_dim in self.extra_dimensions:
                if extra_dim is None:
                    extra_dim = Mapping()
                extra_dim = extra_dim if isinstance(extra_dim, str) else extra_dim.to_dict()
                extra_dim_list.append(extra_dim)
            map_dict.update({"extra_dimensions": extra_dim_list})
        map_dict.update({"parameter_type": self.parameter_type})
        return map_dict
    
    def is_valid(self, parent_pivot: bool):
        # check that parameter mapping has a valid name mapping
        name_valid, msg = valid_mapping_or_value(self.name)
        if not name_valid:
            return False, "Parameter mapping must have a valid name mapping, current name mapping is not valid: " + msg
        if self.parameter_type != 'definition':
            # check if value mapping exists
            if not (self.is_pivoted() or parent_pivot):
                value_valid, msg = valid_mapping_or_value(self.value)
                if not value_valid:
                    return False, "Parameter mapping must have a valid value mapping, current name mapping is not valid: " + msg
            # check that "single value" and "1d array" doesn't have any extra dimensions
            if self.parameter_type in ["single value", "1d array"] and self.extra_dimensions:
                return False, "Parameter mapping of type 'single value' or '1d array' cannot have any extra dimension mappings specified."
            # check that extra dimension exists if needed
            if self.parameter_type in ["time series", "time pattern"]:
                if self.extra_dimensions is None or len(self.extra_dimensions) != 1:
                    return False, "Parameter mapping of type 'time series' or 'time pattern' must have 1 extra dimensions mapping specified."
                for extra_dim in self.extra_dimensions:
                    extra_dim_valid, msg = valid_mapping_or_value(extra_dim)
                    if not extra_dim_valid:
                        return False, "Parameter mapping of type 'time series' or 'time pattern' must have valid extra dimensions mapping, current is not valid: " + msg
        return True, ""


class ParameterColumnCollectionMapping:
    """
    Class for holding and validating Mapping specification::
    
        ParameterColumnCollectionMapping {
            map_type: 'parameter_column_collection'
            parameters: [ParameterColumnMapping]
            extra_dimensions: [Mapping] | None
        }
    """

    def __init__(self, parameters=None, extra_dimensions=None):
        self._parameters = None
        self._extra_dimensions = None
        self.parameters = parameters
        self.extra_dimensions = extra_dimensions
        self._map_type = PARAMETERCOLUMNCOLLECTION

    def non_pivoted_columns(self):
        non_pivoted_columns = []
        if self.parameters is not None:
            for parameter in self.parameters:
                non_pivoted_columns.extend(parameter.non_pivoted_columns())
        if self.extra_dimensions is not None:
            for extra_dim in self.extra_dimensions:
                if isinstance(extra_dim, Mapping) and extra_dim.map_type == COLUMN:
                    non_pivoted_columns.append(extra_dim.value_reference)
        return non_pivoted_columns

    def last_pivot_row(self):
        last_pivot_rows = []
        if self.extra_dimensions is not None:
            last_pivot_rows += [m.last_pivot_row() for m in self.extra_dimensions if isinstance(m, Mapping)]
        return max(last_pivot_rows, default=None)

    def is_pivoted(self):
        if self.extra_dimensions is not None:
            return any(ed.is_pivoted() for ed in self.extra_dimensions if isinstance(ed, Mapping))
        return False

    @property
    def parameters(self):
        return self._parameters

    @property
    def extra_dimensions(self):
        return self._extra_dimensions

    @parameters.setter
    def parameters(self, parameters=None):
        if parameters is not None and not isinstance(parameters, list):
            raise ValueError(f"""parameters must be a None or list, instead got {type(parameters)}""")
        for i, parameter in enumerate(parameters):
            if not isinstance(parameter, ParameterColumnMapping):
                raise ValueError(
                    f"""parameters must be a list with all ParameterColumnMapping, instead got {type(parameter)} on index {i}"""
                )
        self._parameters = parameters

    @extra_dimensions.setter
    def extra_dimensions(self, extra_dimensions=None):
        if extra_dimensions is not None and not all(isinstance(ex, (Mapping, str)) for ex in extra_dimensions):
            ed_types = [type(ed) for ed in extra_dimensions]
            raise TypeError(f"""extra_dimensions must be a list of Mapping or str, instead got {ed_types}""")
        self._extra_dimensions = extra_dimensions

    @classmethod
    def from_dict(cls, map_dict):
        if not isinstance(map_dict, dict):
            raise TypeError("map_dict must be a dict, instead got {type(map_dict)}")
        parameters = map_dict.get("parameters", None)
        if isinstance(parameters, list):
            for i, parameter in enumerate(parameters):
                if isinstance(parameter, int):
                    parameters[i] = ParameterColumnMapping(column=parameter)
                else:
                    parameters[i] = ParameterColumnMapping.from_dict(parameter)
        extra_dimensions = map_dict.get("extra_dimensions", None)
        if isinstance(extra_dimensions, list):
            extra_dimensions = [mapping_from_dict_int_str(ed) for ed in extra_dimensions]
        return ParameterColumnCollectionMapping(parameters, extra_dimensions)

    def to_dict(self):
        map_dict = {"map_type": self._map_type}
        if self.parameters is not None:
            parameter = [p if isinstance(p, str) else p.to_dict() for p in self.parameters]
            map_dict.update({"parameters": parameter})
        if self.extra_dimensions is not None:
            extra_dim = [ed if isinstance(ed, str) else ed.to_dict() for ed in self.extra_dimensions]
            map_dict.update({"extra_dimensions": extra_dim})
        return map_dict


class ParameterColumnMapping:
    """
    Class for holding and validating Mapping specification::
    
        ParameterColumnMapping {
            map_type: 'parameter_column'
            name: str | None #overrides column name
            column: str | int
            append_str: str | None
            prepend_str: str | None]
        }
    """

    def __init__(self, name=None, column=None, append_str=None, prepend_str=None):
        self._name = None
        self._column = None
        self._append_str = None
        self._prepend_str = None

        self.name = name
        self.column = column
        self.append_str = append_str
        self.prepend_str = prepend_str
        self._map_type = PARAMETERCOLUMN

    def non_pivoted_columns(self):
        non_pivoted_columns = []
        if self.column is not None:
            non_pivoted_columns = [self.column]
        return non_pivoted_columns

    def last_pivot_row(self):
        return -1

    def is_pivoted(self):
        return False

    @property
    def name(self):
        return self._name

    @property
    def column(self):
        return self._column

    @property
    def append_str(self):
        return self._append_str

    @property
    def prepend_str(self):
        return self._prepend_str

    @name.setter
    def name(self, name=None):
        if name is not None and not isinstance(name, (str,)):
            raise ValueError(f"""name must be a None or str, instead got {type(name)}""")
        self._name = name

    @column.setter
    def column(self, column=None):
        if column is not None and not isinstance(column, (str, int)):
            raise ValueError(f"""column must be a None, str or int, instead got {type(column)}""")
        self._column = column

    @append_str.setter
    def append_str(self, append_str=None):
        if append_str is not None and not isinstance(append_str, (str,)):
            raise TypeError(f"""append_str must be a None or str, instead got {type(append_str)}""")
        self._append_str = append_str

    @prepend_str.setter
    def prepend_str(self, prepend_str=None):
        if prepend_str is not None and not isinstance(prepend_str, (str,)):
            raise TypeError(f"""prepend_str must be a None or str, instead got {type(prepend_str)}""")
        self._prepend_str = prepend_str

    @classmethod
    def from_dict(cls, map_dict):
        if not isinstance(map_dict, dict):
            raise ValueError("map_dict must be a dict")
        name = map_dict.get("name", None)
        column = map_dict.get("column", None)
        append_str = map_dict.get("append_str", None)
        prepend_str = map_dict.get("prepend_str", None)
        return ParameterColumnMapping(name, column, append_str, prepend_str)

    def to_dict(self):
        map_dict = {"map_type": self._map_type}
        if self.name is not None:
            map_dict.update({"name": self.name})
        if self.column is not None:
            map_dict.update({"column": self.column})
        if self.append_str is not None:
            map_dict.update({"append_str": self.append_str})
        if self.prepend_str is not None:
            map_dict.update({"prepend_str": self.prepend_str})
        return map_dict


class ObjectClassMapping:
    """
    Class for holding and validating Mapping specification::

        ObjectClassMapping {
            map_type: 'object'
            name: str | Mapping
            objects: Mapping | str | None
            parameters: ParameterMapping | ParameterColumnCollectionMapping | None
        }
    """

    def __init__(self, name=None, obj=None, parameters=None, skip_columns=None, read_start_row=0):
        self._name = None
        self._object = None
        self._parameters = None
        self._skip_columns = None
        self._read_start_row = None
        self.name = name
        self.object = obj
        self.parameters = parameters
        self.skip_columns = skip_columns
        self.read_start_row = read_start_row
        self._map_type = OBJECTCLASS

    def non_pivoted_columns(self):
        non_pivoted_columns = []
        if self.name is not None:
            if isinstance(self.name, Mapping) and self.name.map_type == COLUMN:
                non_pivoted_columns.append(self.name.value_reference)
        if self.object is not None:
            if isinstance(self.object, Mapping) and self.object.map_type == COLUMN:
                non_pivoted_columns.append(self.object.value_reference)
        if self.parameters is not None:
            non_pivoted_columns.extend(self.parameters.non_pivoted_columns())
        return non_pivoted_columns

    def last_pivot_row(self):
        last_pivot_row = -1
        if isinstance(self.name, Mapping):
            last_pivot_row = self.name.last_pivot_row()
        if isinstance(self.object, Mapping):
            last_pivot_row = max(last_pivot_row, self.object.last_pivot_row())
        if isinstance(self.parameters, (ParameterMapping, ParameterColumnCollectionMapping)):
            last_pivot_row = max(last_pivot_row, self.parameters.last_pivot_row())
        return last_pivot_row

    def is_pivoted(self):
        if isinstance(self.name, Mapping) and self.name.is_pivoted():
            return True
        if isinstance(self.object, Mapping) and self.object.is_pivoted():
            return True
        if isinstance(self.parameters, (ParameterMapping, ParameterColumnCollectionMapping)) and self.parameters.is_pivoted():
            return True
        return False

    @property
    def read_start_row(self):
        return self._read_start_row

    @property
    def skip_columns(self):
        return self._skip_columns

    @property
    def name(self):
        return self._name

    @property
    def object(self):
        return self._object

    @property
    def parameters(self):
        return self._parameters

    @read_start_row.setter
    def read_start_row(self, row):
        if not isinstance(row, int):
            raise TypeError(f"row must be int, instead got {type(row)}")
        if row < 0:
            raise ValueError(f"row must be >= 0, istead was: {row}")
        self._read_start_row = row

    @name.setter
    def name(self, name=None):
        if name is not None and not isinstance(name, (str, Mapping)):
            raise TypeError(
                f"""name must be a None, str or Mapping,
                            instead got {type(name)}"""
            )
        self._name = name

    @object.setter
    def object(self, obj=None):
        if obj is not None and not isinstance(obj, (str, Mapping)):
            raise ValueError(
                f"""obj must be None, str or Mapping,
                             instead got {type(obj)}"""
            )
        self._object = obj

    @parameters.setter
    def parameters(self, parameters=None):
        if parameters is not None and not isinstance(parameters, (ParameterMapping, ParameterColumnCollectionMapping)):
            raise ValueError(
                f"""parameters must be a None, ParameterMapping or
                             ParameterColumnCollectionMapping, instead got
                             {type(parameters)}"""
            )
        self._parameters = parameters

    @skip_columns.setter
    def skip_columns(self, skip_columns=None):
        if skip_columns is None:
            self._skip_columns = None
        else:
            if isinstance(skip_columns, (str, int)):
                skip_columns = [skip_columns]
            if isinstance(skip_columns, list):
                for i, column in enumerate(skip_columns):
                    if not isinstance(column, (str, int)):
                        raise TypeError(
                            f"""skip_columns must be str, int or
                                        list of str, int, instead got list
                                        with {type(column)} on index {i}"""
                        )
            else:
                raise TypeError(
                    f"""skip_columns must be str, int or list of
                                str, int, instead {type(skip_columns)}"""
                )
            self._skip_columns = skip_columns

    @classmethod
    def from_dict(cls, map_dict):
        if not isinstance(map_dict, dict):
            raise TypeError("map_dict must be a dict, instead got {type(map_dict)}")
        if map_dict.get("map_type", None) != OBJECTCLASS:
            raise ValueError(
                f'''map_dict must contain field "map_type"
                             with value: "{OBJECTCLASS}"'''
            )
        name = mapping_from_dict_int_str(map_dict.get("name", None))
        obj = mapping_from_dict_int_str(map_dict.get("object", None))
        parameters = map_dict.get("parameters", None)
        skip_columns = map_dict.get("skip_columns", None)
        read_start_row = map_dict.get("read_start_row", 0)
        if isinstance(parameters, dict):
            p_type = parameters.get("map_type", None)
            if p_type == PARAMETER:
                parameters = ParameterMapping.from_dict(parameters)
            elif p_type == PARAMETERCOLUMNCOLLECTION:
                parameters = ParameterColumnCollectionMapping.from_dict(parameters)
        elif isinstance(parameters, list) and all(isinstance(p, (int, dict)) for p in parameters):
            parameters = {"map_type": PARAMETERCOLUMNCOLLECTION, "parameters": list(parameters)}
            parameters = ParameterColumnCollectionMapping.from_dict(parameters)

        return ObjectClassMapping(name, obj, parameters, skip_columns, read_start_row)

    def to_dict(self):
        map_dict = {"map_type": self._map_type}
        if self.name is not None:
            if isinstance(self.name, Mapping):
                map_dict.update(name=self.name.to_dict())
            else:
                map_dict.update(name=self.name)
        if self.object is not None:
            map_dict.update(object=self.object if isinstance(self.object, str) else self.object.to_dict())
        if self.parameters is not None:
            map_dict.update(parameters=self.parameters.to_dict())
        if self.skip_columns:
            map_dict.update(skip_columns=self.skip_columns)
        map_dict.update(read_start_row=self.read_start_row)
        return map_dict
    
    def is_valid(self):
        # check that parameter mapping has a valid name mapping
        name_valid, msg = valid_mapping_or_value(self.name)
        if not name_valid:
            return False, "Object class mapping must have a valid name mapping, current name mapping is not valid: " + msg
        
        #check that object mapping is valid if it exists:
        if isinstance(self.object, Mapping):
            object_valid, msg = self.object.is_valid()
            if not object_valid:
                return False, "Object class mapping has an invalid object mapping: " + msg

        if self.parameters is not None:
            # check if parameter mapping is valid if of definition type:
            param_valid, msg = self.parameters.is_valid(self.is_pivoted())
            if not param_valid:
                return False, "Object class mapping has an invalid parameters mapping: " + msg

            # check that object is valid if we have a parameter mapping that has a value mapping
            if self.parameters.parameter_type != 'definition':
                object_valid, msg = valid_mapping_or_value(self.object)
                if not object_valid:
                    return False, "Object class mapping has a parameter mapping but object mapping is not valid: " + msg
        return True, ""


class RelationshipClassMapping:
    """
    Class for holding and validating Mapping specification::
    
        RelationshipClassMapping {
            map_type: 'relationship'
            name:  str | Mapping
            object_classes: [str | Mapping] | None
            objects: [str | Mapping] | None
            parameters: ParameterMapping | ParameterColumnCollectionMapping | None
        }
    """

    def __init__(
        self, name=None, object_classes=None, objects=None, parameters=None, skip_columns=None, import_objects=False, read_start_row=0
    ):
        self._map_type = RELATIONSHIPCLASS
        self._name = None
        self._object_classes = None
        self._objects = None
        self._parameters = None
        self._skip_columns = None
        self._import_objects = None
        self._read_start_row = None
        self.name = name
        self.object_classes = object_classes
        self.objects = objects
        self.parameters = parameters
        self.skip_columns = skip_columns
        self.import_objects = import_objects
        self.read_start_row = read_start_row

    def non_pivoted_columns(self):
        non_pivoted_columns = []
        if self.name is not None:
            if isinstance(self.name, Mapping) and self.name.map_type == COLUMN:
                non_pivoted_columns.append(self.name.value_reference)
        if self.object_classes is not None:
            for object_class in self.object_classes:
                if isinstance(object_class, Mapping) and object_class.map_type == COLUMN:
                    non_pivoted_columns.append(object_class.value_reference)
        if self.objects is not None:
            for obj in self.objects:
                if isinstance(obj, Mapping) and obj.map_type == COLUMN:
                    non_pivoted_columns.append(obj.value_reference)
        if self.parameters is not None:
            non_pivoted_columns.extend(self.parameters.non_pivoted_columns())
        return non_pivoted_columns

    def last_pivot_row(self):
        """Gets the highest rownumber of pivoted mapping, returns None if not pivoted
        
        Returns:
            [int] -- highest pivoted row
        """
        last_pivot_row = -1
        if isinstance(self.name, Mapping):
            last_pivot_row = self.name.last_pivot_row()
        if self.object_classes is not None:
            for object_class in self.object_classes:
                if isinstance(object_class, Mapping):
                    last_pivot_row = max(last_pivot_row, object_class.last_pivot_row())
        if self.objects is not None:
            for obj in self.objects:
                if isinstance(obj, Mapping):
                    last_pivot_row = max(last_pivot_row, obj.last_pivot_row())
        if self.parameters is not None:
            last_pivot_row = max(last_pivot_row, self.parameters.last_pivot_row())
        return last_pivot_row

    def is_pivoted(self):
        """Check if mapping is pivoted
        
        Returns:
            [bool] -- True/False if mapping is pivoted
        """
        if isinstance(self.name, Mapping) and self.name.is_pivoted():
            return True
        if self.object_classes is not None and any(oc.is_pivoted() for oc in self.object_classes if isinstance(oc, Mapping)):
            return True
        if self.objects is not None and any(o.is_pivoted() for o in self.objects if isinstance(o, Mapping)):
            return True
        if self.parameters is not None and self.parameters.is_pivoted():
            return True
        return False

    @property
    def read_start_row(self):
        return self._read_start_row

    @property
    def import_objects(self):
        return self._import_objects

    @property
    def name(self):
        return self._name

    @property
    def object_classes(self):
        return self._object_classes

    @property
    def objects(self):
        return self._objects

    @property
    def parameters(self):
        return self._parameters

    @property
    def skip_columns(self):
        return self._skip_columns

    @read_start_row.setter
    def read_start_row(self, row):
        if not isinstance(row, int):
            raise TypeError(f"row must be int, instead got {type(row)}")
        if row < 0:
            raise ValueError(f"row must be >= 0, istead was: {row}")
        self._read_start_row = row

    @import_objects.setter
    def import_objects(self, import_objects):
        if import_objects:
            self._import_objects = True
        else:
            self._import_objects = False

    @skip_columns.setter
    def skip_columns(self, skip_columns=None):
        if skip_columns is None:
            self._skip_columns = None
        else:
            if isinstance(skip_columns, (str, int)):
                skip_columns = [skip_columns]
            if isinstance(skip_columns, list):
                for i, column in enumerate(skip_columns):
                    if not isinstance(column, (str, int)):
                        raise TypeError(
                            f"""skip_columns must be str, int or
                                        list of str, int, instead got list
                                        with {type(column)} on index {i}"""
                        )
            else:
                raise TypeError(
                    f"""skip_columns must be str, int or list of
                                str, int, instead {type(skip_columns)}"""
                )
            self._skip_columns = skip_columns

    @name.setter
    def name(self, name=None):
        if name is not None and not isinstance(name, (str, Mapping)):
            raise ValueError(
                f"""name must be a None, str or Mapping,
                             instead got {type(name)}"""
            )
        self._name = name

    @object_classes.setter
    def object_classes(self, object_classes=None):
        if object_classes is not None and not all(isinstance(o, (Mapping, str)) or o is None for o in object_classes):
            raise TypeError("name must be a None, str or Mapping | str}")
        self._object_classes = object_classes

    @objects.setter
    def objects(self, objects=None):
        if objects is not None and not all(isinstance(o, (Mapping, str)) or o is None for o in objects):
            raise TypeError("objects must be a None, or list of Mapping | str")
        self._objects = objects

    @parameters.setter
    def parameters(self, parameters=None):
        if parameters is not None and not isinstance(parameters, (ParameterMapping, ParameterColumnCollectionMapping)):
            raise ValueError(
                f"""parameters must be a None, ParameterMapping or
                             ParameterColumnCollectionMapping,
                             instead got {type(parameters)}"""
            )
        self._parameters = parameters

    @classmethod
    def from_dict(cls, map_dict):
        if not isinstance(map_dict, dict):
            raise ValueError("map_dict must be a dict")
        if map_dict.get("map_type", None) != RELATIONSHIPCLASS:
            raise ValueError(
                f'''map_dict must contain field "map_type"
                             with value: "{RELATIONSHIPCLASS}"'''
            )
        name = mapping_from_dict_int_str(map_dict.get("name", None))
        objects = map_dict.get("objects", None)
        if isinstance(objects, list):
            objects = [mapping_from_dict_int_str(o) for o in objects]
        object_classes = map_dict.get("object_classes", None)
        if isinstance(object_classes, list):
            object_classes = [mapping_from_dict_int_str(o, COLUMN_NAME) for o in object_classes]
        parameters = map_dict.get("parameters", None)
        if isinstance(parameters, dict):
            p_type = parameters.get("map_type", None)
            if p_type == PARAMETER:
                parameters = ParameterMapping.from_dict(parameters)
            elif p_type == PARAMETERCOLUMNCOLLECTION:
                parameters = ParameterColumnCollectionMapping.from_dict(parameters)
        elif isinstance(parameters, list) and all(isinstance(p, (int, dict)) for p in parameters):
            parameters = {"map_type": PARAMETERCOLUMNCOLLECTION, "parameters": list(parameters)}
            parameters = ParameterColumnCollectionMapping.from_dict(parameters)
        skip_columns = map_dict.get("skip_columns", None)
        import_objects = map_dict.get("import_objects", False)
        read_start_row = map_dict.get("read_start_row", False)
        return RelationshipClassMapping(name, object_classes, objects, parameters, skip_columns, import_objects, read_start_row)

    def to_dict(self):
        map_dict = {"map_type": self._map_type, "import_objects": self._import_objects}
        if self.name is not None:
            if isinstance(self.name, Mapping):
                map_dict.update(name=self.name.to_dict())
            else:
                map_dict.update(name=self.name)
        if self.object_classes is not None:
            map_dict.update(object_classes=[o if isinstance(o, str) else o.to_dict() for o in self.object_classes])
        if self.objects is not None:
            map_dict.update(objects=[o if isinstance(o, str) else o.to_dict() for o in self.objects])
        if self.parameters is not None:
            map_dict.update(parameters=self.parameters.to_dict())
        if self.skip_columns:
            map_dict.update(skip_columns=self.skip_columns)
        map_dict.update(read_start_row=self.read_start_row)
        return map_dict
    
    def is_valid(self):
        # check that parameter mapping has a valid name mapping
        name_valid, msg = valid_mapping_or_value(self.name)
        if not name_valid:
            return False, "Relationship class mapping must have a valid name mapping, current name mapping is not valid: " + msg
        
        #check that object class mapping is valid:
        if not self.object_classes:
            return False, "Relationship class mapping must have object class mappings."
        for i, obj_class in enumerate(self.object_classes):
            oc_valid, msg = valid_mapping_or_value(obj_class)
            if not oc_valid:
                return False, "Relationship class mapping must have valid object class mappings. Object class mapping {i} is not valid: " + msg

        has_objects = False
        all_objects_valid = True
        if self.objects:
            has_objects = True
            all_objects_valid = True
            for i, obj in enumerate(self.objects):
                obj_valid, msg = valid_mapping_or_value(obj)
                if not obj_valid:
                    all_objects_valid = False
                    obj_msg = "Object mapping {i} is not valid: " + msg
            
        if self.objects:
            if len(self.object_classes) != len(self.objects):
                return False, "Relationship class mapping must have same number of object class mappings as object mappings."
            if not all_objects_valid:
                return False, "Relationship class mapping must have all valid object mappings. " + obj_msg

        if self.parameters is not None:
            # check if parameter mapping is valid if of definition type:
            param_valid, msg = self.parameters.is_valid(self.is_pivoted())
            if not param_valid:
                return False, "Relationship class mapping has an invalid parameters mapping: " + msg

            # check that object is valid if we have a parameter mapping that has a value mapping
            if self.parameters.parameter_type != 'definition' and not all_objects_valid:
                return False, "Relationship class mapping has a parameter mapping but all object mappings are not valid: " + msg
        return True, ""


class DataMapping:
    """
    Class for holding and validating Mapping specification::
    
        DataMapping {
            map_type: 'collection'
            mappings: List[ObjectClassMapping | RelationshipClassMapping]
        }
    """

    def __init__(self, mappings=None, has_header=False):
        if mappings is None:
            mappings = []
        self._mappings = []
        self._has_header = False
        self.mappings = mappings
        self.has_header = has_header

    def non_pivoted_columns(self):
        non_pivoted_columns = []
        if self.mappings is not None:
            for mapping in self.mappings:
                non_pivoted_columns.extend(mapping.non_pivoted_columns())
        return non_pivoted_columns

    def last_pivot_row(self):
        last_pivot_rows = [-1]
        if self.mappings is not None:
            last_pivot_rows += [m.last_pivot_row() for m in self.mappings if m is not None]
        return max(last_pivot_rows)

    def is_pivoted(self):
        if self.mappings is not None:
            return any(m.is_pivoted() for m in self.mappings)
        return False

    @property
    def mappings(self):
        return self._mappings

    @mappings.setter
    def mappings(self, mappings):
        if not isinstance(mappings, list):
            raise TypeError("mappings must be list")
        if mappings and not all(isinstance(m, (RelationshipClassMapping, ObjectClassMapping)) for m in mappings):
            raise TypeError("""All mappings must be RelationshipClassMapping or ObjectClassMapping""")
        self._mappings = mappings

    @property
    def has_header(self):
        return self._has_header

    @has_header.setter
    def has_header(self, has_header):
        self._has_header = bool(has_header)

    def to_dict(self):
        map_dict = {"has_header": self.has_header}
        if self.mappings:
            map_dict.update(mappings=[m.to_dict() for m in self.mappings])
        return map_dict

    @classmethod
    def from_dict(cls, map_dict):
        if not isinstance(map_dict, dict):
            raise ValueError("map_dict must be a dict")
        has_header = map_dict.get("has_header", False)
        mappings = map_dict.get("mappings", [])
        parsed_mappings = []
        for mapping in mappings:
            map_type = mapping.get("map_type", None)
            if map_type == OBJECTCLASS:
                parsed_mappings.append(ObjectClassMapping.from_dict(mapping))
            elif map_type == RELATIONSHIPCLASS:
                parsed_mappings.append(RelationshipClassMapping.from_dict(mapping))
            else:
                raise TypeError(
                    f"""Invalid 'map_type', expected RelationshipClassMapping, ObjectClassMapping, or
                    compatible dictionary, got {map_type}"""
                )
        return DataMapping(parsed_mappings, has_header)
    
    def is_valid(self):
        msg = ""
        is_valid = True
        for mapping in self.mappings:
            mapping_valid, mapping_msg = mapping.is_valid()
            msg = msg + mapping_msg
            is_valid = is_valid and mapping_valid
        return is_valid, msg


def create_read_parameter_functions(mapping, pivoted_data, pivoted_cols, data_header, is_pivoted):
    """Creates functions for reading parameter name, field and value from
    ParameterColumnCollectionMapping or ParameterMapping objects"""
    if mapping is None:
        return {"name": (None, None, None), "value": (None, None, None)}
    if not isinstance(mapping, (ParameterColumnCollectionMapping, ParameterMapping)):
        raise ValueError(
            f"""mapping must be ParameterColumnCollectionMapping or ParameterMapping, instead got {type(mapping)}"""
        )
    if isinstance(mapping, ParameterColumnCollectionMapping):
        # parameter names from header or mapping name.
        p_n_reads = False
        p_n_num = len(pivoted_cols)
        p_n = []
        if mapping.parameters:
            for parameter, column in zip(mapping.parameters, pivoted_cols):
                if parameter.name is None:
                    p_n.append(data_header[column])
                else:
                    p_n.append(parameter.name)
            if len(p_n) == 1:
                p_n = p_n[0]

            def p_n_getter(_):
                return p_n

            p_v_num = len(pivoted_cols)
            p_v_getter = itemgetter(*pivoted_cols)
            p_v_reads = True
        else:
            # no data
            return {"name": (None, None, None), "value": (None, None, None)}
    else:
        # general ParameterMapping type
        p_n_getter, p_n_num, p_n_reads = create_pivot_getter_function(
            mapping.name, pivoted_data, pivoted_cols, data_header
        )

        if is_pivoted:
            # if mapping is pivoted values for parameters are read from
            # pivoted columns
            if pivoted_cols:
                p_v_num = len(pivoted_cols)
                p_v_getter = itemgetter(*pivoted_cols)
                p_v_reads = True
            else:
                p_v_num = None
                p_v_getter = None
                p_v_reads = None
        else:
            p_v_getter, p_v_num, p_v_reads = create_pivot_getter_function(
                mapping.value, pivoted_data, pivoted_cols, data_header
            )
        if mapping.parameter_type == "definition":
            p_v_num = None
            p_v_getter = None
            p_v_reads = None

    # extra dimensions for parameter
    if mapping.extra_dimensions and p_v_getter is not None:
        # create functions to get extra_dimensions if there is a value getter
        ed_getters, ed_num, ed_reads_data = create_getter_list(
            mapping.extra_dimensions, pivoted_data, pivoted_cols, data_header
        )
        p_v_getter = ed_getters + [p_v_getter]
        p_v_num = ed_num + [p_v_num]
        p_v_reads = ed_reads_data + [p_v_reads]
        # create a function that returns a tuple with extra dimensions and value
        p_v_getter, p_v_num, p_v_reads = create_getter_function_from_function_list(p_v_getter, p_v_num, p_v_reads)
        has_ed = True
    else:
        p_v_getter = p_v_getter
        p_v_num = p_v_num
        p_v_reads = p_v_reads
        has_ed = False

    getters = {
        "name": (p_n_getter, p_n_num, p_n_reads),
        "value": (p_v_getter, p_v_num, p_v_reads),
        "has_extra_dimensions": has_ed,
    }
    return getters


def create_getter_list(mapping, pivoted_data, pivoted_cols, data_header):
    """Creates a list of getter functions from a list of Mappings"""
    if mapping is None:
        return [], [], []

    obj_getters = []
    obj_num = []
    obj_reads = []
    for o in mapping:
        o, num, reads = create_pivot_getter_function(o, pivoted_data, pivoted_cols, data_header)
        obj_getters.append(o)
        obj_num.append(num)
        obj_reads.append(reads)
    return obj_getters, obj_num, obj_reads


def dict_to_map(map_dict):
    """Creates Mapping object from a dict"""
    if isinstance(map_dict, dict):
        map_type = map_dict.get("map_type", None)
        if map_type == MAPPINGCOLLECTION:
            mapping = DataMapping.from_dict(map_dict)
        elif map_type == RELATIONSHIPCLASS:
            mapping = RelationshipClassMapping.from_dict(map_dict)
        elif map_type == OBJECTCLASS:
            mapping = ObjectClassMapping.from_dict(map_dict)
        else:
            raise ValueError(
                f"""invalid "map_type" value, expected "{MAPPINGCOLLECTION}", "{RELATIONSHIPCLASS}"
                or "{OBJECTCLASS}", got {map_type}"""
            )
    else:
        raise TypeError(f"map_dict must be a dict, instead it was: {type(map_dict)}")
    return mapping


def type_class_list_from_spec(types, num_sections, skip_sections=None):
    if skip_sections is None:
        skip_sections = []
    do_nothing = lambda x: x
    type_conv_list = []
    for section in range(num_sections):
        type_class = types.get(section, None)
        if section in skip_sections:
            type_class = do_nothing
        elif type_class is None:
            type_class = do_nothing
        elif type_class not in SUPPORTED_TYPES:
            raise ValueError(f"Unsupported type of {type_class} specified for column: {c}")
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
        raise TypeConversionError(
            f"Could not convert value: '{value}' to type: '{type_converter.__name__}'",
        )

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
    if isinstance(mapping, DataMapping):
        map_list = mapping.mappings
    else:
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

    if isinstance(mapping, dict):
        mapping = dict_to_map(mapping)
    elif isinstance(mapping, list):
        # NOTE: No need to check types here, DataMapping.@mappings.setter does it already
        mapping = DataMapping(mappings=[dict_to_map(m) if isinstance(m, dict) else m for m in mapping])

    if isinstance(mapping, DataMapping):
        mappings = mapping.mappings
    else:
        mappings = [mapping]

    # find max pivot row since mapping can have different number of pivoted rows.
    last_pivot_row = -1
    has_pivot = False
    for map_ in mappings:
        if mapping.is_pivoted():
            has_pivot = True
            last_pivot_row = max(last_pivot_row, mapping.last_pivot_row())
    
    # get pivoted rows of data.
    raw_pivoted_data = []
    if has_pivot:
        for row_number in range(last_pivot_row + 1):
            raw_pivoted_data.append(next(data_source))
    num_pivoted_rows = len(raw_pivoted_data)

    # get a list of reader functions
    readers = []
    min_read_data_from_row = math.inf
    for m in mappings:
        pivoted_data, pivot_type_errors = get_pivoted_data(iter(raw_pivoted_data), m, num_cols, data_header, row_types)
        errors.extend(pivot_type_errors)
        read_data_from_row = max(m.last_pivot_row() + 1, m.read_start_row)
        r = create_mapping_readers(m, num_cols, pivoted_data, data_header)
        readers.extend([(key, reader, reads_row, read_data_from_row) for key, reader, reads_row in r])
        min_read_data_from_row = min(min_read_data_from_row, read_data_from_row)

    
    data = {
        "object_classes": [],
        "objects": [],
        "object_parameters": [],
        "object_parameter_values": [],
        "relationship_classes": [],
        "relationships": [],
        "relationship_parameters": [],
        "relationship_parameter_values": [],
    }
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
            try:
                row_data = convert_row_types(row_data)
            except TypeConversionError as e:
                errors.append((row_number, e))
                continue
            try:
                # read the row with each reader
                for key, reader, read_data_from_row in row_readers:
                    if row_number >= read_data_from_row:
                        data[key].extend([row_value for row_value in reader(row_data) if all(v is not None for v in row_value)])
            except IndexError as e:
                errors.append((row_number, e))

    # pack extra dimensions into list of list
    # FIXME: This should probably be moved somewhere else
    new_data = {}
    for k, v in data.items():
        if any(parameter_type in k for parameter_type in ("time series", "time pattern", "1d array", "2d array")) and v:
            v = sorted(v, key=lambda x: x[:-1])
            new = []
            if "time series" in k:
                for keys, values in itertools.groupby(v, key=lambda x: x[:-1]):
                    values = [items[-1] for items in values if all(i is not None for i in items[-1])]
                    if values:
                        indexes = [items[0] for items in values]
                        values = [items[1] for items in values]
                        new.append(keys + (TimeSeriesVariableResolution(indexes, values, False, False),))
            if "time pattern" in k:
                for keys, values in itertools.groupby(v, key=lambda x: x[:-1]):
                    values = [items[-1] for items in values if all(i is not None for i in items[-1])]
                    if values:
                        indexes = [items[0] for items in values]
                        values = [items[1] for items in values]
                        new.append(keys + (TimePattern(indexes, values),))
            if "1d array" in k:
                for keys, values in itertools.groupby(v, key=lambda x: x[:-1]):
                    values = [value[-1] for value in values if value[-1] is not None]
                    if values:
                        new.append(keys + (values,))
            if "2d array" in k:
                for keys, values in itertools.groupby(v, key=lambda x: x[:-1]):
                    values = [value[-1] for value in values if all(v is not None in value[-1])]
                    if values:
                        new.append(keys + (values,))

            if "object_parameter_values" in k:
                if "object_parameter_values" in new_data:
                    new_data["object_parameter_values"] = new_data["object_parameter_values"].extend(new)
                else:
                    new_data["object_parameter_values"] = new
            else:
                if "relationship_parameter_values" in new_data:
                    new_data["relationship_parameter_values"] = new_data["relationship_parameter_values"].extend(new)
                else:
                    new_data["relationship_parameter_values"] = new

    if "object_parameter_values" not in data:
        data["object_parameter_values"] = []
    if "relationship_parameter_values" not in data:
        data["relationship_parameter_values"] = []
    data["object_parameter_values"].extend(new_data.get("object_parameter_values", []))
    data["relationship_parameter_values"].extend(new_data.get("relationship_parameter_values", []))

    # remove time series and time pattern raw data
    existing_keys = list(data.keys())
    for key in ["time series", "time pattern", "2d array", "1d array"]:
        for existing_key in existing_keys:
            if key in existing_key:
                data.pop(existing_key, None)
    return data, errors


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


def create_mapping_readers(mapping, num_cols, pivoted_data, data_header=None):
    """Creates a list of functions that return data from a row of a data source
    from ObjectClassMapping or RelationshipClassMapping objects."""
    if data_header is None:
        data_header = []

    # make sure all column references are found
    non_pivoted_columns = mapping.non_pivoted_columns()
    int_non_piv_cols = []
    for pc in non_pivoted_columns:
        if isinstance(pc, str):
            if pc not in data_header:
                raise IndexError(
                    f"""mapping contains string reference to data header but reference "{pc}"
                    could not be found in header."""
                )
            pc = data_header.index(pc)
        if pc >= num_cols:
            raise IndexError(f"""mapping contains invalid index: {pc}, data column number: {num_cols}""")
        int_non_piv_cols.append(pc)

    if isinstance(mapping.parameters, ParameterColumnCollectionMapping) and mapping.parameters.parameters:
        # if we are using a parameter column collection and we have column
        # references then only use those columns for pivoting
        pivoted_cols = []
        for p in mapping.parameters.parameters:
            pc = p.column
            if isinstance(pc, str):
                pc = data_header.index(pc)
            pivoted_cols.append(pc)

    elif mapping.is_pivoted():
        # paramater column mapping is not in use and we have a pivoted mapping
        pivoted_cols = set(range(num_cols)).difference(set(int_non_piv_cols))
        # remove skipped columns
        if mapping.skip_columns:
            for skip_c in mapping.skip_columns:
                if isinstance(skip_c, str):
                    if skip_c in data_header:
                        skip_c = data_header.index(skip_c)
                pivoted_cols.discard(skip_c)
    else:
        # no pivoted mapping
        pivoted_cols = []

    parameter_getters = create_read_parameter_functions(
        mapping.parameters, pivoted_data, pivoted_cols, data_header, mapping.is_pivoted()
    )
    p_n_getter, p_n_num, p_n_reads = parameter_getters["name"]
    p_v_getter, p_v_num, p_v_reads = parameter_getters["value"]

    readers = []

    if mapping.parameters is not None and mapping.parameters.parameter_type in ["time series", "time pattern", "1d array", "2d array"]:
        pv_key = "object_parameter_values" + mapping.parameters.parameter_type
        pv_r_key = "relationship_parameter_values" + mapping.parameters.parameter_type
    else:
        pv_key = "object_parameter_values"
        pv_r_key = "relationship_parameter_values"

    if isinstance(mapping, ObjectClassMapping):
        # getter for object class and objects
        oc_getter, oc_num, oc_reads = create_pivot_getter_function(
            mapping.name, pivoted_data, pivoted_cols, data_header
        )
        readers.append(("object_classes",) + create_final_getter_function([oc_getter], [oc_num], [oc_reads]))
        o_getter, o_num, o_reads = create_pivot_getter_function(mapping.object, pivoted_data, pivoted_cols, data_header)

        readers.append(
            ("objects",) + create_final_getter_function([oc_getter, o_getter], [oc_num, o_num], [oc_reads, o_reads])
        )

        readers.append(
            ("object_parameters",)
            + create_final_getter_function([oc_getter, p_n_getter], [oc_num, p_n_num], [oc_reads, p_n_reads])
        )

        readers.append(
            (pv_key,)
            + create_final_getter_function(
                [oc_getter, o_getter, p_n_getter, p_v_getter],
                [oc_num, o_num, p_n_num, p_v_num],
                [oc_reads, o_reads, p_n_reads, p_v_reads],
            )
        )
    else:
        # getters for relationship class and relationships
        rc_getter, rc_num, rc_reads = create_pivot_getter_function(
            mapping.name, pivoted_data, pivoted_cols, data_header
        )
        list_wrap = True if len(mapping.object_classes) == 1 else False
        rc_oc_getter, rc_oc_num, rc_oc_reads = create_getter_function_from_function_list(
            *create_getter_list(mapping.object_classes, pivoted_data, pivoted_cols, data_header), list_wrap=list_wrap
        )
        readers.append(
            ("relationship_classes",)
            + create_final_getter_function([rc_getter, rc_oc_getter], [rc_num, rc_oc_num], [rc_reads, rc_oc_reads])
        )
        list_wrap = True if len(mapping.objects) == 1 else False
        r_getter, r_num, r_reads = create_getter_function_from_function_list(
            *create_getter_list(mapping.objects, pivoted_data, pivoted_cols, data_header), list_wrap=list_wrap
        )
        readers.append(
            ("relationships",)
            + create_final_getter_function([rc_getter, r_getter], [rc_num, r_num], [rc_reads, r_reads])
        )

        readers.append(
            ("relationship_parameters",)
            + create_final_getter_function([rc_getter, p_n_getter], [rc_num, p_n_num], [rc_reads, p_n_reads])
        )
        readers.append(
            (pv_r_key,)
            + create_final_getter_function(
                [rc_getter, r_getter, p_n_getter, p_v_getter],
                [rc_num, r_num, p_n_num, p_v_num],
                [rc_reads, r_reads, p_n_reads, p_v_reads],
            )
        )

        # add readers to object classes an objects
        if mapping.import_objects and mapping.object_classes and mapping.objects:
            for oc, o in zip(mapping.object_classes, mapping.objects):
                oc_getter, oc_num, oc_reads = create_pivot_getter_function(oc, pivoted_data, pivoted_cols, data_header)
                o_getter, o_num, o_reads = create_pivot_getter_function(o, pivoted_data, pivoted_cols, data_header)
                readers.append(("object_classes",) + create_final_getter_function([oc_getter], [oc_num], [oc_reads]))
                readers.append(
                    ("objects",)
                    + create_final_getter_function([oc_getter, o_getter], [oc_num, o_num], [oc_reads, o_reads])
                )

    # remove any readers that doesn't read anything.
    readers = [r for r in readers if r[1] is not None]

    return readers


def create_final_getter_function(function_list, function_output_len_list, reads_data_list):
    """Creates a single function that will return a list of items
    If there are any None in the function_list then return an empty list
    """
    if any(f is None for f in function_list):
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
    """Function that take a list of getter functions and returns one function
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
    if not function_list:
        # empty list
        return None, None, None
    if any(f is None for f in function_list):
        # incomplete list
        return None, None, None

    if not all(l in (l, max(len_output_list)) for l in len_output_list):
        raise ValueError(
            """len_output_list each element in list must be 1
                         or max(len_output_list)"""
        )

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


def create_pivot_getter_function(mapping, pivoted_data, pivoted_cols, data_header):
    """Creates a function that returns data given a list of data,
    ex. a row in a csv, from a Mapping or constant. If mapping is a constant
    then that value will be returned, if Mapping is a pivoted row header reference
    then thoose values will be returned.

    mapping = "constant" will return::

        def getter(row):
            return "constant"

    mapping.value_reference = 0 and mapping.map_type = COLUMN::

        def getter(row):
            return row[0]

    etc...

    Returns:

        A tuple (getter, num, reads_data).
        
        * getter: function that takes a row and returns data, getter(row)
        * num: number of elements in the list of data the function returns.
          If 1 it returns the value instead of a list
        * reads_data: boolean if getter actually reads data from input
    """
    if mapping is None:
        return None, None, None

    if data_header is None:
        data_header = []

    if type(mapping) == Mapping:
        if mapping.map_type == ROW:
            # pivoted values, read from row of pivoted_data or data_header
            if mapping.value_reference == -1:
                # special case, read pivoted rows from data header instead
                # of pivoted data.
                read_from = data_header
            else:
                read_from = pivoted_data[mapping.value_reference]
            if pivoted_cols:
                piv_values = [read_from[i] for i in pivoted_cols]
                num = len(piv_values)
                if len(piv_values) == 1:
                    piv_values = piv_values[0]

                def getter_fcn(x):
                    return piv_values

                getter = getter_fcn
                reads_data = False
            else:
                # no data
                getter = None
                num = None
                reads_data = None
        elif mapping.map_type == COLUMN:
            # column value
            ref = mapping.value_reference
            if type(ref) == str:
                ref = data_header.index(ref)
            getter = itemgetter(ref)
            num = 1
            reads_data = True
        else:
            # column name ref
            ref = mapping.value_reference
            if type(ref) == str:
                ref = data_header.index(ref)
            ref = data_header[ref]

            def getter_fcn(x):
                return ref

            getter = getter_fcn
            num = 1
            reads_data = False
    elif type(mapping) == str:
        # constant, just return str
        def getter_fcn(x):
            return mapping

        getter = getter_fcn
        num = 1
        reads_data = False

    return getter, num, reads_data
