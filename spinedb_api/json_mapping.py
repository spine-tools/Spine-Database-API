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

from collections import Sequence
import itertools
import math
from operator import itemgetter
from .parameter_value import TimeSeriesVariableResolution, TimePattern, ParameterValueFormatError
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


class MappingBase:
    """
    Class for holding and validating Mapping specification:
    
        Mapping {
            map_type: 'column' | 'row' | 'column_name'
            value_reference: str | int
            append_str: str
            prepend_str: str
        }
    """

    MAP_TYPE = ""

    def __init__(self, reference=None, append_str=None, prepend_str=None):

        # this needs to be before value_reference because value_reference uses
        # self.map_type
        self._reference = None
        self._append_str = None
        self._prepend_str = None
        self.reference = reference
        self.append_str = append_str
        self.prepend_str = prepend_str

    @property
    def append_str(self):
        return self._append_str

    @property
    def prepend_str(self):
        return self._prepend_str

    @property
    def reference(self):
        return self._reference

    @append_str.setter
    def append_str(self, append_str):
        if append_str is not None and not isinstance(append_str, str):
            raise ValueError(f"append_str must be a None or str, instead got {type(append_str)}")
        self._append_str = append_str

    @prepend_str.setter
    def prepend_str(self, prepend_str):
        if prepend_str is not None and not isinstance(prepend_str, str):
            raise ValueError(f"prepend_str must be None or str, instead got {type(prepend_str)}")
        self._prepend_str = prepend_str

    @reference.setter
    def reference(self, reference):
        """Setter method for reference, should be implemented in subclasses
        """
        NotImplementedError()

    def is_pivoted(self):
        """Should return True if Mapping type is reading columns in a row, pivoted."""
        NotImplementedError()

    def last_pivot_row(self):
        """Returns the last row that is pivoted"""
        return -1

    def to_dict(self):
        """Creates a dict representation of mapping, should be compatible with json.dumps and json.loads"""
        map_dict = {"map_type": self.MAP_TYPE}
        if self.reference is not None:
            map_dict.update({"reference": self.reference})
        if self.append_str is not None:
            map_dict.update({"append_str": self.append_str})
        if self.prepend_str is not None:
            map_dict.update({"prepend_str": self.prepend_str})
        return map_dict

    @classmethod
    def from_dict(cls, map_dict):
        """Creates a mapping object from dict representation of mapping
        
        Should return an instance of the subclass
        """
        NotImplementedError()

    def is_valid(self):
        """Should return True or False if mapping is ready to read data.
        """
        if self.reference is None:
            return False
        return True

    def returns_value(self):
        return self.is_valid()


class NoneMapping(MappingBase):
    """Class for holding a reference to a column by number or header string
    
    Arguments:
        MappingBase {[type]} -- [description]
    
    Raises:
        TypeError: [description]
    
    Returns:
        [type] -- [description]
    """

    MAP_TYPE = "None"

    def __init__(self, *args, **kwargs):
        super(NoneMapping, self).__init__(*args, **kwargs)

    @MappingBase.reference.setter
    def reference(self, reference):
        """Setter method for reference, should be implemented in subclasses
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
    """Class for holding a reference to a column by number or header string
    
    Arguments:
        MappingBase {[type]} -- [description]
    
    Raises:
        TypeError: [description]
    
    Returns:
        [type] -- [description]
    """

    MAP_TYPE = "constant"

    @MappingBase.reference.setter
    def reference(self, reference):
        """Setter method for reference, should be implemented in subclasses
        """
        if reference is not None and not isinstance(reference, str):
            raise TypeError(f"reference must be str or None, instead got: {type(reference).__name__}")
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
        append_str = map_dict.get("append_str", None)
        prepend_str = map_dict.get("prepend_str", None)
        reference = map_dict.get("reference", None)
        if reference is None:
            reference = map_dict.get("value_reference", None)
        return cls(reference, append_str, prepend_str)

    def create_getter_function(self, pivoted_columns, pivoted_data, data_header):
        constant = str(self.reference)

        def getter(_):
            return constant

        return getter, 1, False


class ColumnMapping(ConstantMapping):
    """Class for holding a reference to a column by number or header string
    
    Arguments:
        MappingBase {[type]} -- [description]
    
    Raises:
        TypeError: [description]
    
    Returns:
        [type] -- [description]
    """

    MAP_TYPE = "column"

    @MappingBase.reference.setter
    def reference(self, reference):
        """Setter method for reference, should be implemented in subclasses
        """
        if reference is not None and not isinstance(reference, (str, int)):
            raise TypeError(f"reference must be int, str or None, instead got: {type(reference).__name__}")
        if isinstance(reference, int) and reference < 0:
            raise ValueError(f"If reference is an int, it must be >= 0, instead got: {reference}")
        self._reference = reference

    def create_getter_function(self, pivoted_columns, pivoted_data, data_header):
        ref = self.reference
        if isinstance(ref, str):
            ref = data_header.index(ref)
        getter = itemgetter(ref)
        num = 1
        reads_data = True
        return getter, num, reads_data


class ColumnHeaderMapping(ColumnMapping):
    MAP_TYPE = "column_header"
    """Class for holding a reference to a column header by number or header string
    
    Arguments:
        MappingBase {[type]} -- [description]
    
    Raises:
        TypeError: [description]
    
    Returns:
        [type] -- [description]
    """

    def create_getter_function(self, pivoted_columns, pivoted_data, data_header):
        ref = self.reference
        if isinstance(ref, str):
            ref = data_header.index(ref)
        constant = data_header[ref]

        def getter(_):
            return constant

        num = 1
        reads_data = False
        return getter, num, reads_data


class RowMapping(MappingBase):
    MAP_TYPE = "row"
    """Class for holding a reference to a row number or headers
    
    Arguments:
        MappingBase {[type]} -- [description]
    
    Raises:
        TypeError: [description]
    
    Returns:
        [type] -- [description]
    """

    @MappingBase.reference.setter
    def reference(self, reference):
        """Setter method for reference, should be implemented in subclasses
        """
        if reference is not None and not isinstance(reference, int):
            raise TypeError(f"reference must be int or None, instead got: {type(reference).__name__}")
        if isinstance(reference, int) and reference < -1:
            raise ValueError(f"If reference is an int, it must be >= -1, instead got: {reference}")
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
        append_str = map_dict.get("append_str", None)
        prepend_str = map_dict.get("prepend_str", None)
        reference = map_dict.get("reference", None)
        return RowMapping(reference, append_str, prepend_str)

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


class TimeSeriesOptions:
    """
    Class for holding parameter type-specific options for time series parameter values.

    Attributes:
        repeat (bool): time series repeat flag
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
        if not self.name.returns_value():
            return False, "Parameter mapping must have a valid name mapping that returns a value"
        return True, ""

    def create_getter_list(self, is_pivoted, pivoted_columns, pivoted_data, data_header):
        if self.name.returns_value():
            getter, num, reads = self.name.create_getter_function(pivoted_columns, pivoted_data, data_header)
        else:
            getter, num, reads = (None, None, None)
        return {"name": (getter, num, reads)}


class ParameterValueMapping(ParameterDefinitionMapping):
    PARAMETER_TYPE = "single value"

    def __init__(self, name=None, value=None):
        super(ParameterValueMapping, self).__init__(name)
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
        if not (self.is_pivoted() or parent_pivot) and not self.value.returns_value():
            return False, "Parameter value mapping must be a valid mapping that returns a value"
        return True, ""

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


class ParameterListMapping(ParameterValueMapping):
    NUM_EXTRA_DIMENSIONS = 1
    PARAMETER_TYPE = "1d array"

    def __init__(self, name=None, value=None, extra_dimension=None):
        super(ParameterListMapping, self).__init__(name, value)
        self._extra_dimensions = [mappingbase_from_dict_int_str(None) for _ in range(self.NUM_EXTRA_DIMENSIONS)]
        self.extra_dimensions = extra_dimension

    @property
    def extra_dimensions(self):
        return self._extra_dimensions

    @extra_dimensions.setter
    def extra_dimensions(self, extra_dimensions):
        if extra_dimensions is None:
            extra_dimensions = [mappingbase_from_dict_int_str(None) for _ in range(self.NUM_EXTRA_DIMENSIONS)]
        if not isinstance(extra_dimensions, (list, tuple)):
            raise TypeError(
                f"extra_dimensions must be a list or tuple of MappingBase, int, str, dict, instead got: {type(extra_dimensions).__name__}"
            )
        if len(extra_dimensions) != self.NUM_EXTRA_DIMENSIONS:
            raise ValueError(
                f"extra_dimensions must be of length: {self.NUM_EXTRA_DIMENSIONS} instead got len: {len(extra_dimensions)}"
            )
        self._extra_dimensions = [mappingbase_from_dict_int_str(ed) for ed in extra_dimensions]

    def non_pivoted_columns(self):
        non_pivoted_columns = super().non_pivoted_columns()
        non_pivoted_columns.extend(ed.reference for ed in self.extra_dimensions if isinstance(ed, ColumnMapping) and ed.returns_value())
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
        map_dict.update({"parameter_type": self.PARAMETER_TYPE})
        map_dict.update({"extra_dimensions": [ed.to_dict() for ed in self.extra_dimensions]})
        return map_dict

    def is_valid(self, parent_pivot: bool):
        # check that parameter mapping has a valid name mapping
        valid, msg = super().is_valid(parent_pivot)
        if not valid:
            return False, msg
        if not all(ed.returns_value() for ed in self.extra_dimensions):
            return False, "All mappings in extra_dimensions must be valid and return a value"
        return True, ""

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

        getters["value"] = (value_getter, value_num, value_reads)
        getters["has_extra_dimensions"]: has_ed

        return getters

    def raw_data_to_type(self, data):
        out = []
        data = sorted(data, key=lambda x: x[:-1])
        for keys, values in itertools.groupby(data, key=lambda x: x[:-1]):
            values = [value[-1][-1] for value in values if value[-1][-1] is not None]
            if values:
                out.append(keys + (values,))
        return out


class ParameterTimeSeriesMapping(ParameterListMapping):
    NUM_EXTRA_DIMENSIONS = 1
    PARAMETER_TYPE = "time series"

    def __init__(self, name=None, value=None, extra_dimension=None, options=None):
        super(ParameterTimeSeriesMapping, self).__init__(name, value, extra_dimension)
        self._options = TimeSeriesOptions()
        self.options = options

    @property
    def options(self):
        return self._options

    @options.setter
    def options(self, options):
        if options is None:
            options = TimeSeriesOptions()
        if not isinstance(options, TimeSeriesOptions):
            raise TypeError(
                f"options must be a TimeSeriesOptions, instead got: {type(options).__name__}"
            )
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
                out.append(keys + (TimeSeriesVariableResolution(indexes, values, self.options.ignore_year, self.options.repeat),))
        return out


class ParameterTimePatternMapping(ParameterListMapping):
    NUM_EXTRA_DIMENSIONS = 1
    PARAMETER_TYPE = "time pattern"

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


class EntityClassMapping:
    """
    Class for holding and validating Mapping specification::

        ObjectClassMapping {
            map_type: 'object'
            name: str | Mapping
            objects: Mapping | str | None
            parameters: ParameterMapping | None
        }
    """

    MAP_TYPE = "EnitityClass"

    def __init__(self, name=None, parameters=None, skip_columns=None, read_start_row=0):
        self._name = NoneMapping()
        self._parameters = NoneMapping()
        self._skip_columns = []
        self._read_start_row = 0
        self.name = name
        self.parameters = parameters
        self.skip_columns = skip_columns
        self.read_start_row = read_start_row
        self._map_type = OBJECTCLASS

    def non_pivoted_columns(self):
        non_pivoted_columns = []
        if isinstance(self.name, ColumnMapping) and self.name.returns_value():
            non_pivoted_columns.append(self.name.reference)
        if isinstance(self.parameters, ParameterDefinitionMapping):
            non_pivoted_columns.extend(self.parameters.non_pivoted_columns())
        return non_pivoted_columns

    def last_pivot_row(self):
        last_pivot_row = -1
        last_pivot_row = max(self.name.last_pivot_row(), last_pivot_row)
        last_pivot_row = max(self.parameters.last_pivot_row(), last_pivot_row)
        return last_pivot_row

    def is_pivoted(self):
        return self.name.is_pivoted() or self.parameters.is_pivoted()

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
    def name(self, name):
        self._name = mappingbase_from_dict_int_str(name)

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
            raise TypeError(f"map_dict must be a dict, instead got {type(map_dict).__name__}")

        map_type = map_dict.get("map_type", None)
        if map_type is not None and map_type != cls.MAP_TYPE:
            raise ValueError(f"If field 'map_type' is specified, it must be {cls.MAP_TYPE}, instead got {map_type}")
        name = map_dict.get("name", None)
        parameters = map_dict.get("parameters", None)
        skip_columns = map_dict.get("skip_columns", [])
        read_start_row = map_dict.get("read_start_row", 0)
        return EntityClassMapping(name, parameters, skip_columns, read_start_row)

    def to_dict(self):
        map_dict = {"map_type": self.MAP_TYPE}
        map_dict.update(name=self.name.to_dict())
        map_dict.update(parameters=self.parameters.to_dict())
        map_dict.update(skip_columns=self.skip_columns)
        map_dict.update(read_start_row=self.read_start_row)
        return map_dict

    def is_valid(self):
        # check that parameter mapping has a valid name mapping
        if not self.name.is_valid():
            return False, "name mapping must be valid"
        if not isinstance(self.parameters, NoneMapping):
            valid, msg = self.parameters.is_valid(self.is_pivoted())
            if not valid:
                return False, "Parameter mapping must be valid, parameter mapping error: " + msg
        return True, ""

    def pivoted_columns(self, data_header, num_cols):
        # make sure all column references are found
        non_pivoted_columns = self.non_pivoted_columns()
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
        if self.is_pivoted():
            # paramater column mapping is not in use and we have a pivoted mapping
            pivoted_cols = set(range(num_cols)).difference(set(int_non_piv_cols))
            # remove skipped columns
            for skip_c in self.skip_columns:
                if isinstance(skip_c, str):
                    if skip_c in data_header:
                        skip_c = data_header.index(skip_c)
                pivoted_cols.discard(skip_c)
        else:
            # no pivoted mapping
            pivoted_cols = []
        return pivoted_cols

    def create_getter_list(self, pivoted_columns, pivoted_data, data_header):
        """Creates a list of getter functions from a list of Mappings"""
        readers = dict()
        getter, num, reads = (None, None, None)
        if self.name.returns_value():
            getter, num, reads = self.name.create_getter_function(pivoted_columns, pivoted_data, data_header)
        readers["class_name"] = (getter, num, reads)
        if isinstance(self.parameters, ParameterDefinitionMapping):
            par_readers = self.parameters.create_getter_list(self.is_pivoted(), pivoted_columns, pivoted_data, data_header)
            if "name" in par_readers:
                par_readers["parameter name"] = par_readers.pop("name")
            if "value" in par_readers:
                par_readers["parameter value"] = par_readers.pop("value")
            readers.update(**par_readers)
        return readers

    def create_mapping_readers(self, num_columns, pivoted_data, data_header):
        return []


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

    def __init__(self, *args, objects=None, **kwargs):
        super(ObjectClassMapping, self).__init__(*args, **kwargs)
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
        return ObjectClassMapping(name=name, objects=objects, parameters=parameters, skip_columns=skip_columns, read_start_row=read_start_row)

    def to_dict(self):
        map_dict = super().to_dict()
        map_dict.update(objects=self.objects.to_dict())
        return map_dict

    def is_valid(self):
        # check that parameter mapping has a valid name mapping
        valid, msg = super().is_valid()
        if not valid:
            return valid, msg
        if not self.objects.is_valid():
            return False, "object mapping must be valid"
        if isinstance(self.parameters, ParameterValueMapping):
            if not self.objects.returns_value():
                return False, "A parameter value mapping needs a valid object mapping"
        return True, ""

    def create_getter_list(self, pivoted_columns, pivoted_data, data_header):
        """Creates a list of getter functions from a list of Mappings"""
        readers = super().create_getter_list(pivoted_columns, pivoted_data, data_header)
        getter, num, reads = (None, None, None)
        if self.objects.returns_value():
            getter, num, reads = self.objects.create_getter_function(pivoted_columns, pivoted_data, data_header)
        readers["objects"] = (getter, num, reads)
        return readers

    def create_mapping_readers(self, num_columns, pivoted_data, data_header):
        pivoted_columns = self.pivoted_columns(data_header, num_columns)
        readers = super().create_mapping_readers(pivoted_columns, pivoted_data, data_header)
        component_readers = self.create_getter_list(pivoted_columns, pivoted_data, data_header)
        name_getter, name_num, name_reads = component_readers["class_name"]
        o_getter, o_num, o_reads = component_readers["objects"]
        readers.append(
            ("object_classes",)
            + create_final_getter_function([name_getter], [name_num], [name_reads])
        )
        readers.append(
            ("objects",)
            + create_final_getter_function([name_getter, o_getter], [name_num, o_num], [name_reads, o_reads])
        )
        if isinstance(self.parameters, ParameterDefinitionMapping):
            par_name_getter, par_name_num, par_name_reads = component_readers["parameter name"]
            readers.append(
                ("object_parameters",)
                + create_final_getter_function(
                    [name_getter, par_name_getter], [name_num, par_name_num], [name_reads, par_name_reads]
                )
            )
        if isinstance(self.parameters, ParameterValueMapping):
            par_val_name = "object_parameter_values"
            par_value_getter, par_value_num, par_value_reads = component_readers["parameter value"]
            readers.append(
                (par_val_name,)
                + create_final_getter_function(
                    [name_getter, o_getter, par_name_getter, par_value_getter],
                    [name_num, o_num, par_name_num, par_value_num],
                    [name_reads, o_reads, par_name_reads, par_value_reads],
                )
            )
        return readers


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

    def __init__(self, *args, object_classes=None, objects=None, import_objects=False, **kwargs):
        super(RelationshipClassMapping, self).__init__(*args, **kwargs)
        self._objects = NoneMapping()
        self._object_classes = NoneMapping()
        self._import_objects = False
        self.object_classes = object_classes
        self.objects = objects
        self.import_objects = import_objects

    def non_pivoted_columns(self):
        non_pivoted_columns = super().non_pivoted_columns()
        non_pivoted_columns.extend(o.reference for o in self.objects if isinstance(o, ColumnMapping) and o.returns_value())
        non_pivoted_columns.extend(oc.reference for oc in self.object_classes if isinstance(oc, ColumnMapping) and oc.returns_value())
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
                f"objects must be same of as object_classes: {len(self.object_classes)} instead got len: {len(objects)}"
            )
        self._objects = [mappingbase_from_dict_int_str(o) for o in objects]

    @property
    def import_objects(self):
        return self._import_objects

    @import_objects.setter
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
            skip_columns=skip_columns,
            read_start_row=read_start_row,
            import_objects=import_objects
        )

    def to_dict(self):
        map_dict = super().to_dict()
        map_dict.update(objects=[o.to_dict() for o in self.objects])
        map_dict.update(object_classes=[oc.to_dict() for oc in self.object_classes])
        map_dict.update(import_objects=self.import_objects)
        return map_dict

    def is_valid(self):
        # check that parameter mapping has a valid name mapping
        valid, msg = super().is_valid()
        if not valid:
            return valid, msg
        if not all(o.is_valid() for o in self.objects):
            return False, "all object mappings must be valid"
        if not all(oc.is_valid() for oc in self.object_classes):
            return False, "all object class mappings must be valid"
        if isinstance(self.parameters, ParameterValueMapping):
            # if we have a parameter value mapping we need to have objects and object classes mapping
            # that returns data.
            if not all(o.returns_value() for o in self.objects) or not all(oc.returns_value() for oc in self.object_classes):
                return False, "parameter value mapping need all object or object_class mappings to return values"
        return True, ""

    def create_getter_list(self, pivoted_columns, pivoted_data, data_header):
        """Creates a list of getter functions from a list of Mappings"""
        readers = super().create_getter_list(pivoted_columns, pivoted_data, data_header)
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
        
        readers["objects"] = (o_getter, o_num, o_reads)
        readers["object_classes"] = (oc_getter, oc_num, oc_reads)
        return readers

    def create_mapping_readers(self, num_columns, pivoted_data, data_header):
        pivoted_columns = self.pivoted_columns(data_header, num_columns)
        readers = super().create_mapping_readers(pivoted_columns, pivoted_data, data_header)
        component_readers = self.create_getter_list(pivoted_columns, pivoted_data, data_header)
        name_getter, name_num, name_reads = component_readers["class_name"]
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
                single_o_getter, single_o_num, single_o_reads = o.create_getter_function(pivoted_columns, pivoted_data, data_header)
                readers.append(("object_classes",) + create_final_getter_function([oc_getter], [oc_num], [oc_reads]))
                readers.append(
                    ("objects",)
                    + create_final_getter_function([oc_getter, single_o_getter], [oc_num, single_o_num], [oc_reads, single_o_reads])
                )
        par_val_name = "relationship_parameter_values"
        if isinstance(self.parameters, ParameterDefinitionMapping):
            par_name_getter, par_name_num, par_name_reads = component_readers["parameter name"]
            readers.append(
                ("relationship_parameters",)
                + create_final_getter_function(
                    [name_getter, par_name_getter], [name_num, par_name_num], [name_reads, par_name_reads]
                )
            )
        if isinstance(self.parameters, ParameterValueMapping):
            par_value_getter, par_value_num, par_value_reads = component_readers["parameter value"]
            par_val_name = "relationship_parameter_values"
            readers.append(
                (par_val_name,)
                + create_final_getter_function(
                    [name_getter, o_getter, par_name_getter, par_value_getter],
                    [name_num, o_num, par_name_num, par_value_num],
                    [name_reads, o_reads, par_name_reads, par_value_reads],
                )
            )
        return readers


def mappingbase_from_value(value, default_map=NoneMapping):
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


def mapping_from_dict(map_dict, default_map=NoneMapping):
    type_str_to_class = {
        "row": RowMapping,
        "column": ColumnMapping,
        "column_name": ColumnHeaderMapping,
        "column_header": ColumnHeaderMapping,
        "constant": ConstantMapping,
        "None": NoneMapping,
    }
    map_type_str = map_dict.get("map_type", None)
    if map_type_str == "column_name":
        map_dict["map_type"] = "column_header"
    if "value_reference" in map_dict and "reference" not in map_dict:
        map_dict["reference"] = map_dict["value_reference"]
    map_class = type_str_to_class.get(map_type_str, default_map)
    return map_class.from_dict(map_dict)


def mappingbase_from_dict_int_str(value, default_map=NoneMapping):
    """Creates Mapping object if `value` is a `dict` or `int`;
    if `str` or `None` returns same value. If `int`, the Mapping is created
    with map_type == column (default) unless other type is specified
    """
    if value is None:
        return default_map()
    if isinstance(value, MappingBase):
        return value
    if isinstance(value, dict):
        return mapping_from_dict(value)
    elif isinstance(value, (int, str)):
        return mappingbase_from_value(value)
    else:
        raise TypeError(f"value must be dict, int or str, instead got {type(value)}")


def parameter_mapping_from_dict(map_dict, default_map=ParameterValueMapping):
    if map_dict is None:
        return NoneMapping()
    if map_dict.get("map_type", "") == "None":
        return NoneMapping()

    parameter_type_to_class = {
        "definition": ParameterDefinitionMapping,
        "single value": ParameterValueMapping,
        "1d array": ParameterListMapping,
        "time series": ParameterTimeSeriesMapping,
        "time pattern": ParameterTimePatternMapping,
    }
    parameter_type = map_dict.get("parameter_type", None)
    map_class = parameter_type_to_class.get(parameter_type, default_map)
    if parameter_type is None:
        map_dict.update(parameter_type=map_class.PARAMETER_TYPE)
        
    return parameter_type_to_class.get(parameter_type, default_map).from_dict(map_dict)


def dict_to_map(map_dict):
    """Creates Mapping object from a dict"""
    if not isinstance(map_dict, dict):
        raise TypeError(f"map_dict must be a dict, instead it was: {type(map_dict)}")
    map_type = map_dict.get("map_type", None)
    if map_type == RELATIONSHIPCLASS:
        mapping = RelationshipClassMapping.from_dict(map_dict)
    elif map_type == OBJECTCLASS:
        mapping = ObjectClassMapping.from_dict(map_dict)
    else:
        raise ValueError(
            f"""invalid "map_type" value, expected "{RELATIONSHIPCLASS}"
            or "{OBJECTCLASS}", got {map_type}"""
        )
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
        elif isinstance(map_, EntityClassMapping):
            mappings = [map_]
        else:
            raise TypeError(f"mapping must be a dict, ObjectClassMapping, RelationshipClassMapping or list of those, instead was: {type(map_).__name__}")

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
                            [row_value for row_value in reader(row_data) if all(v is not None for v in row_value)]
                        )
            except IndexError as e:
                errors.append((row_number, e))
    # convert parameter values to right class and put all data in one dict
    new_data = {}
    for key, v in data.items():
        map_i, k = key
        if "parameter_values" in k:
            current_mapping = mappings[map_i]
            merged_data[k].extend(current_mapping.parameters.raw_data_to_type(v))
        else:
            merged_data[k].extend(v)
    return merged_data, errors


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
