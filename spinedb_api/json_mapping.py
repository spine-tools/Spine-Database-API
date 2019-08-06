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


def none_to_minus_inf(value):
    """Returns minus infinity if value is None, otherwise returns the value.
    Used by `max(some_list, key=none_to_minus_inf)`.
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
    Class for holding and validating Mapping specification:
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


class ParameterMapping:
    """Class for holding and validating Mapping specification:
    ParameterMapping {
            map_type: 'parameter'
            name: Mapping | str
            value: Mapping | None
            extra_dimensions: [Mapping] | None
    }
    """

    def __init__(self, name=None, value=None, extra_dimensions=None):

        self._name = None
        self._value = None
        self._extra_dimensions = None
        self.name = name
        self.value = value
        self.extra_dimensions = extra_dimensions
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
            for ed in self.extra_dimensions:
                if isinstance(ed, Mapping) and ed.map_type == COLUMN:
                    non_pivoted_columns.append(ed.value_reference)
        return non_pivoted_columns

    def last_pivot_row(self):
        last_pivot_rows = []
        if isinstance(self.name, Mapping):
            last_pivot_rows.append(self.name.last_pivot_row())
        if self.extra_dimensions is not None:
            last_pivot_rows += [m.last_pivot_row() for m in self.extra_dimensions if isinstance(m, Mapping)]
        return max(last_pivot_rows, key=none_to_minus_inf, default=None)

    def is_pivoted(self):
        if isinstance(self.name, Mapping) and self.name.is_pivoted():
            return True
        if self.extra_dimensions is not None:
            return any(ed.is_pivoted() for ed in self.extra_dimensions if isinstance(ed, Mapping))
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
        if isinstance(extra_dimensions, list):
            extra_dimensions = [mapping_from_dict_int_str(ed) for ed in extra_dimensions]
        return ParameterMapping(name, value, extra_dimensions)

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
            ed_list = []
            for ed in self.extra_dimensions:
                if ed is None:
                    ed = Mapping()
                ed = ed if isinstance(ed, str) else ed.to_dict()
                ed_list.append(ed)
            map_dict.update({"extra_dimensions": ed_list})
        return map_dict


class ParameterColumnCollectionMapping:
    """Class for holding and validating Mapping specification:
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
            for p in self.parameters:
                non_pivoted_columns.extend(p.non_pivoted_columns())
        if self.extra_dimensions is not None:
            for ed in self.extra_dimensions:
                if isinstance(ed, Mapping) and ed.map_type == COLUMN:
                    non_pivoted_columns.append(ed.value_reference)
        return non_pivoted_columns

    def last_pivot_row(self):
        last_pivot_row = []
        if self.extra_dimensions is not None:
            last_pivot_rows += [m.last_pivot_row() for m in self.extra_dimensions if isinstance(m, Mapping)]
        return max(last_pivot_rows, key=none_to_minus_inf, default=None)

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
        for i, p in enumerate(parameters):
            if not isinstance(p, ParameterColumnMapping):
                raise ValueError(
                    f"""parameters must be a list with all ParameterColumnMapping, instead got {type(p)} on index {i}"""
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
            for i, p in enumerate(parameters):
                if isinstance(p, int):
                    parameters[i] = ParameterColumnMapping(column=p)
                else:
                    parameters[i] = ParameterColumnMapping.from_dict(p)
        extra_dimensions = map_dict.get("extra_dimensions", None)
        if isinstance(extra_dimensions, list):
            extra_dimensions = [mapping_from_dict_int_str(ed) for ed in extra_dimensions]
        return ParameterColumnCollectionMapping(parameters, extra_dimensions)

    def to_dict(self):
        map_dict = {"map_type": self._map_type}
        if self.parameters is not None:
            p = [p if isinstance(p, str) else p.to_dict() for p in self.parameters]
            map_dict.update({"parameters": p})
        if self.extra_dimensions is not None:
            ed = [ed if isinstance(ed, str) else ed.to_dict() for ed in self.extra_dimensions]
            map_dict.update({"extra_dimensions": ed})
        return map_dict


class ParameterColumnMapping:
    """Class for holding and validating Mapping specification:
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
        return None

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
        if name is not None and type(name) not in (str,):
            raise ValueError(f"""name must be a None or str, instead got {type(name)}""")
        self._name = name

    @column.setter
    def column(self, column=None):
        if column is not None and type(column) not in (str, int):
            raise ValueError(f"""column must be a None, str or int, instead got {type(column)}""")
        self._column = column

    @append_str.setter
    def append_str(self, append_str=None):
        if append_str is not None and type(append_str) not in (str,):
            raise TypeError(f"""append_str must be a None or str, instead got {type(append_str)}""")
        self._append_str = append_str

    @prepend_str.setter
    def prepend_str(self, prepend_str=None):
        if prepend_str is not None and type(prepend_str) not in (str,):
            raise TypeError(f"""prepend_str must be a None or str, instead got {type(prepend_str)}""")
        self._prepend_str = prepend_str

    @classmethod
    def from_dict(self, map_dict):
        if type(map_dict) != dict:
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
    """Class for holding and validating Mapping specification:
    ObjectClassMapping {
            map_type: 'object'
            name: str | Mapping
            objects: Mapping | str | None
            parameters: ParameterMapping | ParameterColumnCollectionMapping | None
    }
    """

    def __init__(self, name=None, obj=None, parameters=None, skip_columns=None):
        self._name = None
        self._object = None
        self._parameters = None
        self._skip_columns = None
        self.name = name
        self.object = obj
        self.parameters = parameters
        self.skip_columns = skip_columns
        self._map_type = OBJECTCLASS

    def non_pivoted_columns(self):
        non_pivoted_columns = []
        if self.name is not None:
            if type(self.name) == Mapping and self.name.map_type == COLUMN:
                non_pivoted_columns.append(self.name.value_reference)
        if self.object is not None:
            if type(self.object) == Mapping and self.object.map_type == COLUMN:
                non_pivoted_columns.append(self.object.value_reference)
        if self.parameters is not None:
            non_pivoted_columns.extend(self.parameters.non_pivoted_columns())
        return non_pivoted_columns

    def last_pivot_row(self):
        last_pivot_row = None
        if type(self.name) == Mapping:
            last_pivot_row = self.name.last_pivot_row()
        if type(self.object) == Mapping:
            last_pivot_row = max(last_pivot_row, self.object.last_pivot_row(), key=none_to_minus_inf)
        if type(self.parameters) in (ParameterMapping, ParameterColumnCollectionMapping):
            last_pivot_row = max(last_pivot_row, self.parameters.last_pivot_row(), key=none_to_minus_inf)
        return last_pivot_row

    def is_pivoted(self):
        pivoted = False
        if type(self.name) == Mapping and self.name.is_pivoted():
            return True
        if type(self.object) == Mapping and self.object.is_pivoted():
            return True
        if type(self.parameters) in (ParameterMapping, ParameterColumnCollectionMapping):
            return self.parameters.is_pivoted()
        return False

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

    @name.setter
    def name(self, name=None):
        if name is not None and type(name) not in (str, Mapping):
            raise TypeError(
                f"""name must be a None, str or Mapping,
                            instead got {type(name)}"""
            )
        self._name = name

    @object.setter
    def object(self, obj=None):
        if obj is not None and type(obj) not in (str, Mapping):
            raise ValueError(
                f"""obj must be None, str or Mapping,
                             instead got {type(obj)}"""
            )
        self._object = obj

    @parameters.setter
    def parameters(self, parameters=None):
        if parameters is not None and type(parameters) not in (ParameterMapping, ParameterColumnCollectionMapping):
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
            if type(skip_columns) in (str, int):
                skip_columns = [skip_columns]
            if type(skip_columns) == list:
                for i, c in enumerate(skip_columns):
                    if type(c) not in (str, int):
                        raise TypeError(
                            f"""skip_columns must be str, int or
                                        list of str, int, instead got list
                                        with {type(c)} on index {i}"""
                        )
            else:
                raise TypeError(
                    f"""skip_columns must be str, int or list of
                                str, int, instead {type(skip_columns)}"""
                )
            self._skip_columns = skip_columns

    @classmethod
    def from_dict(cls, map_dict):
        if type(map_dict) != dict:
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
        if type(parameters) == dict:
            p_type = parameters.get("map_type", None)
            if p_type == PARAMETER:
                parameters = ParameterMapping.from_dict(parameters)
            elif p_type == PARAMETERCOLUMNCOLLECTION:
                parameters = ParameterColumnCollectionMapping.from_dict(parameters)
        elif type(parameters) == list and all(type(p) in (int, dict) for p in parameters):
            parameters = {"map_type": PARAMETERCOLUMNCOLLECTION, "parameters": list(parameters)}
            parameters = ParameterColumnCollectionMapping.from_dict(parameters)

        return ObjectClassMapping(name, obj, parameters, skip_columns)

    def to_dict(self):
        map_dict = {"map_type": self._map_type}
        if self.name is not None:
            if type(self.name) == Mapping:
                map_dict.update(name=self.name.to_dict())
            else:
                map_dict.update(name=self.name)
        if self.object is not None:
            map_dict.update(object=self.object if isinstance(self.object, str) else self.object.to_dict())
        if self.parameters is not None:
            map_dict.update(parameters=self.parameters.to_dict())
        if self.skip_columns:
            map_dict.update(skip_columns=self.skip_columns)
        return map_dict


class RelationshipClassMapping:
    """Class for holding and validating Mapping specification:
    RelationshipClassMapping {
        map_type: 'relationship'
        name:  str | Mapping
        object_classes: [str | Mapping] | None
        objects: [str | Mapping] | None
        parameters: ParameterMapping | ParameterColumnCollectionMapping | None
    }
    """

    def __init__(
        self, name=None, object_classes=None, objects=None, parameters=None, skip_columns=None, import_objects=False
    ):
        self._map_type = RELATIONSHIPCLASS
        self._name = None
        self._object_classes = None
        self._objects = None
        self._parameters = None
        self._skip_columns = None
        self._import_objects = None
        self.name = name
        self.object_classes = object_classes
        self.objects = objects
        self.parameters = parameters
        self.skip_columns = skip_columns
        self.import_objects = import_objects

    def non_pivoted_columns(self):
        non_pivoted_columns = []
        if self.name is not None:
            if type(self.name) == Mapping and self.name.map_type == COLUMN:
                non_pivoted_columns.append(self.name.value_reference)
        if self.object_classes is not None:
            for oc in self.object_classes:
                if type(oc) == Mapping and oc.map_type == COLUMN:
                    non_pivoted_columns.append(oc.value_reference)
        if self.objects is not None:
            for o in self.objects:
                if type(o) == Mapping and o.map_type == COLUMN:
                    non_pivoted_columns.append(o.value_reference)
        if self.parameters is not None:
            non_pivoted_columns.extend(self.parameters.non_pivoted_columns())
        return non_pivoted_columns

    def last_pivot_row(self):
        last_pivot_row = None
        if type(self.name) == Mapping:
            last_pivot_row = self.name.last_pivot_row()
        if self.object_classes is not None:
            for oc in self.object_classes:
                if type(oc) == Mapping:
                    last_pivot_row = max(last_pivot_row, oc.last_pivot_row(), key=none_to_minus_inf)
        if self.objects is not None:
            for o in self.objects:
                if type(o) == Mapping:
                    last_pivot_row = max(last_pivot_row, o.last_pivot_row(), key=none_to_minus_inf)
        if self.parameters is not None:
            last_pivot_row = max(last_pivot_row, self.parameters.last_pivot_row(), key=none_to_minus_inf)
        return last_pivot_row

    def is_pivoted(self):
        pivoted = False
        if type(self.name) == Mapping and self.name.is_pivoted():
            return True
        if self.object_classes is not None and any(
            oc.is_pivoted() for oc in self.object_classes if type(oc) == Mapping
        ):
            return True
        if self.objects is not None and any(o.is_pivoted() for o in self.objects if type(o) == Mapping):
            return True
        if self.parameters is not None:
            return self.parameters.is_pivoted()
        return False

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
            if type(skip_columns) in (str, int):
                skip_columns = [skip_columns]
            if type(skip_columns) == list:
                for i, c in enumerate(skip_columns):
                    if type(c) not in (str, int):
                        raise TypeError(
                            f"""skip_columns must be str, int or
                                        list of str, int, instead got list
                                        with {type(c)} on index {i}"""
                        )
            else:
                raise TypeError(
                    f"""skip_columns must be str, int or list of
                                str, int, instead {type(skip_columns)}"""
                )
            self._skip_columns = skip_columns

    @name.setter
    def name(self, name=None):
        if name is not None and type(name) not in (str, Mapping):
            raise ValueError(
                f"""name must be a None, str or Mapping,
                             instead got {type(name)}"""
            )
        self._name = name

    @object_classes.setter
    def object_classes(self, object_classes=None):
        if object_classes is not None and not all(type(o) in (Mapping, str) or o == None for o in object_classes):
            raise TypeError("name must be a None, str or Mapping | str}")
        self._object_classes = object_classes

    @objects.setter
    def objects(self, objects=None):
        if objects is not None and not all(type(o) in (Mapping, str) or o == None for o in objects):
            raise TypeError("objects must be a None, or list of Mapping | str")
        self._objects = objects

    @parameters.setter
    def parameters(self, parameters=None):
        if parameters is not None and type(parameters) not in (ParameterMapping, ParameterColumnCollectionMapping):
            raise ValueError(
                f"""parameters must be a None, ParameterMapping or
                             ParameterColumnCollectionMapping,
                             instead got {type(parameters)}"""
            )
        self._parameters = parameters

    @classmethod
    def from_dict(self, map_dict):
        if type(map_dict) != dict:
            raise ValueError("map_dict must be a dict")
        if map_dict.get("map_type", None) != RELATIONSHIPCLASS:
            raise ValueError(
                f'''map_dict must contain field "map_type"
                             with value: "{RELATIONSHIPCLASS}"'''
            )
        name = mapping_from_dict_int_str(map_dict.get("name", None))
        objects = map_dict.get("objects", None)
        if type(objects) == list:
            objects = [mapping_from_dict_int_str(o) for o in objects]
        object_classes = map_dict.get("object_classes", None)
        if type(object_classes) == list:
            object_classes = [mapping_from_dict_int_str(o, COLUMN_NAME) for o in object_classes]
        parameters = map_dict.get("parameters", None)
        if type(parameters) == dict:
            p_type = parameters.get("map_type", None)
            if p_type == PARAMETER:
                parameters = ParameterMapping.from_dict(parameters)
            elif p_type == PARAMETERCOLUMNCOLLECTION:
                parameters = ParameterColumnCollectionMapping.from_dict(parameters)
        elif type(parameters) == list and all(type(p) in (int, dict) for p in parameters):
            parameters = {"map_type": PARAMETERCOLUMNCOLLECTION, "parameters": list(parameters)}
            parameters = ParameterColumnCollectionMapping.from_dict(parameters)
        skip_columns = map_dict.get("skip_columns", None)
        import_objects = map_dict.get("import_objects", False)
        return RelationshipClassMapping(name, object_classes, objects, parameters, skip_columns, import_objects)

    def to_dict(self):
        map_dict = {"map_type": self._map_type, "import_objects": self._import_objects}
        if self.name is not None:
            if type(self.name) == Mapping:
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
        return map_dict


class DataMapping:
    """Class for holding and validating Mapping specification:
    DataMapping {
            map_type: 'collection'
            mappings: List[ObjectClassMapping | RelationshipClassMapping]
    }
    """

    def __init__(self, mappings=None, has_header=False):
        if mappings == None:
            mappings = []
        self._mappings = []
        self._has_header = False
        self.mappings = mappings
        self.has_header = has_header

    def non_pivoted_columns(self):
        non_pivoted_columns = []
        if self.mappings is not None:
            for m in self.mappings:
                non_pivoted_columns.extend(m.non_pivoted_columns())
        return non_pivoted_columns

    def last_pivot_row(self):
        if self.mappings is not None:
            return max([m.last_pivot_row() for m in self.mappings if m is not None], key=none_to_minus_inf)
        return None

    def is_pivoted(self):
        if self.mappings is not None:
            return any(m.is_pivoted() for m in self.mappings)
        return False

    @property
    def mappings(self):
        return self._mappings

    @mappings.setter
    def mappings(self, mappings):
        if type(mappings) != list:
            raise TypeError("mappings must be list")
        if mappings and not all(type(m) in (RelationshipClassMapping, ObjectClassMapping) for m in mappings):
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
    def from_dict(self, map_dict):
        if type(map_dict) != dict:
            raise ValueError("map_dict must be a dict")
        has_header = map_dict.get("has_header", False)
        mappings = map_dict.get("mappings", [])
        parsed_mappings = []
        for m in mappings:
            map_type = m.get("map_type", None)
            if map_type == OBJECTCLASS:
                parsed_mappings.append(ObjectClassMapping.from_dict(m))
            elif map_type == RELATIONSHIPCLASS:
                parsed_mappings.append(RelationshipClassMapping.from_dict(m))
            else:
                raise TypeError(
                    """All mappings in field mappings must be a
                                RelationshipClassMapping or ObjectClassMapping
                                compatible dictionary"""
                )
        return DataMapping(parsed_mappings, has_header)


def create_read_parameter_functions(mapping, pivoted_data, pivoted_cols, data_header, is_pivoted):
    """Creates functions for reading parameter name, field and value from
    ParameterColumnCollectionMapping or ParameterMapping objects"""
    if mapping is None:
        return {"name": (None, None, None), "value": (None, None, None)}
    if type(mapping) not in (ParameterColumnCollectionMapping, ParameterMapping):
        raise ValueError(
            f"""mapping must be ParameterColumnCollectionMapping
                         or ParameterMapping, instead got {type(mapping)}"""
        )
    if type(mapping) == ParameterColumnCollectionMapping:
        # parameter names from header or mapping name.
        p_n_reads = False
        p_n_num = len(pivoted_cols)
        p_n = []
        if mapping.parameters:
            for p, c in zip(mapping.parameters, pivoted_cols):
                if p.name is None:
                    p_n.append(data_header[c])
                else:
                    p_n.append(p.name)
            if len(p_n) == 1:
                p_n = p_n[0]

            def p_n_getter(row):
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
    if type(map_dict) == dict:
        map_type = map_dict.get("map_type", None)
        if map_type == MAPPINGCOLLECTION:
            mapping = DataMapping.from_dict(map_dict)
        elif map_type == RELATIONSHIPCLASS:
            mapping = RelationshipClassMapping.from_dict(map_dict)
        elif map_type == OBJECTCLASS:
            mapping = ObjectClassMapping.from_dict(map_dict)
        else:
            raise ValueError(
                f'''map_dict must contain field: "map_type" with
                              value "{MAPPINGCOLLECTION}", "{RELATIONSHIPCLASS}"
                              or "{OBJECTCLASS}"'''
            )
    else:
        raise TypeError(f"map_dict must be a dict, instead it was: {type(map_dict)}")
    return mapping


def read_with_mapping(data_source, mapping, num_cols, data_header=None):
    """reads data_source line by line with supplied Mapping object or dict
    that can be translated into a Mapping object"""

    if isinstance(mapping, dict):
        mapping = dict_to_map(mapping)
    elif isinstance(mapping, list):
        # NOTE: No need to check types here, DataMapping.@mappings.setter does it already
        mapping = DataMapping(mappings=[dict_to_map(m) if isinstance(m, dict) else m for m in mapping])

    # if we have a pivot in the map, read those rows first to create getters.
    pivoted_data = []
    if mapping.is_pivoted():
        for i in range(mapping.last_pivot_row() + 1):
            # TODO: if data_source iterator ends before all pivoted rows are collected.
            pivoted_data.append(next(data_source))

    if type(mapping) == DataMapping:
        mappings = mapping.mappings
    else:
        mappings = [mapping]

    # get a list of reader functions
    readers = []
    for m in mappings:
        r = create_mapping_readers(m, num_cols, pivoted_data, data_header)
        readers.extend(r)

    # run funcitons that read from header or pivoted area first
    # select only readers that actually need to read row data
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
    row_readers = []
    errors = []
    for key, func, reads_rows in readers:
        if key not in data:
            data[key] = []
        if reads_rows:
            row_readers.append((key, func))
        else:
            data[key].extend(func(None))

    # read each row in data source
    if row_readers:
        for row_number, row_data in enumerate(data_source):
            try:
                # read the row with each reader
                for key, reader in row_readers:
                    data[key].extend(reader(row_data))
            except IndexError as e:
                errors.append((row_number, e))

    # pack extra dimensions into list of list
    # FIXME: This should probably be moved somewhere else
    new_data = {}
    for k, v in data.items():
        if k in ("object_parameter_values_ed", "relationship_parameter_values_ed") and v:
            v = sorted(v, key=lambda x: x[:-1])
            new = []
            for keys, values in itertools.groupby(v, key=lambda x: x[:-1]):
                # FIXME: Temporary keep only the two last value of values with multiple
                # dimensions until data storing specs are specified.
                packed_vals = [{items[-1][-2]: items[-1][-1]} for items in values]
                packed_vals = json.dumps(packed_vals)
                new.append(keys + (packed_vals,))
            if k == "object_parameter_values_ed":
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

    data.pop("object_parameter_values_ed", None)
    data.pop("relationship_parameter_values_ed", None)

    # remove None values from parameter_values
    for k, v in data.items():
        if k in ("object_parameter_values", "relationship_parameter_values"):
            data[k] = [item for item in v if item[-1] != None]

    return data, errors


def create_mapping_readers(mapping, num_cols, pivoted_data, data_header=None):
    """Creates a list of functions that returns data from a row of a data source
    from ObjectClassMapping or RelationshipClassMapping objects."""
    if data_header is None:
        data_header = []

    # make sure all column references are found
    non_pivoted_columns = mapping.non_pivoted_columns()
    int_non_piv_cols = []
    for pc in non_pivoted_columns:
        if type(pc) == str:
            if pc not in data_header:
                raise IndexError(
                    f"""mapping contains string
                                 refrence to data header but reference
                                 "{pc}" could not be found in header."""
                )
            pc = data_header.index(pc)
        if pc >= num_cols:
            raise IndexError(
                f"""mapping contains invalid index: {pc},
                             data column number: {num_cols}"""
            )
        int_non_piv_cols.append(pc)

    if type(mapping.parameters) == ParameterColumnCollectionMapping and mapping.parameters.parameters:
        # if we are using a parameter column collection and we have column
        # references then only use thoose columns for pivoting
        pivoted_cols = []
        for p in mapping.parameters.parameters:
            pc = p.column
            if type(pc) == str:
                pc = data_header.index(pc)
            pivoted_cols.append(pc)

    elif mapping.is_pivoted():
        # paramater column mapping is not in use and we have a pivoted mapping
        pivoted_cols = set(range(num_cols)).difference(set(int_non_piv_cols))
        # remove skipped columns
        if mapping.skip_columns:
            for skip_c in mapping.skip_columns:
                if type(skip_c) == str:
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
    if parameter_getters.get("has_extra_dimensions", False):
        pv_key = "object_parameter_values_ed"
        pv_r_key = "relationship_parameter_values_ed"
    else:
        pv_key = "object_parameter_values"
        pv_r_key = "relationship_parameter_values"

    if type(mapping) == ObjectClassMapping:
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

    Ex:
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

    mapping = "constant" will return:
        def getter(row):
            return "constant"
    mapping.value_reference = 0 and mapping.map_type = COLUMN
        def getter(row):
            return row[0]
    etc...

    returns:
        getter: function that takes a row and returns data, getter(row)
        num: int how long list of data the funciton returns.
             if 1 it returns the value instead of a list
        reads_data: boolean if getter actually reads data from input
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
