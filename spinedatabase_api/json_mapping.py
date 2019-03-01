######################################################################################################################
# Copyright (C) 2017 - 2018 Spine project consortium
# This file is part of Spine Toolbox.
# Spine Toolbox is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option)
# any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
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


# Constants for json spec
ROW = 'row'
COLUMN = 'column'
COLUMN_NAME = 'column_name'
OBJECTCLASS = 'ObjectClass'
RELATIONSHIPCLASS = 'RelationshipClass'
PARAMETER = 'parameter'
PARAMETERCOLUMN = 'parameter_column'
PARAMETERCOLUMNCOLLECTION = 'parameter_column_collection'
MAPPINGCOLLECTION = 'collection'


def mapping_from_dict_int_str(value, map_type=COLUMN):
    """Creates Mapping object if dict or int,
    if str or None returns same value. If int the Mapping is created 
    with map_type == column (default) unless other type is specified
    """
    if value is None:
        return None
    if type(value) == dict:
        return Mapping.from_dict(value)
    elif type(value) == int:
        return Mapping(map_type=map_type, value_reference=value)
    elif type(value) == str:
        return value
    else:
        raise TypeError(f'value must be dict, int or str, instead got {type(value)}')


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
    _required_fields = ('map_type',)
    def __init__(self,
                 map_type=COLUMN,
                 value_reference=None,
                 append_str=None,
                 prepend_str=None):

        # this needs to be before value_reference because value_reference uses
        # self.map_type
        self.map_type = map_type 
        self.value_reference = value_reference
        self.append_str = append_str
        self.prepend_str = prepend_str
    
    @property
    def append_str(self):
        return self.__append_str
    
    @property
    def prepend_str(self):
        return self.__prepend_str
    
    @property
    def map_type(self):
        return self.__map_type
    
    @property
    def value_reference(self):
        return self.__value_reference

    @append_str.setter
    def append_str(self, append_str=None):
        if append_str is not None and type(append_str) != str:
            raise ValueError(f"append_str must be a None or str, instead got {type(append_str)}")
        self.__append_str = append_str
    
    @prepend_str.setter
    def prepend_str(self, prepend_str=None):
        if prepend_str is not None and type(prepend_str) != str:
            raise ValueError(f"prepend_str must be None or str, instead got {type(prepend_str)}")
        self.__prepend_str = prepend_str
    
    @map_type.setter
    def map_type(self, map_type=COLUMN):
        if map_type not in [ROW, COLUMN, COLUMN_NAME]:
            raise ValueError(f"map_type must be '{ROW}', '{COLUMN}' or '{COLUMN_NAME}', instead got '{map_type}'")
        self.__map_type = map_type
    
    @value_reference.setter
    def value_reference(self, value_reference=None):
        if value_reference is not None and type(value_reference) not in (str, int):
            raise ValueError(f'value_reference must be str or int, instead got {type(value_reference)}')
        if type(value_reference) == str and self.map_type == ROW:
            raise ValueError(f'value_reference cannot be str if map_type = "{self.map_type}"')
        if type(value_reference) == int:
            if value_reference < 0 and self.map_type in (COLUMN, COLUMN_NAME):
                raise ValueError(f'value_reference must be >= 0 if map_type is "{COLUMN}" or "{COLUMN_NAME}"')
            if value_reference < -1 and self.map_type == ROW:
                raise ValueError(f'value_reference must be >= -1 if map_type is "{ROW}"')
        self.__value_reference = value_reference
    
    def is_pivoted(self):
        """Returns True if Mapping type is ROW"""
        return self.map_type == ROW
    
    def to_dict(self):
        map_dict = {'value_reference': self.value_reference, 
                    'map_type': self.map_type}
        if self.append_str is not None:
            map_dict.update({'append_str': self.append_str})
        if self.prepend_str is not None:
            map_dict.update({'prepend_str': self.prepend_str})
        return map_dict
    
    @classmethod
    def from_dict(self, map_dict):
        if type(map_dict) != dict:
            raise TypeError(f'map_dict must be a dict, instead got {type(map_dict)}')
        if not all(k in map_dict.keys() for k in self._required_fields):
            raise KeyError('dict must contain keys: {}'.format(self._required_fields))
        map_type = map_dict['map_type']
        append_str = map_dict.get('append_str', None)
        prepend_str = map_dict.get('prepend_str', None)
        value_reference = map_dict.get('value_reference', None)
        return Mapping(map_type, value_reference, append_str, prepend_str)


class ParameterMapping:
    """Class for holding and validating Mapping specification:
    ParameterMapping {
            map_type: 'parameter'
            name: Mapping | str
            value: Mapping | None
            field: 'value' | 'json' | Mapping | None
            extra_dimensions: [Mapping] | None
    }
    """
    def __init__(self,
                 name=None,
                 value=None,
                 field=None,
                 extra_dimensions=None):
        self.name = name
        self.value = value
        self.field = field
        self.extra_dimensions = extra_dimensions
        self.__map_type = PARAMETER
    
    def non_pivoted_columns(self):
        non_pivoted_columns = []
        if self.name is not None:
            if type(self.name) == Mapping and self.name.map_type == COLUMN:
                non_pivoted_columns.append(self.name.value_reference)
        if self.value is not None:
            if type(self.value) == Mapping and self.value.map_type == COLUMN:
                non_pivoted_columns.append(self.value.value_reference)
        if self.field is not None:
            if type(self.field) == Mapping and self.field.map_type == COLUMN:
                non_pivoted_columns.append(self.field.value_reference)
        if self.extra_dimensions is not None:
            for ed in self.extra_dimensions:
                if type(ed) == Mapping and ed.map_type == COLUMN:
                    non_pivoted_columns.append(ed.value_reference)
        return non_pivoted_columns
        
    def last_pivot_row(self):
        last_pivot_row = None
        if type(self.name) == Mapping and self.name.map_type == ROW:
            last_pivot_row = self.name.value_reference
        if type(self.field) == Mapping and self.field.map_type == ROW:
            if last_pivot_row is not None:
                last_pivot_row = max(last_pivot_row, self.field.value_reference)
            else:
                last_pivot_row = self.field.value_reference
        if self.extra_dimensions is not None:
            for m in self.extra_dimensions:
                if last_pivot_row is not None:
                    last_pivot_row = max(last_pivot_row, m.value_reference)
                else:
                    last_pivot_row = m.value_reference
        return last_pivot_row

    def is_pivoted(self):
        pivoted = False
        if type(self.name) == Mapping:
            pivoted = self.name.is_pivoted()
        if self.extra_dimensions is not None:
            for ed in self.extra_dimensions:
                if type(ed) == Mapping:
                    pivoted = pivoted | ed.is_pivoted()
        return pivoted
    
    @property
    def name(self):
        return self.__name
    
    @property
    def value(self):
        return self.__value
    
    @property
    def field(self):
        return self.__field
    
    @property
    def extra_dimensions(self):
        return self.__extra_dimensions
    
    @name.setter
    def name(self, name=None):
        if name is not None and type(name) not in (str, Mapping):
            raise ValueError(f"""name must be a None, str or Mapping, 
                             instead got {type(name)}""")
        self.__name = name
    
    @value.setter
    def value(self, value=None):
        if value is not None and type(value) != Mapping:
            raise ValueError(f"""value must be a None or Mapping, 
                             instead got {type(value)}""")
        self.__value = value
    
    @field.setter
    def field(self, field=None):
        if type(field) == str:
            field = field.lower()
            if field not in ('value','json'):
                raise ValueError(f"""field string must be 'value' or 'json', 
                                 instead got '{field}'""")
        elif type(field) != Mapping:
            raise TypeError(f'''field must be str or Mapping, 
                            instead got {type(field)}''')
        self.__field = field
    
    @extra_dimensions.setter
    def extra_dimensions(self, extra_dimensions=None):
        if (extra_dimensions is not None 
            and not all(type(ex) in (Mapping, str) for ex in extra_dimensions)):
            ed_types = [type(ed) for ed in extra_dimensions]
            raise TypeError(f'''extra_dimensions must be a list of Mapping 
                            or str, instead got {ed_types}''')
        self.__extra_dimensions = extra_dimensions
    
    @classmethod
    def from_dict(self, map_dict):
        if type(map_dict) != dict:
            raise ValueError('map_dict must be a dict')
        name = mapping_from_dict_int_str(map_dict.get('name', None))
        value = mapping_from_dict_int_str(map_dict.get('value', None))
        field = mapping_from_dict_int_str(map_dict.get('field', 'value'))
        extra_dimensions = map_dict.get('extra_dimensions', None)
        if type(extra_dimensions) == list:
            extra_dimensions = [mapping_from_dict_int_str(ed) 
                                for ed in extra_dimensions]
        return ParameterMapping(name, value, field, extra_dimensions)
    
    def to_dict(self):
        map_dict = {'map_type': self.__map_type}
        if self.name is not None:
            if type(self.name) == Mapping:
                map_dict.update({'name', self.name.to_dict()})
            else:
                map_dict.update({'name', self.name})
        if self.value is not None:
            if type(self.value) == Mapping:
                map_dict.update({'value', self.value.to_dict()})
            else:
                map_dict.update({'value', self.value})
        if type(self.field) == Mapping:
            map_dict.update({'field', self.field.to_dict()})
        else:
            map_dict.update({'field', self.field})
        if self.extra_dimensions is not None:
            ed = [ed.to_dict() for ed in self.extra_dimensions]
            map_dict.update({'extra_dimensions': ed})
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
        self.parameters = parameters
        self.extra_dimensions = extra_dimensions
        self.__map_type = PARAMETERCOLUMNCOLLECTION
    
    def non_pivoted_columns(self):
        non_pivoted_columns = []
        if self.parameters is not None:
            for p in self.parameters:
                non_pivoted_columns.extend(p.non_pivoted_columns())
        if self.extra_dimensions is not None:
            for ed in self.extra_dimensions:
                if type(ed) == Mapping and ed.map_type == COLUMN:
                    non_pivoted_columns.append(ed.value_reference)
        return non_pivoted_columns
        
    def last_pivot_row(self):
        last_pivot_row = None
        if self.extra_dimensions is not None:
            for m in self.extra_dimensions:
                if last_pivot_row is not None:
                    last_pivot_row = max(last_pivot_row, m.value_reference)
                else:
                    last_pivot_row = m.value_reference
        return last_pivot_row

    def is_pivoted(self):
        pivoted = False
        if self.extra_dimensions is not None:
            for ed in self.extra_dimensions:
                if type(ed) == Mapping:
                    pivoted = pivoted | ed.is_pivoted()
        return pivoted
    
    @property
    def parameters(self):
        return self.__parameters
    
    @property
    def extra_dimensions(self):
        return self.__extra_dimensions
    
    @parameters.setter
    def parameters(self, parameters=None):
        if parameters is not None and type(parameters) not in (list,):
            raise ValueError(f"""parameters must be a None or list, 
                             instead got {type(parameters)}""")
        for i, p in enumerate(parameters):
            if type(p) != ParameterColumnMapping:
                raise ValueError(f"""parameters must be a list with all 
                                 ParameterColumnMapping, instead got 
                                 {type(p)} on index {i}""")
        self.__parameters = parameters

    @extra_dimensions.setter
    def extra_dimensions(self, extra_dimensions=None):
        if (extra_dimensions is not None 
            and not all(type(ex) in (Mapping, str) for ex in extra_dimensions)):
            ed_types = [type(ed) for ed in extra_dimensions]
            raise TypeError(f'''extra_dimensions must be a list of 
                            Mapping or str, instead got {ed_types}''')
        self.__extra_dimensions = extra_dimensions
    
    @classmethod
    def from_dict(self, map_dict):
        if type(map_dict) != dict:
            raise TypeError('map_dict must be a dict, instead got {type(map_dict)}')
        parameters = map_dict.get('parameters', None)
        if type(parameters) == list:
            for i, p in enumerate(parameters):
                if type(p) == int:
                    parameters[i] = ParameterColumnMapping(column = p)
                else:
                    parameters[i] = ParameterColumnMapping.from_dict(p)
        extra_dimensions = map_dict.get('extra_dimensions', None)
        if type(extra_dimensions) == list:
            extra_dimensions = [mapping_from_dict_int_str(ed) 
                                for ed in extra_dimensions]
        return ParameterColumnCollectionMapping(parameters, extra_dimensions)
    
    def to_dict(self):
        map_dict = {'map_type': self.__map_type}
        if self.parameters is not None:
            p = [p.to_dict() for p in self.parameters]
            map_dict.update({'parameters': p})
        if self.extra_dimensions is not None:
            ed = [ed.to_dict() for ed in self.extra_dimensions]
            map_dict.update({'extra_dimensions': ed})
        return map_dict


class ParameterColumnMapping:
    """Class for holding and validating Mapping specification:
    ParameterColumnMapping {
        map_type: 'parameter_column'
        name: str | None #overrides column name
        column: str | int
        field: 'value' | 'json'
        append_str: str | None
        prepend_str: str | None]
    }
    """
    def __init__(self, name=None, column=None,
                 field='value', append_str=None, prepend_str=None):
        self.name = name
        self.column = column
        self.field = field
        self.append_str = append_str
        self.prepend_str = prepend_str
        self.__map_type = PARAMETERCOLUMN

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
        return self.__name
    
    @property
    def field(self):
        return self.__field
    
    @property
    def column(self):
        return self.__column
    
    @property
    def append_str(self):
        return self.__append_str
    
    @property
    def prepend_str(self):
        return self.__prepend_str
    
    @name.setter
    def name(self, name=None):
        if name is not None and type(name) not in (str,):
            raise ValueError(f"""name must be a None or str, 
                             instead got {type(name)}""")
        self.__name = name
    
    @column.setter
    def column(self, column=None):
        if column is not None and type(column) not in (str, int):
            raise ValueError(f"""column must be a None, str or int, 
                             instead got {type(column)}""")
        self.__column = column
    
    @field.setter
    def field(self, field=None):
        if type(field) == str:
            field = field.lower()
            if field not in ('value','json'):
                raise ValueError(f"""field string must be 'value' or 'json', 
                                 instead got '{field}'""")
        else:
            raise ValueError(f'field must be str, instead got {type(field)}')
        self.__field = field
    
    @append_str.setter
    def append_str(self, append_str=None):
        if append_str is not None and type(append_str) not in (str,):
            raise TypeError(f"""append_str must be a None or str, instead 
                            got {type(append_str)}""")
        self.__append_str = append_str
    
    @prepend_str.setter
    def prepend_str(self, prepend_str=None):
        if prepend_str is not None and type(prepend_str) not in (str,):
            raise TypeError(f"""prepend_str must be a None or str, instead 
                            got {type(prepend_str)}""")
        self.__prepend_str = prepend_str

    @classmethod
    def from_dict(self, map_dict):
        if type(map_dict) != dict:
            raise ValueError('map_dict must be a dict')
        name = map_dict.get('name', None)
        field = map_dict.get('field', 'value')
        column = map_dict.get('column', None)
        append_str = map_dict.get('append_str', None)
        prepend_str = map_dict.get('prepend_str', None)
        return ParameterColumnMapping(name, column, field,
                                      append_str, prepend_str)

    def to_dict(self):
        map_dict = {'map_type': self.__map_type}
        if self.name is not None:
            map_dict.update({'name', self.name})
        if self.column is not None:
            map_dict.update({'column', self.column})
        if self.field is not None:
            map_dict.update({'field', self.field})
        if self.append_str is not None:
            map_dict.update({'append_str', self.append_str})
        if self.prepend_str is not None:
            map_dict.update({'prepend_str', self.prepend_str})
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
    def __init__(self, name=None, obj=None, parameters=None):
        self.name = name
        self.object = obj
        self.parameters = parameters
        self.__map_type = OBJECTCLASS
    
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
        if type(self.name) == Mapping and self.name.map_type == ROW:
            last_pivot_row = self.name.value_reference
        if type(self.object) == Mapping and self.object.map_type == ROW:
            if last_pivot_row is not None:
                last_pivot_row = max(last_pivot_row, self.object.value_reference)
            else:
                last_pivot_row = self.object.value_reference
        if type(self.parameters) in (ParameterMapping, ParameterColumnCollectionMapping):
            if last_pivot_row is not None:
                last_pivot_row = max(last_pivot_row, self.parameters.last_pivot_row())
            else:
                last_pivot_row = self.parameters.last_pivot_row()
            
        return last_pivot_row
    
    def is_pivoted(self):
        pivoted = False
        if type(self.name) == Mapping:
            pivoted = self.name.is_pivoted()
        if type(self.object) == Mapping:
            pivoted = pivoted | self.object.is_pivoted()
        if type(self.parameters) in (ParameterMapping, ParameterColumnCollectionMapping):
            pivoted = pivoted | self.parameters.is_pivoted()
        return pivoted
    
    @property
    def name(self, name=None):
        return self.__name
    
    @property
    def object(self, obj=None):
        return self.__object
    
    @property
    def parameters(self, parameters=None):
        return self.__parameters

    @name.setter
    def name(self, name=None):
        if name is not None and type(name) not in (str, Mapping):
            raise TypeError(f"""name must be a None, str or Mapping, 
                            instead got {type(name)}""")
        self.__name = name
    
    @object.setter
    def object(self, obj=None):
        if obj is not None and type(obj) != Mapping:
            raise ValueError(f"""obj must be None or Mapping, 
                             instead got {type(obj)}""")
        self.__object = obj
    
    @parameters.setter
    def parameters(self, parameters=None):
        if (parameters is not None 
            and type(parameters) not in (ParameterMapping,
                                         ParameterColumnCollectionMapping)):
            raise ValueError(f"""parameters must be a None, ParameterMapping or
                             ParameterColumnCollectionMapping, instead got 
                             {type(parameters)}""")
        self.__parameters = parameters
    
    @classmethod
    def from_dict(cls, map_dict):
        if type(map_dict) != dict:
            raise TypeError('map_dict must be a dict, instead got {type(map_dict)}')
        if map_dict.get('map_type',None) != OBJECTCLASS:
            raise ValueError(f'''map_dict must contain field "map_type" 
                             with value: "{OBJECTCLASS}"''')
        name = mapping_from_dict_int_str(map_dict.get('name', None))
        obj = mapping_from_dict_int_str(map_dict.get('object',None))
        parameters = map_dict.get('parameters',None)
        if type(parameters) == dict:
            p_type = parameters.get('map_type', None)
            if p_type == PARAMETER:
                parameters = ParameterMapping.from_dict(parameters)
            elif p_type == PARAMETERCOLUMNCOLLECTION:
                parameters = ParameterColumnCollectionMapping.from_dict(parameters)
        elif (type(parameters) == list 
              and all(type(p) in (int, dict) for p in parameters)):
            parameters = {'map_type': PARAMETERCOLUMNCOLLECTION,
                          'parameters': list(parameters)}
            parameters = ParameterColumnCollectionMapping.from_dict(parameters)
            
        return ObjectClassMapping(name, obj, parameters)
    
    def to_dict(self):
        map_dict = {'map_type': self.__map_type}
        if self.name is not None:
            if type(self.name) == Mapping:
                map_dict.update(name = self.name.to_dict())
            else:
                map_dict.update(name = self.name)
        if self.object is not None:
            map_dict.update(object = self.object.to_dict)
        if self.parameters is not None:
            map_dict.update(parameters = [p.to_dict() for p in self.parameters])
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
    def __init__(self, name=None, object_classes=None,
                 objects=None, parameters=None):
        self.__map_type = RELATIONSHIPCLASS
        self.name = name
        self.object_classes = object_classes
        self.objects = objects
        self.parameters = parameters
    
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
        if type(self.name) == Mapping and self.name.map_type == ROW:
            last_pivot_row = self.name.value_reference
        if self.objects is not None:
            for o in self.objects:
                if last_pivot_row is not None:
                    last_pivot_row = max(last_pivot_row, o.value_reference)
                else:
                    last_pivot_row = o.value_reference
        if self.parameters is not None:
            if last_pivot_row is not None:
                last_pivot_row = max(last_pivot_row,
                                     self.parameters.last_pivot_row())
            else:
                last_pivot_row = self.parameters.last_pivot_row()
            
        return last_pivot_row
    
    def is_pivoted(self):
        pivoted = False
        if type(self.name) == Mapping:
            pivoted = self.name.is_pivoted()
        if self.objects is not None:
            pivoted = pivoted | any(o.is_pivoted() for o in self.objects)
        if self.parameters is not None:
            pivoted = pivoted | self.parameters.is_pivoted()
        return pivoted
    
    @property
    def name(self, name=None):
        return self.__name
    
    @property
    def object_classes(self, object_classes=None):
        return self.__object_classes
    
    @property
    def objects(self, objects=None):
        return self.__objects
    
    @property
    def parameters(self, parameters=None):
        return self.__parameters
    
    @name.setter
    def name(self, name=None):
        if name is not None and type(name) not in (str, Mapping):
            raise ValueError(f"""name must be a None, str or Mapping, 
                             instead got {type(name)}""")
        self.__name = name

    @object_classes.setter
    def object_classes(self, object_classes=None):
        if (object_classes is not None 
            and not all(type(o) in (Mapping, str) for o in object_classes)):
            raise TypeError("name must be a None, str or Mapping | str}")
        if not all(o.map_type == COLUMN_NAME 
                   for o in object_classes if type(o) == Mapping):
            raise ValueError(f"""All Mappings in object_classes must
                             be of map_type=='{COLUMN_NAME}'""")
        self.__object_classes = object_classes
    
    @objects.setter
    def objects(self, objects=None):
        if objects is not None and not all(type(o) in (Mapping, str) for o in objects):
            raise TypeError("objects must be a None, or list of Mapping | str")
        self.__objects = objects
    
    @parameters.setter
    def parameters(self, parameters=None):
        if (parameters is not None 
            and type(parameters) not in (ParameterMapping,
                                         ParameterColumnCollectionMapping)):
            raise ValueError(f"""parameters must be a None, ParameterMapping or
                             ParameterColumnCollectionMapping,
                             instead got {type(parameters)}""")
        self.__parameters = parameters
    
    @classmethod
    def from_dict(self, map_dict):
        if type(map_dict) != dict:
            raise ValueError('map_dict must be a dict')
        if map_dict.get('map_type',None) != RELATIONSHIPCLASS:
            raise ValueError(f'''map_dict must contain field "map_type" 
                             with value: "{RELATIONSHIPCLASS}"''')
        name = mapping_from_dict_int_str(map_dict.get('name', None))
        objects = map_dict.get('objects',None)
        if type(objects) == list:
            objects = [mapping_from_dict_int_str(o) for o in objects]
        object_classes = map_dict.get('object_classes',None)
        if type(object_classes) == list:
            object_classes = [mapping_from_dict_int_str(o, COLUMN_NAME) 
                              for o in object_classes]
        parameters = map_dict.get('parameters',None)
        if type(parameters) == dict:
            p_type = parameters.get('map_type', None)
            if p_type == PARAMETER:
                parameters = ParameterMapping.from_dict(parameters)
            elif p_type == PARAMETERCOLUMNCOLLECTION:
                parameters = ParameterColumnCollectionMapping.from_dict(parameters)
        elif type(parameters) == list and all(type(p) in (int, dict) 
                                              for p in parameters):
            parameters = {'map_type': PARAMETERCOLUMNCOLLECTION,
                          'parameters': list(parameters)}
            parameters = ParameterColumnCollectionMapping.from_dict(parameters)
        return RelationshipClassMapping(name, object_classes,
                                        objects, parameters)
    
    def to_dict(self):
        map_dict = {'map_type': self.__map_type}
        if self.name is not None:
            if type(self.name) == Mapping:
                map_dict.update(name = self.name.to_dict())
            else:
                map_dict.update(name = self.name)
        if self.object_classes is not None:
            map_dict.update(objects = [o.to_dict() for o in self.object_classes])
        if self.objects is not None:
            map_dict.update(objects = [o.to_dict() for o in self.objects])
        if self.parameters is not None:
            map_dict.update(parameters = [p.to_dict() for p in self.parameters])
        return map_dict


class DataMapping:
    """Class for holding and validating Mapping specification:
    DataMapping {
            map_type: 'collection'
            mappings: List[ObjectClassMapping | RelationshipClassMapping]
    }
    """
    def __init__(self, mappings=None, has_header=False):
        self.mappings = mappings
        self.has_header = has_header
    
    def non_pivoted_columns(self):
        non_pivoted_columns = []
        if self.mappings is not None:
            for m in self.mappings:
                non_pivoted_columns.extend(m.non_pivoted_columns())
        return non_pivoted_columns

    def last_pivot_row(self):
        last_pivot_row = None
        if self.mappings is not None:
            for m in self.mappings:
                m_pivot_row = m.last_pivot_row()
                if m is not None:
                    if last_pivot_row is None:
                        last_pivot_row = m_pivot_row
                    else:
                        last_pivot_row = max(m_pivot_row, last_pivot_row)
        return last_pivot_row
    
    def is_pivoted(self):
        has_pivot = False
        if self.mappings is not None:
            has_pivot = any(m.is_pivoted() for m in self.mappings)
        return has_pivot

    @property
    def mappings(self):
        return self.__mappings
    
    @mappings.setter
    def mappings(self, map_list):
        if (map_list is not None 
            and not all(type(m) in (RelationshipClassMapping,
                                    ObjectClassMapping) for m in map_list)):
            raise TypeError('''All mappings in map_list must be 
                            RelationshipClassMapping or ObjectClassMapping''')
        self.__mappings = map_list
    
    @property
    def has_header(self):
        return self.__header_row
    
    @has_header.setter
    def has_header(self, has_header):
        self.__has_header = bool(has_header)
    
    def to_dict(self):
        map_dict = {'has_header': self.has_header}
        if self.mappings:
            map_dict.update(mappings = [m.to_dict() for m in self.mappings])
        return map_dict

    @classmethod
    def from_dict(self, map_dict):
        if type(map_dict) != dict:
            raise ValueError('map_dict must be a dict')
        has_header = map_dict.get('has_header', False)
        mappings = map_dict.get('mappings', [])
        parsed_mappings = []
        for m in mappings:
            map_type = m.get('map_type', None)
            if map_type == OBJECTCLASS:
                parsed_mappings.append(ObjectClassMapping.from_dict(m))
            elif map_type == RELATIONSHIPCLASS:
                parsed_mappings.append(RelationshipClassMapping.from_dict(m))
            else:
                raise TypeError('''All mappings in field mappings must be a 
                                RelationshipClassMapping or ObjectClassMapping 
                                compatible dictionary''')
        return DataMapping(parsed_mappings, has_header)


def create_read_parameter_functions(mapping, pivoted_data,
                                    pivoted_cols, data_header, is_pivoted):
    """Creates functions for reading parameter name, field and value from
    ParameterColumnCollectionMapping or ParameterMapping objects"""
    if mapping is None:
        return {'name': (None, None, None),
                'field': (None, None, None),
                'value': (None, None, None)}
    if type(mapping) not in (ParameterColumnCollectionMapping, ParameterMapping):
        raise ValueError(f'''mapping must be ParameterColumnCollectionMapping 
                         or ParameterMapping, instead got {type(mapping)}''')
    if type(mapping) == ParameterColumnCollectionMapping:
        # parameter names from header or mapping name.
        p_n_reads = False
        p_f_reads = False
        p_n_num = len(pivoted_cols)
        p_f_num = len(pivoted_cols)
        p_f = []
        p_n = []
        if mapping.parameters:
            for p, c in zip(mapping.parameters, pivoted_cols):
                p_f.append(p.field)
                if p.name is None:
                    p_n.append(data_header[c])
                else:
                    p_n.append(p.name)
            if len(p_n) == 1:
                p_n = p_n[0]
                p_f = p_f[0]
            def p_n_getter(row):
                return p_n
            def p_f_getter(row):
                return p_f
            p_v_num = len(pivoted_cols)
            p_v_getter = itemgetter(*pivoted_cols)
            p_v_reads = True
        else:
            # no data
            return {'name': (None, None, None),
                    'field': (None, None, None),
                    'value': (None, None, None)}
    else:
        # general ParameterMapping type
        p_n_getter, p_n_num, p_n_reads = \
            create_pivot_getter_function(mapping.name, pivoted_data,
                                         pivoted_cols, data_header)
        p_f_getter, p_f_num, p_f_reads = \
            create_pivot_getter_function(mapping.field, pivoted_data,
                                         pivoted_cols, data_header)

        if is_pivoted:
            # if mapping is pivoted values for parameters are read from 
            # pivoted columns
            p_v_num = len(pivoted_cols)
            p_v_getter = itemgetter(*pivoted_cols)
            p_v_reads = True
        else:
            p_v_getter, p_v_num, p_v_reads = \
                create_pivot_getter_function(mapping.value, pivoted_data,
                                             pivoted_cols, data_header)
    
    # extra dimensions for parameter
    if mapping.extra_dimensions and p_v_getter is not None:
        # create functions to get extra_dimensions if there is a value getter
        ed_getters, ed_num, ed_reads_data = \
            create_getter_list(mapping.extra_dimensions, pivoted_data,
                               pivoted_cols, data_header)
        p_v_getter = ed_getters + [p_v_getter]
        p_v_num = ed_num + [p_v_num]
        p_v_reads = ed_reads_data + [p_v_reads]
        # create a function that returns a tuple with extra dimensions and value
        p_v_getter, p_v_num, p_v_reads = \
            create_getter_function_from_function_list(p_v_getter, p_v_num,
                                                      p_v_reads)
    else:
        p_v_getter = p_v_getter
        p_v_num = p_v_num
        p_v_reads = p_v_reads
    
    getters = {'name': (p_n_getter, p_n_num, p_n_reads),
               'field': (p_f_getter, p_f_num, p_f_reads),
               'value': (p_v_getter, p_v_num, p_v_reads)}
    return getters


def create_getter_list(mapping, pivoted_data, pivoted_cols, data_header):
    """Creates a list of getter functions from a list of Mappings"""
    if mapping is None:
        return [], [], []
    
    obj_getters = []
    obj_num = []
    obj_reads = []
    for o in mapping:
        o, num, reads = create_pivot_getter_function(o, pivoted_data,
                                                     pivoted_cols, data_header)
        obj_getters.append(o)
        obj_num.append(num)
        obj_reads.append(reads)
    return obj_getters, obj_num, obj_reads


def dict_to_map(map_dict):
    """Creates Mapping object from a dict"""
    if type(map_dict) == dict:
        map_type = map_dict.get('map_type', None)
        if map_type == MAPPINGCOLLECTION:
            mapping = DataMapping.from_dict(map_dict)
        elif map_type == RELATIONSHIPCLASS:
            mapping = RelationshipClassMapping.from_dict(map_dict)
        elif map_type == OBJECTCLASS:
            mapping = ObjectClassMapping.from_dict(map_dict)
        else:
            raise ValueError(f'''map_dict must containg field: "map_type" with 
                              value "{MAPPINGCOLLECTION}", "{RELATIONSHIPCLASS}"
                              or "{OBJECTCLASS}"''')
    else:
        raise TypeError(f'map_dict must be a dict, instead it was: {type(map_dict)}')
    return mapping


def read_with_mapping(data_source, mapping, num_cols, data_header=None):
    """reads data_source line by line with supplied Mapping object or dict
    that can be translated into a Mapping object"""
    
    # create a list of ObjectClassMappings or RelationshipClassMappings
    if type(mapping) == dict:
        mapping = dict_to_map(mapping)
    elif type(mapping) not in (ObjectClassMapping,
                               RelationshipClassMapping,
                               DataMapping):
        raise TypeError(f''''mapping must be ObjectClassMapping, 
                        RelationshipClassMapping or DataMapping, 
                        instead got: {type(mapping)}''')

    # if we have a pivot the map, read those rows first to create getters.
    pivoted_data = []
    if mapping.is_pivoted():
        for i in range(mapping.last_pivot_row()+1):
            #TODO: if data_source iterator ends before all pivoted rows are collected.
            pivoted_data.append(next(data_source))
    
    
    if type(mapping) == DataMapping:
        collection = mapping.mappings
    else:
        collection = [mapping]
    
    # get a list of reader functions
    readers = []
    for m in collection:
        r = create_mapping_readers(m, num_cols, pivoted_data, data_header)
        readers.extend(r)
    
    # run funcitons that reads from header or pivoted area first
    # select only readers that actually needs to read row data
    data = {}
    row_readers = []
    errors = []
    for key, func, reads_rows in readers:
        if not key in data:
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
            except Exception as e:
                errors.append((row_number, e))
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
                raise IndexError(f'''mapping contains string 
                                 refrence to data header but reference 
                                 "{pc}" could not be found in header.''')
            pc = data_header.index(pc)
        if pc >= num_cols:
            raise IndexError(f'''mapping contains invalid index: {pc}, 
                             data column number: {num_cols}''')
        int_non_piv_cols.append(pc)

    if (type(mapping.parameters) == ParameterColumnCollectionMapping 
        and mapping.parameters.parameters):
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
    else:
        # no pivoted mapping
        pivoted_cols = []
    
    parameter_getters = \
        create_read_parameter_functions(mapping.parameters, pivoted_data,
                                        pivoted_cols, data_header, 
                                        mapping.is_pivoted())
    p_n_getter, p_n_num, p_n_reads = parameter_getters['name']
    p_f_getter, p_f_num, p_f_reads = parameter_getters['field']
    p_v_getter, p_v_num, p_v_reads = parameter_getters['value']

    if type(mapping) == ObjectClassMapping:
        # getter for object class and objects
        oc_getter, oc_num, oc_reads = \
            create_pivot_getter_function(mapping.name, pivoted_data,
                                         pivoted_cols, data_header)
        o_getter, o_num, o_reads = \
            create_pivot_getter_function(mapping.object, pivoted_data,
                                         pivoted_cols, data_header)
        rc_getter, rc_num, rc_reads = (None, None, None)
        rc_oc_getter, rc_oc_num, rc_oc_reads = (None, None, None)
        r_getter, r_num, r_reads = (None, None, None)
    else:
        # getters for relationship class and relationships
        rc_getter, rc_num, rc_reads = \
            create_pivot_getter_function(mapping.name, pivoted_data,
                                         pivoted_cols, data_header)
        rc_oc_getter, rc_oc_num, rc_oc_reads = \
            create_getter_function_from_function_list(
                    *create_getter_list(mapping.object_classes, pivoted_data,
                                        pivoted_cols, data_header))
        r_getter, r_num, r_reads = \
            create_getter_function_from_function_list(
                    *create_getter_list(mapping.objects, pivoted_data,
                                        pivoted_cols, data_header))
        
        oc_getter, oc_num, oc_reads = (None, None, None)
        o_getter, o_num, o_reads = (None, None, None)
    
    # create function from list of functions
    oc_function, oc_reads = \
        create_final_getter_function([oc_getter],
                                     [oc_num],
                                     [oc_reads])
    o_function, o_reads = \
        create_final_getter_function([oc_getter, o_getter],
                                     [oc_num, o_num],
                                     [oc_reads, o_reads])
    p_function, p_reads = \
        create_final_getter_function([oc_getter, p_n_getter],
                                     [oc_num, p_n_num],
                                     [oc_reads, p_n_reads])
    pv_function, pv_reads = \
        create_final_getter_function([oc_getter, o_getter, p_n_getter, p_f_getter, p_v_getter],
                                     [oc_num, o_num, p_n_num, p_f_num, p_v_num],
                                     [oc_reads, o_reads, p_n_reads, p_f_reads, p_v_reads])
    rc_function, rc_reads = \
        create_final_getter_function([rc_getter, rc_oc_getter],
                                     [rc_num, rc_oc_num],
                                     [rc_reads, rc_oc_reads])
    r_function, r_reads = \
        create_final_getter_function([rc_getter, r_getter],
                                     [rc_num, r_num],
                                     [rc_reads, r_reads])
    r_p_function, r_p_reads = \
        create_final_getter_function([rc_getter, p_n_getter],
                                     [rc_num, p_n_num],
                                     [rc_reads, p_n_reads])
    r_pv_function, r_pv_reads = \
        create_final_getter_function([rc_getter, r_getter, p_n_getter, p_f_getter, p_v_getter],
                                     [rc_num, r_num, p_n_num, p_f_num, p_v_num],
                                     [rc_reads, r_reads, p_n_reads, p_f_reads, p_v_reads])

    readers = [('object_classes',oc_function, oc_reads),
               ('objects',o_function, o_reads),
               ('object_parameters',p_function, p_reads),
               ('object_parameter_values',pv_function, pv_reads),
               ('relationship_classes',rc_function, rc_reads),
               ('relationships',r_function, r_reads),
               ('relationship_parameters',r_p_function, r_p_reads),
               ('relationship_parameter_values',r_pv_function, r_pv_reads)]
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
        getter, _, reads_data = create_getter_function_from_function_list(function_list,
                                                                          function_output_len_list,
                                                                          reads_data_list,
                                                                          True)
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
        raise ValueError('''len_output_list each element in list must be 1 
                         or max(len_output_list)''')
    
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
                pivot_value_getter = itemgetter(*pivoted_cols)
                piv_values = pivot_value_getter(read_from)
                def getter_fcn(x):
                    return piv_values
                getter = getter_fcn
                num = len(piv_values)
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
