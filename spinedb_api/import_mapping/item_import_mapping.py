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
Classes for item import mappings.

:author: P. Vennström (VTT)
:date:   22.02.2018
"""
from distutils.util import strtobool
from .single_import_mapping import (
    NoneMapping,
    ConstantMapping,
    ColumnMapping,
    single_mapping_from_value,
    create_getter_list,
    create_final_getter_function,
    create_getter_function_from_function_list,
    multiple_append,
)
from .parameter_import_mapping import (
    ParameterMappingBase,
    ParameterDefinitionMapping,
    ParameterValueMapping,
    parameter_mapping_from_dict,
)


class ItemMappingBase:
    """Base class for item mappings."""

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

    def component_names(self):  # pylint: disable=no-self-use
        return []

    def component_mappings(self):  # pylint: disable=no-self-use
        return []

    def _optional_component_names(self):  # pylint: disable=no-self-use
        return []

    def set_component_by_name(self, name, mapping):  # pylint: disable=no-self-use
        return False

    @property
    def skip_columns(self):
        return self._skip_columns

    @skip_columns.setter
    def skip_columns(self, skip_columns=None):
        if skip_columns is None:
            self._skip_columns = []
            return
        if isinstance(skip_columns, (str, int)):
            self._skip_columns = [skip_columns]
            return
        if isinstance(skip_columns, list):
            i = next(iter(i for i, column in enumerate(skip_columns) if not isinstance(column, (str, int))), None)
            if i is not None:
                raise TypeError(
                    "skip_columns must be str, int or list of str, int, "
                    f"instead got list with {type(skip_columns[i]).__name__} on index {i}"
                )
            self._skip_columns = skip_columns
            return
        raise TypeError(f"skip_columns must be str, int or list of str, int, instead {type(skip_columns).__name__}")

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

    def is_valid(self):
        issues = [self.component_issues(k) for k in range(len(self.component_names()))]
        issues = [x for x in issues if x]
        if not issues:
            return True, ""
        return False, ", ".join(issues)

    def is_pivoted(self):
        return any(m.is_pivoted() for m in self.component_mappings())

    def non_pivoted_columns(self):
        return [m.reference for m in self.component_mappings() if isinstance(m, ColumnMapping) and m.returns_value()]

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
        return max(*(m.last_pivot_row() for m in self.component_mappings()), -1)

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
    """Base class for named item mappings such as entity classes, alternatives and scenarios."""

    def __init__(self, name, skip_columns, read_start_row):
        """
        Args:
            name (str or SingleMappingBase, optional): mapping for the item name
            skip_columns (list, optional): a list of columns to skip while mapping
            read_start_row (int): skip this many rows while mapping
        """
        super().__init__(skip_columns, read_start_row)
        self._name = None
        self.name = name

    def component_mappings(self):
        return super().component_mappings() + [self.name]

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = single_mapping_from_value(name)

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
    Base class for entity class mappings.
    """

    MAP_TYPE = None

    def __init__(self, name, parameters, import_objects, skip_columns, read_start_row):
        """
        Args:
            name (str or spinedb_api.SingleMappingBase, optional): mapping for the class name
            parameters (str or spinedb_api.ParameterMappingBase, optional): mapping for the parameters of the class
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
        if isinstance(self.parameters, ParameterMappingBase):
            non_pivoted_columns.extend(self.parameters.non_pivoted_columns())
        return non_pivoted_columns

    def last_pivot_row(self):
        return max(super().last_pivot_row(), self._parameters.last_pivot_row())

    def is_pivoted(self):
        return super().is_pivoted() or self._parameters.is_pivoted()

    def has_parameters(self):
        """Returns True if this mapping has parameters, otherwise returns False."""
        return True

    def is_valid(self):
        issues = [self.component_issues(k) for k in range(len(self.component_names()))]
        if not isinstance(self._parameters, NoneMapping):
            issues += [self._parameters.component_issues(k) for k in range(len(self._parameters.component_names()))]
        issues = [x for x in issues if x]
        if not issues:
            return True, ""
        return False, ", ".join(issues)

    @property
    def parameters(self):
        return self._parameters

    @parameters.setter
    def parameters(self, parameters=None):
        if parameters is None:
            parameters = NoneMapping()
        if not isinstance(parameters, (ParameterMappingBase, NoneMapping)):
            raise ValueError(
                f"parameters must be None, ParameterMappingBase or NoneMapping, "
                f"instead got {type(parameters).__name__}"
            )
        if isinstance(parameters, ParameterMappingBase):
            parameters.set_parent(self)
        self._parameters = parameters

    def to_dict(self):
        map_dict = super().to_dict()
        map_dict["parameters"] = self.parameters.to_dict()
        map_dict["import_objects"] = self._import_objects
        return map_dict

    def _create_getters(self, pivoted_columns, pivoted_data, data_header):
        """Creates a dict of getter functions."""
        getters = super()._create_getters(pivoted_columns, pivoted_data, data_header)
        if isinstance(self._parameters, ParameterMappingBase):
            parameter_getters = self._parameters.create_getter_list(pivoted_columns, pivoted_data, data_header)
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
    Object class mapping:

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

    def component_names(self):
        return super().component_names() + ["Object class names", "Object names"]

    def component_mappings(self):
        return super().component_mappings() + [self.objects]

    def _optional_component_names(self):  # pylint: disable=no-self-use
        optional_component_names = super()._optional_component_names()
        if not isinstance(self._parameters, ParameterValueMapping):
            optional_component_names += ["Object names"]  # NOTE: Object names are optional if no parameter values
        return optional_component_names

    def set_component_by_name(self, name, mapping):
        if name == "Object class names":
            self.name = mapping
            return True
        if name == "Object names":
            self.objects = mapping
            return True
        return super().set_component_by_name(name, mapping)

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
        self._objects = single_mapping_from_value(objects)

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

    def _create_getters(self, pivoted_columns, pivoted_data, data_header):
        """See base class."""
        readers = super()._create_getters(pivoted_columns, pivoted_data, data_header)
        if self._objects.returns_value():
            readers["objects"] = self._objects.create_getter_function(pivoted_columns, pivoted_data, data_header)
            return readers
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
    Object group mapping:

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

    def component_names(self):
        return super().component_names() + ["Object class names", "Group names", "Member names"]

    def component_mappings(self):
        return super().component_mappings() + [self.groups, self.members]

    def set_component_by_name(self, name, mapping):
        if name == "Object class names":
            self.name = mapping
            return True
        if name == "Group names":
            self.groups = mapping
            return True
        if name == "Member names":
            self.members = mapping
            return True
        return super().set_component_by_name(name, mapping)

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
        self._groups = single_mapping_from_value(groups)

    @property
    def members(self):
        return self._members

    @members.setter
    def members(self, members):
        self._members = single_mapping_from_value(members)

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

    def _create_getters(self, pivoted_columns, pivoted_data, data_header):
        """See base class."""
        getters = super()._create_getters(pivoted_columns, pivoted_data, data_header)
        if self._groups.returns_value():
            getters["groups"] = self._groups.create_getter_function(pivoted_columns, pivoted_data, data_header)
        else:
            getters["groups"] = (None, None, None)
        if self._members.returns_value():
            getters["members"] = self._members.create_getter_function(pivoted_columns, pivoted_data, data_header)
        else:
            getters["members"] = (None, None, None)
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
    Relationship class mapping:

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

    def _object_class_component_names(self):
        return [f"Object class names {i+1}" for i in range(len(self.object_classes))]

    def _object_component_names(self):
        return [f"Object names {i+1}" for i in range(len(self.objects))]

    def component_names(self):
        component_names = super().component_names() + ["Relationship class names"]
        if self.object_classes:
            component_names += self._object_class_component_names()
        if self.objects:
            component_names += self._object_component_names()
        return component_names

    def component_mappings(self):
        component_mappings = super().component_mappings()
        if self.object_classes:
            component_mappings.extend(list(self.object_classes))
        if self.objects:
            component_mappings.extend(list(self.objects))
        return component_mappings

    def _optional_component_names(self):
        optional_component_names = super()._optional_component_names()
        if not isinstance(self._parameters, ParameterValueMapping):
            optional_component_names += self._object_component_names()
        return optional_component_names

    def set_component_by_name(self, name, mapping):
        if name == "Relationship class names":
            self.name = mapping
            return True
        try:
            ind = self._object_class_component_names().index(name)
            self.object_classes[ind] = mapping
            return True
        except ValueError:
            try:
                ind = self._object_class_component_names().index(name)
                self.objects[ind] = mapping
                return True
            except ValueError:
                return super().set_component_by_name(name, mapping)

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
                f"objects must be a list or tuple of SingleMappingBase, int, str, dict, instead got: {type(objects).__name__}"
            )
        if len(objects) != len(self.object_classes):
            raise ValueError(
                f"objects must be of same length as object_classes: {len(self.object_classes)} "
                f"instead got length: {len(objects)}"
            )
        self._objects = [single_mapping_from_value(o) for o in objects]

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
                f"object_classes must be a list or tuple of SingleMappingBase, int, str, dict, instead got: {type(object_classes).__name__}"
            )
        self._object_classes = [single_mapping_from_value(oc) for oc in object_classes]

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
        Alternative mapping.

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
            name (str or SingleMappingBase, optional): mapping for the item name
            skip_columns (list, optional): a list of columns to skip while mapping
            read_start_row (int): skip this many rows while mapping
        """
        super().__init__(name, skip_columns, read_start_row)

    def component_names(self):
        return super().component_names() + ["Alternative names"]

    def set_component_by_name(self, name, mapping):
        if name == "Alternative names":
            self.name = mapping
            return True
        super().set_component_by_name(name, mapping)

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
        Scenario mapping.

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
            name (str or SingleMappingBase, optional): mapping for the scenario name
            active (str or Mapping, optional): mapping for scenario's active flag
            skip_columns (list, optional): a list of columns to skip while mapping
            read_start_row (int): skip this many rows while mapping
        """
        super().__init__(name, skip_columns, read_start_row)
        if active is not None:
            self._active = single_mapping_from_value(active)
        else:
            self._active = ConstantMapping("false")

    def component_names(self):
        return super().component_names() + ["Scenario names", "Scenario active flags"]

    def component_mappings(self):
        return super().component_mappings() + [self.active]

    def _component_issues(self, name, mapping):
        if name == "Scenario active flags" and isinstance(mapping, ConstantMapping) and mapping.is_valid():
            try:
                strtobool(mapping.reference)
            except ValueError as err:
                return str(err)
        return super()._component_issues(name, mapping)

    def set_component_by_name(self, name, mapping):
        if name == "Scenario names":
            self.name = mapping
            return True
        if name == "Scenario active flags":
            self.active = mapping
            return True
        return super().set_component_by_name(name, mapping)

    @property
    def active(self):
        return self._active

    @active.setter
    def active(self, active):
        self._active = single_mapping_from_value(active)

    def _create_active_readers(self, pivoted_columns, pivoted_data, data_header):
        if self._active.returns_value():
            getter, num, reads_data = self._active.create_getter_function(pivoted_columns, pivoted_data, data_header)
            bool_getter = lambda x: bool(strtobool(str(getter(x))))  # pylint: disable=not-callable
            return bool_getter, num, reads_data
        return (None, None, None)

    def _create_getters(self, pivoted_columns, pivoted_data, data_header):
        getters = super()._create_getters(pivoted_columns, pivoted_data, data_header)
        getters["active"] = self._create_active_readers(pivoted_columns, pivoted_data, data_header)
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
        Scenario alternative mapping.

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
        self._scenario_name = single_mapping_from_value(scenario_name)
        self._alternative_name = single_mapping_from_value(alternative_name)
        self._before_alternative_name = single_mapping_from_value(before_alternative_name)

    def component_names(self):
        return super().component_names() + ["Scenario names", "Alternative names", "Before Alternative names"]

    def component_mappings(self):
        return super().component_mappings() + [self.scenario_name, self.alternative_name, self.before_alternative_name]

    def _optional_component_names(self):
        # TODO: Is this correct?
        return super()._optional_component_names() + ["Alternative names", "Before Alternative names"]

    def set_component_by_name(self, name, mapping):
        if name == "Scenario names":
            self.scenario_name = mapping
            return True
        if name == "Alternative names":
            self.alternative_name = mapping
            return True
        if name == "Before Alternative names":
            self.before_alternative_name = mapping
            return True
        return super().set_component_by_name(name, mapping)

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
        self._scenario_name = single_mapping_from_value(scenario_name)

    @alternative_name.setter
    def alternative_name(self, alternative_name):
        self._alternative_name = single_mapping_from_value(alternative_name)

    @before_alternative_name.setter
    def before_alternative_name(self, before_alternative_name):
        self._before_alternative_name = single_mapping_from_value(before_alternative_name)

    def create_mapping_readers(self, num_columns, pivoted_data, data_header):
        pivoted_columns = self.pivoted_columns(data_header, num_columns)
        if self._scenario_name.returns_value():
            (
                scenario_name_getter,
                scenario_name_length,
                scenario_name_reads,
            ) = self._scenario_name.create_getter_function(pivoted_columns, pivoted_data, data_header)
        else:
            scenario_name_getter, scenario_name_length, scenario_name_reads = None, None, None
        if self._alternative_name.returns_value():
            alt_getter, alt_length, alt_reads = self._alternative_name.create_getter_function(
                pivoted_columns, pivoted_data, data_header
            )
        else:
            alt_getter, alt_length, alt_reads = None, None, None
        if self._before_alternative_name.returns_value():
            (
                before_name_getter,
                before_name_length,
                before_name_reads,
            ) = self._before_alternative_name.create_getter_function(pivoted_columns, pivoted_data, data_header)
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
        if isinstance(instance, NamedItemMapping):
            return ScenarioAlternativeMapping(
                instance.name, skip_columns=instance.skip_columns, read_start_row=instance.read_start_row
            )
        return ScenarioAlternativeMapping(skip_columns=instance.skip_columns, read_start_row=instance.read_start_row)


class ToolMapping(NamedItemMapping):
    """
        Tool mapping.

        specification:

            ToolMapping {
                map_type: 'Tool'
                name: str | Mapping
            }
    """

    MAP_TYPE = "Tool"

    def __init__(self, name=None, skip_columns=None, read_start_row=0):
        """
        Args:
            name (str or SingleMappingBase, optional): mapping for the item name
            skip_columns (list, optional): a list of columns to skip while mapping
            read_start_row (int): skip this many rows while mapping
        """
        super().__init__(name, skip_columns, read_start_row)

    def component_names(self):
        return super().component_names() + ["Tool names"]

    def set_component_by_name(self, name, mapping):
        if name == "Tool names":
            self.name = mapping
            return True
        return super().set_component_by_name(name, mapping)

    def create_mapping_readers(self, num_columns, pivoted_data, data_header):
        pivoted_columns = self.pivoted_columns(data_header, num_columns)
        getters = self._create_getters(pivoted_columns, pivoted_data, data_header)
        name_getter, name_num, name_reads = getters["item_name"]
        readers = [("tools",) + create_final_getter_function([name_getter], [name_num], [name_reads])]
        return readers

    @classmethod
    def from_dict(cls, map_dict):
        if not isinstance(map_dict, dict):
            raise TypeError(f"map_dict must be a dict, instead got {type(map_dict).__name__}")
        name = map_dict.get("name", None)
        skip_columns = map_dict.get("skip_columns", [])
        read_start_row = map_dict.get("read_start_row", 0)
        return ToolMapping(name, skip_columns, read_start_row)

    @classmethod
    def from_instance(cls, instance):
        """See base class."""
        if isinstance(instance, NamedItemMapping):
            return ToolMapping(
                instance.name, skip_columns=instance.skip_columns, read_start_row=instance.read_start_row
            )
        return ToolMapping(skip_columns=instance.skip_columns, read_start_row=instance.read_start_row)


class FeatureMappingMixin:
    def __init__(self, *args, entity_class_name=None, parameter_definition_name=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._entity_class_name = single_mapping_from_value(entity_class_name)
        self._parameter_definition_name = single_mapping_from_value(parameter_definition_name)

    def component_names(self):
        return super().component_names() + ["Entity class names", "Parameter names"]

    def set_component_by_name(self, name, mapping):
        if name == "Entity class names":
            self.entity_class_name = mapping
            return True
        if name == "Parameter names":
            self.parameter_definition_name = mapping
            return True
        return super().set_component_by_name(name, mapping)

    def component_mappings(self):
        return super().component_mappings() + [self.entity_class_name, self.parameter_definition_name]

    @property
    def entity_class_name(self):
        return self._entity_class_name

    @property
    def parameter_definition_name(self):
        return self._parameter_definition_name

    @entity_class_name.setter
    def entity_class_name(self, entity_class_name):
        self._entity_class_name = single_mapping_from_value(entity_class_name)

    @parameter_definition_name.setter
    def parameter_definition_name(self, parameter_definition_name):
        self._parameter_definition_name = single_mapping_from_value(parameter_definition_name)

    def _create_entity_class_name_readers(self, pivoted_columns, pivoted_data, data_header):
        if self._entity_class_name.returns_value():
            return self._entity_class_name.create_getter_function(pivoted_columns, pivoted_data, data_header)
        return None, None, None

    def _create_parameter_definition_name_readers(self, pivoted_columns, pivoted_data, data_header):
        if self._parameter_definition_name.returns_value():
            return self._parameter_definition_name.create_getter_function(pivoted_columns, pivoted_data, data_header)
        return None, None, None


class FeatureMapping(FeatureMappingMixin, ItemMappingBase):
    """
        Feature mapping.

        specification:

            FeatureMapping {
                map_type: 'Feature'
                entity_class_name: str | Mapping
                parameter_definition_name: str | Mapping
            }
    """

    MAP_TYPE = "Feature"

    def __init__(
        self, entity_class_name=None, parameter_definition_name=None, skip_columns=None, read_start_row=0,
    ):
        super().__init__(
            skip_columns,
            read_start_row,
            entity_class_name=entity_class_name,
            parameter_definition_name=parameter_definition_name,
        )

    def create_mapping_readers(self, num_columns, pivoted_data, data_header):
        pivoted_columns = self.pivoted_columns(data_header, num_columns)
        ent_cls_name_getter, ent_cls_name_length, ent_cls_name_reads = self._create_entity_class_name_readers(
            pivoted_columns, pivoted_data, data_header
        )
        param_def_getter, param_def_length, param_def_reads = self._create_parameter_definition_name_readers(
            pivoted_columns, pivoted_data, data_header
        )
        functions = [ent_cls_name_getter, param_def_getter]
        output_lengths = [ent_cls_name_length, param_def_length]
        reads_data = [ent_cls_name_reads, param_def_reads]
        readers = [("features",) + create_final_getter_function(functions, output_lengths, reads_data)]
        return readers

    @classmethod
    def from_dict(cls, map_dict):
        if not isinstance(map_dict, dict):
            raise TypeError(f"map_dict must be a dict, instead got {type(map_dict).__name__}")
        entity_class_name = map_dict.get("entity_class_name", None)
        parameter_definition_name = map_dict.get("parameter_definition_name", None)
        skip_columns = map_dict.get("skip_columns", [])
        read_start_row = map_dict.get("read_start_row", 0)
        return FeatureMapping(entity_class_name, parameter_definition_name, skip_columns, read_start_row)

    def to_dict(self):
        map_dict = super().to_dict()
        map_dict["entity_class_name"] = self._entity_class_name.to_dict()
        map_dict["parameter_definition_name"] = self._parameter_definition_name.to_dict()
        return map_dict

    @classmethod
    def from_instance(cls, instance):
        """See base class."""
        if isinstance(instance, FeatureMapping):
            return FeatureMapping(
                instance._entity_class_name,
                instance._parameter_definition_name,
                instance.skip_columns,
                instance.read_start_row,
            )
        return FeatureMapping(skip_columns=instance.skip_columns, read_start_row=instance.read_start_row)


class ToolFeatureMapping(FeatureMappingMixin, ToolMapping):
    """
        Tool feature mapping.

        specification:

            ToolFeatureMapping {
                map_type: 'ToolFeature'
                tool_name: str | Mapping
                entity_class_name: str | Mapping
                parameter_definition_name: str | Mapping
                required: str | Mapping
            }
    """

    MAP_TYPE = "ToolFeature"

    def __init__(
        self,
        name=None,
        entity_class_name=None,
        parameter_definition_name=None,
        required=None,
        skip_columns=None,
        read_start_row=0,
    ):
        super().__init__(
            name,
            skip_columns,
            read_start_row,
            entity_class_name=entity_class_name,
            parameter_definition_name=parameter_definition_name,
        )
        if required is not None:
            self._required = single_mapping_from_value(required)
        else:
            self._required = ConstantMapping("false")

    def component_names(self):
        return super().component_names() + ["Tool feature required flags"]

    def component_mappings(self):
        return super().component_mappings() + [self.required]

    def _optional_component_names(self):
        return super()._optional_component_names() + ["Tool feature required flags"]

    def set_component_by_name(self, name, mapping):
        if name == "Tool feature required flags":
            self.required = mapping
            return True
        return super().set_component_by_name(name, mapping)

    def _component_issues(self, name, mapping):
        if name == "Tool feature required flags" and isinstance(mapping, ConstantMapping) and mapping.is_valid():
            try:
                strtobool(mapping.reference)
            except ValueError as err:
                return str(err)
        return super()._component_issues(name, mapping)

    @property
    def required(self):
        return self._required

    @required.setter
    def required(self, required):
        self._required = single_mapping_from_value(required)

    def _create_required_readers(self, pivoted_columns, pivoted_data, data_header):
        if self._required.returns_value():
            getter, num, reads_data = self._required.create_getter_function(pivoted_columns, pivoted_data, data_header)
            bool_getter = lambda x: bool(strtobool(str(getter(x))))  # pylint: disable=not-callable
            return bool_getter, num, reads_data
        return (None, None, None)

    def create_mapping_readers(self, num_columns, pivoted_data, data_header):
        pivoted_columns = self.pivoted_columns(data_header, num_columns)
        tool_name_getter, tool_name_length, tool_name_reads = self._create_getters(
            pivoted_columns, pivoted_data, data_header
        )["item_name"]
        ent_cls_name_getter, ent_cls_name_length, ent_cls_name_reads = self._create_entity_class_name_readers(
            pivoted_columns, pivoted_data, data_header
        )
        param_def_getter, param_def_length, param_def_reads = self._create_parameter_definition_name_readers(
            pivoted_columns, pivoted_data, data_header
        )
        req_getter, req_length, req_reads = self._create_required_readers(pivoted_columns, pivoted_data, data_header)
        functions = [tool_name_getter, ent_cls_name_getter, param_def_getter, req_getter]
        output_lengths = [tool_name_length, ent_cls_name_length, param_def_length, req_length]
        reads_data = [tool_name_reads, ent_cls_name_reads, param_def_reads, req_reads]
        readers = [("tool_features",) + create_final_getter_function(functions, output_lengths, reads_data)]
        return readers

    @classmethod
    def from_dict(cls, map_dict):
        if not isinstance(map_dict, dict):
            raise TypeError(f"map_dict must be a dict, instead got {type(map_dict).__name__}")
        name = map_dict.get("name", None)
        entity_class_name = map_dict.get("entity_class_name", None)
        parameter_definition_name = map_dict.get("parameter_definition_name", None)
        required = map_dict.get("required", None)
        skip_columns = map_dict.get("skip_columns", [])
        read_start_row = map_dict.get("read_start_row", 0)
        return ToolFeatureMapping(
            name, entity_class_name, parameter_definition_name, required, skip_columns, read_start_row
        )

    def to_dict(self):
        map_dict = super().to_dict()
        map_dict["entity_class_name"] = self._entity_class_name.to_dict()
        map_dict["parameter_definition_name"] = self._parameter_definition_name.to_dict()
        map_dict["required"] = self._required.to_dict()
        return map_dict

    @classmethod
    def from_instance(cls, instance):
        """See base class."""
        if isinstance(instance, ToolFeatureMapping):
            return ToolFeatureMapping(
                instance._name,
                instance._entity_class_name,
                instance._parameter_definition_name,
                instance._required,
                instance.skip_columns,
                instance.read_start_row,
            )
        if isinstance(instance, FeatureMapping):
            return ToolFeatureMapping(
                None,
                instance._entity_class_name,
                instance._parameter_definition_name,
                instance._required,
                instance.skip_columns,
                instance.read_start_row,
            )
        if isinstance(instance, NamedItemMapping):
            return ToolFeatureMapping(
                instance._name, skip_columns=instance.skip_columns, read_start_row=instance.read_start_row
            )
        return ToolFeatureMapping(skip_columns=instance.skip_columns, read_start_row=instance.read_start_row)


class ToolFeatureMethodMapping(FeatureMappingMixin, ToolMapping):
    """
        Tool feature method mapping.

        specification:

            ToolFeatureMethodMapping {
                map_type: 'ToolFeatureMethod'
                tool_name: str | Mapping
                entity_class_name: str | Mapping
                parameter_definition_name: str | Mapping
                method: str | Mapping
            }
    """

    MAP_TYPE = "ToolFeatureMethod"

    def __init__(
        self,
        name=None,
        entity_class_name=None,
        parameter_definition_name=None,
        method=None,
        skip_columns=None,
        read_start_row=0,
    ):
        super().__init__(
            name,
            skip_columns,
            read_start_row,
            entity_class_name=entity_class_name,
            parameter_definition_name=parameter_definition_name,
        )
        self._method = single_mapping_from_value(method)

    def component_names(self):
        return super().component_names() + ["Tool feature methods"]

    def component_mappings(self):
        return super().component_mappings() + [self.method]

    def set_component_by_name(self, name, mapping):
        if name == "Tool feature methods":
            self.method = mapping
            return True
        return super().set_component_by_name(name, mapping)

    @property
    def method(self):
        return self._method

    @method.setter
    def method(self, method):
        self._method = single_mapping_from_value(method)

    def _create_method_readers(self, pivoted_columns, pivoted_data, data_header):
        if self._method.returns_value():
            return self._method.create_getter_function(pivoted_columns, pivoted_data, data_header)
        return (None, None, None)

    def create_mapping_readers(self, num_columns, pivoted_data, data_header):
        pivoted_columns = self.pivoted_columns(data_header, num_columns)
        tool_name_getter, tool_name_length, tool_name_reads = self._create_getters(
            pivoted_columns, pivoted_data, data_header
        )["item_name"]
        ent_cls_name_getter, ent_cls_name_length, ent_cls_name_reads = self._create_entity_class_name_readers(
            pivoted_columns, pivoted_data, data_header
        )
        param_def_getter, param_def_length, param_def_reads = self._create_parameter_definition_name_readers(
            pivoted_columns, pivoted_data, data_header
        )
        meth_getter, meth_length, meth_reads = self._create_method_readers(pivoted_columns, pivoted_data, data_header)
        functions = [tool_name_getter, ent_cls_name_getter, param_def_getter, meth_getter]
        output_lengths = [tool_name_length, ent_cls_name_length, param_def_length, meth_length]
        reads_data = [tool_name_reads, ent_cls_name_reads, param_def_reads, meth_reads]
        readers = [("tool_feature_methods",) + create_final_getter_function(functions, output_lengths, reads_data)]
        return readers

    @classmethod
    def from_dict(cls, map_dict):
        if not isinstance(map_dict, dict):
            raise TypeError(f"map_dict must be a dict, instead got {type(map_dict).__name__}")
        name = map_dict.get("name", None)
        entity_class_name = map_dict.get("entity_class_name", None)
        parameter_definition_name = map_dict.get("parameter_definition_name", None)
        method = map_dict.get("method", None)
        skip_columns = map_dict.get("skip_columns", [])
        read_start_row = map_dict.get("read_start_row", 0)
        return ToolFeatureMethodMapping(
            name, entity_class_name, parameter_definition_name, method, skip_columns, read_start_row
        )

    def to_dict(self):
        map_dict = super().to_dict()
        map_dict["name"] = self._name.to_dict()
        map_dict["entity_class_name"] = self._entity_class_name.to_dict()
        map_dict["parameter_definition_name"] = self._parameter_definition_name.to_dict()
        map_dict["method"] = self._method.to_dict()
        return map_dict

    @classmethod
    def from_instance(cls, instance):
        """See base class."""
        if isinstance(instance, ToolFeatureMethodMapping):
            return ToolFeatureMethodMapping(
                instance._name,
                instance._entity_class_name,
                instance._parameter_definition_name,
                instance._method,
                instance.skip_columns,
                instance.read_start_row,
            )
        if isinstance(instance, ToolFeatureMapping):
            return ToolFeatureMethodMapping(
                instance._name,
                instance._entity_class_name,
                instance._parameter_definition_name,
                None,
                instance.skip_columns,
                instance.read_start_row,
            )
        if isinstance(instance, FeatureMapping):
            return ToolFeatureMethodMapping(
                None,
                instance._entity_class_name,
                instance._parameter_definition_name,
                None,
                instance.skip_columns,
                instance.read_start_row,
            )
        if isinstance(instance, NamedItemMapping):
            return ToolFeatureMethodMapping(
                instance._name, skip_columns=instance.skip_columns, read_start_row=instance.read_start_row
            )
        return ToolFeatureMethodMapping(skip_columns=instance.skip_columns, read_start_row=instance.read_start_row)


def item_mapping_from_dict(map_dict):
    """Creates ItemMappingBase object from a dict"""
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
        ToolMapping,
        FeatureMapping,
        ToolFeatureMapping,
        ToolFeatureMethodMapping,
    )
    mapping_classes = {c.MAP_TYPE: c for c in mapping_classes}
    mapping_class = mapping_classes.get(map_type)
    if mapping_class is not None:
        return mapping_class.from_dict(map_dict)
    raise ValueError(f"""invalid "map_type" value, expected any of {", ".join(mapping_classes)}, got {map_type}""")


def _parameter_readers(object_or_relationship, parameters_mapping, class_getters, entity_getters, component_readers):
    """
    Creates a list of parameter readers.

    Args:
        object_or_relationship (str): either "object" or "relationship"
        parameters_mapping (ParameterMappingBase): mapping for parameters
        class_getters (tuple): a tuple consisting of entity class name getters
        entity_getters (tuple): a tuple consisting of entity name getters
        component_readers (dict): a mapping from reader name to reader tuple

    Returns:
        list: readers for parameter definitions and (optionally) values, or empty list if not applicable
    """
    if isinstance(parameters_mapping, ParameterValueMapping):
        parameter_lists = ([], [], [])
        multiple_append(parameter_lists, class_getters)
        multiple_append(parameter_lists, component_readers["parameter_name"])
        parameter_value_lists = ([], [], [])
        multiple_append(parameter_value_lists, class_getters)
        multiple_append(parameter_value_lists, entity_getters)
        multiple_append(parameter_value_lists, component_readers["parameter_name"])
        multiple_append(parameter_value_lists, component_readers["parameter_value"])
        alt_getters = component_readers.get("alternative_name")
        if alt_getters:
            multiple_append(parameter_value_lists, alt_getters)
        return [
            (object_or_relationship + "_parameters",) + create_final_getter_function(*parameter_lists),
            (object_or_relationship + "_parameter_values",) + create_final_getter_function(*parameter_value_lists),
        ]
    if isinstance(parameters_mapping, ParameterDefinitionMapping):
        parameter_lists = ([], [], [])
        multiple_append(parameter_lists, class_getters)
        multiple_append(parameter_lists, component_readers["parameter_name"])
        default_value_getters = component_readers.get("parameter_default_value")
        if default_value_getters:
            multiple_append(parameter_lists, default_value_getters)
        value_list_name_getters = component_readers.get("parameter_value_list_name")
        if value_list_name_getters:
            multiple_append(parameter_lists, value_list_name_getters)
        return [(object_or_relationship + "_parameters",) + create_final_getter_function(*parameter_lists)]
    return []
