######################################################################################################################
# Copyright (C) 2017-2022 Spine project consortium
# Copyright Spine Database API contributors
# This file is part of Spine Database API.
# Spine Database API is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser
# General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

from operator import itemgetter
import time
from .helpers import name_from_elements
from .parameter_value import to_database, from_database, ParameterValueFormatError
from .db_mapping_base import MappedItemBase


def item_factory(item_type):
    return {
        "commit": CommitItem,
        "entity_class": EntityClassItem,
        "superclass_subclass": SuperclassSubclassItem,
        "entity": EntityItem,
        "entity_alternative": EntityAlternativeItem,
        "entity_group": EntityGroupItem,
        "parameter_definition": ParameterDefinitionItem,
        "parameter_value": ParameterValueItem,
        "parameter_value_list": ParameterValueListItem,
        "list_value": ListValueItem,
        "alternative": AlternativeItem,
        "scenario": ScenarioItem,
        "scenario_alternative": ScenarioAlternativeItem,
        "metadata": MetadataItem,
        "entity_metadata": EntityMetadataItem,
        "parameter_value_metadata": ParameterValueMetadataItem,
    }.get(item_type, MappedItemBase)


_ENTITY_BYNAME_VALUE = "A tuple with the entity name as single element if the entity is zero-dimensional, or the element names if the entity is multi-dimensional."


class CommitItem(MappedItemBase):
    fields = {
        "comment": {"type": str, "value": "A comment describing the commit."},
        "date": {"type": str, "value": "Date and time of the commit in ISO 8601 format."},
        "user": {"type": str, "value": "Username of the committer."},
    }
    _unique_keys = (("date",),)
    is_protected = True

    def commit(self, commit_id):
        raise RuntimeError("Commits are created automatically when session is committed.")


class EntityClassItem(MappedItemBase):
    fields = {
        "name": {"type": str, "value": "The class name."},
        "dimension_name_list": {
            "type": tuple,
            "value": "The dimension names for a multi-dimensional class.",
            "optional": True,
        },
        "description": {"type": str, "value": "The class description.", "optional": True},
        "display_icon": {
            "type": int,
            "value": "An integer representing an icon within your application.",
            "optional": True,
        },
        "display_order": {"type": int, "value": "Not in use at the moment.", "optional": True},
        "hidden": {"type": int, "value": "Not in use at the moment.", "optional": True},
        "active_by_default": {
            "type": bool,
            "value": "Default activity for the entity alternatives of the class.",
            "optional": True,
        },
    }
    _defaults = {"description": None, "display_icon": None, "display_order": 99, "hidden": False}
    _unique_keys = (("name",),)
    _references = {"dimension_id_list": ("entity_class", "id")}
    _external_fields = {"dimension_name_list": ("dimension_id_list", "name")}
    _alt_references = {("dimension_name_list",): ("entity_class", ("name",))}
    _internal_fields = {"dimension_id_list": (("dimension_name_list",), "id")}
    _private_fields = {"dimension_count"}

    def __init__(self, *args, **kwargs):
        dimension_id_list = kwargs.get("dimension_id_list")
        if dimension_id_list is None:
            dimension_id_list = ()
        if isinstance(dimension_id_list, str):
            dimension_id_list = (int(id_) for id_ in dimension_id_list.split(","))
        kwargs["dimension_id_list"] = tuple(dimension_id_list)
        super().__init__(*args, **kwargs)

    def __getitem__(self, key):
        if key in ("superclass_id", "superclass_name"):
            return self._get_ref("superclass_subclass", {"subclass_id": self["id"]}, strong=False).get(key)
        return super().__getitem__(key)

    def polish(self):
        error = super().polish()
        if error:
            return error
        if "active_by_default" not in self:
            self["active_by_default"] = bool(dict.get(self, "dimension_id_list"))

    def merge(self, other):
        dimension_id_list = other.pop("dimension_id_list", None)
        error = (
            "can't modify dimensions of an entity class"
            if dimension_id_list is not None and dimension_id_list != self["dimension_id_list"]
            else ""
        )
        merged, super_error = super().merge(other)
        return merged, " and ".join([x for x in (super_error, error) if x])

    def commit(self, _commit_id):
        super().commit(None)


class EntityItem(MappedItemBase):
    fields = {
        "entity_class_name": {"type": str, "value": "The entity class name."},
        "name": {"type": str, "value": "The entity name."},
        "element_name_list": {"type": tuple, "value": "The element names if the entity is multi-dimensional."},
        "entity_byname": {
            "type": tuple,
            "value": "A tuple with the entity name as single element if the entity is zero-dimensional,"
            "or the element names if it is multi-dimensional.",
        },
        "description": {"type": str, "value": "The entity description.", "optional": True},
    }

    _defaults = {"description": None}
    _unique_keys = (("entity_class_name", "name"), ("entity_class_name", "entity_byname"))
    _references = {"class_id": ("entity_class", "id"), "element_id_list": ("entity", "id")}
    _external_fields = {
        "entity_class_name": ("class_id", "name"),
        "dimension_id_list": ("class_id", "dimension_id_list"),
        "dimension_name_list": ("class_id", "dimension_name_list"),
        "superclass_id": ("class_id", "superclass_id"),
        "superclass_name": ("class_id", "superclass_name"),
        "element_name_list": ("element_id_list", "name"),
        "element_byname_list": ("element_id_list", "entity_byname"),
    }
    _alt_references = {
        ("entity_class_name",): ("entity_class", ("name",)),
        ("dimension_name_list", "element_name_list"): ("entity", ("entity_class_name", "name")),
    }
    _internal_fields = {
        "class_id": (("entity_class_name",), "id"),
        "element_id_list": (("dimension_name_list", "element_name_list"), "id"),
    }

    def __init__(self, *args, **kwargs):
        element_id_list = kwargs.get("element_id_list")
        if element_id_list is None:
            element_id_list = ()
        if isinstance(element_id_list, str):
            element_id_list = (int(id_) for id_ in element_id_list.split(","))
        kwargs["element_id_list"] = tuple(element_id_list)
        super().__init__(*args, **kwargs)

    @classmethod
    def unique_values_for_item(cls, item, skip_keys=()):
        """Overriden to also yield unique values for the superclass."""
        for key, value in super().unique_values_for_item(item, skip_keys=skip_keys):
            yield key, value
            sc_value = tuple(item.get("superclass_name" if k == "entity_class_name" else k) for k in key)
            if None not in sc_value:
                yield (key, sc_value)

    def _byname_iter(self, entity):
        element_id_list = entity["element_id_list"]
        if not element_id_list:
            yield entity["name"]
        else:
            for el_id in element_id_list:
                element = self._get_ref("entity", {"id": el_id})
                yield from self._byname_iter(element)

    def __getitem__(self, key):
        if key == "entity_byname":
            return tuple(self._byname_iter(self))
        return super().__getitem__(key)

    def resolve_internal_fields(self, skip_keys=()):
        """Overriden to translate byname into element name list."""
        error = super().resolve_internal_fields(skip_keys=skip_keys)
        if error:
            return error
        byname = dict.pop(self, "entity_byname", None)
        if byname is None:
            return
        dim_count = len(self["dimension_id_list"])
        if not dim_count:
            self["name"] = byname[0]
            return
        byname_remainder = list(byname)
        element_name_list, _ = self._element_name_list_recursive(self["entity_class_name"], byname_remainder)
        if len(element_name_list) < dim_count:
            return f"too few elements given for entity ({byname})"
        if byname_remainder:
            return f"too many elements given for entity ({byname})"
        self["element_name_list"] = element_name_list
        return self._do_resolve_internal_field("element_id_list")

    def _element_name_list_recursive(self, class_name, entity_byname):
        """Returns the element name list corresponding to given class and byname.
        If the class is multi-dimensional then recurses for each dimension.
        If the class is a superclass then it tries for each subclass until finding something useful.
        """
        class_names = [
            x["subclass_name"] for x in self._db_map.get_items("superclass_subclass", superclass_name=class_name)
        ] or [class_name]
        for class_name_ in class_names:
            dimension_name_list = self._db_map.get_item("entity_class", name=class_name_).get("dimension_name_list")
            if not dimension_name_list:
                continue
            byname_backup = list(entity_byname)
            element_name_list = tuple(
                self._db_map.get_item(
                    "entity",
                    **dict(
                        zip(
                            ("entity_byname", "entity_class_name"),
                            self._element_name_list_recursive(dim_name, entity_byname),
                        )
                    ),
                ).get("name")
                for dim_name in dimension_name_list
            )
            if None not in element_name_list:
                return element_name_list, class_name_
            entity_byname = byname_backup
        name = entity_byname.pop(0) if entity_byname else None
        return (name,), class_name

    def polish(self):
        error = super().polish()
        if error:
            return error
        dim_name_lst, el_name_lst = dict.get(self, "dimension_name_list"), dict.get(self, "element_name_list")
        if dim_name_lst and el_name_lst:
            for dim_name, el_name in zip(dim_name_lst, el_name_lst):
                if not self._db_map.get_item("entity", entity_class_name=dim_name, name=el_name, fetch=False):
                    return f"element '{el_name}' is not an instance of class '{dim_name}'"
        if self.get("name") is not None:
            return
        base_name = name_from_elements(self["element_name_list"])
        name = base_name
        index = 1
        while any(
            self._db_map.get_item("entity", entity_class_name=self[k], name=name)
            for k in ("entity_class_name", "superclass_name")
            if self[k] is not None
        ):
            name = f"{base_name}_{index}"
            index += 1
        self["name"] = name


class EntityGroupItem(MappedItemBase):
    fields = {
        "entity_class_name": {"type": str, "value": "The entity class name."},
        "group_name": {"type": str, "value": "The group entity name."},
        "member_name": {"type": str, "value": "The member entity name."},
    }
    _unique_keys = (("entity_class_name", "group_name", "member_name"),)
    _references = {
        "entity_class_id": ("entity_class", "id"),
        "entity_id": ("entity", "id"),
        "member_id": ("entity", "id"),
    }
    _external_fields = {
        "entity_class_name": ("entity_class_id", "name"),
        "dimension_id_list": ("entity_class_id", "dimension_id_list"),
        "group_name": ("entity_id", "name"),
        "member_name": ("member_id", "name"),
    }
    _alt_references = {
        ("entity_class_name",): ("entity_class", ("name",)),
        ("entity_class_name", "group_name"): ("entity", ("entity_class_name", "name")),
        ("entity_class_name", "member_name"): ("entity", ("entity_class_name", "name")),
    }
    _internal_fields = {
        "entity_class_id": (("entity_class_name",), "id"),
        "entity_id": (("entity_class_name", "group_name"), "id"),
        "member_id": (("entity_class_name", "member_name"), "id"),
    }

    def __getitem__(self, key):
        if key == "class_id":
            return self["entity_class_id"]
        if key == "group_id":
            return self["entity_id"]
        return super().__getitem__(key)

    def commit(self, _commit_id):
        super().commit(None)


class EntityAlternativeItem(MappedItemBase):
    fields = {
        "entity_class_name": {"type": str, "value": "The entity class name."},
        "entity_byname": {
            "type": tuple,
            "value": _ENTITY_BYNAME_VALUE,
        },
        "alternative_name": {"type": str, "value": "The alternative name."},
        "active": {
            "type": bool,
            "value": "Whether the entity is active in the alternative - defaults to True.",
            "optional": True,
        },
    }
    _defaults = {"active": True}
    _unique_keys = (("entity_class_name", "entity_byname", "alternative_name"),)
    _references = {
        "entity_id": ("entity", "id"),
        "entity_class_id": ("entity_class", "id"),
        "alternative_id": ("alternative", "id"),
    }
    _external_fields = {
        "entity_class_id": ("entity_id", "class_id"),
        "entity_class_name": ("entity_class_id", "name"),
        "dimension_id_list": ("entity_class_id", "dimension_id_list"),
        "dimension_name_list": ("entity_class_id", "dimension_name_list"),
        "entity_name": ("entity_id", "name"),
        "entity_byname": ("entity_id", "entity_byname"),
        "element_id_list": ("entity_id", "element_id_list"),
        "element_name_list": ("entity_id", "element_name_list"),
        "alternative_name": ("alternative_id", "name"),
    }
    _alt_references = {
        ("entity_class_name", "entity_byname"): ("entity", ("entity_class_name", "entity_byname")),
        ("alternative_name",): ("alternative", ("name",)),
    }
    _internal_fields = {
        "entity_id": (("entity_class_name", "entity_byname"), "id"),
        "alternative_id": (("alternative_name",), "id"),
    }


class ParsedValueBase(MappedItemBase):
    _private_fields = {"list_value_id"}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._parsed_value = None

    @property
    def parsed_value(self):
        if self._parsed_value is None:
            self._parsed_value = self._make_parsed_value()
        return self._parsed_value

    @property
    def _value_key(self):
        raise NotImplementedError()

    @property
    def _type_key(self):
        raise NotImplementedError()

    def _make_parsed_value(self):
        try:
            return from_database(self[self._value_key], self[self._type_key])
        except ParameterValueFormatError as error:
            return error

    def update(self, other):
        self._parsed_value = None
        super().update(other)

    def __getitem__(self, key):
        if key == "parsed_value":
            return self.parsed_value
        return super().__getitem__(key)

    def _something_to_update(self, other):
        if self._value_key in other and self._type_key in other:
            other_value_type = other[self._type_key]
            if self.type != other_value_type:
                return True
            other_value = other[self._value_key]
            if self.value != other_value:
                try:
                    other_parsed_value = from_database(other_value, other_value_type)
                    if self.parsed_value != other_parsed_value:
                        return True
                    other = other.copy()
                    _ = other.pop(self._value_key, None)
                    _ = other.pop(self._type_key, None)
                except ParameterValueFormatError:
                    pass
        return super()._something_to_update(other)


class ParameterItemBase(ParsedValueBase):
    @property
    def _value_key(self):
        raise NotImplementedError()

    @property
    def _type_key(self):
        raise NotImplementedError()

    def _value_not_in_list_error(self, parsed_value, list_name):
        raise NotImplementedError()

    @classmethod
    def ref_types(cls):
        return super().ref_types() | {"list_value"}

    @property
    def list_value_id(self):
        return self["list_value_id"]

    def resolve(self):
        d = super().resolve()
        list_value_id = d.get("list_value_id")
        if list_value_id is not None:
            d[self._value_key] = to_database(list_value_id)[0]
        return d

    def polish(self):
        self["list_value_id"] = None
        error = super().polish()
        if error:
            return error
        list_name = self["parameter_value_list_name"]
        if list_name is None:
            self["list_value_id"] = None
            return
        type_ = super().__getitem__(self._type_key)
        if type_ == "list_value_ref":
            return
        value = super().__getitem__(self._value_key)
        parsed_value = from_database(value, type_)
        if parsed_value is None:
            return
        list_value_id = self._db_map.get_item(
            "list_value", parameter_value_list_name=list_name, value=value, type=type_
        ).get("id")
        if list_value_id is None:
            return self._value_not_in_list_error(parsed_value, list_name)
        self["list_value_id"] = list_value_id
        self[self._type_key] = "list_value_ref"


class ParameterDefinitionItem(ParameterItemBase):
    fields = {
        "entity_class_name": {"type": str, "value": "The entity class name."},
        "name": {"type": str, "value": "The parameter name."},
        "default_value": {"type": bytes, "value": "The default value.", "optional": True},
        "default_type": {"type": str, "value": "The default value type.", "optional": True},
        "parameter_value_list_name": {
            "type": str,
            "value": "The parameter value list name if any.",
            "optional": True,
        },
        "description": {"type": str, "value": "The parameter description.", "optional": True},
    }
    _defaults = {"description": None, "default_value": None, "default_type": None, "parameter_value_list_id": None}
    _unique_keys = (("entity_class_name", "name"),)
    _references = {"entity_class_id": ("entity_class", "id")}
    _external_fields = {
        "entity_class_name": ("entity_class_id", "name"),
        "dimension_id_list": ("entity_class_id", "dimension_id_list"),
        "dimension_name_list": ("entity_class_id", "dimension_name_list"),
    }
    _alt_references = {
        ("entity_class_name",): ("entity_class", ("name",)),
        ("parameter_value_list_name",): ("parameter_value_list", ("name",)),
    }
    _internal_fields = {
        "entity_class_id": (("entity_class_name",), "id"),
        "parameter_value_list_id": (("parameter_value_list_name",), "id"),
    }

    @property
    def _value_key(self):
        return "default_value"

    @property
    def _type_key(self):
        return "default_type"

    def __getitem__(self, key):
        if key == "value_list_id":
            return super().__getitem__("parameter_value_list_id")
        if key == "parameter_value_list_id":
            return dict.get(self, key)
        if key == "parameter_value_list_name":
            return self._get_full_ref("parameter_value_list_id", "parameter_value_list", "id", strong=False).get("name")
        if key in ("default_value", "default_type"):
            list_value_id = self["list_value_id"]
            if list_value_id is not None:
                list_value_key = {"default_value": "value", "default_type": "type"}[key]
                return self._get_ref("list_value", {"id": list_value_id}, strong=False).get(list_value_key)
            return dict.get(self, key)
        return super().__getitem__(key)

    def merge(self, other):
        other_parameter_value_list_id = other.get("parameter_value_list_id")
        if (
            other_parameter_value_list_id is not None
            and other_parameter_value_list_id != self["parameter_value_list_id"]
            and any(
                x["parameter_definition_id"] == self["id"]
                for x in self._db_map.mapped_table("parameter_value").valid_values()
            )
        ):
            del other["parameter_value_list_id"]
            error = "can't modify the parameter value list of a parameter that already has values"
        else:
            error = ""
        merged, super_error = super().merge(other)
        return merged, " and ".join([x for x in (super_error, error) if x])

    def _value_not_in_list_error(self, parsed_value, list_name):
        return f"default value {parsed_value} of {self['name']} is not in {list_name}"


class ParameterValueItem(ParameterItemBase):
    fields = {
        "entity_class_name": {"type": str, "value": "The entity class name."},
        "parameter_definition_name": {"type": str, "value": "The parameter name."},
        "entity_byname": {
            "type": tuple,
            "value": _ENTITY_BYNAME_VALUE,
        },
        "value": {"type": bytes, "value": "The value."},
        "type": {"type": str, "value": "The value type.", "optional": True},
        "alternative_name": {"type": str, "value": "The alternative name - defaults to 'Base'.", "optional": True},
    }
    _unique_keys = (("entity_class_name", "parameter_definition_name", "entity_byname", "alternative_name"),)
    _references = {
        "entity_class_id": ("entity_class", "id"),
        "parameter_definition_id": ("parameter_definition", "id"),
        "entity_id": ("entity", "id"),
        "alternative_id": ("alternative", "id"),
    }
    _external_fields = {
        "entity_class_name": ("entity_class_id", "name"),
        "dimension_id_list": ("entity_class_id", "dimension_id_list"),
        "dimension_name_list": ("entity_class_id", "dimension_name_list"),
        "parameter_definition_name": ("parameter_definition_id", "name"),
        "parameter_value_list_id": ("parameter_definition_id", "parameter_value_list_id"),
        "parameter_value_list_name": ("parameter_definition_id", "parameter_value_list_name"),
        "entity_name": ("entity_id", "name"),
        "entity_byname": ("entity_id", "entity_byname"),
        "element_id_list": ("entity_id", "element_id_list"),
        "element_name_list": ("entity_id", "element_name_list"),
        "alternative_name": ("alternative_id", "name"),
    }
    _alt_references = {
        ("entity_class_name",): ("entity_class", ("name",)),
        ("entity_class_name", "parameter_definition_name"): ("parameter_definition", ("entity_class_name", "name")),
        ("entity_class_name", "entity_byname"): ("entity", ("entity_class_name", "entity_byname")),
        ("alternative_name",): ("alternative", ("name",)),
    }
    _internal_fields = {
        "entity_class_id": (("entity_class_name",), "id"),
        "parameter_definition_id": (("entity_class_name", "parameter_definition_name"), "id"),
        "entity_id": (("entity_class_name", "entity_byname"), "id"),
        "alternative_id": (("alternative_name",), "id"),
    }

    @property
    def _value_key(self):
        return "value"

    @property
    def _type_key(self):
        return "type"

    def __getitem__(self, key):
        if key == "parameter_id":
            return super().__getitem__("parameter_definition_id")
        if key == "parameter_name":
            return super().__getitem__("parameter_definition_name")
        if key in ("value", "type"):
            list_value_id = self["list_value_id"]
            if list_value_id:
                return self._get_ref("list_value", {"id": list_value_id}, strong=False).get(key)
        return super().__getitem__(key)

    def _value_not_in_list_error(self, parsed_value, list_name):
        return (
            f"value {parsed_value} of {self['parameter_definition_name']} for {self['entity_byname']} "
            f"is not in {list_name}"
        )


class ParameterValueListItem(MappedItemBase):
    fields = {"name": {"type": str, "value": "The parameter value list name."}}
    _unique_keys = (("name",),)


class ListValueItem(ParsedValueBase):
    fields = {
        "parameter_value_list_name": {"type": str, "value": "The parameter value list name."},
        "value": {"type": bytes, "value": "The value."},
        "type": {"type": str, "value": "The value type.", "optional": True},
        "index": {"type": int, "value": "The value index.", "optional": True},
    }
    _unique_keys = (("parameter_value_list_name", "value_and_type"), ("parameter_value_list_name", "index"))
    _references = {"parameter_value_list_id": ("parameter_value_list", "id")}
    _external_fields = {"parameter_value_list_name": ("parameter_value_list_id", "name")}
    _alt_references = {("parameter_value_list_name",): ("parameter_value_list", ("name",))}
    _internal_fields = {"parameter_value_list_id": (("parameter_value_list_name",), "id")}

    @property
    def _value_key(self):
        return "value"

    @property
    def _type_key(self):
        return "type"

    def __getitem__(self, key):
        if key == "value_and_type":
            return (self["value"], self["type"])
        return super().__getitem__(key)


class AlternativeItem(MappedItemBase):
    fields = {
        "name": {"type": str, "value": "The alternative name."},
        "description": {"type": str, "value": "The alternative description.", "optional": True},
    }
    _defaults = {"description": None}
    _unique_keys = (("name",),)


class ScenarioItem(MappedItemBase):
    fields = {
        "name": {"type": str, "value": "The scenario name."},
        "description": {"type": str, "value": "The scenario description.", "optional": True},
        "active": {"type": bool, "value": "Not in use at the moment.", "optional": True},
    }
    _defaults = {"active": False, "description": None}
    _unique_keys = (("name",),)

    def __getitem__(self, key):
        if key == "alternative_id_list":
            return [x["alternative_id"] for x in self.sorted_scenario_alternatives]
        if key == "alternative_name_list":
            return [x["alternative_name"] for x in self.sorted_scenario_alternatives]
        if key == "sorted_scenario_alternatives":
            self._db_map.do_fetch_all("scenario_alternative")
            return sorted(
                (
                    x
                    for x in self._db_map.mapped_table("scenario_alternative").valid_values()
                    if x["scenario_id"] == self["id"]
                ),
                key=itemgetter("rank"),
            )
        return super().__getitem__(key)


class ScenarioAlternativeItem(MappedItemBase):
    fields = {
        "scenario_name": {"type": str, "value": "The scenario name."},
        "alternative_name": {"type": str, "value": "The alternative name."},
        "rank": {"type": int, "value": "The rank - higher has precedence."},
    }
    _unique_keys = (("scenario_name", "alternative_name"), ("scenario_name", "rank"))
    _references = {"scenario_id": ("scenario", "id"), "alternative_id": ("alternative", "id")}
    _external_fields = {"scenario_name": ("scenario_id", "name"), "alternative_name": ("alternative_id", "name")}
    _alt_references = {("scenario_name",): ("scenario", ("name",)), ("alternative_name",): ("alternative", ("name",))}
    _internal_fields = {"scenario_id": (("scenario_name",), "id"), "alternative_id": (("alternative_name",), "id")}

    def __getitem__(self, key):
        # The 'before' is to be interpreted as, this scenario alternative goes *before* the before_alternative.
        # Since ranks go from 1 to the alternative count, the first alternative will have the second as the 'before',
        # the second will have the third, etc, and the last will have None.
        # Note that alternatives with higher ranks overwrite the values of those with lower ranks.
        if key == "before_alternative_name":
            return self._get_ref("alternative", {"id": self["before_alternative_id"]}, strong=False).get("name")
        if key == "before_alternative_id":
            scenario = self._get_ref("scenario", {"id": self["scenario_id"]}, strong=False)
            try:
                return scenario["alternative_id_list"][self["rank"]]
            except IndexError:
                return None
        return super().__getitem__(key)


class MetadataItem(MappedItemBase):
    fields = {
        "name": {"type": str, "value": "The metadata entry name."},
        "value": {"type": str, "value": "The metadata entry value."},
    }
    _unique_keys = (("name", "value"),)


class EntityMetadataItem(MappedItemBase):
    fields = {
        "entity_class_name": {"type": str, "value": "The entity class name."},
        "entity_byname": {"type": tuple, "value": _ENTITY_BYNAME_VALUE},
        "metadata_name": {"type": str, "value": "The metadata entry name."},
        "metadata_value": {"type": str, "value": "The metadata entry value."},
    }
    _unique_keys = (("entity_class_name", "entity_byname", "metadata_name", "metadata_value"),)
    _references = {
        "entity_id": ("entity", "id"),
        "metadata_id": ("metadata", "id"),
    }
    _external_fields = {
        "entity_class_name": ("entity_id", "entity_class_name"),
        "entity_byname": ("entity_id", "entity_byname"),
        "metadata_name": ("metadata_id", "name"),
        "metadata_value": ("metadata_id", "value"),
    }
    _alt_references = {
        (
            "entity_class_name",
            "entity_byname",
        ): ("entity", ("entity_class_name", "entity_byname")),
        ("metadata_name", "metadata_value"): ("metadata", ("name", "value")),
    }
    _internal_fields = {
        "entity_id": (("entity_class_name", "entity_byname"), "id"),
        "metadata_id": (("metadata_name", "metadata_value"), "id"),
    }


class ParameterValueMetadataItem(MappedItemBase):
    fields = {
        "entity_class_name": {"type": str, "value": "The entity class name."},
        "parameter_definition_name": {"type": str, "value": "The parameter name."},
        "entity_byname": {
            "type": tuple,
            "value": _ENTITY_BYNAME_VALUE,
        },
        "alternative_name": {"type": str, "value": "The alternative name."},
        "metadata_name": {"type": str, "value": "The metadata entry name."},
        "metadata_value": {"type": str, "value": "The metadata entry value."},
    }
    _unique_keys = (
        (
            "entity_class_name",
            "parameter_definition_name",
            "entity_byname",
            "alternative_name",
            "metadata_name",
            "metadata_value",
        ),
    )
    _references = {"parameter_value_id": ("parameter_value", "id"), "metadata_id": ("metadata", "id")}
    _external_fields = {
        "entity_class_name": ("parameter_value_id", "entity_class_name"),
        "parameter_definition_name": ("parameter_value_id", "parameter_definition_name"),
        "entity_byname": ("parameter_value_id", "entity_byname"),
        "alternative_name": ("parameter_value_id", "alternative_name"),
        "metadata_name": ("metadata_id", "name"),
        "metadata_value": ("metadata_id", "value"),
    }
    _alt_references = {
        ("entity_class_name", "parameter_definition_name", "entity_byname", "alternative_name"): (
            "parameter_value",
            ("entity_class_name", "parameter_definition_name", "entity_byname", "alternative_name"),
        ),
        ("metadata_name", "metadata_value"): ("metadata", ("name", "value")),
    }
    _internal_fields = {
        "parameter_value_id": (
            ("entity_class_name", "parameter_definition_name", "entity_byname", "alternative_name"),
            "id",
        ),
        "metadata_id": (("metadata_name", "metadata_value"), "id"),
    }


class SuperclassSubclassItem(MappedItemBase):
    fields = {
        "superclass_name": {"type": str, "value": "The superclass name."},
        "subclass_name": {"type": str, "value": "The subclass name."},
    }
    _unique_keys = (("subclass_name",),)
    _references = {"superclass_id": ("entity_class", "id"), "subclass_id": ("entity_class", "id")}
    _external_fields = {
        "superclass_name": ("superclass_id", "name"),
        "subclass_name": ("subclass_id", "name"),
    }
    _alt_references = {
        ("superclass_name",): ("entity_class", ("name",)),
        ("subclass_name",): ("entity_class", ("name",)),
    }
    _internal_fields = {"superclass_id": (("superclass_name",), "id"), "subclass_id": (("subclass_name",), "id")}

    def _subclass_entities(self):
        return self._db_map.get_items("entity", class_id=self["subclass_id"], fetch=False)

    def check_mutability(self):
        if self._subclass_entities():
            return "can't set or modify the superclass for a class that already has entities"
        return super().check_mutability()

    def commit(self, _commit_id):
        super().commit(None)
