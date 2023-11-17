######################################################################################################################
# Copyright (C) 2017-2022 Spine project consortium
# This file is part of Spine Database API.
# Spine Database API is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser
# General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General
# Public License for more details. You should have received a copy of the GNU Lesser General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
######################################################################################################################

from operator import itemgetter

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


class CommitItem(MappedItemBase):
    fields = {
        "comment": ("str", "A comment describing the commit."),
        "date": {"datetime", "Date and time of the commit."},
        "user": {"str", "Username of the committer."},
    }
    _unique_keys = (("date",),)

    def commit(self, commit_id):
        raise RuntimeError("Commits are created automatically when session is committed.")


class EntityClassItem(MappedItemBase):
    fields = {
        "name": ("str", "The class name."),
        "dimension_name_list": ("tuple, optional", "The dimension names for a multi-dimensional class."),
        "description": ("str, optional", "The class description."),
        "display_icon": ("int, optional", "An integer representing an icon within your application."),
        "display_order": ("int, optional", "Not in use at the moment."),
        "hidden": ("bool, optional", "Not in use at the moment."),
    }
    _defaults = {"description": None, "display_icon": None, "display_order": 99, "hidden": False}
    _unique_keys = (("name",),)
    _references = {"dimension_id_list": ("entity_class", "id")}
    _external_fields = {"dimension_name_list": ("dimension_id_list", "name")}
    _alt_references = {("dimension_name_list",): ("entity_class", ("name",))}
    _internal_fields = {"dimension_id_list": (("dimension_name_list",), "id")}

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
        "class_name": ("str", "The entity class name."),
        "name": ("str", "The entity name."),
        "element_name_list": ("tuple", "The element names if the entity is multi-dimensional."),
        "byname": (
            "tuple",
            "A tuple with the entity name as single element if the entity is zero-dimensional, "
            "or the element names if it is multi-dimensional.",
        ),
        "description": ("str, optional", "The entity description."),
    }
    _defaults = {"description": None}
    _unique_keys = (("class_name", "name"), ("class_name", "byname"))
    _references = {"class_id": ("entity_class", "id"), "element_id_list": ("entity", "id")}
    _external_fields = {
        "class_name": ("class_id", "name"),
        "dimension_id_list": ("class_id", "dimension_id_list"),
        "dimension_name_list": ("class_id", "dimension_name_list"),
        "superclass_id": ("class_id", "superclass_id"),
        "superclass_name": ("class_id", "superclass_name"),
        "element_name_list": ("element_id_list", "name"),
    }
    _alt_references = {
        ("class_name",): ("entity_class", ("name",)),
        ("dimension_name_list", "element_name_list"): ("entity", ("class_name", "name")),
    }
    _internal_fields = {
        "class_id": (("class_name",), "id"),
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
        yield from super().unique_values_for_item(item, skip_keys=skip_keys)
        key = ("class_name", "name")
        if key not in skip_keys:
            value = tuple(item.get(k) for k in ("superclass_name", "name"))
            if None not in value:
                yield key, value

    def _element_name_list_iter(self, entity):
        element_id_list = entity["element_id_list"]
        if not element_id_list:
            yield entity["name"]
        else:
            for el_id in element_id_list:
                element = self._get_ref("entity", {"id", el_id})
                yield from self._element_name_list_iter(element)

    def __getitem__(self, key):
        if key == "root_element_name_list":
            return tuple(self._element_name_list_iter(self))
        if key == "byname":
            return self["element_name_list"] or (self["name"],)
        return super().__getitem__(key)

    def polish(self):
        error = super().polish()
        if error:
            return error
        dim_name_lst, el_name_lst = dict.get(self, "dimension_name_list"), dict.get(self, "element_name_list")
        if dim_name_lst and el_name_lst:
            for dim_name, el_name in zip(dim_name_lst, el_name_lst):
                if not self._db_map.get_item("entity", class_name=dim_name, name=el_name, fetch=False):
                    return f"element '{el_name}' is not an instance of class '{dim_name}'"
        if self.get("name") is not None:
            return
        base_name = name_from_elements(self["element_name_list"])
        name = base_name
        index = 1
        while any(
            self._db_map.get_item("entity", class_name=self[k], name=name) for k in ("class_name", "superclass_name")
        ):
            name = f"{base_name}_{index}"
            index += 1
        self["name"] = name


class EntityGroupItem(MappedItemBase):
    fields = {
        "class_name": ("str", "The entity class name."),
        "group_name": ("str", "The group entity name."),
        "member_name": ("str", "The member entity name."),
    }
    _unique_keys = (("class_name", "group_name", "member_name"),)
    _references = {
        "entity_class_id": ("entity_class", "id"),
        "entity_id": ("entity", "id"),
        "member_id": ("entity", "id"),
    }
    _external_fields = {
        "class_name": ("entity_class_id", "name"),
        "dimension_id_list": ("entity_class_id", "dimension_id_list"),
        "group_name": ("entity_id", "name"),
        "member_name": ("member_id", "name"),
    }
    _alt_references = {
        ("class_name",): ("entity_class", ("name",)),
        ("class_name", "group_name"): ("entity", ("class_name", "name")),
        ("class_name", "member_name"): ("entity", ("class_name", "name")),
    }
    _internal_fields = {
        "entity_class_id": (("class_name",), "id"),
        "entity_id": (("class_name", "group_name"), "id"),
        "member_id": (("class_name", "member_name"), "id"),
    }

    def __getitem__(self, key):
        if key == "class_id":
            return self["entity_class_id"]
        if key == "group_id":
            return self["entity_id"]
        return super().__getitem__(key)


class EntityAlternativeItem(MappedItemBase):
    fields = {
        "entity_class_name": ("str", "The entity class name."),
        "entity_byname": (
            "tuple",
            "A tuple with the entity name as single element if the entity is zero-dimensional, "
            "or the element names if it is multi-dimensional.",
        ),
        "alternative_name": ("str", "The alternative name."),
        "active": ("bool, optional", "Whether the entity is active in the alternative - defaults to True."),
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
        "entity_byname": ("entity_id", "byname"),
        "element_id_list": ("entity_id", "element_id_list"),
        "element_name_list": ("entity_id", "element_name_list"),
        "alternative_name": ("alternative_id", "name"),
    }
    _alt_references = {
        ("entity_class_name", "entity_byname"): ("entity", ("class_name", "byname")),
        ("alternative_name",): ("alternative", ("name",)),
    }
    _internal_fields = {
        "entity_id": (("entity_class_name", "entity_byname"), "id"),
        "alternative_id": (("alternative_name",), "id"),
    }


class ParsedValueBase(MappedItemBase):
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
        other = other.copy()
        if self._value_key in other and self._type_key in other:
            try:
                other_parsed_value = from_database(other[self._value_key], other[self._type_key])
                if self.parsed_value != other_parsed_value:
                    return True
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
        "entity_class_name": ("str", "The entity class name."),
        "name": ("str", "The parameter name."),
        "default_value": ("any, optional", "The default value."),
        "default_type": ("str, optional", "The default value type."),
        "parameter_value_list_name": ("str, optional", "The parameter value list name if any."),
        "description": ("str, optional", "The parameter description."),
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
            return self._get_ref("parameter_value_list", {"id": self["parameter_value_list_id"]}, strong=False).get(
                "name"
            )
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
        "entity_class_name": ("str", "The entity class name."),
        "parameter_definition_name": ("str", "The parameter name."),
        "entity_byname": (
            "tuple",
            "A tuple with the entity name as single element if the entity is zero-dimensional, "
            "or the element names if the entity is multi-dimensional.",
        ),
        "value": ("any", "The value."),
        "type": ("str", "The value type."),
        "alternative_name": ("str, optional", "The alternative name - defaults to 'Base'."),
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
        "entity_byname": ("entity_id", "byname"),
        "element_id_list": ("entity_id", "element_id_list"),
        "element_name_list": ("entity_id", "element_name_list"),
        "alternative_name": ("alternative_id", "name"),
    }
    _alt_references = {
        ("entity_class_name",): ("entity_class", ("name",)),
        ("entity_class_name", "parameter_definition_name"): ("parameter_definition", ("entity_class_name", "name")),
        ("entity_class_name", "entity_byname"): ("entity", ("class_name", "byname")),
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
    fields = {"name": ("str", "The parameter value list name.")}
    _unique_keys = (("name",),)


class ListValueItem(ParsedValueBase):
    fields = {
        "parameter_value_list_name": ("str", "The parameter value list name."),
        "value": ("any", "The value."),
        "type": ("str", "The value type."),
        "index": ("int, optional", "The value index."),
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
        "name": ("str", "The alternative name."),
        "description": ("str, optional", "The alternative description."),
    }
    _defaults = {"description": None}
    _unique_keys = (("name",),)


class ScenarioItem(MappedItemBase):
    fields = {
        "name": ("str", "The scenario name."),
        "description": ("str, optional", "The scenario description."),
        "active": ("bool, optional", "Not in use at the moment."),
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
        "scenario_name": ("str", "The scenario name."),
        "alternative_name": ("str", "The alternative name."),
        "rank": ("int", "The rank - the higher has precedence."),
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
    fields = {"name": ("str", "The metadata entry name."), "value": ("str", "The metadata entry value.")}
    _unique_keys = (("name", "value"),)


class EntityMetadataItem(MappedItemBase):
    fields = {
        "entity_name": ("str", "The entity name."),
        "metadata_name": ("str", "The metadata entry name."),
        "metadata_value": ("str", "The metadata entry value."),
    }
    _unique_keys = (("entity_name", "metadata_name", "metadata_value"),)
    _references = {"entity_id": ("entity", "id"), "metadata_id": ("metadata", "id")}
    _external_fields = {
        "entity_name": ("entity_id", "name"),
        "metadata_name": ("metadata_id", "name"),
        "metadata_value": ("metadata_id", "value"),
    }
    _alt_references = {
        ("entity_class_name", "entity_byname"): ("entity", ("class_name", "byname")),
        ("metadata_name", "metadata_value"): ("metadata", ("name", "value")),
    }
    _internal_fields = {
        "entity_id": (("entity_class_name", "entity_byname"), "id"),
        "metadata_id": (("metadata_name", "metadata_value"), "id"),
    }


class ParameterValueMetadataItem(MappedItemBase):
    fields = {
        "parameter_definition_name": ("str", "The parameter name."),
        "entity_byname": (
            "tuple",
            "A tuple with the entity name as single element if the entity is zero-dimensional, "
            "or the element names if it is multi-dimensional.",
        ),
        "alternative_name": ("str", "The alternative name."),
        "metadata_name": ("str", "The metadata entry name."),
        "metadata_value": ("str", "The metadata entry value."),
    }
    _unique_keys = (
        ("parameter_definition_name", "entity_byname", "alternative_name", "metadata_name", "metadata_value"),
    )
    _references = {"parameter_value_id": ("parameter_value", "id"), "metadata_id": ("metadata", "id")}
    _external_fields = {
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
    fields = {"superclass_name": ("str", "The superclass name."), "subclass_name": ("str", "The subclass name.")}
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
        return self._db_map.get_items("entity", class_id=self["subclass_id"])

    def check_mutability(self):
        if self._subclass_entities():
            return "can't set or modify the superclass for a class that already has entities"
        return super().check_mutability()
