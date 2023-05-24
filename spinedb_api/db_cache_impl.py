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
"""
DB cache implementation.

"""
import uuid
from operator import itemgetter
from .parameter_value import from_database, ParameterValueFormatError
from .db_cache_base import DBCacheBase, CacheItemBase


class DBCache(DBCacheBase):
    def __init__(self, db_map, chunk_size=None):
        """
        Args:
            db_map (DatabaseMapping)
        """
        super().__init__(chunk_size=chunk_size)
        self._db_map = db_map

    @staticmethod
    def _item_factory(item_type):
        return {
            "entity_class": EntityClassItem,
            "entity": EntityItem,
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
        }.get(item_type, CacheItemBase)

    def _query(self, item_type):
        if self._db_map.closed:
            return None
        sq_name = {
            "entity_class": "wide_entity_class_sq",
            "entity": "wide_entity_sq",
            "parameter_value_list": "parameter_value_list_sq",
            "list_value": "list_value_sq",
            "alternative": "alternative_sq",
            "scenario": "scenario_sq",
            "scenario_alternative": "scenario_alternative_sq",
            "entity_group": "entity_group_sq",
            "parameter_definition": "parameter_definition_sq",
            "parameter_value": "parameter_value_sq",
            "metadata": "metadata_sq",
            "entity_metadata": "entity_metadata_sq",
            "parameter_value_metadata": "parameter_value_metadata_sq",
            "commit": "commit_sq",
        }[item_type]
        return self._db_map.query(getattr(self._db_map, sq_name))


class EntityClassItem(CacheItemBase):
    _defaults = {"description": None, "display_icon": None, "display_order": 99, "hidden": False}
    _unique_keys = (("name",),)
    _references = {"dimension_name_list": ("dimension_id_list", ("entity_class", "name"))}
    _inverse_references = {"dimension_id_list": (("dimension_name_list",), ("entity_class", ("name",)))}

    def __init__(self, *args, **kwargs):
        dimension_id_list = kwargs.get("dimension_id_list")
        if dimension_id_list is None:
            dimension_id_list = ()
        if isinstance(dimension_id_list, str):
            dimension_id_list = (int(id_) for id_ in dimension_id_list.split(","))
        kwargs["dimension_id_list"] = tuple(dimension_id_list)
        super().__init__(*args, **kwargs)

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


class EntityItem(CacheItemBase):
    _defaults = {"description": None}
    _unique_keys = (("class_name", "name"), ("class_name", "byname"))
    _references = {
        "class_name": ("class_id", ("entity_class", "name")),
        "dimension_id_list": ("class_id", ("entity_class", "dimension_id_list")),
        "dimension_name_list": ("class_id", ("entity_class", "dimension_name_list")),
        "element_name_list": ("element_id_list", ("entity", "name")),
    }
    _inverse_references = {
        "class_id": (("class_name",), ("entity_class", ("name",))),
        "element_id_list": (("dimension_name_list", "element_name_list"), ("entity", ("class_name", "name"))),
    }

    def __init__(self, *args, **kwargs):
        element_id_list = kwargs.get("element_id_list")
        if element_id_list is None:
            element_id_list = ()
        if isinstance(element_id_list, str):
            element_id_list = (int(id_) for id_ in element_id_list.split(","))
        kwargs["element_id_list"] = tuple(element_id_list)
        super().__init__(*args, **kwargs)

    def __getitem__(self, key):
        if key == "byname":
            return self["element_name_list"] or (self["name"],)
        return super().__getitem__(key)

    def polish(self):
        error = super().polish()
        if error:
            return error
        if "name" in self:
            return
        base_name = self["class_name"] + "_" + "__".join(self["element_name_list"])
        name = base_name
        table_cache = self._db_cache.table_cache(self._item_type)
        while table_cache.unique_key_value_to_id(("class_name", "name"), (self["class_name"], name)) is not None:
            name = base_name + "_" + uuid.uuid4().hex
        self["name"] = name


class EntityGroupItem(CacheItemBase):
    _unique_keys = (("group_name", "member_name"),)
    _references = {
        "class_name": ("entity_class_id", ("entity_class", "name")),
        "group_name": ("entity_id", ("entity", "name")),
        "member_name": ("member_id", ("entity", "name")),
        "dimension_id_list": ("entity_class_id", ("entity_class", "dimension_id_list")),
    }
    _inverse_references = {
        "entity_class_id": (("class_name",), ("entity_class", ("name",))),
        "entity_id": (("class_name", "group_name"), ("entity", ("class_name", "name"))),
        "member_id": (("class_name", "member_name"), ("entity", ("class_name", "name"))),
    }

    def __getitem__(self, key):
        if key == "class_id":
            return self["entity_class_id"]
        if key == "group_id":
            return self["entity_id"]
        return super().__getitem__(key)


class ParsedValueBase(CacheItemBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._parsed_value = None

    @property
    def parsed_value(self):
        if self._parsed_value is None:
            self._parsed_value = self._make_parsed_value()
        return self._parsed_value

    def _make_parsed_value(self):
        raise NotImplementedError()

    def update(self, other):
        self._parsed_value = None
        super().update(other)

    def __getitem__(self, key):
        if key == "parsed_value":
            return self.parsed_value
        return super().__getitem__(key)


class ParameterDefinitionItem(ParsedValueBase):
    _defaults = {"description": None, "default_value": None, "default_type": None, "parameter_value_list_id": None}
    _unique_keys = (("entity_class_name", "name"),)
    _references = {
        "entity_class_name": ("entity_class_id", ("entity_class", "name")),
        "dimension_id_list": ("entity_class_id", ("entity_class", "dimension_id_list")),
        "dimension_name_list": ("entity_class_id", ("entity_class", "dimension_name_list")),
    }
    _inverse_references = {
        "entity_class_id": (("entity_class_name",), ("entity_class", ("name",))),
        "parameter_value_list_id": (("parameter_value_list_name",), ("parameter_value_list", ("name",))),
    }

    @property
    def list_value_id(self):
        if dict.get(self, "default_type") == "list_value_ref":
            return int(dict.__getitem__(self, "default_value"))
        return None

    def _make_parsed_value(self):
        try:
            return from_database(self["default_value"], self["default_type"])
        except ParameterValueFormatError as error:
            return error

    def __getitem__(self, key):
        if key == "parameter_name":
            return super().__getitem__("name")
        if key == "value_list_id":
            return super().__getitem__("parameter_value_list_id")
        if key == "parameter_value_list_id":
            return dict.get(self, key)
        if key == "parameter_value_list_name":
            return self._get_ref("parameter_value_list", self["parameter_value_list_id"], strong=False).get("name")
        if key in ("default_value", "default_type"):
            list_value_id = self.list_value_id
            if list_value_id is not None:
                list_value_key = {"default_value": "value", "default_type": "type"}[key]
                return self._get_ref("list_value", list_value_id, strong=False).get(list_value_key)
            return dict.get(self, key)
        if key == "list_value_id":
            return self.list_value_id
        return super().__getitem__(key)

    def polish(self):
        error = super().polish()
        if error:
            return error
        default_type = self["default_type"]
        default_value = self["default_value"]
        list_name = self["parameter_value_list_name"]
        if list_name is None:
            return
        if default_type == "list_value_ref":
            return
        parsed_value = from_database(default_value, default_type)
        if parsed_value is None:
            return
        list_value_id = self._db_cache.table_cache("list_value").unique_key_value_to_id(
            ("parameter_value_list_name", "value", "type"), (list_name, default_value, default_type)
        )
        if list_value_id is None:
            return f"default value {parsed_value} of {self['name']} is not in {list_name}"
        self["default_value"] = list_value_id
        self["default_type"] = "list_value_ref"

    def _asdict(self):
        d = super()._asdict()
        if d.get("default_type") == "list_value_ref":
            d["default_value"] = str(d["default_value"]).encode()
        return d

    def merge(self, other):
        other_parameter_value_list_id = other.get("parameter_value_list_id")
        if (
            other_parameter_value_list_id is not None
            and other_parameter_value_list_id != self["parameter_value_list_id"]
            and any(
                x["parameter_definition_id"] == self["id"]
                for x in self._db_cache.table_cache("parameter_value").values()
            )
        ):
            del other["parameter_value_list_id"]
            error = "can't modify the parameter value list of a parameter that already has values"
        else:
            error = ""
        merged, super_error = super().merge(other)
        return merged, " and ".join([x for x in (super_error, error) if x])


class ParameterValueItem(ParsedValueBase):
    _unique_keys = (("parameter_definition_name", "entity_byname", "alternative_name"),)
    _references = {
        "entity_class_name": ("entity_class_id", ("entity_class", "name")),
        "dimension_id_list": ("entity_class_id", ("entity_class", "dimension_id_list")),
        "dimension_name_list": ("entity_class_id", ("entity_class", "dimension_name_list")),
        "parameter_definition_name": ("parameter_definition_id", ("parameter_definition", "name")),
        "parameter_value_list_id": ("parameter_definition_id", ("parameter_definition", "parameter_value_list_id")),
        "parameter_value_list_name": ("parameter_definition_id", ("parameter_definition", "parameter_value_list_name")),
        "entity_name": ("entity_id", ("entity", "name")),
        "entity_byname": ("entity_id", ("entity", "byname")),
        "element_id_list": ("entity_id", ("entity", "element_id_list")),
        "element_name_list": ("entity_id", ("entity", "element_name_list")),
        "alternative_name": ("alternative_id", ("alternative", "name")),
    }
    _inverse_references = {
        "entity_class_id": (("entity_class_name",), ("entity_class", ("name",))),
        "parameter_definition_id": (
            ("entity_class_name", "parameter_definition_name"),
            ("parameter_definition", ("entity_class_name", "name")),
        ),
        "entity_id": (("entity_class_name", "entity_byname"), ("entity", ("class_name", "byname"))),
        "alternative_id": (("alternative_name",), ("alternative", ("name",))),
    }

    @property
    def list_value_id(self):
        if dict.__getitem__(self, "type") == "list_value_ref":
            return int(dict.__getitem__(self, "value"))
        return None

    def _make_parsed_value(self):
        try:
            return from_database(self["value"], self["type"])
        except ParameterValueFormatError as error:
            return error

    def __getitem__(self, key):
        if key == "parameter_id":
            return super().__getitem__("parameter_definition_id")
        if key == "parameter_name":
            return super().__getitem__("parameter_definition_name")
        if key in ("value", "type"):
            list_value_id = self.list_value_id
            if list_value_id:
                return self._get_ref("list_value", list_value_id, strong=False).get(key)
        if key == "list_value_id":
            return self.list_value_id
        return super().__getitem__(key)

    def polish(self):
        list_name = self["parameter_value_list_name"]
        if list_name is None:
            return
        type_ = self["type"]
        if type_ == "list_value_ref":
            return
        value = self["value"]
        parsed_value = from_database(value, type_)
        if parsed_value is None:
            return
        list_value_id = self._db_cache.table_cache("list_value").unique_key_value_to_id(
            ("parameter_value_list_name", "value", "type"), (list_name, value, type_)
        )
        if list_value_id is None:
            return (
                f"value {parsed_value} of {self['parameter_definition_name']} for {self['entity_byname']} "
                f"is not in {list_name}"
            )
        self["value"] = list_value_id
        self["type"] = "list_value_ref"

    def _asdict(self):
        d = super()._asdict()
        if d.get("type") == "list_value_ref":
            d["value"] = str(d["value"]).encode()
        return d


class ParameterValueListItem(CacheItemBase):
    _unique_keys = (("name",),)


class ListValueItem(ParsedValueBase):
    _unique_keys = (("parameter_value_list_name", "value", "type"), ("parameter_value_list_name", "index"))
    _references = {"parameter_value_list_name": ("parameter_value_list_id", ("parameter_value_list", "name"))}
    _inverse_references = {
        "parameter_value_list_id": (("parameter_value_list_name",), ("parameter_value_list", ("name",))),
    }

    def _make_parsed_value(self):
        try:
            return from_database(self["value"], self["type"])
        except ParameterValueFormatError as error:
            return error


class AlternativeItem(CacheItemBase):
    _defaults = {"description": None}
    _unique_keys = (("name",),)


class ScenarioItem(CacheItemBase):
    _defaults = {"active": False, "description": None}
    _unique_keys = (("name",),)

    @property
    def sorted_alternatives(self):
        self._db_cache.fetch_all("scenario_alternative")
        return sorted(
            (x for x in self._db_cache.get("scenario_alternative", {}).values() if x["scenario_id"] == self["id"]),
            key=itemgetter("rank"),
        )

    def __getitem__(self, key):
        if key == "alternative_id_list":
            return [x["alternative_id"] for x in self.sorted_alternatives]
        if key == "alternative_name_list":
            return [x["alternative_name"] for x in self.sorted_alternatives]
        return super().__getitem__(key)


class ScenarioAlternativeItem(CacheItemBase):
    _unique_keys = (("scenario_name", "alternative_name"), ("scenario_name", "rank"))
    _references = {
        "scenario_name": ("scenario_id", ("scenario", "name")),
        "alternative_name": ("alternative_id", ("alternative", "name")),
    }
    _inverse_references = {
        "scenario_id": (("scenario_name",), ("scenario", ("name",))),
        "alternative_id": (("alternative_name",), ("alternative", ("name",))),
    }

    def __getitem__(self, key):
        # The 'before' is to be interpreted as, this scenario alternative goes *before* the before_alternative.
        # Since ranks go from 1 to the alternative count, the first alternative will have the second as the 'before',
        # the second will have the third, etc, and the last will have None.
        # Note that alternatives with higher ranks overwrite the values of those with lower ranks.
        if key == "before_alternative_name":
            return self._get_ref("alternative", self["before_alternative_id"], strong=False).get("name")
        if key == "before_alternative_id":
            scenario = self._get_ref("scenario", self["scenario_id"], strong=False)
            try:
                return scenario["alternative_id_list"][self["rank"]]
            except IndexError:
                return None
        return super().__getitem__(key)


class MetadataItem(CacheItemBase):
    _unique_keys = (("name", "value"),)


class EntityMetadataItem(CacheItemBase):
    _unique_keys = (("entity_name", "metadata_name", "metadata_value"),)
    _references = {
        "entity_name": ("entity_id", ("entity", "name")),
        "metadata_name": ("metadata_id", ("metadata", "name")),
        "metadata_value": ("metadata_id", ("metadata", "value")),
    }
    _inverse_references = {
        "entity_id": (("entity_class_name", "entity_byname"), ("entity", ("class_name", "byname"))),
        "metadata_id": (("metadata_name", "metadata_value"), ("metadata", ("name", "value"))),
    }


class ParameterValueMetadataItem(CacheItemBase):
    _unique_keys = (
        ("parameter_definition_name", "entity_byname", "alternative_name", "metadata_name", "metadata_value"),
    )
    _references = {
        "parameter_definition_name": ("parameter_value_id", ("parameter_value", "parameter_definition_name")),
        "entity_byname": ("parameter_value_id", ("parameter_value", "entity_byname")),
        "alternative_name": ("parameter_value_id", ("parameter_value", "alternative_name")),
        "metadata_name": ("metadata_id", ("metadata", "name")),
        "metadata_value": ("metadata_id", ("metadata", "value")),
    }
    _inverse_references = {
        "parameter_value_id": (
            ("parameter_definition_name", "entity_byname", "alternative_name"),
            ("parameter_value", ("parameter_definition_name", "entity_byname", "alternative_name")),
        ),
        "metadata_id": (("metadata_name", "metadata_value"), ("metadata", ("name", "value"))),
    }
