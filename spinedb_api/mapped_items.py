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
from __future__ import annotations
from collections.abc import Iterator
from contextlib import suppress
import inspect
from operator import itemgetter
import re
from typing import ClassVar, Optional, Union
from . import arrow_value
from .db_mapping_base import DatabaseMappingBase, MappedItemBase, MappedTable
from .exception import SpineDBAPIError
from .helpers import DisplayStatus, name_from_dimensions, name_from_elements
from .parameter_value import (
    RANK_1_TYPES,
    UNPARSED_NULL_VALUE,
    VALUE_TYPES,
    Map,
    ParameterValue,
    ParameterValueFormatError,
    fancy_type_to_type_and_rank,
    from_database,
    to_database,
    type_and_rank_to_fancy_type,
)
from .temp_id import TempId

_ENTITY_BYNAME_VALUE = (
    "A tuple with the entity name as single element if the entity is 0-dimensional, "
    "or the 0-dimensional element names if it is multi-dimensional."
)

_ENTITY_CLASS_BYNAME_VALUE = (
    "A tuple with the class name as single element if the class is 0-dimensional, "
    "or the 0-dimensional class names if it is multi-dimensional."
)


class CommitItem(MappedItemBase):
    item_type = "commit"
    fields = {
        "comment": {"type": str, "value": "A comment describing the commit."},
        "date": {"type": str, "value": "Date and time of the commit in ISO 8601 format."},
        "user": {"type": str, "value": "Username of the committer."},
    }
    unique_keys = (("date",),)
    required_key_combinations = (("date",), ("comment",), ("user",))
    is_protected = True

    def commit(self, commit_id):
        raise RuntimeError("Commits are created automatically when session is committed.")


class EntityClassItem(MappedItemBase):
    item_type = "entity_class"
    fields = {
        "name": {"type": str, "value": "The class name."},
        "dimension_name_list": {
            "type": tuple,
            "value": "The dimension names for a multi-dimensional class.",
            "optional": True,
        },
        "entity_class_byname": {"type": tuple, "value": _ENTITY_CLASS_BYNAME_VALUE},
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
    _defaults = {
        "description": None,
        "display_icon": None,
        "display_order": 99,
        "hidden": False,
        "active_by_default": True,
    }
    unique_keys = (("name",),)
    required_key_combinations = (("name",),)
    _references = {"dimension_id_list": "entity_class"}
    _weak_references = {"superclass_name": "superclass_subclass"}
    _external_fields = {
        "dimension_name_list": ("dimension_id_list", "name"),
        "entity_class_byname": ("dimension_id_list", "entity_class_byname"),
    }
    _alt_references = {("dimension_name_list",): ("entity_class", ("name",))}
    _internal_fields = {"dimension_id_list": (("dimension_name_list",), "id")}
    _private_fields = {"dimension_count"}
    fields_not_requiring_cascade_update = {"description", "display_icon", "display_order" "hidden", "active_by_default"}

    def __init__(self, *args, **kwargs):
        dimension_id_list = kwargs.get("dimension_id_list")
        if dimension_id_list is None:
            dimension_id_list = ()
            if "name" not in kwargs and "dimension_name_list" in kwargs:
                kwargs["name"] = name_from_dimensions(kwargs["dimension_name_list"])
        if isinstance(dimension_id_list, str):
            dimension_id_list = (int(id_) for id_ in dimension_id_list.split(","))
        kwargs["dimension_id_list"] = tuple(dimension_id_list)
        super().__init__(*args, **kwargs)

    def __getitem__(self, key):
        if key == "superclass_id" or key == "superclass_name":
            mapped_table = self.db_map.mapped_table("superclass_subclass")
            try:
                superclass_subclass = mapped_table.find_item({"subclass_id": self["id"]}, fetch=True)
            except SpineDBAPIError:
                return None
            return superclass_subclass[key]
        if key == "entity_class_byname":
            entity_class_table = self.db_map.mapped_table("entity_class")
            return tuple(_byname_iter(self, "dimension_id_list", entity_class_table))
        return super().__getitem__(key)

    def merge(self, other):
        dimension_id_list = other.pop("dimension_id_list", None)
        if dimension_id_list is not None and dimension_id_list != self["dimension_id_list"]:
            raise SpineDBAPIError("can't modify dimensions of an entity class")
        return super().merge(other)

    def commit(self, _commit_id):
        super().commit(None)


_unfetched = object()
_unset = object()
_ENTITY_LOCATION_FIELDS = {"lat", "lon", "alt", "shape_name", "shape_blob"}


class EntityItem(MappedItemBase):
    item_type = "entity"
    fields = {
        "entity_class_name": {"type": str, "value": "The entity class name."},
        "name": {"type": str, "value": "The entity name."},
        "element_name_list": {"type": tuple, "value": "The element names if the entity is multi-dimensional."},
        "entity_byname": {
            "type": tuple,
            "value": _ENTITY_BYNAME_VALUE,
        },
        "description": {"type": str, "value": "The entity description.", "optional": True},
        "lat": {"type": float, "value": "The latitude of entity.", "optional": True},
        "lon": {"type": float, "value": "The longitude of entity.", "optional": True},
        "alt": {"type": float, "value": "The altitude of entity.", "optional": True},
        "shape_name": {"type": str, "value": "The name of the entity's shape.", "optional": True},
        "shape_blob": {"type": str, "value": "The shape as GEOJSON string.", "optional": True},
    }

    _defaults = {"description": None}
    unique_keys = (("entity_class_name", "name"), ("entity_class_name", "entity_byname"))
    required_key_combinations = (("name", "entity_byname"), ("entity_class_name", "class_id"))
    _references = {"class_id": "entity_class", "element_id_list": "entity"}
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
    fields_not_requiring_cascade_update = {"description", "lat", "lon", "alt", "shape_name", "shape_blob"}

    def __init__(self, *args, **kwargs):
        element_id_list = kwargs.get("element_id_list")
        if element_id_list is None:
            element_id_list = ()
            if "name" not in kwargs and "element_name_list" in kwargs:
                kwargs["name"] = name_from_elements(kwargs["element_name_list"])
        elif isinstance(element_id_list, str):
            element_id_list = (int(id_) for id_ in element_id_list.split(","))
        kwargs["element_id_list"] = tuple(element_id_list)
        self._location_id: Optional[Union[TempId, object]] = _unfetched
        self._init_location = self._pop_location_data(kwargs)
        if not self._init_location and "commit_id" in kwargs:
            self._init_location = None
        super().__init__(*args, **kwargs)

    @classmethod
    def unique_values_for_item(cls, item):
        """Overridden to also yield unique values for the superclass."""
        for key, value in super().unique_values_for_item(item):
            yield key, value
            sc_value = tuple(item.get("superclass_name" if k == "entity_class_name" else k) for k in key)
            if None not in sc_value:
                yield (key, sc_value)

    def __getitem__(self, key):
        if key == "entity_byname":
            entity_table = self.db_map.mapped_table("entity")
            return tuple(_byname_iter(self, "element_id_list", entity_table))
        elif key in _ENTITY_LOCATION_FIELDS:
            location_item = self._get_location_item(self.db_map.mapped_table("entity_location"))
            if location_item is None or location_item.removed:
                return None
            return location_item[key]
        elif key == "entity_location_id":
            return self._get_location_id()
        return super().__getitem__(key)

    def _asdict(self):
        d = super()._asdict()
        if self._init_location is None:
            location_item = self._get_location_item(self.db_map.mapped_table("entity_location"))
            if location_item is not None and not location_item.removed:
                d["lat"] = location_item["lat"]
                d["lon"] = location_item["lon"]
                d["alt"] = location_item["alt"]
                d["shape_name"] = location_item["shape_name"]
                d["shape_blob"] = location_item["shape_blob"]
            else:
                d.update(
                    {
                        "lat": None,
                        "lon": None,
                        "alt": None,
                        "shape_name": None,
                        "shape_blob": None,
                    }
                )
        else:
            d.update(self._init_location)
        return d

    def merge(self, other):
        other_location = {
            "lat": other.get("lat", _unset),
            "lon": other.get("lon", _unset),
            "alt": other.get("alt", _unset),
            "shape_name": other.get("shape_name", _unset),
            "shape_blob": other.get("shape_blob", _unset),
        }
        merged, updated_fields = super().merge(other)
        if merged is None:
            return None, set()
        updated_fields |= self._merge_existing_location(other_location, self)
        merged.update(other_location)
        return merged, updated_fields

    def update(self, other):
        if any(location_key in other for location_key in _ENTITY_LOCATION_FIELDS):
            location = {
                "lat": other.pop("lat", _unset),
                "lon": other.pop("lon", _unset),
                "alt": other.pop("alt", _unset),
                "shape_name": other.pop("shape_name", _unset),
                "shape_blob": other.pop("shape_blob", _unset),
            }
            self._update_location(location)
            if not other:
                return
        super().update(other)

    def _update_location(self, location: dict) -> None:
        location_table = self.db_map.mapped_table("entity_location")
        existing_location_item = self._get_location_item(location_table)
        if existing_location_item is not None:
            if existing_location_item.removed:
                existing_location_item.public_item.restore()
            self._merge_existing_location(location, existing_location_item)
            if all(value is None for value in location.values()):
                self.db_map.remove(location_table, id=dict.__getitem__(existing_location_item, "id"))
                self._location_id = None
                return
            location["id"] = dict.__getitem__(existing_location_item, "id")
            existing_location_item.update(location)
        else:
            location = {key: value if value is not _unset else None for key, value in location.items()}
            added_location = self.db_map.add(location_table, entity_id=dict.__getitem__(self, "id"), **location)
            self._location_id = added_location["id"]

    @staticmethod
    def _merge_existing_location(location_update: dict, location_item: dict) -> set[str]:
        unequal_fields = set()
        for field in ("lat", "lon", "alt", "shape_name", "shape_blob"):
            if location_update[field] is _unset:
                location_update[field] = location_item[field]
            elif location_update[field] != location_update[field]:
                unequal_fields.add(field)
        return unequal_fields

    def resolve_internal_fields(self, skip_keys: tuple[str, ...] = ()) -> None:
        """Overridden to translate byname into element name list."""
        super().resolve_internal_fields(skip_keys=skip_keys)
        try:
            byname = dict.pop(self, "entity_byname")
        except KeyError:
            return
        if not self["dimension_id_list"]:
            self["name"] = byname[0]
            return
        byname_remainder = list(byname)
        element_name_list, _ = self._element_name_list_recursive(self["entity_class_name"], byname_remainder)
        if byname_remainder:
            raise SpineDBAPIError(f"too many elements given for entity ({byname})")
        self["element_name_list"] = element_name_list
        self._do_resolve_internal_field("element_id_list")

    def _element_name_list_recursive(self, class_name: str, entity_byname: list[str]) -> tuple[tuple[str, ...], str]:
        """Returns the element name list corresponding to given class and byname.

        If the class is multi-dimensional then recurses for each dimension.
        If the class is a superclass then it tries for each subclass until finding something useful.
        """
        subclasses = [x.mapped_item for x in self.db_map.find_superclass_subclasses(superclass_name=class_name)]
        entity_class_table = self.db_map.mapped_table("entity_class")
        if subclasses:
            classes = [entity_class_table.find_item_by_id(subclass["subclass_id"]) for subclass in subclasses]
        else:
            classes = [entity_class_table.find_item_by_unique_key({"name": class_name})]
        entity_table = self.db_map.mapped_table("entity")
        for entity_class in classes:
            dimension_name_list = entity_class["dimension_name_list"]
            if not dimension_name_list:
                continue
            if len(entity_byname) < len(dimension_name_list):
                raise SpineDBAPIError(f"too few elements given for entity ({entity_byname}) in class {class_name}")
            byname_backup = list(entity_byname)
            element_name_list = []
            for dimension in dimension_name_list:
                element_name, element_class_name = self._element_name_list_recursive(dimension, entity_byname)
                try:
                    entity = entity_table.find_item_by_unique_key(
                        {"entity_byname": element_name, "entity_class_name": element_class_name}
                    )
                except SpineDBAPIError:
                    element_name_list.append(None)
                else:
                    element_name_list.append(entity["name"])
            if None not in element_name_list:
                return tuple(element_name_list), entity_class["name"]
            if entity_class is classes[-1]:
                list_containing_missing_element = tuple(
                    byname_backup if not entity_byname else byname_backup[: -len(entity_byname)]
                )
                raise SpineDBAPIError(
                    f"non-existent elements in byname {list_containing_missing_element} for class {class_name}"
                )
            entity_byname = byname_backup
        name = entity_byname.pop(0) if entity_byname else None
        return (name,), class_name

    def polish(self):
        super().polish()
        entity_table = self.db_map.mapped_table("entity")
        dim_name_lst = dict.get(self, "dimension_name_list")
        if dim_name_lst:
            el_name_lst = dict.get(self, "element_name_list")
            if el_name_lst:
                for dim_name, el_name in zip(dim_name_lst, el_name_lst):
                    try:
                        entity_table.find_item_by_unique_key(
                            {"entity_class_name": dim_name, "name": el_name}, fetch=False
                        )
                    except SpineDBAPIError:
                        raise SpineDBAPIError(f"element '{el_name}' is not an instance of class '{dim_name}'")
        if "name" in self:
            return
        base_name = name_from_elements(self["element_name_list"])
        name = base_name
        index = 1
        while True:
            names_found = 2
            for k in ("entity_class_name", "superclass_name"):
                if (class_name := self[k]) is not None:
                    try:
                        entity_table.find_item_by_unique_key({"entity_class_name": class_name, "name": name})
                    except SpineDBAPIError:
                        names_found -= 1
                else:
                    names_found -= 1
            if names_found == 0:
                break
            name = f"{base_name}_{index}"
            index += 1
        self["name"] = name

    def check_mutability(self):
        superclass_subclass_table = self.db_map.mapped_table("superclass_subclass")
        if self.db_map.find(superclass_subclass_table, superclass_id=self["class_id"]):
            raise SpineDBAPIError("an entity class that is a superclass cannot have entities")
        super().check_mutability()

    @staticmethod
    def _pop_location_data(item: dict) -> dict[str, str]:
        location = {}
        if "lat" in item:
            if "lon" not in item:
                raise SpineDBAPIError("cannot set latitude without longitude")
            location["lat"] = item.pop("lat")
            location["lon"] = item.pop("lon")
            with suppress(KeyError):
                location["alt"] = item.pop("alt")
        elif "lon" in item:
            raise SpineDBAPIError("cannot set longitude without latitude")
        elif "alt" in item:
            raise SpineDBAPIError("cannot set altitude without latitude and longitude")
        if "shape_name" in item:
            if "shape_blob" not in item:
                raise SpineDBAPIError("cannot set shape_name without shape_blob")
            location["shape_name"] = item.pop("shape_name")
            location["shape_blob"] = item.pop("shape_blob")
        elif "shape_blob" in item:
            raise SpineDBAPIError("cannot set shape_blob without shape_name")
        return location

    def added_to_mapped_table(self) -> None:
        super().added_to_mapped_table()
        if self._init_location:
            self._init_location["entity_id"] = dict.__getitem__(self, "id")
            location_table = self.db_map.mapped_table("entity_location")
            item = location_table.add_item(self._init_location)
            self._location_id = item["id"]
        else:
            self._location_id = _unfetched
        self._init_location = None

    def _get_location_id(self) -> TempId:
        if self._location_id is _unfetched:
            location_table = self.db_map.mapped_table("entity_location")
            try:
                location = location_table.find_item_by_unique_key({"entity_id": dict.__getitem__(self, "id")})
            except SpineDBAPIError:
                self._location_id = None
            else:
                self._location_id = dict.__getitem__(location, "id")
        return self._location_id

    def _get_location_item(self, location_table: MappedTable) -> Optional[EntityLocationItem]:
        if self._location_id is None:
            return None
        if self._location_id is _unfetched:
            try:
                location = location_table.find_item_by_unique_key({"entity_id": dict.__getitem__(self, "id")})
            except SpineDBAPIError:
                self._location_id = None
                return None
            self._location_id = dict.__getitem__(location, "id")
        else:
            location = location_table.find_item_by_id(self._location_id)
        return location


class EntityGroupItem(MappedItemBase):
    item_type = "entity_group"
    fields = {
        "entity_class_name": {"type": str, "value": "The entity class name."},
        "group_name": {"type": str, "value": "The group entity name."},
        "member_name": {"type": str, "value": "The member entity name."},
    }
    unique_keys = (("entity_class_name", "group_name", "member_name"),)
    required_key_combinations = (
        ("entity_class_name", "entity_class_id", "entity_id"),
        ("group_name", "entity_id"),
        ("member_name", "member_id"),
    )
    _references = {
        "entity_class_id": "entity_class",
        "entity_id": "entity",
        "member_id": "entity",
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
    item_type = "entity_alternative"
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
    unique_keys = (("entity_class_name", "entity_byname", "alternative_name"),)
    required_key_combinations = (
        ("entity_class_name", "entity_class_id", "entity_id"),
        ("entity_byname", "entity_id"),
        ("alternative_name", "alternative_id"),
    )
    _references = {
        "entity_id": "entity",
        "entity_class_id": "entity_class",
        "alternative_id": "alternative",
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


class DisplayModeItem(MappedItemBase):
    item_type = "display_mode"
    fields = {
        "name": {"type": str, "value": "The display mode name."},
        "description": {"type": str, "value": "The display mode description.", "optional": True},
    }
    _defaults = {"description": None}
    unique_keys = (("name",),)
    required_key_combinations = (("name",),)


class EntityClassDisplayModeItem(MappedItemBase):
    item_type = "entity_class_display_mode"
    fields = {
        "entity_class_name": {"type": str, "value": "The entity class name."},
        "display_mode_name": {"type": int, "value": "The display mode name."},
        "display_order": {"type": int, "value": "The display order."},
        "display_status": {"type": str, "value": "The display status."},
        "display_font_color": {"type": str, "value": "The color of the font.", "optional": True},
        "display_background_color": {"type": str, "value": "The  color of the background.", "optional": True},
    }
    _defaults = {
        "display_status": DisplayStatus.visible.name,
        "display_font_color": None,
        "display_background_color": None,
    }
    unique_keys = (
        (
            "entity_class_name",
            "display_mode_name",
        ),
    )
    required_key_combinations = (("entity_class_name", "entity_class_id"), ("display_mode_name", "display_mode_id"))
    _references = {
        "entity_class_id": "entity_class",
        "display_mode_id": "display_mode",
    }
    _external_fields = {
        "entity_class_name": ("entity_class_id", "name"),
        "display_mode_name": ("display_mode_id", "name"),
    }
    _alt_references = {
        ("entity_class_name",): ("entity_class", ("name",)),
        ("display_mode_name",): ("display_mode", ("name",)),
    }
    _internal_fields = {
        "entity_class_id": (("entity_class_name",), "id"),
        "display_mode_id": (("display_mode_name",), "id"),
    }

    COLOR_RE = re.compile("[a-fA-F0-9]{6}")

    def first_invalid_key(self):
        error = super().first_invalid_key()
        if error:
            return error
        for color_field in ("display_font_color", "display_background_color"):
            if (color := self[color_field]) is not None:
                if self.COLOR_RE.match(color) is None:
                    return color_field
        return None


class ParsedValueBase(MappedItemBase):
    _private_fields = {"list_value_id"}
    value_key: ClassVar[str] = "value"
    type_key: ClassVar[str] = "type"

    def __init__(self, *args, **kwargs):
        parsed_value = None
        try:
            parsed_value = kwargs.pop("parsed_value")
        except KeyError:
            pass
        else:
            kwargs[self.value_key], kwargs[self.type_key] = to_database(parsed_value)
        super().__init__(*args, **kwargs)
        self._parsed_value = parsed_value
        self._arrow_value = None

    @property
    def parsed_value(self):
        if self._parsed_value is None:
            self._parsed_value = self._make_parsed_value()
        return self._parsed_value

    def has_value_been_parsed(self):
        """Returns True if parsed_value property has been used."""
        return self._parsed_value is not None

    def first_invalid_key(self):
        invalid_key = super().first_invalid_key()
        if invalid_key is not None:
            return invalid_key
        value = self[self.value_key]
        if value is not None:
            if value != UNPARSED_NULL_VALUE and self[self.type_key] is None:
                return self.type_key
        return None

    def _make_parsed_value(self):
        try:
            return from_database(self[self.value_key], self[self.type_key])
        except ParameterValueFormatError as error:
            return error

    def __getitem__(self, key):
        if key == "parsed_value":
            return self.parsed_value
        if key == "arrow_value":
            if self._arrow_value is None:
                self._arrow_value = arrow_value.from_database(self[self.value_key], self[self.type_key])
            return self._arrow_value
        return super().__getitem__(key)

    def merge(self, other):
        merged, updated_fields = super().merge(other)
        if not merged:
            return merged, updated_fields
        if self.value_key in merged:
            self._parsed_value = None
            self._arrow_value = None
        return merged, updated_fields

    def _strip_equal_fields(self, other):
        undefined = object()
        other_parsed_value = undefined
        other_value = undefined
        other_type = undefined
        if "parsed_value" in other:
            other = dict(other)
            other_parsed_value = other.pop("parsed_value")
            other.pop(self.value_key, None)
            other.pop(self.type_key, None)
        if self.value_key in other:
            other = dict(other)
            other_value = other.pop(self.value_key)
            other_type = other.pop(self.type_key, self[self.type_key])
        other = super()._strip_equal_fields(other)
        if other_parsed_value is not undefined:
            if self.parsed_value != other_parsed_value:
                other[self.value_key], other[self.type_key] = to_database(other_parsed_value)
        elif other_type is not undefined and other_value is not undefined:
            if self[self.type_key] != other_type or (
                self[self.value_key] != other_value and self.parsed_value != from_database(other_value, other_type)
            ):
                other[self.value_key] = other_value
                other[self.type_key] = other_type
        return other


class ParameterItemBase(ParsedValueBase):
    def _value_not_in_list_error(self, parsed_value, list_name):
        raise NotImplementedError()

    @classmethod
    def ref_types(cls):
        return super().ref_types() | {"list_value"}

    @property
    def list_value_id(self):
        return self["list_value_id"]

    def _asdict(self):
        d = super()._asdict()
        if d[self.type_key] == "list_value_ref":
            d[self.type_key] = self.__getitem__(self.type_key)
        return d

    def resolve(self):
        d = super().resolve()
        list_value_id = d.get("list_value_id")
        if list_value_id is not None:
            d[self.value_key] = to_database(list_value_id)[0]
            d[self.type_key] = "list_value_ref"
        return d

    def polish(self):
        self["list_value_id"] = None
        super().polish()
        list_name = self["parameter_value_list_name"]
        if list_name is None:
            self["list_value_id"] = None
            return
        try:
            type_ = super().__getitem__(self.type_key)
        except KeyError:
            if isinstance(self, ParameterValueItem):
                raise SpineDBAPIError(
                    f"parameter value {self['parameter_definition_name']} for class {self['entity_class_name']}, "
                    f"entity {self['entity_byname']}, alternative {self['alternative_name']} has no list value"
                )
            raise SpineDBAPIError(f"parameter {self['name']} for class {self['entity_class_name']} has no list value")
        if type_ == "list_value_ref":
            return
        value = super().__getitem__(self.value_key)
        parsed_value = from_database(value, type_)
        if parsed_value is None:
            return
        mapped_table = self.db_map.mapped_table("list_value")
        try:
            list_value = mapped_table.find_item_by_unique_key(
                {"parameter_value_list_name": list_name, "value": value, "type": type_}
            )
        except SpineDBAPIError:
            raise SpineDBAPIError(self._value_not_in_list_error(parsed_value, list_name))
        self["list_value_id"] = list_value["id"]
        self[self.type_key] = "list_value_ref"


class ParameterDefinitionItem(ParameterItemBase):
    item_type = "parameter_definition"
    value_key = "default_value"
    type_key = "default_type"
    fields = {
        "entity_class_name": {"type": str, "value": "The entity class name."},
        "name": {"type": str, "value": "The parameter name."},
        "parameter_type_list": {"type": tuple, "value": "List of valid value types.", "optional": True},
        "default_value": {"type": bytes, "value": "The default value's database representation.", "optional": True},
        "default_type": {"type": str, "value": "The default value's type.", "optional": True},
        "parsed_value": {"type": ParameterValue, "value": "The default value.", "optional": True},
        "parameter_value_list_name": {
            "type": str,
            "value": "The parameter value list name if any.",
            "optional": True,
        },
        "description": {"type": str, "value": "The parameter description.", "optional": True},
    }
    _defaults = {"description": None, "default_value": None, "default_type": None, "parameter_value_list_id": None}
    unique_keys = (("entity_class_name", "name"),)
    required_key_combinations = (("entity_class_name", "entity_class_id"), ("name",))
    _references = {"entity_class_id": "entity_class", "parameter_value_list_id": "parameter_value_list"}
    _weak_references = {"list_value_id": "list_value"}
    _soft_references = {"parameter_value_list_id"}
    _external_fields = {
        "entity_class_name": ("entity_class_id", "name"),
        "dimension_id_list": ("entity_class_id", "dimension_id_list"),
        "dimension_name_list": ("entity_class_id", "dimension_name_list"),
        "parameter_value_list_name": ("parameter_value_list_id", "name"),
    }
    _alt_references = {
        ("entity_class_name",): ("entity_class", ("name",)),
        ("parameter_value_list_name",): ("parameter_value_list", ("name",)),
    }
    _internal_fields = {
        "entity_class_id": (("entity_class_name",), "id"),
        "parameter_value_list_id": (("parameter_value_list_name",), "id"),
    }
    fields_not_requiring_cascade_update = {
        "description",
        "parameter_type_list",
        "default_value" "default_type",
        "parsed_value",
    }

    def __init__(self, db_map, **kwargs):
        super().__init__(db_map, **kwargs)
        self._init_type_list = kwargs.get("parameter_type_list")

    def __getitem__(self, key):
        if key == "parameter_type_id_list":
            return tuple(x["id"] for x in self._sorted_parameter_types())
        if key == "parameter_type_list":
            mapped_table = self.db_map.mapped_table("parameter_type")
            self.db_map.do_fetch_all(mapped_table)
            return tuple(type_and_rank_to_fancy_type(x["type"], x["rank"]) for x in self._sorted_parameter_types())
        if key == "value_list_id":
            return super().__getitem__("parameter_value_list_id")
        if key == "parameter_value_list_id":
            return dict.get(self, key)
        if key == "parameter_value_list_name":
            value_list = self._get_full_ref("parameter_value_list_id", "parameter_value_list")
            return value_list["name"] if value_list is not None else None
        if key in ("default_value", "default_type"):
            list_value_id: TempId = self["list_value_id"]
            if list_value_id is not None:
                list_value_key = {"default_value": "value", "default_type": "type"}[key]
                mapped_table = self.db_map.mapped_table("list_value")
                return mapped_table.find_item_by_id(list_value_id)[list_value_key]
            return dict.get(self, key)
        return super().__getitem__(key)

    def _sorted_parameter_types(self):
        mapped_table = self.db_map.mapped_table("parameter_type")
        self.db_map.do_fetch_all(mapped_table)
        if "id" in self:
            id_ = dict.__getitem__(self, "id")
            return sorted(
                (x for x in mapped_table.valid_values() if x["parameter_definition_id"] == id_),
                key=lambda i: (i["type"], i["rank"]),
            )
        name = dict.__getitem__(self, "name")
        class_name = self["entity_class_name"]
        return sorted(
            (
                x
                for x in mapped_table.valid_values()
                if x["parameter_definition_name"] == name and x["entity_class_name"] == class_name
            ),
            key=lambda i: (i["type"], i["rank"]),
        )

    def _asdict(self):
        d = super()._asdict()
        if "parameter_type_list" not in d:
            d["parameter_type_list"] = self.__getitem__("parameter_type_list")
        return d

    def merge(self, other):
        other_parameter_value_list_id = other.get("parameter_value_list_id")
        if (
            other_parameter_value_list_id is not None
            and other_parameter_value_list_id != self["parameter_value_list_id"]
            and any(
                x["parameter_definition_id"] == self["id"]
                for x in self.db_map.mapped_table("parameter_value").valid_values()
            )
        ):
            del other["parameter_value_list_id"]
            raise SpineDBAPIError("can't modify the parameter value list of a parameter that already has values")
        other_type_list = other.get("parameter_type_list")
        if other_type_list is not None and other_type_list != self.__getitem__("parameter_type_list"):
            try:
                self._make_new_type_items(other_type_list)
            except SpineDBAPIError as type_error:
                del other["parameter_type_list"]
                raise type_error
        return super().merge(other)

    def _make_new_type_items(self, new_type_list):
        new_types = set(new_type_list)
        current_types = set(self["parameter_type_list"])
        items_to_add = []
        type_table = self.db_map.mapped_table("parameter_type")
        class_name = self["entity_class_name"]
        parameter_name = self["name"]
        for type_to_add in new_types - current_types:
            type_, rank = fancy_type_to_type_and_rank(type_to_add)
            type_item = type_table.make_candidate_item(
                {
                    "entity_class_name": class_name,
                    "parameter_definition_name": parameter_name,
                    "type": type_,
                    "rank": rank,
                }
            )
            items_to_add.append(type_item)
        return items_to_add

    def _update_types(self, new_type_list, type_items_to_add):
        new_types = set(new_type_list)
        current_types = set(self["parameter_type_list"])
        type_table = self.db_map.mapped_table("parameter_type")
        class_name = self["entity_class_name"]
        parameter_name = self["name"]
        types_to_remove = current_types - new_types
        for type_to_remove in types_to_remove:
            type_, rank = fancy_type_to_type_and_rank(type_to_remove)
            type_item = type_table.find_item_by_unique_key(
                {
                    "entity_class_name": class_name,
                    "parameter_definition_name": parameter_name,
                    "type": type_,
                    "rank": rank,
                }
            )
            type_item.cascade_remove()
        for type_to_add in type_items_to_add:
            type_table.add_item(type_to_add)

    def _value_not_in_list_error(self, parsed_value, list_name):
        return f"default value {parsed_value} of {self['name']} is not in {list_name}"

    def added_to_mapped_table(self):
        super().added_to_mapped_table()
        if self._init_type_list is None:
            return
        type_table = self.db_map.mapped_table("parameter_type")
        for fancy_type in self._init_type_list:
            type_, rank = fancy_type_to_type_and_rank(fancy_type)
            type_table.add_item(
                {
                    "entity_class_id": self["entity_class_id"],
                    "parameter_definition_id": self["id"],
                    "type": type_,
                    "rank": rank,
                }
            )
        self._init_type_list = None

    def cascade_update(self, update_referrers):
        updated_type_list = self.pop("_updated_parameter_type_list", None)
        if updated_type_list is not None:
            new_type_items = self._make_new_type_items(updated_type_list)
            self._update_types(updated_type_list, new_type_items)
        super().cascade_update(update_referrers)

    def update(self, other):
        other_type_list = other.pop("parameter_type_list", None)
        if other_type_list is not None and other_type_list != self["parameter_type_list"]:
            other["_updated_parameter_type_list"] = other_type_list
        super().update(other)


class ParameterTypeItem(MappedItemBase):
    item_type = "parameter_type"
    fields = {
        "entity_class_name": {"type": str, "value": "The entity class name."},
        "parameter_definition_name": {"type": str, "value": "The parameter name."},
        "rank": {"type": int, "value": "The rank of the type."},
        "type": {"type": str, "value": "The value type."},
    }
    unique_keys = (("entity_class_name", "parameter_definition_name", "type", "rank"),)
    required_key_combinations = (
        ("entity_class_name", "entity_class_id", "parameter_definition_id"),
        ("parameter_definition_name", "parameter_definition_id"),
        ("type",),
        ("rank",),
    )
    _references = {"entity_class_id": "entity_class", "parameter_definition_id": "parameter_definition"}
    _external_fields = {
        "entity_class_id": ("parameter_definition_id", "entity_class_id"),
        "entity_class_name": ("entity_class_id", "name"),
        "parameter_definition_name": ("parameter_definition_id", "name"),
    }
    _alt_references = {
        ("entity_class_name",): ("entity_class", ("name",)),
        (
            "entity_class_name",
            "parameter_definition_name",
        ): ("parameter_definition", ("entity_class_name", "name")),
    }
    _internal_fields = {
        "parameter_definition_id": (
            (
                "entity_class_name",
                "parameter_definition_name",
            ),
            "id",
        ),
    }

    def first_invalid_key(self):
        invalid_key = super().first_invalid_key()
        if invalid_key is not None:
            return invalid_key
        value_type = self["type"]
        if value_type not in VALUE_TYPES:
            return "type"
        rank = self["rank"]
        if value_type == Map.TYPE:
            return "rank" if rank < 1 else None
        if value_type in RANK_1_TYPES:
            return "rank" if rank != 1 else None
        return "rank" if rank != 0 else None


class ParameterValueItem(ParameterItemBase):
    item_type = "parameter_value"
    fields = {
        "entity_class_name": {"type": str, "value": "The entity class name."},
        "parameter_definition_name": {"type": str, "value": "The parameter name."},
        "entity_byname": {
            "type": tuple,
            "value": _ENTITY_BYNAME_VALUE,
        },
        "value": {"type": bytes, "value": "The value's database representation."},
        "type": {"type": str, "value": "The value's type. Optional only when value is null.", "optional": True},
        "parsed_value": {"type": ParameterValue, "value": "The value.", "optional": True},
        "alternative_name": {"type": str, "value": "The alternative name - defaults to 'Base'.", "optional": True},
    }
    unique_keys = (("entity_class_name", "parameter_definition_name", "entity_byname", "alternative_name"),)
    required_key_combinations = (
        ("entity_class_name", "entity_class_id", "parameter_definition_id", "entity_id"),
        ("parameter_definition_name", "parameter_definition_id"),
        ("entity_byname", "entity_id"),
        ("alternative_name", "alternative_id"),
    )
    _references = {
        "entity_class_id": "entity_class",
        "parameter_definition_id": "parameter_definition",
        "entity_id": "entity",
        "alternative_id": "alternative",
    }
    _weak_references = {"value_list_id": "value_list"}
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

    def __getitem__(self, key):
        if key == "parameter_id":
            return super().__getitem__("parameter_definition_id")
        if key == "parameter_name":
            return super().__getitem__("parameter_definition_name")
        if key in ("value", "type"):
            list_value_id: TempId = self["list_value_id"]
            if list_value_id:
                mapped_table = self.db_map.mapped_table("list_value")
                return mapped_table.find_item_by_id(list_value_id)[key]
        return super().__getitem__(key)

    def _value_not_in_list_error(self, parsed_value, list_name):
        return (
            f"value {parsed_value} of {self['parameter_definition_name']} for {self['entity_byname']} "
            f"is not in {list_name}"
        )

    def check_mutability(self):
        if (
            dict.__getitem__(self, self.type_key) is None
            and dict.__getitem__(self, self.value_key) != UNPARSED_NULL_VALUE
        ):
            raise SpineDBAPIError("'type' is missing")


class ParameterValueListItem(MappedItemBase):
    item_type = "parameter_value_list"
    fields = {"name": {"type": str, "value": "The parameter value list name."}}
    unique_keys = (("name",),)
    required_key_combinations = (("name",),)


class ListValueItem(ParsedValueBase):
    item_type = "list_value"
    fields = {
        "parameter_value_list_name": {"type": str, "value": "The parameter value list name."},
        "value": {"type": bytes, "value": "The value's database representation."},
        "type": {"type": str, "value": "The value's type.", "optional": True},
        "parsed_value": {"type": ParameterValue, "value": "The value.", "optional": True},
        "index": {"type": int, "value": "The value index.", "optional": True},
    }
    unique_keys = (("parameter_value_list_name", "value_and_type"), ("parameter_value_list_name", "index"))
    required_key_combinations = (
        ("parameter_value_list_name", "parameter_value_list_id"),
        (
            "value_and_type",
            "type",
        ),
        (
            "value_and_type",
            "value",
        ),
        ("index",),
    )
    _references = {"parameter_value_list_id": "parameter_value_list"}
    _external_fields = {"parameter_value_list_name": ("parameter_value_list_id", "name")}
    _alt_references = {("parameter_value_list_name",): ("parameter_value_list", ("name",))}
    _internal_fields = {"parameter_value_list_id": (("parameter_value_list_name",), "id")}

    def __getitem__(self, key):
        if key == "value_and_type":
            return (self["value"], self["type"])
        return super().__getitem__(key)


class AlternativeItem(MappedItemBase):
    item_type = "alternative"
    fields = {
        "name": {"type": str, "value": "The alternative name."},
        "description": {"type": str, "value": "The alternative description.", "optional": True},
    }
    _defaults = {"description": None}
    unique_keys = (("name",),)
    required_key_combinations = (("name",),)
    fields_not_requiring_cascade_update = {"description"}


class ScenarioItem(MappedItemBase):
    item_type = "scenario"
    fields = {
        "name": {"type": str, "value": "The scenario name."},
        "description": {"type": str, "value": "The scenario description.", "optional": True},
        "active": {"type": bool, "value": "Not in use at the moment.", "optional": True},
    }
    _defaults = {"active": False, "description": None}
    unique_keys = (("name",),)
    required_key_combinations = (("name",),)
    fields_not_requiring_cascade_update = {"description", "active"}

    def __getitem__(self, key):
        if key == "alternative_id_list":
            return [x["alternative_id"] for x in self["sorted_scenario_alternatives"]]
        if key == "alternative_name_list":
            return [x["alternative_name"] for x in self["sorted_scenario_alternatives"]]
        if key == "sorted_scenario_alternatives":
            mapped_table = self.db_map.mapped_table("scenario_alternative")
            self.db_map.do_fetch_all(mapped_table)
            return sorted(
                (x for x in mapped_table.valid_values() if x["scenario_id"] == self["id"]),
                key=itemgetter("rank"),
            )
        return super().__getitem__(key)


class ScenarioAlternativeItem(MappedItemBase):
    item_type = "scenario_alternative"
    fields = {
        "scenario_name": {"type": str, "value": "The scenario name."},
        "alternative_name": {"type": str, "value": "The alternative name."},
        "rank": {"type": int, "value": "The rank - higher has precedence."},
    }
    unique_keys = (("scenario_name", "alternative_name"), ("scenario_name", "rank"))
    required_key_combinations = (("scenario_name", "scenario_id"), ("alternative_name", "alternative_id"), ("rank",))
    _references = {"scenario_id": "scenario", "alternative_id": "alternative"}
    _external_fields = {"scenario_name": ("scenario_id", "name"), "alternative_name": ("alternative_id", "name")}
    _alt_references = {("scenario_name",): ("scenario", ("name",)), ("alternative_name",): ("alternative", ("name",))}
    _internal_fields = {"scenario_id": (("scenario_name",), "id"), "alternative_id": (("alternative_name",), "id")}

    def __getitem__(self, key):
        # The 'before' is to be interpreted as, this scenario alternative goes *before* the before_alternative.
        # Since ranks go from 1 to the alternative count, the first alternative will have the second as the 'before',
        # the second will have the third, etc., and the last will have None.
        # Note that alternatives with higher ranks overwrite the values of those with lower ranks.
        if key == "before_alternative_name":
            mapped_table = self.db_map.mapped_table("alternative")
            before_alternative_id = self._before_alternative_id()
            if before_alternative_id is None:
                return None
            before_alternative = mapped_table.find_item_by_id(before_alternative_id)
            return before_alternative["name"]
        if key == "before_alternative_id":
            return self._before_alternative_id()
        return super().__getitem__(key)

    def _before_alternative_id(self) -> Optional[TempId]:
        mapped_table = self.db_map.mapped_table("scenario_alternative")
        self.db_map.do_fetch_all(mapped_table)
        min_rank = super().__getitem__("rank")
        scenario_id = super().__getitem__("scenario_id")
        before_alternative_id = None
        before_alternative_rank = None
        for scenario_alternative in mapped_table.values():
            rank = scenario_alternative["rank"]
            if (
                not scenario_alternative.is_valid()
                or scenario_alternative["scenario_id"] != scenario_id
                or rank <= min_rank
            ):
                continue
            if before_alternative_id is None or rank < before_alternative_rank:
                before_alternative_id = scenario_alternative["alternative_id"]
                before_alternative_rank = rank
        return before_alternative_id


class MetadataItem(MappedItemBase):
    item_type = "metadata"
    fields = {
        "name": {"type": str, "value": "The metadata entry name."},
        "value": {"type": str, "value": "The metadata entry value."},
    }
    unique_keys = (("name", "value"),)
    required_key_combinations = (("name",), ("value",))


class EntityMetadataItem(MappedItemBase):
    item_type = "entity_metadata"
    fields = {
        "entity_class_name": {"type": str, "value": "The entity class name."},
        "entity_byname": {"type": tuple, "value": _ENTITY_BYNAME_VALUE},
        "metadata_name": {"type": str, "value": "The metadata entry name."},
        "metadata_value": {"type": str, "value": "The metadata entry value."},
    }
    unique_keys = (("entity_class_name", "entity_byname", "metadata_name", "metadata_value"),)
    required_key_combinations = (
        ("entity_class_name", "entity_class_id", "entity_id"),
        ("entity_byname", "entity_id"),
        ("metadata_name", "metadata_id"),
        ("metadata_value", "metadata_id"),
    )
    _references = {
        "entity_id": "entity",
        "metadata_id": "metadata",
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
    item_type = "parameter_value_metadata"
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
    unique_keys = (
        (
            "entity_class_name",
            "parameter_definition_name",
            "entity_byname",
            "alternative_name",
            "metadata_name",
            "metadata_value",
        ),
    )
    required_key_combinations = (
        ("entity_class_name", "entity_class_id", "parameter_definition_id", "entity_id", "parameter_value_id"),
        ("entity_byname", "entity_id", "parameter_value_id"),
        ("parameter_definition_name", "parameter_definition_id", "parameter_value_id"),
        ("alternative_name", "alternative_id", "parameter_value_id"),
        ("metadata_name", "metadata_id"),
        ("metadata_value", "metadata_id"),
    )
    _references = {"parameter_value_id": "parameter_value", "metadata_id": "metadata"}
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
    item_type = "superclass_subclass"
    fields = {
        "superclass_name": {"type": str, "value": "The superclass name."},
        "subclass_name": {"type": str, "value": "The subclass name."},
    }
    unique_keys = (("subclass_name",),)
    required_key_combinations = (
        ("superclass_name", "superclass_id"),
        ("subclass_name", "subclass_id"),
    )
    _references = {"superclass_id": "entity_class", "subclass_id": "entity_class"}
    _external_fields = {
        "superclass_name": ("superclass_id", "name"),
        "subclass_name": ("subclass_id", "name"),
    }
    _alt_references = {
        ("superclass_name",): ("entity_class", ("name",)),
        ("subclass_name",): ("entity_class", ("name",)),
    }
    _internal_fields = {"superclass_id": (("superclass_name",), "id"), "subclass_id": (("subclass_name",), "id")}

    def _check_superclass_validity(self, superclass: EntityClassItem):
        if len(superclass["dimension_id_list"]) != 0:
            raise SpineDBAPIError("superclass cannot have more than zero dimensions")
        entity_table = self.db_map.mapped_table("entity")
        if self.db_map.find(entity_table, class_id=superclass["id"]):
            raise SpineDBAPIError("cannot turn a class that has entities into superclass")

    def _check_subclass_validity(
        self,
        superclass_id: TempId,
        subclass: EntityClassItem,
        entity_class_table: MappedTable,
        superclass_subclass_table,
    ) -> None:
        dimension_count = len(subclass["dimension_name_list"])
        self.db_map.do_fetch_all(superclass_subclass_table)
        for existing_record in superclass_subclass_table.values():
            if existing_record["superclass_id"] != superclass_id or not existing_record.is_valid():
                continue
            existing = entity_class_table[existing_record["subclass_id"]]
            if len(existing["dimension_name_list"]) != dimension_count:
                raise SpineDBAPIError("subclass has different dimension count to existing subclasses")
        if _is_superclass_recursive(subclass, entity_class_table, self.db_map):
            raise SpineDBAPIError("subclass or any of its dimensions cannot be a superclass")

    def check_mutability(self):
        entity_table = self.db_map.mapped_table("entity")
        if self.db_map.find(entity_table, class_id=self["subclass_id"]):
            raise SpineDBAPIError("can't set or modify the superclass for a class that already has entities")
        superclass_id = self["superclass_id"]
        entity_class_table = self.db_map.mapped_table("entity_class")
        superclass = entity_class_table.find_item_by_id(superclass_id)
        self._check_superclass_validity(superclass)
        subclass = entity_class_table.find_item_by_id(self["subclass_id"])
        superclass_subclass_table = self.db_map.mapped_table("superclass_subclass")
        self._check_subclass_validity(superclass_id, subclass, entity_class_table, superclass_subclass_table)
        return super().check_mutability()

    def commit(self, _commit_id):
        super().commit(None)


def _is_superclass_recursive(
    entity_class: EntityClassItem, entity_class_table: MappedTable, db_map: DatabaseMappingBase
) -> bool:
    if db_map.find_superclass_subclasses(superclass_id=entity_class["id"]):
        return True
    return any(
        _is_superclass_recursive(entity_class_table.find_item_by_id(id_), entity_class_table, db_map)
        for id_ in entity_class["dimension_id_list"]
    )


class EntityLocationItem(MappedItemBase):
    item_type = "entity_location"
    fields = {
        "entity_class_name": {"type": str, "value": "The entity class name."},
        "entity_byname": {"type": tuple, "value": _ENTITY_BYNAME_VALUE},
        "lat": {"type": float, "value": "Latitude.", "optional": True},
        "lon": {"type": float, "value": "Longitude.", "optional": True},
        "alt": {"type": float, "value": "Altitude.", "optional": True},
        "shape_name": {"type": str, "value": "Name identifying the shape.", "optional": True},
        "shape_blob": {"type": str, "value": "Shape as GEOJSON feature.", "optional": True},
    }
    _defaults = {"lat": None, "lon": None, "alt": None, "shape_name": None, "shape_blob": None}
    unique_keys = (("entity_class_name", "entity_byname"),)
    required_key_combinations = (
        ("entity_class_name", "entity_class_id", "entity_id"),
        ("entity_byname", "entity_id"),
    )
    _references = {
        "entity_id": "entity",
        "entity_class_id": "entity_class",
    }
    _external_fields = {
        "entity_class_id": ("entity_id", "class_id"),
        "entity_class_name": ("entity_class_id", "name"),
        "entity_name": ("entity_id", "name"),
        "entity_byname": ("entity_id", "entity_byname"),
    }
    _alt_references = {
        ("entity_class_name", "entity_byname"): ("entity", ("entity_class_name", "entity_byname")),
    }
    _internal_fields = {
        "entity_id": (("entity_class_name", "entity_byname"), "id"),
    }

    def check_mutability(self):
        latitude = dict.__getitem__(self, "lat")
        longitude = dict.__getitem__(self, "lon")
        if latitude is not None:
            if longitude is None:
                raise SpineDBAPIError("latitude cannot be set if longitude is None")
        elif longitude is not None:
            raise SpineDBAPIError("longitude cannot be set if latitude is None")
        if dict.__getitem__(self, "alt") is not None and latitude is None and longitude is None:
            raise SpineDBAPIError("altitude cannot be set if latitude and longitude are None")
        name = dict.__getitem__(self, "shape_name")
        blob = dict.__getitem__(self, "shape_blob")
        if name is not None:
            if blob is None:
                raise SpineDBAPIError("shape_name cannot be set if shape_blob is None")
        elif blob is not None:
            raise SpineDBAPIError("shape_blob cannot be set if shape_name is None")


ITEM_CLASSES = tuple(
    x for x in tuple(locals().values()) if inspect.isclass(x) and issubclass(x, MappedItemBase) and x != MappedItemBase
)
ITEM_CLASS_BY_TYPE = {klass.item_type: klass for klass in ITEM_CLASSES}


def _byname_iter(item: Union[EntityClassItem, EntityItem], id_list_name: str, table: MappedTable) -> Iterator[str]:
    id_list = item[id_list_name]
    if not id_list:
        yield item["name"]
    else:
        find_by_id = table.find_item_by_id
        for id_ in id_list:
            try:
                element = find_by_id(id_)
            except SpineDBAPIError:
                raise KeyError(id_)
            yield from _byname_iter(element, id_list_name, table)
