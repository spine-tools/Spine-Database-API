#############################################################################
# Copyright (C) 2017 - 2018 VTT Technical Research Centre of Finland
#
# This file is part of Spine Database API.
#
# Spine Spine Database API is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#############################################################################

"""
A class to perform integrity checks over a Spine db ORM.

:author: Manuel Marin (KTH)
:date:   11.8.2018
"""

import json
from .exception import SpineIntegrityError


class DatabaseMappingCheckMixin:
    """A mixin to perform integrity checks for insert and update operations over a Spine db ORM.
    NOTE: To check for an update we simulate the removal of the current instance,
    and then check for an insert of the updated instance.
    """

    def __init__(self, *args, **kwargs):
        """Initialize class."""
        super().__init__(*args, **kwargs)

    def check_object_classes_for_insert(self, *kwargs_list, strict=False):
        """Check that object classes respect integrity constraints for an insert operation."""
        intgr_error_log = []
        checked_kwargs_list = list()
        object_class_names = {x.name: x.id for x in self.object_class_list()}
        for kwargs in kwargs_list:
            try:
                self.check_object_class(kwargs, object_class_names)
                checked_kwargs_list.append(kwargs)
                # If the check passes, append kwargs to `object_class_names` for next iteration.
                object_class_names[
                    kwargs["name"]
                ] = None  # TODO: check if this is problematic?
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_kwargs_list, intgr_error_log

    def check_object_classes_for_update(self, *kwargs_list, strict=False):
        """Check that object classes respect integrity constraints for an update operation.
        """
        intgr_error_log = []
        checked_kwargs_list = list()
        object_class_dict = {x.id: {"name": x.name} for x in self.object_class_list()}
        object_class_names = {x.name for x in self.object_class_list()}
        for kwargs in kwargs_list:
            try:
                id = kwargs["id"]
            except KeyError:
                msg = "Missing object class identifier."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            try:
                # Simulate removal of current instance
                updated_kwargs = object_class_dict.pop(id)
                object_class_names.remove(updated_kwargs["name"])
            except KeyError:
                msg = "Object class not found."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            # Check for an insert of the updated instance
            try:
                updated_kwargs.update(kwargs)
                self.check_object_class(updated_kwargs, object_class_names)
                checked_kwargs_list.append(kwargs)
                # If the check passes, reinject the updated instance for next iteration.
                object_class_dict[id] = updated_kwargs
                object_class_names.add(updated_kwargs["name"])
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_kwargs_list, intgr_error_log

    def check_object_class(self, kwargs, object_class_names):
        """Raise a `SpineIntegrityError` if the object class given by `kwargs` violates any
        integrity constraints.
        """
        try:
            name = kwargs["name"]
        except KeyError:
            raise SpineIntegrityError("Missing object class name.")
        if name in object_class_names:
            raise SpineIntegrityError(
                "There can't be more than one object class called '{}'.".format(name),
                id=object_class_names[name],
            )

    def check_objects_for_insert(self, *kwargs_list, strict=False):
        """Check that objects respect integrity constraints for an insert operation."""
        intgr_error_log = []
        checked_kwargs_list = list()
        object_names = {(x.class_id, x.name): x.id for x in self.object_list()}
        object_class_id_list = [x.id for x in self.object_class_list()]
        for kwargs in kwargs_list:
            try:
                self.check_object(kwargs, object_names, object_class_id_list)
                checked_kwargs_list.append(kwargs)
                object_names[kwargs["class_id"], kwargs["name"]] = None
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_kwargs_list, intgr_error_log

    def check_objects_for_update(self, *kwargs_list, strict=False):
        """Check that objects respect integrity constraints for an update operation."""
        intgr_error_log = []
        checked_kwargs_list = list()
        object_list = self.object_list()
        object_names = {(x.class_id, x.name): x.id for x in self.object_list()}
        object_dict = {
            x.id: {"name": x.name, "class_id": x.class_id} for x in object_list
        }
        object_class_id_list = [x.id for x in self.object_class_list()]
        for kwargs in kwargs_list:
            try:
                id = kwargs["id"]
            except KeyError:
                msg = "Missing object identifier."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            try:
                updated_kwargs = object_dict.pop(id)
                del object_names[updated_kwargs["class_id"], updated_kwargs["name"]]
            except KeyError:
                msg = "Object not found."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            try:
                updated_kwargs.update(kwargs)
                self.check_object(updated_kwargs, object_names, object_class_id_list)
                checked_kwargs_list.append(kwargs)
                object_dict[id] = updated_kwargs
                object_names[updated_kwargs["class_id"], updated_kwargs["name"]] = id
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_kwargs_list, intgr_error_log

    def check_object(self, kwargs, object_names, object_class_id_list):
        """Raise a `SpineIntegrityError` if the object given by `kwargs` violates any
        integrity constraints."""
        try:
            class_id = kwargs["class_id"]
        except KeyError:
            raise SpineIntegrityError("Missing object class identifier.")
        if class_id not in object_class_id_list:
            raise SpineIntegrityError("Object class not found.")
        try:
            name = kwargs["name"]
        except KeyError:
            raise SpineIntegrityError("Missing object name.")
        if (class_id, name) in object_names:
            raise SpineIntegrityError(
                "There's already an object called '{}' in the same class.".format(name),
                id=object_names[class_id, name],
            )

    def check_wide_relationship_classes_for_insert(
        self, *wide_kwargs_list, strict=False
    ):
        """Check that relationship classes respect integrity constraints for an insert operation."""
        intgr_error_log = []
        checked_wide_kwargs_list = list()
        relationship_class_names = {
            x.name: x.id for x in self.wide_relationship_class_list()
        }
        object_class_id_list = [x.id for x in self.object_class_list()]
        for wide_kwargs in wide_kwargs_list:
            try:
                self.check_wide_relationship_class(
                    wide_kwargs, relationship_class_names, object_class_id_list
                )
                checked_wide_kwargs_list.append(wide_kwargs)
                relationship_class_names[wide_kwargs["name"]] = None
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_wide_kwargs_list, intgr_error_log

    def check_wide_relationship_classes_for_update(
        self, *wide_kwargs_list, strict=False
    ):
        """Check that relationship classes respect integrity constraints for an update operation."""
        intgr_error_log = []
        checked_wide_kwargs_list = list()
        wide_relationship_class_list = self.wide_relationship_class_list()
        relationship_class_names = {
            x.name: x.id for x in self.wide_relationship_class_list()
        }
        relationship_class_dict = {
            x.id: {
                "name": x.name,
                "object_class_id_list": [
                    int(y) for y in x.object_class_id_list.split(",")
                ],
            }
            for x in wide_relationship_class_list
        }
        object_class_id_list = [x.id for x in self.object_class_list()]
        for wide_kwargs in wide_kwargs_list:
            try:
                id = wide_kwargs["id"]
            except KeyError:
                msg = "Missing relationship class identifier."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            try:
                updated_wide_kwargs = relationship_class_dict.pop(id)
                del relationship_class_names[updated_wide_kwargs["name"]]
            except KeyError:
                msg = "Relationship class not found."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            try:
                updated_wide_kwargs.update(wide_kwargs)
                self.check_wide_relationship_class(
                    updated_wide_kwargs,
                    list(relationship_class_dict.values()),
                    object_class_id_list,
                )
                checked_wide_kwargs_list.append(wide_kwargs)
                relationship_class_dict[id] = updated_wide_kwargs
                relationship_class_names[updated_wide_kwargs["name"]] = id
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_wide_kwargs_list, intgr_error_log

    def check_wide_relationship_class(
        self, wide_kwargs, relationship_class_names, object_class_id_list
    ):
        """Raise a `SpineIntegrityError` if the relationship class given by `kwargs` violates any
        integrity constraints."""
        try:
            given_object_class_id_list = wide_kwargs["object_class_id_list"]
        except KeyError:
            raise SpineIntegrityError("Missing object class identifier.")
        if len(given_object_class_id_list) == 0:
            raise SpineIntegrityError("At least one object class is needed.")
        if not all([id in object_class_id_list for id in given_object_class_id_list]):
            raise SpineIntegrityError("Object class not found.")
        try:
            name = wide_kwargs["name"]
        except KeyError:
            raise SpineIntegrityError("Missing relationship class name.")
        if name in relationship_class_names:
            raise SpineIntegrityError(
                "There can't be more than one relationship class called '{}'.".format(
                    name
                ),
                id=relationship_class_names[name],
            )

    def check_wide_relationships_for_insert(self, *wide_kwargs_list, strict=False):
        """Check that relationships respect integrity constraints for an insert operation."""
        intgr_error_log = []
        checked_wide_kwargs_list = list()
        wide_relationship_list = self.wide_relationship_list()
        relationship_names = {
            (x.class_id, x.name): x.id for x in wide_relationship_list
        }
        relationship_objects = {
            (x.class_id, x.object_id_list): x.id for x in wide_relationship_list
        }
        relationship_class_dict = {
            x.id: {
                "object_class_id_list": [
                    int(y) for y in x.object_class_id_list.split(",")
                ],
                "name": x.name,
            }
            for x in self.wide_relationship_class_list()
        }
        object_dict = {
            x.id: {"class_id": x.class_id, "name": x.name} for x in self.object_list()
        }
        for wide_kwargs in wide_kwargs_list:
            try:
                self.check_wide_relationship(
                    wide_kwargs,
                    relationship_names,
                    relationship_objects,
                    relationship_class_dict,
                    object_dict,
                )
                checked_wide_kwargs_list.append(wide_kwargs)
                relationship_names[wide_kwargs["class_id"], wide_kwargs["name"]] = None
                join_object_id_list = ",".join(
                    [str(x) for x in wide_kwargs["object_id_list"]]
                )
                relationship_objects[
                    wide_kwargs["class_id"], join_object_id_list
                ] = None
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_wide_kwargs_list, intgr_error_log

    def check_wide_relationships_for_update(self, *wide_kwargs_list, strict=False):
        """Check that relationships respect integrity constraints for an update operation."""
        intgr_error_log = []
        checked_wide_kwargs_list = list()
        wide_relationship_list = self.wide_relationship_list()
        relationship_names = {
            (x.class_id, x.name): x.id for x in wide_relationship_list
        }
        relationship_objects = {
            (x.class_id, x.object_id_list): x.id for x in wide_relationship_list
        }
        relationship_dict = {
            x.id: {
                "class_id": x.class_id,
                "name": x.name,
                "object_id_list": [int(y) for y in x.object_id_list.split(",")],
            }
            for x in wide_relationship_list
        }
        relationship_class_dict = {
            x.id: {
                "object_class_id_list": [
                    int(y) for y in x.object_class_id_list.split(",")
                ],
                "name": x.name,
            }
            for x in self.wide_relationship_class_list()
        }
        object_dict = {
            x.id: {"class_id": x.class_id, "name": x.name} for x in self.object_list()
        }
        for wide_kwargs in wide_kwargs_list:
            try:
                id = wide_kwargs["id"]
            except KeyError:
                msg = "Missing relationship identifier."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            try:
                updated_wide_kwargs = relationship_dict.pop(id)
                del relationship_names[
                    updated_wide_kwargs["class_id"], updated_wide_kwargs["name"]
                ]
                join_object_id_list = ",".join(
                    [str(x) for x in updated_wide_kwargs["object_id_list"]]
                )
                del relationship_objects[
                    updated_wide_kwargs["class_id"], join_object_id_list
                ]
            except KeyError:
                msg = "Relationship not found."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            try:
                updated_wide_kwargs.update(wide_kwargs)
                self.check_wide_relationship(
                    updated_wide_kwargs,
                    relationship_names,
                    relationship_objects,
                    relationship_class_dict,
                    object_dict,
                )
                checked_wide_kwargs_list.append(wide_kwargs)
                relationship_dict[id] = updated_wide_kwargs
                relationship_names[
                    updated_wide_kwargs["class_id"], updated_wide_kwargs["name"]
                ] = id
                join_object_id_list = ",".join(
                    [str(x) for x in updated_wide_kwargs["object_id_list"]]
                )
                relationship_objects[
                    updated_wide_kwargs["class_id"], join_object_id_list
                ] = id
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_wide_kwargs_list, intgr_error_log

    def check_wide_relationship(
        self,
        wide_kwargs,
        relationship_names,
        relationship_objects,
        relationship_class_dict,
        object_dict,
    ):
        """Raise a `SpineIntegrityError` if the relationship given by `kwargs` violates any integrity constraints."""
        try:
            name = wide_kwargs["name"]
        except KeyError:
            raise SpineIntegrityError("Missing relationship name.")
        try:
            class_id = wide_kwargs["class_id"]
        except KeyError:
            raise SpineIntegrityError("Missing relationship class identifier.")
        if (class_id, name) in relationship_names:
            raise SpineIntegrityError(
                "There's already a relationship called '{}' in the same class.".format(
                    name
                ),
                id=relationship_names[class_id, name],
            )
        try:
            object_class_id_list = relationship_class_dict[class_id][
                "object_class_id_list"
            ]
        except KeyError:
            raise SpineIntegrityError("Relationship class not found.")
        try:
            object_id_list = wide_kwargs["object_id_list"]
        except KeyError:
            raise SpineIntegrityError("Missing object identifier.")
        try:
            given_object_class_id_list = [
                object_dict[id]["class_id"] for id in object_id_list
            ]
        except KeyError as e:
            raise SpineIntegrityError("Object id '{}' not found.".format(e))
        if given_object_class_id_list != object_class_id_list:
            object_name_list = [object_dict[id]["name"] for id in object_id_list]
            relationship_class_name = relationship_class_dict[class_id]["name"]
            raise SpineIntegrityError(
                "Incorrect objects '{}' for relationship class '{}'.".format(
                    object_name_list, relationship_class_name
                )
            )
        join_object_id_list = ",".join([str(x) for x in object_id_list])
        if (class_id, join_object_id_list) in relationship_objects:
            object_name_list = [object_dict[id]["name"] for id in object_id_list]
            relationship_class_name = relationship_class_dict[class_id]["name"]
            raise SpineIntegrityError(
                "There's already a relationship between objects {} in class {}.".format(
                    object_name_list, relationship_class_name
                ),
                id=relationship_objects[class_id, join_object_id_list],
            )

    def check_parameter_definitions_for_insert(self, *kwargs_list, strict=False):
        """Check that parameter definitions respect integrity constraints for an insert operation."""
        intgr_error_log = []
        checked_kwargs_list = list()
        obj_parameter_definition_names = {}
        rel_parameter_definition_names = {}
        for x in self.parameter_list():
            if x.object_class_id:
                obj_parameter_definition_names[x.object_class_id, x.name] = x.id
            elif x.relationship_class_id:
                rel_parameter_definition_names[x.relationship_class_id, x.name] = x.id
        object_class_dict = {x.id: x.name for x in self.object_class_list()}
        relationship_class_dict = {
            x.id: x.name for x in self.wide_relationship_class_list()
        }
        parameter_value_list_dict = {
            x.id: x.value_list for x in self.wide_parameter_value_list_list()
        }
        for kwargs in kwargs_list:
            try:
                self.check_parameter_definition(
                    kwargs,
                    obj_parameter_definition_names,
                    rel_parameter_definition_names,
                    object_class_dict,
                    relationship_class_dict,
                    parameter_value_list_dict,
                )
                checked_kwargs_list.append(kwargs)
                object_class_id = kwargs.get("object_class_id", None)
                relationship_class_id = kwargs.get("relationship_class_id", None)
                if object_class_id:
                    obj_parameter_definition_names[
                        object_class_id, kwargs["name"]
                    ] = None
                elif relationship_class_id:
                    rel_parameter_definition_names[
                        relationship_class_id, kwargs["name"]
                    ] = None
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_kwargs_list, intgr_error_log

    def check_parameter_definitions_for_update(self, *kwargs_list, strict=False):
        """Check that parameter definitions respect integrity constraints for an update operation."""
        intgr_error_log = []
        checked_kwargs_list = list()
        parameter_list = self.parameter_list()  # Query db only once
        obj_parameter_definition_names = {}
        rel_parameter_definition_names = {}
        for x in parameter_list:
            if x.object_class_id:
                obj_parameter_definition_names[x.object_class_id, x.name] = x.id
            elif x.relationship_class_id:
                rel_parameter_definition_names[x.relationship_class_id, x.name] = x.id
        parameter_definition_dict = {
            x.id: {
                "name": x.name,
                "object_class_id": x.object_class_id,
                "relationship_class_id": x.relationship_class_id,
                "parameter_value_list_id": x.parameter_value_list_id,
                "default_value": x.default_value,
            }
            for x in parameter_list
        }
        object_class_dict = {x.id: x.name for x in self.object_class_list()}
        relationship_class_dict = {
            x.id: x.name for x in self.wide_relationship_class_list()
        }
        parameter_value_list_dict = {
            x.id: x.value_list for x in self.wide_parameter_value_list_list()
        }
        for kwargs in kwargs_list:
            try:
                id = kwargs["id"]
            except KeyError:
                msg = "Missing parameter definition identifier."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            try:
                updated_kwargs = parameter_definition_dict.pop(id)
                object_class_id = updated_kwargs["object_class_id"]
                relationship_class_id = updated_kwargs["relationship_class_id"]
                if object_class_id:
                    del obj_parameter_definition_names[
                        object_class_id, updated_kwargs["name"]
                    ]
                elif relationship_class_id:
                    del rel_parameter_definition_names[
                        relationship_class_id, updated_kwargs["name"]
                    ]
            except KeyError:
                msg = "Parameter not found."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            try:
                # Allow turning an object class parameter into a relationship class parameter, and viceversa
                if "object_class_id" in kwargs:
                    kwargs.setdefault("relationship_class_id", None)
                if "relationship_class_id" in kwargs:
                    kwargs.setdefault("object_class_id", None)
                updated_kwargs.update(kwargs)
                self.check_parameter_definition(
                    updated_kwargs,
                    obj_parameter_definition_names,
                    rel_parameter_definition_names,
                    object_class_dict,
                    relationship_class_dict,
                    parameter_value_list_dict,
                )
                checked_kwargs_list.append(kwargs)
                object_class_id = kwargs.get("object_class_id", None)
                relationship_class_id = kwargs.get("relationship_class_id", None)
                if object_class_id:
                    obj_parameter_definition_names[object_class_id, kwargs["name"]] = id
                elif relationship_class_id:
                    rel_parameter_definition_names[
                        relationship_class_id, kwargs["name"]
                    ] = id
                parameter_definition_dict[id] = updated_kwargs
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_kwargs_list, intgr_error_log

    def check_parameter_definition(
        self,
        kwargs,
        obj_parameter_definition_names,
        rel_parameter_definition_names,
        object_class_dict,
        relationship_class_dict,
        parameter_value_list_dict,
    ):
        """Raise a `SpineIntegrityError` if the parameter definition given by `kwargs` violates any
        integrity constraints."""
        object_class_id = kwargs.get("object_class_id", None)
        relationship_class_id = kwargs.get("relationship_class_id", None)
        if object_class_id and relationship_class_id:
            try:
                object_class_name = object_class_dict[object_class_id]
            except KeyError:
                object_class_name = "id " + object_class_id
            try:
                relationship_class_name = relationship_class_dict[relationship_class_id]
            except KeyError:
                relationship_class_name = "id " + relationship_class_id
            raise SpineIntegrityError(
                "Can't associate a parameter to both object class '{}' and relationship class '{}'.".format(
                    object_class_name, relationship_class_name
                )
            )
        if object_class_id:
            if object_class_id not in object_class_dict:
                raise SpineIntegrityError("Object class not found.")
            try:
                name = kwargs["name"]
            except KeyError:
                raise SpineIntegrityError("Missing parameter name.")
            if (object_class_id, name) in obj_parameter_definition_names:
                raise SpineIntegrityError(
                    "There's already a parameter called '{}' in this class.".format(
                        name
                    ),
                    id=obj_parameter_definition_names[object_class_id, name],
                )
        elif relationship_class_id:
            if relationship_class_id not in relationship_class_dict:
                raise SpineIntegrityError("Relationship class not found.")
            try:
                name = kwargs["name"]
            except KeyError:
                raise SpineIntegrityError("Missing parameter name.")
            if (relationship_class_id, name) in rel_parameter_definition_names:
                raise SpineIntegrityError(
                    "There's already a parameter called '{}' in this class.".format(
                        name
                    ),
                    id=rel_parameter_definition_names[relationship_class_id, name],
                )
        else:
            raise SpineIntegrityError(
                "Missing object class or relationship class identifier."
            )
        value_list = None
        if "parameter_value_list_id" in kwargs:
            parameter_value_list_id = kwargs["parameter_value_list_id"]
            if parameter_value_list_id:
                if parameter_value_list_id not in parameter_value_list_dict:
                    raise SpineIntegrityError("Invalid parameter value list.")
                value_list = parameter_value_list_dict[parameter_value_list_id].split(
                    ","
                )

        default_value = kwargs.get("default_value")
        if default_value is not None:
            try:
                json.loads(default_value)
            except json.JSONDecodeError as err:
                raise SpineIntegrityError(
                    "Couldn't decode default value '{}' as JSON: {}".format(
                        default_value, err
                    )
                )
            if (
                default_value is not None
                and value_list is not None
                and default_value not in value_list
            ):
                raise SpineIntegrityError(
                    "The value '{}' is not a valid default value "
                    "for the associated list (valid values are: {})".format(
                        default_value, ", ".join(value_list)
                    )
                )

    def check_parameter_values_for_insert(self, *kwargs_list, strict=False):
        """Check that parameter values respect integrity constraints for an insert operation."""
        intgr_error_log = []
        checked_kwargs_list = list()
        object_parameter_values = {
            (x.object_id, x.parameter_definition_id): x.id
            for x in self.parameter_value_list()
            if x.object_id
        }
        relationship_parameter_values = {
            (x.relationship_id, x.parameter_definition_id): x.id
            for x in self.parameter_value_list()
            if x.relationship_id
        }
        parameter_definition_dict = {
            x.id: {
                "name": x.name,
                "object_class_id": x.object_class_id,
                "relationship_class_id": x.relationship_class_id,
                "parameter_value_list_id": x.parameter_value_list_id,
            }
            for x in self.parameter_list()
        }
        object_dict = {
            x.id: {"class_id": x.class_id, "name": x.name} for x in self.object_list()
        }
        relationship_dict = {
            x.id: {"class_id": x.class_id, "name": x.name}
            for x in self.wide_relationship_list()
        }
        parameter_value_list_dict = {
            x.id: x.value_list for x in self.wide_parameter_value_list_list()
        }
        for kwargs in kwargs_list:
            try:
                self.check_parameter_value(
                    kwargs,
                    object_parameter_values,
                    relationship_parameter_values,
                    parameter_definition_dict,
                    object_dict,
                    relationship_dict,
                    parameter_value_list_dict,
                )
                checked_kwargs_list.append(kwargs)
                # Update sets of tuples (object_id, parameter_definition_id)
                # and (relationship_id, parameter_definition_id)
                object_id = kwargs.get("object_id", None)
                relationship_id = kwargs.get("relationship_id", None)
                if object_id:
                    object_parameter_values[
                        object_id, kwargs["parameter_definition_id"]
                    ] = None
                elif relationship_id:
                    relationship_parameter_values[
                        relationship_id, kwargs["parameter_definition_id"]
                    ] = None
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_kwargs_list, intgr_error_log

    def check_parameter_values_for_update(self, *kwargs_list, strict=False):
        """Check that parameter values respect integrity constraints for an update operation."""
        intgr_error_log = []
        checked_kwargs_list = list()
        parameter_value_dict = {
            x.id: {
                "parameter_definition_id": x.parameter_definition_id,
                "object_id": x.object_id,
                "relationship_id": x.relationship_id,
            }
            for x in self.parameter_value_list()
        }
        object_parameter_values = {
            (x.object_id, x.parameter_definition_id): x.id
            for x in self.parameter_value_list()
            if x.object_id
        }
        relationship_parameter_values = {
            (x.relationship_id, x.parameter_definition_id): x.id
            for x in self.parameter_value_list()
            if x.relationship_id
        }
        parameter_definition_dict = {
            x.id: {
                "name": x.name,
                "object_class_id": x.object_class_id,
                "relationship_class_id": x.relationship_class_id,
                "parameter_value_list_id": x.parameter_value_list_id,
            }
            for x in self.parameter_list()
        }
        object_dict = {
            x.id: {"class_id": x.class_id, "name": x.name} for x in self.object_list()
        }
        relationship_dict = {
            x.id: {"class_id": x.class_id, "name": x.name}
            for x in self.wide_relationship_list()
        }
        parameter_value_list_dict = {
            x.id: x.value_list for x in self.wide_parameter_value_list_list()
        }
        for kwargs in kwargs_list:
            try:
                id = kwargs["id"]
            except KeyError:
                msg = "Missing parameter value identifier."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            try:
                updated_kwargs = parameter_value_dict.pop(id)
                # Remove current tuples (object_id, parameter_definition_id)
                # and (relationship_id, parameter_definition_id)
                object_id = updated_kwargs.get("object_id", None)
                relationship_id = updated_kwargs.get("relationship_id", None)
                if object_id:
                    del object_parameter_values[
                        object_id, updated_kwargs["parameter_definition_id"]
                    ]
                elif relationship_id:
                    del relationship_parameter_values[
                        relationship_id, updated_kwargs["parameter_definition_id"]
                    ]
            except KeyError:
                msg = "Parameter value not found."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            try:
                # Allow turning an object parameter value into a relationship parameter value, and viceversa
                if "object_id" in kwargs:
                    kwargs.setdefault("relationship_id", None)
                if "relationship_id" in kwargs:
                    kwargs.setdefault("object_id", None)
                updated_kwargs.update(kwargs)
                self.check_parameter_value(
                    updated_kwargs,
                    object_parameter_values,
                    relationship_parameter_values,
                    parameter_definition_dict,
                    object_dict,
                    relationship_dict,
                    parameter_value_list_dict,
                )
                checked_kwargs_list.append(kwargs)
                parameter_value_dict[id] = updated_kwargs
                # Add updated tuples (object_id, parameter_definition_id)
                # and (relationship_id, parameter_definition_id)
                object_id = updated_kwargs.get("object_id", None)
                relationship_id = updated_kwargs.get("relationship_id", None)
                if object_id:
                    object_parameter_values[
                        object_id, updated_kwargs["parameter_definition_id"]
                    ] = id
                elif relationship_id:
                    relationship_parameter_values[
                        relationship_id, updated_kwargs["parameter_definition_id"]
                    ] = id
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_kwargs_list, intgr_error_log

    def check_parameter_value(
        self,
        kwargs,
        object_parameter_values,
        relationship_parameter_values,
        parameter_definition_dict,
        object_dict,
        relationship_dict,
        parameter_value_list_dict,
    ):
        """Raise a `SpineIntegrityError` if the parameter value given by `kwargs` violates any integrity constraints."""
        try:
            parameter_definition_id = kwargs["parameter_definition_id"]
        except KeyError:
            raise SpineIntegrityError("Missing parameter identifier.")
        try:
            parameter_definition = parameter_definition_dict[parameter_definition_id]
        except KeyError:
            raise SpineIntegrityError("Parameter not found.")
        value = kwargs.get("value")
        if value is not None:
            try:
                json.loads(value)
            except json.JSONDecodeError as err:
                raise SpineIntegrityError(
                    "Couldn't decode '{}' as JSON: {}".format(value, err)
                )
            parameter_value_list_id = parameter_definition["parameter_value_list_id"]
            if parameter_value_list_id in parameter_value_list_dict:
                value_list = parameter_value_list_dict[parameter_value_list_id].split(
                    ","
                )
                if value and value not in value_list:
                    valid_values = ", ".join(value_list)
                    raise SpineIntegrityError(
                        "The value '{}' is not a valid value for parameter '{}' (valid values are: {})".format(
                            value, parameter_definition["name"], valid_values
                        )
                    )
        object_id = kwargs.get("object_id", None)
        relationship_id = kwargs.get("relationship_id", None)
        if object_id and relationship_id:
            try:
                object_name = object_dict[object_id]["name"]
            except KeyError:
                object_name = "object id " + object_id
            try:
                relationship_name = relationship_dict[relationship_id]["name"]
            except KeyError:
                relationship_name = "relationship id " + relationship_id
            raise SpineIntegrityError(
                "Can't associate a parameter value to both object '{}' and relationship '{}'.".format(
                    object_name, relationship_name
                )
            )
        if object_id:
            try:
                object_class_id = object_dict[object_id]["class_id"]
            except KeyError:
                raise SpineIntegrityError("Object not found")
            if object_class_id != parameter_definition["object_class_id"]:
                object_name = object_dict[object_id]["name"]
                parameter_name = parameter_definition["name"]
                raise SpineIntegrityError(
                    "Incorrect object '{}' for parameter '{}'.".format(
                        object_name, parameter_name
                    )
                )
            if (object_id, parameter_definition_id) in object_parameter_values:
                object_name = object_dict[object_id]["name"]
                parameter_name = parameter_definition["name"]
                raise SpineIntegrityError(
                    "The value of parameter '{}' for object '{}' is already specified.".format(
                        parameter_name, object_name
                    ),
                    id=object_parameter_values[object_id, parameter_definition_id],
                )
        elif relationship_id:
            try:
                relationship_class_id = relationship_dict[relationship_id]["class_id"]
            except KeyError:
                raise SpineIntegrityError("Relationship not found")
            if relationship_class_id != parameter_definition["relationship_class_id"]:
                relationship_name = relationship_dict[relationship_id]["name"]
                parameter_name = parameter_definition["name"]
                raise SpineIntegrityError(
                    "Incorrect relationship '{}' for parameter '{}'.".format(
                        relationship_name, parameter_name
                    )
                )
            if (
                relationship_id,
                parameter_definition_id,
            ) in relationship_parameter_values:
                relationship_name = relationship_dict[relationship_id]["name"]
                parameter_name = parameter_definition["name"]
                raise SpineIntegrityError(
                    "The value of parameter '{}' for relationship '{}' is already specified.".format(
                        parameter_name, relationship_name
                    ),
                    id=relationship_parameter_values[
                        relationship_id, parameter_definition_id
                    ],
                )
        else:
            raise SpineIntegrityError("Missing object or relationship identifier.")

    def check_parameter_tags_for_insert(self, *kwargs_list, strict=False):
        """Check that parameter tags respect integrity constraints for an insert operation."""
        intgr_error_log = []
        checked_kwargs_list = list()
        parameter_tags = {x.tag: x.id for x in self.parameter_tag_list()}
        for kwargs in kwargs_list:
            try:
                self.check_parameter_tag(kwargs, parameter_tags)
                checked_kwargs_list.append(kwargs)
                parameter_tags[kwargs["tag"]] = None
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_kwargs_list, intgr_error_log

    def check_parameter_tags_for_update(self, *kwargs_list, strict=False):
        """Check that parameter tags respect integrity constraints for an update operation.
        """
        intgr_error_log = []
        checked_kwargs_list = list()
        parameter_tag_dict = {x.id: {"tag": x.tag} for x in self.parameter_tag_list()}
        parameter_tags = {x.tag: x.id for x in self.parameter_tag_list()}
        for kwargs in kwargs_list:
            try:
                id = kwargs["id"]
            except KeyError:
                msg = "Missing parameter tag identifier."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            try:
                # 'Remove' current instance
                updated_kwargs = parameter_tag_dict.pop(id)
                del parameter_tags[updated_kwargs["tag"]]
            except KeyError:
                msg = "Parameter tag not found."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            # Check for an insert of the updated instance
            try:
                updated_kwargs.update(kwargs)
                self.check_parameter_tag(updated_kwargs, parameter_tags)
                checked_kwargs_list.append(kwargs)
                parameter_tag_dict[id] = updated_kwargs
                parameter_tags[updated_kwargs["tag"]] = id
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_kwargs_list, intgr_error_log

    def check_parameter_tag(self, kwargs, parameter_tags):
        """Raise a `SpineIntegrityError` if the parameter tag given by `kwargs` violates any
        integrity constraints.
        """
        try:
            tag = kwargs["tag"]
        except KeyError:
            raise SpineIntegrityError("Missing parameter tag.")
        if tag in parameter_tags:
            raise SpineIntegrityError(
                "There can't be more than one '{}' tag.".format(tag),
                id=parameter_tags[tag],
            )

    def check_parameter_definition_tags_for_insert(self, *kwargs_list, strict=False):
        """Check that parameter definition tags respect integrity constraints for an insert operation."""
        intgr_error_log = []
        checked_kwargs_list = list()
        parameter_definition_tags = {
            (x.parameter_definition_id, x.parameter_tag_id): x.id
            for x in self.parameter_definition_tag_list()
        }
        parameter_name_dict = {x.id: x.name for x in self.parameter_list()}
        parameter_tag_dict = {x.id: x.tag for x in self.parameter_tag_list()}
        for kwargs in kwargs_list:
            try:
                self.check_parameter_definition_tag(
                    kwargs,
                    parameter_definition_tags,
                    parameter_name_dict,
                    parameter_tag_dict,
                )
                checked_kwargs_list.append(kwargs)
                parameter_definition_tags[
                    kwargs["parameter_definition_id"], kwargs["parameter_tag_id"]
                ] = None
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_kwargs_list, intgr_error_log

    def check_parameter_definition_tag(
        self, kwargs, parameter_definition_tags, parameter_name_dict, parameter_tag_dict
    ):
        """Raise a `SpineIntegrityError` if the parameter definition tag given by `kwargs` violates any
        integrity constraints.
        """
        try:
            parameter_definition_id = kwargs["parameter_definition_id"]
        except KeyError:
            raise SpineIntegrityError("Missing parameter definition identifier.")
        try:
            parameter_tag_id = kwargs["parameter_tag_id"]
        except KeyError:
            raise SpineIntegrityError("Missing parameter tag identifier.")
        try:
            parameter_name = parameter_name_dict[parameter_definition_id]
        except KeyError:
            raise SpineIntegrityError("Parameter definition not found.")
        try:
            tag = parameter_tag_dict[parameter_tag_id]
        except KeyError:
            raise SpineIntegrityError("Parameter tag not found.")
        if (parameter_definition_id, parameter_tag_id) in parameter_definition_tags:
            raise SpineIntegrityError(
                "Parameter '{0}' already has the tag '{1}'.".format(
                    parameter_name, tag
                ),
                id=parameter_definition_tags[parameter_definition_id, parameter_tag_id],
            )

    def check_wide_parameter_value_lists_for_insert(
        self, *wide_kwargs_list, strict=False
    ):
        """Check that parameter value_lists respect integrity constraints for an insert operation."""
        intgr_error_log = []
        checked_wide_kwargs_list = list()
        parameter_value_list_names = {
            x.name: x.id for x in self.wide_parameter_value_list_list()
        }
        for wide_kwargs in wide_kwargs_list:
            try:
                self.check_wide_parameter_value_list(
                    wide_kwargs, parameter_value_list_names
                )
                checked_wide_kwargs_list.append(wide_kwargs)
                parameter_value_list_names[wide_kwargs["name"]] = None
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_wide_kwargs_list, intgr_error_log

    def check_wide_parameter_value_lists_for_update(
        self, *wide_kwargs_list, strict=False
    ):
        """Check that parameter value_lists respect integrity constraints for an update operation.
        """
        intgr_error_log = []
        checked_wide_kwargs_list = list()
        parameter_value_list_dict = {
            x.id: {"name": x.name, "value_list": x.value_list.split(",")}
            for x in self.wide_parameter_value_list_list()
        }
        parameter_value_list_names = {
            x.name: x.id for x in self.wide_parameter_value_list_list()
        }
        for wide_kwargs in wide_kwargs_list:
            try:
                id = wide_kwargs["id"]
            except KeyError:
                msg = "Missing parameter value list identifier."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            try:
                # 'Remove' current instance
                updated_wide_kwargs = parameter_value_list_dict.pop(id)
                del parameter_value_list_names[updated_wide_kwargs["name"]]
            except KeyError:
                msg = "Parameter value list not found."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            # Check for an insert of the updated instance
            try:
                updated_wide_kwargs.update(wide_kwargs)
                self.check_wide_parameter_value_list(
                    updated_wide_kwargs, parameter_value_list_names
                )
                checked_wide_kwargs_list.append(wide_kwargs)
                parameter_value_list_dict[id] = updated_wide_kwargs
                parameter_value_list_names[updated_wide_kwargs["name"]] = id
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_wide_kwargs_list, intgr_error_log

    def check_wide_parameter_value_list(self, wide_kwargs, parameter_value_list_names):
        """Raise a `SpineIntegrityError` if the parameter value_list given by `wide_kwargs` violates any
        integrity constraints.
        """
        try:
            name = wide_kwargs["name"]
        except KeyError:
            raise SpineIntegrityError("Missing parameter value list name.")
        if name in parameter_value_list_names:
            raise SpineIntegrityError(
                "There can't be more than one parameter value_list called '{}'.".format(
                    name
                ),
                id=parameter_value_list_names[name],
            )
        try:
            value_list = wide_kwargs["value_list"]
        except KeyError:
            raise SpineIntegrityError("Missing list of values.")
        if len(value_list) != len(set(value_list)):
            raise SpineIntegrityError("Values must be unique.")
        for value in value_list:
            if value is None:
                continue
            try:
                json.loads(value)
            except json.JSONDecodeError as err:
                raise SpineIntegrityError(
                    "Unable to decode value '{}' as JSON: {}".format(value, err)
                )
