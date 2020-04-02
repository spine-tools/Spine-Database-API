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

"""Provides :class:`.DatabaseMappingCheckMixin`.

:author: Manuel Marin (KTH)
:date:   11.8.2018
"""
# TODO: Review docstrings, they are almost good

from .exception import SpineIntegrityError
from .check_functions import (
    check_object_class,
    check_object,
    check_wide_relationship_class,
    check_wide_relationship,
    check_parameter_definition,
    check_parameter_value,
    check_parameter_tag,
    check_parameter_definition_tag,
    check_wide_parameter_value_list,
)


# NOTE: To check for an update we simulate the removal of the current instance,
# and then check for an insert of the updated instance.
class DatabaseMappingCheckMixin:
    """Provides methods to check whether insert and update operations violate Spine db integrity constraints.
    """

    @staticmethod
    def check_immutable_fields(current_item, item, immutable_fields):
        for field in immutable_fields:
            if field not in item:
                continue
            value = item[field]
            current_value = current_item[field]
            if value != current_value:
                raise SpineIntegrityError("Cannot change field {0} from {1} to {2}".format(field, current_value, value))

    def check_object_classes_for_insert(self, *items, strict=False):
        """Check whether object classes passed as argument respect integrity constraints
        for an insert operation.

        :param Iterable items: One or more Python :class:`dict` objects representing the items to be checked.

        :param bool strict: Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
            if one of the items violates an integrity constraint.

        :returns:
            - **checked_items** -- A list of items that passed the check.

            - **intgr_error_log** -- A list of :exc:`~.exception.SpineIntegrityError` instances corresponding
              to found violations.
        """
        intgr_error_log = []
        checked_items = list()
        object_class_names = {x.name: x.id for x in self.query(self.object_class_sq)}
        for item in items:
            try:
                check_object_class(item, object_class_names, self.object_class_type)
                checked_items.append(item)
                # If the check passes, append item to `object_class_names` for next iteration.
                object_class_names[item["name"]] = None
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_items, intgr_error_log

    def check_object_classes_for_update(self, *items, strict=False):
        """Check whether object classes passed as argument respect integrity constraints
        for an update operation.

        :param Iterable items: One or more Python :class:`dict` objects representing the items to be checked.

        :param bool strict: Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
            if one of the items violates an integrity constraint.

        :returns:
            - **checked_items** -- A list of items that passed the check.

            - **intgr_error_log** -- A list of :exc:`~.exception.SpineIntegrityError` instances corresponding
              to found violations.
        """
        intgr_error_log = []
        checked_items = list()
        object_class_dict = {x.id: {"name": x.name} for x in self.query(self.object_class_sq)}
        object_class_names = {x.name: x.id for x in self.query(self.object_class_sq)}
        for item in items:
            try:
                id_ = item["id"]
            except KeyError:
                msg = "Missing object class identifier."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            try:
                # Simulate removal of current instance
                updated_item = object_class_dict.pop(id_)
                del object_class_names[updated_item["name"]]
            except KeyError:
                msg = "Object class not found."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            # Check for an insert of the updated instance
            try:
                updated_item.update(item)
                check_object_class(updated_item, object_class_names, self.object_class_type)
                checked_items.append(item)
                # If the check passes, reinject the updated instance for next iteration.
                object_class_dict[id_] = updated_item
                object_class_names[updated_item["name"]] = id_
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_items, intgr_error_log

    def check_objects_for_insert(self, *items, strict=False):
        """Check whether objects passed as argument respect integrity constraints
        for an insert operation.

        :param Iterable items: One or more Python :class:`dict` objects representing the items to be checked.

        :param bool strict: Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
            if one of the items violates an integrity constraint.

        :returns:
            - **checked_items** -- A list of items that passed the check.

            - **intgr_error_log** -- A list of :exc:`~.exception.SpineIntegrityError` instances corresponding
              to found violations.
        """
        intgr_error_log = []
        checked_items = list()
        object_names = {(x.class_id, x.name): x.id for x in self.query(self.object_sq)}
        object_class_id_list = [x.id for x in self.query(self.object_class_sq)]
        for item in items:
            try:
                check_object(item, object_names, object_class_id_list, self.object_entity_type)
                checked_items.append(item)
                object_names[item["class_id"], item["name"]] = None
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_items, intgr_error_log

    def check_objects_for_update(self, *items, strict=False):
        """Check whether objects passed as argument respect integrity constraints
        for an update operation.

        :param Iterable items: One or more Python :class:`dict` objects representing the items to be checked.

        :param bool strict: Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
            if one of the items violates an integrity constraint.

        :returns:
            - **checked_items** -- A list of items that passed the check.

            - **intgr_error_log** -- A list of :exc:`~.exception.SpineIntegrityError` instances corresponding
              to found violations.
        """
        intgr_error_log = []
        checked_items = list()
        object_qry = self.query(self.object_sq)
        object_names = {(x.class_id, x.name): x.id for x in object_qry}
        object_dict = {x.id: {"name": x.name, "class_id": x.class_id} for x in object_qry}
        object_class_id_list = [x.id for x in self.query(self.object_class_sq)]
        for item in items:
            try:
                id_ = item["id"]
            except KeyError:
                msg = "Missing object identifier."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            try:
                updated_item = object_dict.pop(id_)
                del object_names[updated_item["class_id"], updated_item["name"]]
            except KeyError:
                msg = "Object not found."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            try:
                self.check_immutable_fields(updated_item, item, ("class_id",))
                updated_item.update(item)
                check_object(updated_item, object_names, object_class_id_list, self.object_entity_type)
                checked_items.append(item)
                object_dict[id_] = updated_item
                object_names[updated_item["class_id"], updated_item["name"]] = id_
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_items, intgr_error_log

    def check_wide_relationship_classes_for_insert(self, *wide_items, strict=False):
        """Check whether relationship classes passed as argument respect integrity constraints
        for an insert operation.

        :param Iterable wide_items: One or more Python :class:`dict` objects representing the items to be checked.

        :param bool strict: Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
            if one of the items violates an integrity constraint.

        :returns:
            - **checked_items** -- A list of items that passed the check.

            - **intgr_error_log** -- A list of :exc:`~.exception.SpineIntegrityError` instances corresponding
              to found violations.
        """
        intgr_error_log = []
        checked_wide_items = list()
        relationship_class_names = {x.name: x.id for x in self.query(self.wide_relationship_class_sq)}
        object_class_id_list = [x.id for x in self.query(self.object_class_sq)]
        for wide_item in wide_items:
            try:
                check_wide_relationship_class(
                    wide_item, relationship_class_names, object_class_id_list, self.relationship_class_type
                )
                wide_item["type_id"] = self.relationship_class_type
                checked_wide_items.append(wide_item)
                relationship_class_names[wide_item["name"]] = None
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_wide_items, intgr_error_log

    def check_wide_relationship_classes_for_update(self, *wide_items, strict=False):
        """Check whether relationship classes passed as argument respect integrity constraints
        for an update operation.

        :param Iterable wide_items: One or more Python :class:`dict` objects representing the items to be checked.

        :param bool strict: Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
            if one of the items violates an integrity constraint.

        :returns:
            - **checked_items** -- A list of items that passed the check.

            - **intgr_error_log** -- A list of :exc:`~.exception.SpineIntegrityError` instances corresponding
              to found violations.
        """
        intgr_error_log = []
        checked_wide_items = list()
        wide_relationship_class_qry = self.query(self.wide_relationship_class_sq)
        relationship_class_names = {x.name: x.id for x in wide_relationship_class_qry}
        relationship_class_dict = {
            x.id: {"name": x.name, "object_class_id_list": [int(y) for y in x.object_class_id_list.split(",")]}
            for x in wide_relationship_class_qry
        }
        object_class_id_list = [x.id for x in self.query(self.object_class_sq)]
        for wide_item in wide_items:
            try:
                id_ = wide_item["id"]
            except KeyError:
                msg = "Missing relationship class identifier."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            try:
                updated_wide_item = relationship_class_dict.pop(id_)
                del relationship_class_names[updated_wide_item["name"]]
            except KeyError:
                msg = "Relationship class not found."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            try:
                self.check_immutable_fields(updated_wide_item, wide_item, ("object_class_id_list",))
                updated_wide_item.update(wide_item)
                check_wide_relationship_class(
                    updated_wide_item, relationship_class_names, object_class_id_list, self.relationship_class_type
                )
                checked_wide_items.append(wide_item)
                relationship_class_dict[id_] = updated_wide_item
                relationship_class_names[updated_wide_item["name"]] = id_
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_wide_items, intgr_error_log

    def check_wide_relationships_for_insert(self, *wide_items, strict=False):
        """Check whether relationships passed as argument respect integrity constraints
        for an insert operation.

        :param Iterable wide_items: One or more Python :class:`dict` objects representing the items to be checked.

        :param bool strict: Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
            if one of the items violates an integrity constraint.

        :returns:
            - **checked_items** -- A list of items that passed the check.

            - **intgr_error_log** -- A list of :exc:`~.exception.SpineIntegrityError` instances corresponding
              to found violations.
        """
        intgr_error_log = []
        checked_wide_items = list()
        wide_relationship_qry = self.query(self.wide_relationship_sq)
        relationship_names = {(x.class_id, x.name): x.id for x in wide_relationship_qry}
        relationship_objects = {(x.class_id, x.object_id_list): x.id for x in wide_relationship_qry}
        relationship_class_dict = {
            x.id: {"object_class_id_list": [int(y) for y in x.object_class_id_list.split(",")], "name": x.name}
            for x in self.query(self.wide_relationship_class_sq)
        }
        object_dict = {x.id: {"class_id": x.class_id, "name": x.name} for x in self.query(self.object_sq)}
        for wide_item in wide_items:
            try:
                check_wide_relationship(
                    wide_item,
                    relationship_names,
                    relationship_objects,
                    relationship_class_dict,
                    object_dict,
                    self.relationship_entity_type,
                )
                wide_item["type_id"] = self.relationship_entity_type
                wide_item["object_class_id_list"] = [
                    object_dict[id_]["class_id"] for id_ in wide_item["object_id_list"]
                ]
                checked_wide_items.append(wide_item)
                relationship_names[wide_item["class_id"], wide_item["name"]] = None
                join_object_id_list = ",".join([str(x) for x in wide_item["object_id_list"]])
                relationship_objects[wide_item["class_id"], join_object_id_list] = None
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_wide_items, intgr_error_log

    def check_wide_relationships_for_update(self, *wide_items, strict=False):
        """Check whether relationships passed as argument respect integrity constraints
        for an update operation.

        :param Iterable wide_items: One or more Python :class:`dict` objects representing the items to be checked.

        :param bool strict: Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
            if one of the items violates an integrity constraint.

        :returns:
            - **checked_items** -- A list of items that passed the check.

            - **intgr_error_log** -- A list of :exc:`~.exception.SpineIntegrityError` instances corresponding
              to found violations.
        """
        intgr_error_log = []
        checked_wide_items = list()
        wide_relationship_qry = self.query(self.wide_relationship_sq)
        relationship_names = {(x.class_id, x.name): x.id for x in wide_relationship_qry}
        relationship_objects = {(x.class_id, x.object_id_list): x.id for x in wide_relationship_qry}
        relationship_dict = {
            x.id: {
                "class_id": x.class_id,
                "name": x.name,
                "object_id_list": [int(y) for y in x.object_id_list.split(",")],
            }
            for x in wide_relationship_qry
        }
        relationship_class_dict = {
            x.id: {"object_class_id_list": [int(y) for y in x.object_class_id_list.split(",")], "name": x.name}
            for x in self.query(self.wide_relationship_class_sq)
        }
        object_dict = {x.id: {"class_id": x.class_id, "name": x.name} for x in self.query(self.object_sq)}
        for wide_item in wide_items:
            try:
                id_ = wide_item["id"]
            except KeyError:
                msg = "Missing relationship identifier."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            try:
                updated_wide_item = relationship_dict.pop(id_)
                del relationship_names[updated_wide_item["class_id"], updated_wide_item["name"]]
                join_object_id_list = ",".join([str(x) for x in updated_wide_item["object_id_list"]])
                del relationship_objects[updated_wide_item["class_id"], join_object_id_list]
            except KeyError:
                msg = "Relationship not found."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            try:
                self.check_immutable_fields(updated_wide_item, wide_item, ("class_id",))
                updated_wide_item.update(wide_item)
                check_wide_relationship(
                    updated_wide_item,
                    relationship_names,
                    relationship_objects,
                    relationship_class_dict,
                    object_dict,
                    self.relationship_entity_type,
                )
                wide_item["type_id"] = self.relationship_entity_type
                wide_item["object_class_id_list"] = [
                    object_dict[id_]["class_id"] for id_ in wide_item["object_id_list"]
                ]
                checked_wide_items.append(wide_item)
                relationship_dict[id_] = updated_wide_item
                relationship_names[updated_wide_item["class_id"], updated_wide_item["name"]] = id_
                join_object_id_list = ",".join([str(x) for x in updated_wide_item["object_id_list"]])
                relationship_objects[updated_wide_item["class_id"], join_object_id_list] = id_
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_wide_items, intgr_error_log

    def check_parameter_definitions_for_insert(self, *items, strict=False):
        """Check whether parameter definitions passed as argument respect integrity constraints
        for an insert operation.

        :param Iterable items: One or more Python :class:`dict` objects representing the items to be checked.

        :param bool strict: Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
            if one of the items violates an integrity constraint.

        :returns:
            - **checked_items** -- A list of items that passed the check.

            - **intgr_error_log** -- A list of :exc:`~.exception.SpineIntegrityError` instances corresponding
              to found violations.
        """
        intgr_error_log = []
        checked_items = list()
        parameter_definition_names = {
            (x.entity_class_id, x.name): x.id for x in self.query(self.parameter_definition_sq)
        }
        object_class_ids = {x.id for x in self.query(self.object_class_sq)}
        relationship_class_ids = {x.id for x in self.query(self.wide_relationship_class_sq)}
        entity_class_ids = object_class_ids | relationship_class_ids
        parameter_value_list_dict = {x.id: x.value_list for x in self.query(self.wide_parameter_value_list_sq)}
        for item in items:
            checked_item = item.copy()
            object_class_id = checked_item.pop("object_class_id", None)
            relationship_class_id = checked_item.pop("relationship_class_id", None)
            if object_class_id and relationship_class_id:
                e = SpineIntegrityError("Can't associate a parameter to both an object and a relationship class.")
                if strict:
                    raise e
                intgr_error_log.append(e)
                continue
            if object_class_id:
                class_ids = object_class_ids
            elif relationship_class_id:
                class_ids = relationship_class_ids
            else:
                class_ids = entity_class_ids
            entity_class_id = checked_item["entity_class_id"] = (
                object_class_id or relationship_class_id or checked_item.get("entity_class_id")
            )
            try:
                check_parameter_definition(
                    checked_item, parameter_definition_names, class_ids, parameter_value_list_dict
                )
                parameter_definition_names[entity_class_id, checked_item["name"]] = None
                checked_items.append(checked_item)
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_items, intgr_error_log

    def check_parameter_definitions_for_update(self, *items, strict=False):
        """Check whether parameter definitions passed as argument respect integrity constraints
        for an update operation.

        :param Iterable items: One or more Python :class:`dict` objects representing the items to be checked.

        :param bool strict: Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
            if one of the items violates an integrity constraint.

        :returns:
            - **checked_items** -- A list of items that passed the check.

            - **intgr_error_log** -- A list of :exc:`~.exception.SpineIntegrityError` instances corresponding
              to found violations.
        """
        intgr_error_log = []
        checked_items = list()
        parameter_definition_names = {
            (x.entity_class_id, x.name): x.id for x in self.query(self.parameter_definition_sq)
        }
        parameter_definition_dict = {
            x.id: {
                "name": x.name,
                "entity_class_id": x.entity_class_id,
                "object_class_id": x.object_class_id,
                "relationship_class_id": x.relationship_class_id,
                "parameter_value_list_id": x.parameter_value_list_id,
                "default_value": x.default_value,
            }
            for x in self.query(self.parameter_definition_sq)
        }
        entity_class_ids = {x.id for x in self.query(self.entity_class_sq)}
        parameter_value_list_dict = {x.id: x.value_list for x in self.query(self.wide_parameter_value_list_sq)}
        for item in items:
            try:
                id_ = item["id"]
            except KeyError:
                msg = "Missing parameter definition identifier."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            try:
                updated_item = parameter_definition_dict.pop(id_)
                del parameter_definition_names[updated_item["entity_class_id"], updated_item["name"]]
            except KeyError:
                msg = "Parameter not found."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            try:
                self.check_immutable_fields(
                    updated_item, item, ("entity_class_id", "object_class_id", "relationship_class_id")
                )
                updated_item.update(item)
                check_parameter_definition(
                    updated_item, parameter_definition_names, entity_class_ids, parameter_value_list_dict
                )
                parameter_definition_names[updated_item["entity_class_id"], updated_item["name"]] = id_
                parameter_definition_dict[id_] = updated_item
                checked_items.append(item)
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_items, intgr_error_log

    def check_parameter_values_for_insert(self, *items, strict=False):
        """Check whether parameter values passed as argument respect integrity constraints
        for an insert operation.

        :param Iterable items: One or more Python :class:`dict` objects representing the items to be checked.

        :param bool strict: Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
            if one of the items violates an integrity constraint.

        :returns:
            - **checked_items** -- A list of items that passed the check.

            - **intgr_error_log** -- A list of :exc:`~.exception.SpineIntegrityError` instances corresponding
              to found violations.
        """
        intgr_error_log = []
        checked_items = list()
        parameter_values = {(x.entity_id, x.parameter_definition_id): x.id for x in self.query(self.parameter_value_sq)}
        parameter_definition_dict = {
            x.id: {
                "name": x.name,
                "entity_class_id": x.entity_class_id,
                "parameter_value_list_id": x.parameter_value_list_id,
            }
            for x in self.query(self.parameter_definition_sq)
        }
        entity_dict = {x.id: {"class_id": x.class_id, "name": x.name} for x in self.query(self.entity_sq)}
        parameter_value_list_dict = {x.id: x.value_list for x in self.query(self.wide_parameter_value_list_sq)}
        for item in items:
            checked_item = item.copy()
            checked_item["entity_class_id"] = (
                checked_item.pop("object_class_id", None)
                or checked_item.pop("relationship_class_id", None)
                or checked_item.get("entity_class_id")
            )
            entity_id = checked_item["entity_id"] = (
                checked_item.pop("object_id", None)
                or checked_item.pop("relationship_id", None)
                or checked_item.get("entity_id")
            )
            try:
                check_parameter_value(
                    checked_item, parameter_values, parameter_definition_dict, entity_dict, parameter_value_list_dict
                )
                parameter_values[entity_id, checked_item["parameter_definition_id"]] = None
                checked_items.append(checked_item)
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_items, intgr_error_log

    def check_parameter_values_for_update(self, *items, strict=False):
        """Check whether parameter values passed as argument respect integrity constraints
        for an update operation.

        :param Iterable items: One or more Python :class:`dict` objects representing the items to be checked.

        :param bool strict: Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
            if one of the items violates an integrity constraint.

        :returns:
            - **checked_items** -- A list of items that passed the check.

            - **intgr_error_log** -- A list of :exc:`~.exception.SpineIntegrityError` instances corresponding
              to found violations.
        """
        intgr_error_log = []
        checked_items = list()
        parameter_value_dict = {
            x.id: {
                "parameter_definition_id": x.parameter_definition_id,
                "entity_id": x.entity_id,
                "object_id": x.object_id,
                "relationship_id": x.relationship_id,
                "entity_class_id": x.entity_class_id,
                "object_class_id": x.object_class_id,
                "relationship_class_id": x.relationship_class_id,
            }
            for x in self.query(self.parameter_value_sq)
        }
        parameter_values = {(x.entity_id, x.parameter_definition_id): x.id for x in self.query(self.parameter_value_sq)}
        parameter_definition_dict = {
            x.id: {
                "name": x.name,
                "entity_class_id": x.entity_class_id,
                "object_class_id": x.object_class_id,
                "relationship_class_id": x.relationship_class_id,
                "parameter_value_list_id": x.parameter_value_list_id,
            }
            for x in self.query(self.parameter_definition_sq)
        }
        entity_dict = {x.id: {"class_id": x.class_id, "name": x.name} for x in self.query(self.entity_sq)}
        parameter_value_list_dict = {x.id: x.value_list for x in self.query(self.wide_parameter_value_list_sq)}
        for item in items:
            try:
                id_ = item["id"]
            except KeyError:
                msg = "Missing parameter value identifier."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            try:
                updated_item = parameter_value_dict.pop(id_)
                del parameter_values[updated_item["entity_id"], updated_item["parameter_definition_id"]]
            except KeyError:
                msg = "Parameter value not found."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            try:
                self.check_immutable_fields(
                    updated_item,
                    item,
                    (
                        "entity_class_id",
                        "object_class_id",
                        "relationship_class_id",
                        "entity_id",
                        "object_id",
                        "relationship_id",
                        "parameter_definition_id",
                    ),
                )
                updated_item.update(item)
                check_parameter_value(
                    updated_item, parameter_values, parameter_definition_dict, entity_dict, parameter_value_list_dict
                )
                parameter_value_dict[id_] = updated_item
                parameter_values[updated_item["entity_id"], updated_item["parameter_definition_id"]] = id_
                checked_items.append(item)
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_items, intgr_error_log

    def check_parameter_tags_for_insert(self, *items, strict=False):
        """Check whether parameter tags passed as argument respect integrity constraints
        for an insert operation.

        :param Iterable items: One or more Python :class:`dict` objects representing the items to be checked.

        :param bool strict: Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
            if one of the items violates an integrity constraint.

        :returns:
            - **checked_items** -- A list of items that passed the check.

            - **intgr_error_log** -- A list of :exc:`~.exception.SpineIntegrityError` instances corresponding
              to found violations.
        """
        intgr_error_log = []
        checked_items = list()
        parameter_tags = {x.tag: x.id for x in self.query(self.parameter_tag_sq)}
        for item in items:
            try:
                check_parameter_tag(item, parameter_tags)
                checked_items.append(item)
                parameter_tags[item["tag"]] = None
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_items, intgr_error_log

    def check_parameter_tags_for_update(self, *items, strict=False):
        """Check whether parameter tags passed as argument respect integrity constraints
        for an update operation.

        :param Iterable items: One or more Python :class:`dict` objects representing the items to be checked.

        :param bool strict: Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
            if one of the items violates an integrity constraint.

        :returns:
            - **checked_items** -- A list of items that passed the check.

            - **intgr_error_log** -- A list of :exc:`~.exception.SpineIntegrityError` instances corresponding
              to found violations.
        """
        intgr_error_log = []
        checked_items = list()
        parameter_tag_dict = {x.id: {"tag": x.tag} for x in self.query(self.parameter_tag_sq)}
        parameter_tags = {x.tag: x.id for x in self.query(self.parameter_tag_sq)}
        for item in items:
            try:
                id_ = item["id"]
            except KeyError:
                msg = "Missing parameter tag identifier."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            try:
                # 'Remove' current instance
                updated_item = parameter_tag_dict.pop(id_)
                del parameter_tags[updated_item["tag"]]
            except KeyError:
                msg = "Parameter tag not found."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            # Check for an insert of the updated instance
            try:
                updated_item.update(item)
                check_parameter_tag(updated_item, parameter_tags)
                checked_items.append(item)
                parameter_tag_dict[id_] = updated_item
                parameter_tags[updated_item["tag"]] = id_
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_items, intgr_error_log

    def check_parameter_definition_tags_for_insert(self, *items, strict=False):
        """Check whether parameter definition tag items passed as argument respect integrity constraints
        for an insert operation.

        :param Iterable items: One or more Python :class:`dict` objects representing the items to be checked.

        :param bool strict: Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
            if one of the items violates an integrity constraint.

        :returns:
            - **checked_items** -- A list of items that passed the check.

            - **intgr_error_log** -- A list of :exc:`~.exception.SpineIntegrityError` instances corresponding
              to found violations.
        """
        intgr_error_log = []
        checked_items = list()
        parameter_definition_tags = {
            (x.parameter_definition_id, x.parameter_tag_id): x.id for x in self.query(self.parameter_definition_tag_sq)
        }
        parameter_name_dict = {x.id: x.name for x in self.query(self.parameter_definition_sq)}
        parameter_tag_dict = {x.id: x.tag for x in self.query(self.parameter_tag_sq)}
        for item in items:
            try:
                check_parameter_definition_tag(item, parameter_definition_tags, parameter_name_dict, parameter_tag_dict)
                checked_items.append(item)
                parameter_definition_tags[item["parameter_definition_id"], item["parameter_tag_id"]] = None
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_items, intgr_error_log

    def check_wide_parameter_value_lists_for_insert(self, *wide_items, strict=False):
        """Check whether parameter value-lists passed as argument respect integrity constraints
        for an insert operation.

        :param Iterable wide_items: One or more Python :class:`dict` objects representing the items to be checked.

        :param bool strict: Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
            if one of the items violates an integrity constraint.

        :returns:
            - **checked_items** -- A list of items that passed the check.

            - **intgr_error_log** -- A list of :exc:`~.exception.SpineIntegrityError` instances corresponding
              to found violations.
        """
        intgr_error_log = []
        checked_wide_items = list()
        parameter_value_list_names = {x.name: x.id for x in self.query(self.wide_parameter_value_list_sq)}
        for wide_item in wide_items:
            try:
                check_wide_parameter_value_list(wide_item, parameter_value_list_names)
                checked_wide_items.append(wide_item)
                parameter_value_list_names[wide_item["name"]] = None
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_wide_items, intgr_error_log

    def check_wide_parameter_value_lists_for_update(self, *wide_items, strict=False):
        """Check whether parameter value-lists passed as argument respect integrity constraints
        for an update operation.

        :param Iterable wide_items: One or more Python :class:`dict` objects representing the items to be checked.

        :param bool strict: Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
            if one of the items violates an integrity constraint.

        :returns:
            - **checked_items** -- A list of items that passed the check.

            - **intgr_error_log** -- A list of :exc:`~.exception.SpineIntegrityError` instances corresponding
              to found violations.
        """
        intgr_error_log = []
        checked_wide_items = list()
        parameter_value_list_dict = {
            x.id: {"name": x.name, "value_list": x.value_list.split(",")}
            for x in self.query(self.wide_parameter_value_list_sq)
        }
        parameter_value_list_names = {x.name: x.id for x in self.query(self.wide_parameter_value_list_sq)}
        for wide_item in wide_items:
            try:
                id_ = wide_item["id"]
            except KeyError:
                msg = "Missing parameter value list identifier."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            try:
                # 'Remove' current instance
                updated_wide_item = parameter_value_list_dict.pop(id_)
                del parameter_value_list_names[updated_wide_item["name"]]
            except KeyError:
                msg = "Parameter value list not found."
                if strict:
                    raise SpineIntegrityError(msg)
                intgr_error_log.append(SpineIntegrityError(msg))
                continue
            # Check for an insert of the updated instance
            try:
                updated_wide_item.update(wide_item)
                check_wide_parameter_value_list(updated_wide_item, parameter_value_list_names)
                checked_wide_items.append(wide_item)
                parameter_value_list_dict[id_] = updated_wide_item
                parameter_value_list_names[updated_wide_item["name"]] = id_
            except SpineIntegrityError as e:
                if strict:
                    raise e
                intgr_error_log.append(e)
        return checked_wide_items, intgr_error_log
