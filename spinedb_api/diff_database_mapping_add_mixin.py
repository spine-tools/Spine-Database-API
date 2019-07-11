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

"""Provides :class:`.DiffDatabaseMappingAddMixin`.

:author: Manuel Marin (KTH)
:date:   11.8.2018
"""
# TODO: improve docstrings

import warnings
from sqlalchemy import func, MetaData, Table, Column, Integer, String, DateTime, null
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.exc import DBAPIError
from .exception import SpineDBAPIError, SpineIntegrityError
from datetime import datetime, timezone


class DiffDatabaseMappingAddMixin:
    """Provides methods to stage ``INSERT`` operations over a Spine db.
    """

    def __init__(self, *args, **kwargs):
        """Initialize class."""
        super().__init__(*args, **kwargs)
        self.NextId = None
        self._init_next_id()

    def _init_next_id(self):
        """Create `next_id` table if not exists and map it."""
        # TODO: Does this work? What happens if there's already a next_id table with a different definition?
        # Create table
        metadata = MetaData()
        next_id_table = Table(
            "next_id",
            metadata,
            Column("user", String(155), primary_key=True),
            Column("date", String(155), primary_key=True),
            Column("object_class_id", Integer, server_default=null()),
            Column("object_id", Integer, server_default=null()),
            Column("relationship_class_id", Integer, server_default=null()),
            Column("relationship_id", Integer, server_default=null()),
            Column("parameter_definition_id", Integer, server_default=null()),
            Column("parameter_value_id", Integer, server_default=null()),
            Column("parameter_tag_id", Integer, server_default=null()),
            Column("parameter_value_list_id", Integer, server_default=null()),
            Column("parameter_definition_tag_id", Integer, server_default=null()),
        )
        next_id_table.create(self.engine, checkfirst=True)
        # Create mapping...
        Base = automap_base(metadata=metadata)
        Base.prepare()
        try:
            self.NextId = Base.classes.next_id
        except (AttributeError, NoSuchTableError):
            raise SpineTableNotFoundError("next_id", self.db_url)

    def _next_id_with_lock(self):
        """A 'next_id' item to use for adding new items."""
        next_id = self.query(self.NextId).one_or_none()
        if next_id:
            next_id.user = self.username
            next_id.date = datetime.utcnow()
        else:
            next_id = self.NextId(user=self.username, date=datetime.utcnow())
            self.session.add(next_id)
        try:
            # TODO: This flush is supposed to lock the record, so no one can steal our ids.... does it work?
            self.session.flush()
        except DBAPIError as e:
            # TODO: Find a way to try this again, or wait till unlocked
            # Maybe listen for an event?
            self.session.rollback()
            raise SpineDBAPIError("Unable to get next id: {}".format(e.orig.args))
        return self.query(self.NextId).one_or_none()

    def add_object_classes(self, *item_list, strict=False, return_dups=False):
        """Stage object class items for insertion.

        :param Iterable item_list: One or more Python :class:`dict` objects representing the items to be inserted.
        :param bool strict: Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
            if the insertion of one of the items violates an integrity constraint.
        :param bool return_dups: Whether or not already existing and duplicated entries should also be returned.

        :returns:
            - **new_item_list** -- A list of items succesfully staged for insertion.
            - **intgr_error_log** -- A list of :exc:`~.exception.SpineIntegrityError` instances corresponding
              to found violations.
        """
        checked_item_list, intgr_error_log = self.check_object_classes_for_insert(*item_list, strict=strict)
        id_list = self._add_object_classes(*checked_item_list)
        if return_dups:
            id_list.update(set(x.id for x in intgr_error_log if x.id))
        new_item_list = self.object_class_list(id_list=id_list)
        return new_item_list, intgr_error_log

    def _add_object_classes(self, *item_list):
        """Add object classes to database without checking integrity.

        Args:
            item_list (iter): list of dictionaries which correspond to the instances to add
            strict (bool): if True SpineIntegrityError are raised. Otherwise
                they are catched and returned as a log

        Returns:
            id_list (set): added instances' ids
        """
        next_id = self._next_id_with_lock()
        if next_id.object_class_id:
            id = next_id.object_class_id
        else:
            max_id = self.query(func.max(self.ObjectClass.id)).scalar()
            id = max_id + 1 if max_id else 1
        try:
            items_to_add = list()
            id_list = set(range(id, id + len(item_list)))
            for item in item_list:
                item["id"] = id
                items_to_add.append(item)
                id += 1
            self.session.bulk_insert_mappings(self.DiffObjectClass, items_to_add)
            next_id.object_class_id = id
            self.session.commit()
            self.added_item_id["object_class"].update(id_list)
            return id_list
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while inserting object classes: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def add_objects(self, *item_list, strict=False, return_dups=False):
        """Stage object items for insertion.

        :param Iterable item_list: One or more Python :class:`dict` objects representing the items to be inserted.
        :param bool strict: Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
            if the insertion of one of the items violates an integrity constraint.
        :param bool return_dups: Whether or not already existing and duplicated entries should also be returned.

        :returns:
            - **new_item_list** -- A list of items succesfully staged for insertion.
            - **intgr_error_log** -- A list of :exc:`~.exception.SpineIntegrityError` instances corresponding
              to found violations.
        """
        checked_item_list, intgr_error_log = self.check_objects_for_insert(*item_list, strict=strict)
        id_list = self._add_objects(*checked_item_list)
        if return_dups:
            id_list.update(set(x.id for x in intgr_error_log if x.id))
        new_item_list = self.object_list(id_list=id_list)
        return new_item_list, intgr_error_log

    def _add_objects(self, *item_list):
        """Add objects to database without checking integrity.

        Args:
            item_list (iter): list of dictionaries which correspond to the instances to add

        Returns:
            id_list (set): added instances' ids
        """
        next_id = self._next_id_with_lock()
        if next_id.object_id:
            id = next_id.object_id
        else:
            max_id = self.query(func.max(self.Object.id)).scalar()
            id = max_id + 1 if max_id else 1
        try:
            items_to_add = list()
            id_list = set(range(id, id + len(item_list)))
            for item in item_list:
                item["id"] = id
                items_to_add.append(item)
                id += 1
            self.session.bulk_insert_mappings(self.DiffObject, items_to_add)
            next_id.object_id = id
            self.session.commit()
            self.added_item_id["object"].update(id_list)
            return id_list
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while inserting objects: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def add_wide_relationship_classes(self, *wide_item_list, strict=False, return_dups=False):
        """Stage relationship class items for insertion.

        :param Iterable item_list: One or more Python :class:`dict` objects representing the items to be inserted.
        :param bool strict: Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
            if the insertion of one of the items violates an integrity constraint.
        :param bool return_dups: Whether or not already existing and duplicated entries should also be returned.

        :returns:
            - **new_item_list** -- A list of items succesfully staged for insertion.
            - **intgr_error_log** -- A list of :exc:`~.exception.SpineIntegrityError` instances corresponding
              to found violations.
        """
        checked_wide_item_list, intgr_error_log = self.check_wide_relationship_classes_for_insert(
            *wide_item_list, strict=strict
        )
        id_list = self._add_wide_relationship_classes(*checked_wide_item_list)
        if return_dups:
            id_list.update(set(x.id for x in intgr_error_log if x.id))
        new_item_list = self.wide_relationship_class_list(id_list=id_list)
        return new_item_list, intgr_error_log

    def _add_wide_relationship_classes(self, *wide_item_list):
        """Add relationship classes to database without checking integrity.

        Args:
            wide_item_list (iter): list of dictionaries which correspond to the instances to add
            strict (bool): if True SpineIntegrityError are raised. Otherwise
                they are catched and returned as a log

        Returns:
            id_list (set): added instances' ids
        """
        next_id = self._next_id_with_lock()
        if next_id.relationship_class_id:
            id = next_id.relationship_class_id
        else:
            max_id = self.query(func.max(self.RelationshipClass.id)).scalar()
            id = max_id + 1 if max_id else 1
        try:
            items_to_add = list()
            id_list = set(range(id, id + len(wide_item_list)))
            for wide_item in wide_item_list:
                for dimension, object_class_id in enumerate(wide_item["object_class_id_list"]):
                    narrow_item = {
                        "id": id,
                        "dimension": dimension,
                        "object_class_id": object_class_id,
                        "name": wide_item["name"],
                    }
                    items_to_add.append(narrow_item)
                id += 1
            self.session.bulk_insert_mappings(self.DiffRelationshipClass, items_to_add)
            next_id.relationship_class_id = id
            self.session.commit()
            self.added_item_id["relationship_class"].update(id_list)
            return id_list
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while inserting relationship classes: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def add_wide_relationships(self, *wide_item_list, strict=False, return_dups=False):
        """Stage relationship items for insertion.

        :param Iterable item_list: One or more Python :class:`dict` objects representing the items to be inserted.
        :param bool strict: Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
            if the insertion of one of the items violates an integrity constraint.
        :param bool return_dups: Whether or not already existing and duplicated entries should also be returned.

        :returns:
            - **new_item_list** -- A list of items succesfully staged for insertion.
            - **intgr_error_log** -- A list of :exc:`~.exception.SpineIntegrityError` instances corresponding
              to found violations.
        """
        checked_wide_item_list, intgr_error_log = self.check_wide_relationships_for_insert(
            *wide_item_list, strict=strict
        )
        id_list = self._add_wide_relationships(*checked_wide_item_list)
        if return_dups:
            id_list.update(set(x.id for x in intgr_error_log if x.id))
        new_item_list = self.wide_relationship_list(id_list=id_list)
        return new_item_list, intgr_error_log

    def _add_wide_relationships(self, *wide_item_list):
        """Add relationships to database without checking integrity.

        Args:
            wide_item_list (iter): list of dictionaries which correspond to the instances to add

        Returns:
            id_list (set): added instances' ids
        """
        next_id = self._next_id_with_lock()
        if next_id.relationship_id:
            id = next_id.relationship_id
        else:
            max_id = self.query(func.max(self.Relationship.id)).scalar()
            id = max_id + 1 if max_id else 1
        try:
            items_to_add = list()
            id_list = set(range(id, id + len(wide_item_list)))
            for wide_item in wide_item_list:
                for dimension, object_id in enumerate(wide_item["object_id_list"]):
                    narrow_item = {
                        "id": id,
                        "class_id": wide_item["class_id"],
                        "dimension": dimension,
                        "object_id": object_id,
                        "name": wide_item["name"],
                    }
                    items_to_add.append(narrow_item)
                id += 1
            self.session.bulk_insert_mappings(self.DiffRelationship, items_to_add)
            next_id.relationship_id = id
            self.session.commit()
            self.added_item_id["relationship"].update(id_list)
            return id_list
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while inserting relationships: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def add_parameter_definitions(self, *item_list, strict=False, return_dups=False):
        """Stage parameter definition items for insertion.

        :param Iterable item_list: One or more Python :class:`dict` objects representing the items to be inserted.
        :param bool strict: Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
            if the insertion of one of the items violates an integrity constraint.
        :param bool return_dups: Whether or not already existing and duplicated entries should also be returned.

        :returns:
            - **new_item_list** -- A list of items succesfully staged for insertion.
            - **intgr_error_log** -- A list of :exc:`~.exception.SpineIntegrityError` instances corresponding
              to found violations.
        """
        checked_item_list, intgr_error_log = self.check_parameter_definitions_for_insert(*item_list, strict=strict)
        id_list = self._add_parameter_definitions(*checked_item_list)
        if return_dups:
            id_list.update(set(x.id for x in intgr_error_log if x.id))
        new_item_list = self.parameter_definition_list(id_list=id_list)
        return new_item_list, intgr_error_log

    def _add_parameter_definitions(self, *item_list):
        """Add parameters to database without checking integrity.

        Args:
            item_list (iter): list of dictionaries which correspond to the instances to add

        Returns:
            id_list (set): added instances' ids
        """
        next_id = self._next_id_with_lock()
        if next_id.parameter_definition_id:
            id = next_id.parameter_definition_id
        else:
            max_id = self.query(func.max(self.ParameterDefinition.id)).scalar()
            id = max_id + 1 if max_id else 1
        try:
            items_to_add = list()
            id_list = set(range(id, id + len(item_list)))
            for item in item_list:
                item["id"] = id
                items_to_add.append(item)
                id += 1
            self.session.bulk_insert_mappings(self.DiffParameterDefinition, items_to_add)
            next_id.parameter_definition_id = id
            self.session.commit()
            self.added_item_id["parameter_definition"].update(id_list)
            return id_list
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while inserting parameters: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def add_parameter_values(self, *item_list, strict=False, return_dups=False):
        """Stage parameter values items for insertion.

        :param Iterable item_list: One or more Python :class:`dict` objects representing the items to be inserted.
        :param bool strict: Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
            if the insertion of one of the items violates an integrity constraint.
        :param bool return_dups: Whether or not already existing and duplicated entries should also be returned.

        :returns:
            - **new_item_list** -- A list of items succesfully staged for insertion.
            - **intgr_error_log** -- A list of :exc:`~.exception.SpineIntegrityError` instances corresponding
              to found violations.
        """
        checked_item_list, intgr_error_log = self.check_parameter_values_for_insert(*item_list, strict=strict)
        id_list = self._add_parameter_values(*checked_item_list)
        if return_dups:
            id_list.update(set(x.id for x in intgr_error_log if x.id))
        new_item_list = self.parameter_value_list(id_list=id_list)
        return new_item_list, intgr_error_log

    def _add_parameter_values(self, *item_list):
        """Add parameter values to database without checking integrity.

        Returns:
            id_list (set): added instances' ids
        """
        next_id = self._next_id_with_lock()
        if next_id.parameter_value_id:
            id = next_id.parameter_value_id
        else:
            max_id = self.query(func.max(self.ParameterValue.id)).scalar()
            id = max_id + 1 if max_id else 1
        try:
            items_to_add = list()
            id_list = set(range(id, id + len(item_list)))
            for item in item_list:
                item["id"] = id
                items_to_add.append(item)
                id += 1
            self.session.bulk_insert_mappings(self.DiffParameterValue, items_to_add)
            next_id.parameter_value_id = id
            self.session.commit()
            self.added_item_id["parameter_value"].update(id_list)
            return id_list
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while inserting parameter values: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def add_parameter_tags(self, *item_list, strict=False, return_dups=False):
        """Stage parameter tag items for insertion.

        :param Iterable item_list: One or more Python :class:`dict` objects representing the items to be inserted.
        :param bool strict: Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
            if the insertion of one of the items violates an integrity constraint.
        :param bool return_dups: Whether or not already existing and duplicated entries should also be returned.

        :returns:
            - **new_item_list** -- A list of items succesfully staged for insertion.
            - **intgr_error_log** -- A list of :exc:`~.exception.SpineIntegrityError` instances corresponding
              to found violations.
        """
        checked_item_list, intgr_error_log = self.check_parameter_tags_for_insert(*item_list, strict=strict)
        id_list = self._add_parameter_tags(*checked_item_list)
        if return_dups:
            id_list.update(set(x.id for x in intgr_error_log if x.id))
        new_item_list = self.parameter_tag_list(id_list=id_list)
        return new_item_list, intgr_error_log

    def _add_parameter_tags(self, *item_list):
        """Add parameter tags to database without checking integrity.

        Returns:
            id_list (set): added instances' ids
        """
        next_id = self._next_id_with_lock()
        if next_id.parameter_tag_id:
            id = next_id.parameter_tag_id
        else:
            max_id = self.query(func.max(self.ParameterTag.id)).scalar()
            id = max_id + 1 if max_id else 1
        try:
            items_to_add = list()
            id_list = set(range(id, id + len(item_list)))
            for item in item_list:
                item["id"] = id
                items_to_add.append(item)
                id += 1
            self.session.bulk_insert_mappings(self.DiffParameterTag, items_to_add)
            next_id.parameter_tag_id = id
            self.session.commit()
            self.added_item_id["parameter_tag"].update(id_list)
            return id_list
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while inserting parameter tags: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def add_parameter_definition_tags(self, *item_list, strict=False, return_dups=False):
        """Stage parameter definition tag items for insertion.

        :param Iterable item_list: One or more Python :class:`dict` objects representing the items to be inserted.
        :param bool strict: Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
            if the insertion of one of the items violates an integrity constraint.
        :param bool return_dups: Whether or not already existing and duplicated entries should also be returned.

        :returns:
            - **new_item_list** -- A list of items succesfully staged for insertion.
            - **intgr_error_log** -- A list of :exc:`~.exception.SpineIntegrityError` instances corresponding
              to found violations.
        """
        checked_item_list, intgr_error_log = self.check_parameter_definition_tags_for_insert(*item_list, strict=strict)
        id_list = self._add_parameter_definition_tags(*checked_item_list)
        if return_dups:
            id_list.update(set(x.id for x in intgr_error_log if x.id))
        new_item_list = self.parameter_definition_tag_list(id_list=id_list)
        return new_item_list, intgr_error_log

    def _add_parameter_definition_tags(self, *item_list):
        """Add parameter definition tags to database without checking integrity.

        Returns:
            id_list (set): added instances' ids
        """
        next_id = self._next_id_with_lock()
        if next_id.parameter_definition_tag_id:
            id = next_id.parameter_definition_tag_id
        else:
            max_id = self.query(func.max(self.ParameterDefinitionTag.id)).scalar()
            id = max_id + 1 if max_id else 1
        try:
            items_to_add = list()
            id_list = set(range(id, id + len(item_list)))
            for item in item_list:
                item["id"] = id
                items_to_add.append(item)
                id += 1
            self.session.bulk_insert_mappings(self.DiffParameterDefinitionTag, items_to_add)
            next_id.parameter_definition_tag_id = id
            self.session.commit()
            self.added_item_id["parameter_definition_tag"].update(id_list)
            return id_list
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while inserting parameter definition tags: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def add_wide_parameter_value_lists(self, *wide_item_list, strict=False, return_dups=False):
        """Stage parameter value-list items for insertion.

        :param Iterable item_list: One or more Python :class:`dict` objects representing the items to be inserted.
        :param bool strict: Whether or not the method should raise :exc:`~.exception.SpineIntegrityError`
            if the insertion of one of the items violates an integrity constraint.
        :param bool return_dups: Whether or not already existing and duplicated entries should also be returned.

        :returns:
            - **new_item_list** -- A list of items succesfully staged for insertion.
            - **intgr_error_log** -- A list of :exc:`~.exception.SpineIntegrityError` instances corresponding
              to found violations.
        """
        checked_wide_item_list, intgr_error_log = self.check_wide_parameter_value_lists_for_insert(
            *wide_item_list, strict=strict
        )
        id_list = self._add_wide_parameter_value_lists(*checked_wide_item_list)
        if return_dups:
            id_list.update(set(x.id for x in intgr_error_log if x.id))
        new_item_list = self.wide_parameter_value_list_list(id_list=id_list)
        return new_item_list, intgr_error_log

    def _add_wide_parameter_value_lists(self, *wide_item_list):
        """Add wide parameter value_lists to database without checking integrity.

        Returns:
            id_list (set): added instances' ids
        """
        next_id = self._next_id_with_lock()
        if next_id.parameter_value_list_id:
            id = next_id.parameter_value_list_id
        else:
            max_id = self.query(func.max(self.ParameterValueList.id)).scalar()
            id = max_id + 1 if max_id else 1
        try:
            items_to_add = list()
            id_list = set(range(id, id + len(wide_item_list)))
            for wide_item in wide_item_list:
                for k, value in enumerate(wide_item["value_list"]):
                    narrow_item = {"id": id, "name": wide_item["name"], "value_index": k, "value": value}
                    items_to_add.append(narrow_item)
                id += 1
            self.session.bulk_insert_mappings(self.DiffParameterValueList, items_to_add)
            next_id.parameter_value_list_id = id
            self.session.commit()
            self.added_item_id["parameter_value_list"].update(id_list)
            return id_list
        except DBAPIError as e:
            self.session.rollback()
            msg = "DBAPIError while inserting parameter value lists: {}".format(e.orig.args)
            raise SpineDBAPIError(msg)

    def add_object_class(self, **kwargs):
        """Stage an object class item for insertion.

        :raises SpineIntegrityError: if the insertion of the item violates an integrity constraint.

        :returns:
            - **new_item** -- The item succesfully staged for insertion.

        :rtype: :class:`~sqlalchemy.util.KeyedTuple`
        """
        return self.add_object_classes(kwargs, strict=True)[0].one_or_none()

    def add_object(self, **kwargs):
        """Stage an object item for insertion.

        :raises SpineIntegrityError: if the insertion of the item violates an integrity constraint.

        :returns:
            - **new_item** -- The item succesfully staged for insertion.

        :rtype: :class:`~sqlalchemy.util.KeyedTuple`
        """
        return self.add_objects(kwargs, strict=True)[0].one_or_none()

    def add_wide_relationship_class(self, **kwargs):
        """Stage a relationship class item for insertion.

        :raises SpineIntegrityError: if the insertion of the item violates an integrity constraint.

        :returns:
            - **new_item** -- The item succesfully staged for insertion.

        :rtype: :class:`~sqlalchemy.util.KeyedTuple`
        """
        return self.add_wide_relationship_classes(kwargs, strict=True)[0].one_or_none()

    def add_wide_relationship(self, **kwargs):
        """Stage a relationship item for insertion.

        :raises SpineIntegrityError: if the insertion of the item violates an integrity constraint.

        :returns:
            - **new_item** -- The item succesfully staged for insertion.

        :rtype: :class:`~sqlalchemy.util.KeyedTuple`
        """
        return self.add_wide_relationships(kwargs, strict=True)[0].one_or_none()

    def add_parameter_definition(self, **kwargs):
        """Stage a parameter definition item for insertion.

        :raises SpineIntegrityError: if the insertion of the item violates an integrity constraint.

        :returns:
            - **new_item** -- The item succesfully staged for insertion.

        :rtype: :class:`~sqlalchemy.util.KeyedTuple`
        """
        return self.add_parameter_definitions(kwargs, strict=True)[0].one_or_none()

    def add_parameter_value(self, **kwargs):
        """Stage a parameter value item for insertion.

        :raises SpineIntegrityError: if the insertion of the item violates an integrity constraint.

        :returns:
            - **new_item** -- The item succesfully staged for insertion.

        :rtype: :class:`~sqlalchemy.util.KeyedTuple`
        """
        return self.add_parameter_values(kwargs, strict=True)[0].one_or_none()

    def get_or_add_object_class(self, **kwargs):
        """Stage an object class item for insertion if it doesn't already exists in the db.

        :returns:
            - **item** -- The item succesfully staged for insertion or already existing.

        :rtype: :class:`~sqlalchemy.util.KeyedTuple`
        """
        return self.add_object_classes(kwargs, return_dups=True)[0].one_or_none()

    def get_or_add_object(self, **kwargs):
        """Stage an object item for insertion if it doesn't already exists in the db.

        :returns:
            - **item** -- The item succesfully staged for insertion or already existing.

        :rtype: :class:`~sqlalchemy.util.KeyedTuple`
        """
        return self.add_objects(kwargs, return_dups=True)[0].one_or_none()

    def get_or_add_parameter_definition(self, **kwargs):
        """Stage a parameter definition item for insertion if it doesn't already exists in the db.

        :returns:
            - **item** -- The item succesfully staged for insertion or already existing.

        :rtype: :class:`~sqlalchemy.util.KeyedTuple`
        """
        return self.add_parameter_definitions(kwargs, return_dups=True)[0].one_or_none()
