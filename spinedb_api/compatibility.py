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

"""Dirty hacks needed to maintain compatibility in cases where migration alone doesn't do it."""

import sqlalchemy as sa


def convert_tool_feature_method_to_active_by_default(conn, use_existing_tool_feature_method, apply):
    """Transforms default parameter values into active_by_default values, whenever the former are used in a tool filter
    to control entity activity.

    Args:
        conn (Connection)
        use_existing_tool_feature_method (bool): Whether to use existing tool/feature/method definitions.
        apply (bool): if True, apply the transformations

    Returns:
        tuple: list of entity classes to add, update and ids to remove
    """
    meta = sa.MetaData(conn)
    meta.reflect()
    lv_table = meta.tables["list_value"]
    pd_table = meta.tables["parameter_definition"]
    if use_existing_tool_feature_method:
        try:
            # Compute list-value id by parameter definition id for all features and methods
            tfm_table = meta.tables["tool_feature_method"]
            tf_table = meta.tables["tool_feature"]
            f_table = meta.tables["feature"]
            lv_id_by_pdef_id = {
                x["parameter_definition_id"]: x["id"]
                for x in conn.execute(
                    sa.select([lv_table.c.id, f_table.c.parameter_definition_id])
                    .where(tfm_table.c.parameter_value_list_id == lv_table.c.parameter_value_list_id)
                    .where(tfm_table.c.method_index == lv_table.c.index)
                    .where(tf_table.c.id == tfm_table.c.tool_feature_id)
                    .where(f_table.c.id == tf_table.c.feature_id)
                )
            }
        except KeyError:
            use_existing_tool_feature_method = False
    if not use_existing_tool_feature_method:
        # It's a new DB without tool/feature/method or we don't want to use them...
        # we take 'is_active' as feature and JSON "yes" and true as methods
        lv_id_by_pdef_id = {
            x["parameter_definition_id"]: x["id"]
            for x in conn.execute(
                sa.select([lv_table.c.id, lv_table.c.value, pd_table.c.id.label("parameter_definition_id")])
                .where(lv_table.c.parameter_value_list_id == pd_table.c.parameter_value_list_id)
                .where(pd_table.c.name == "is_active")
                .where(lv_table.c.value.in_((b'"yes"', b"true")))
            )
        }
    # Collect 'is_active' default values
    list_value_id = sa.case(
        [(pd_table.c.default_type == "list_value_ref", sa.cast(pd_table.c.default_value, sa.Integer()))], else_=None
    )
    is_active_default_vals = [
        {c: x[c] for c in ("entity_class_id", "parameter_definition_id", "list_value_id")}
        for x in conn.execute(
            sa.select(
                [
                    pd_table.c.entity_class_id,
                    pd_table.c.id.label("parameter_definition_id"),
                    list_value_id.label("list_value_id"),
                ]
            ).where(pd_table.c.id.in_(lv_id_by_pdef_id))
        )
    ]
    # Compute new active_by_default values from 'is_active' default values,
    # where active_by_default is True if the value of 'is_active' is the one from the tool_feature_method specification
    entity_class_items_to_update = {
        x["entity_class_id"]: {
            "active_by_default": False
            if x["list_value_id"] is None
            else x["list_value_id"] == lv_id_by_pdef_id[x["parameter_definition_id"]],
        }
        for x in is_active_default_vals
    }
    updated_items = []
    entity_class_table = meta.tables["entity_class"]
    update_statement = entity_class_table.update()
    for class_id, update in entity_class_items_to_update.items():
        if apply:
            conn.execute(update_statement.where(entity_class_table.c.id == class_id), update)
        update["id"] = class_id
        updated_items.append(update)
    parameter_definitions_to_update = (
        x["parameter_definition_id"] for x in is_active_default_vals if x["list_value_id"] is not None
    )
    update_statement = pd_table.update()
    for definition_id in parameter_definitions_to_update:
        update = {"default_value": None, "default_type": None}
        if apply:
            conn.execute(update_statement.where(pd_table.c.id == definition_id), update)
        update["id"] = definition_id
        updated_items.append(update)
    return [], updated_items, []


def convert_tool_feature_method_to_entity_alternative(conn, use_existing_tool_feature_method, apply):
    """Transforms parameter_value rows into entity_alternative rows, whenever the former are used in a tool filter
    to control entity activity.

    Args:
        conn (Connection)
        use_existing_tool_feature_method (bool): Whether to use existing tool/feature/method definitions.
        apply (bool):

    Returns:
        list: entity_alternative items to add
        list: entity_alternative items to update
        list: parameter_value ids to remove
    """
    meta = sa.MetaData(conn)
    meta.reflect()
    ea_table = meta.tables["entity_alternative"]
    lv_table = meta.tables["list_value"]
    pv_table = meta.tables["parameter_value"]
    if use_existing_tool_feature_method:
        try:
            # Compute list-value id by parameter definition id for all features and methods
            tfm_table = meta.tables["tool_feature_method"]
            tf_table = meta.tables["tool_feature"]
            f_table = meta.tables["feature"]
            lv_id_by_pdef_id = {
                x["parameter_definition_id"]: x["id"]
                for x in conn.execute(
                    sa.select([lv_table.c.id, f_table.c.parameter_definition_id])
                    .where(tfm_table.c.parameter_value_list_id == lv_table.c.parameter_value_list_id)
                    .where(tfm_table.c.method_index == lv_table.c.index)
                    .where(tf_table.c.id == tfm_table.c.tool_feature_id)
                    .where(f_table.c.id == tf_table.c.feature_id)
                )
            }
        except KeyError:
            use_existing_tool_feature_method = False
    if not use_existing_tool_feature_method:
        # It's a new DB without tool/feature/method or we don't want to use them...
        # we take 'is_active' as feature and JSON "yes" and true as methods
        pd_table = meta.tables["parameter_definition"]
        lv_id_by_pdef_id = {
            x["parameter_definition_id"]: x["id"]
            for x in conn.execute(
                sa.select([lv_table.c.id, lv_table.c.value, pd_table.c.id.label("parameter_definition_id")])
                .where(lv_table.c.parameter_value_list_id == pd_table.c.parameter_value_list_id)
                .where(pd_table.c.name == "is_active")
                .where(lv_table.c.value.in_((b'"yes"', b"true")))
            )
        }
    # Collect 'is_active' parameter values
    list_value_id = sa.case(
        [(pv_table.c.type == "list_value_ref", sa.cast(pv_table.c.value, sa.Integer()))], else_=None
    )
    is_active_pvals = [
        {c: x[c] for c in ("id", "entity_id", "alternative_id", "parameter_definition_id", "list_value_id")}
        for x in conn.execute(
            sa.select([pv_table, list_value_id.label("list_value_id")]).where(
                pv_table.c.parameter_definition_id.in_(lv_id_by_pdef_id)
            )
        )
    ]
    # Compute new entity_alternative items from 'is_active' parameter values,
    # where 'active' is True if the value of 'is_active' is the one from the tool_feature_method specification
    current_ea_ids = {(x["entity_id"], x["alternative_id"]): x["id"] for x in conn.execute(sa.select([ea_table]))}
    new_ea_items = {
        (x["entity_id"], x["alternative_id"]): {
            "entity_id": x["entity_id"],
            "alternative_id": x["alternative_id"],
            "active": x["list_value_id"] == lv_id_by_pdef_id[x["parameter_definition_id"]],
        }
        for x in is_active_pvals
    }
    # Add or update entity_alternative records
    ea_items_to_add = [new_ea_items[key] for key in set(new_ea_items) - set(current_ea_ids)]
    ea_items_to_update = [
        {"id": current_ea_ids[key], "active": new_ea_items[key]["active"]}
        for key in set(new_ea_items) & set(current_ea_ids)
    ]
    pval_ids_to_remove = [x["id"] for x in is_active_pvals]
    if apply:
        if ea_items_to_add:
            conn.execute(ea_table.insert(), ea_items_to_add)
        ea_update = ea_table.update()
        for item in ea_items_to_update:
            conn.execute(ea_update.where(ea_table.c.id == item["id"]), {"active": item["active"]})
        # Delete pvals 499 at a time to avoid too many sql variables
        size = 499
        for i in range(0, len(pval_ids_to_remove), size):
            ids = pval_ids_to_remove[i : i + size]
            conn.execute(pv_table.delete().where(pv_table.c.id.in_(ids)))
    return ea_items_to_add, ea_items_to_update, set(pval_ids_to_remove)


def compatibility_transformations(connection, apply=True):
    """Refits any data having an old format and returns changes made.

    Args:
        connection (Connection)
        apply (bool): if True, apply the transformations

    Returns:
        tuple(list, list): list of tuples (tablename, (items_added, items_updated, ids_removed)), and
            list of strings indicating the changes
    """
    ea_items_added, ea_items_updated, pval_ids_removed = convert_tool_feature_method_to_entity_alternative(
        connection, use_existing_tool_feature_method=False, apply=apply
    )
    transformations = []
    info = []
    if ea_items_added or ea_items_updated:
        transformations.append(("entity_alternative", (ea_items_added, ea_items_updated, ())))
    if pval_ids_removed:
        transformations.append(("parameter_value", ((), (), pval_ids_removed)))
    if ea_items_added or ea_items_updated or pval_ids_removed:
        info.append("Convert entity activity control using tool/feature/method into entity_alternative")
    _, ec_items_updated, _ = convert_tool_feature_method_to_active_by_default(
        connection, use_existing_tool_feature_method=False, apply=apply
    )
    if ec_items_updated:
        transformations.append(("entity_class", ((), ec_items_updated, ())))
    return transformations, info
