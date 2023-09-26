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

"""Dirty hacks needed to maintain compatibility in cases where migration alone doesn't do it."""

import sqlalchemy as sa


def convert_tool_feature_method_to_entity_alternative(conn):
    """Transforms parameter_value rows into entity_alternative rows, whenever the former are used in a tool filter
    to control entity activity.

    Args:
        conn (Connection)

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
        # It's a new DB without tool/feature/method
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
    if ea_items_to_add:
        conn.execute(ea_table.insert(), ea_items_to_add)
    if ea_items_to_update:
        conn.execute(ea_table.update(), ea_items_to_update)
    # Delete pvals 499 at a time to avoid too many sql variables
    size = 499
    for i in range(0, len(pval_ids_to_remove), size):
        ids = pval_ids_to_remove[i : i + size]
        conn.execute(pv_table.delete().where(pv_table.c.id.in_(ids)))
    return ea_items_to_add, ea_items_to_update, set(pval_ids_to_remove)


def compatibility_transformations(connection):
    """Refits any data having an old format and returns changes made.

    Args:
        connection (Connection)

    Returns:
        list: list of tuples (tablename, (items_added, items_updated, ids_removed))
        list: list of strings indicating the changes
    """
    ea_items_added, ea_items_updated, pval_ids_removed = convert_tool_feature_method_to_entity_alternative(connection)
    transformations = []
    info = []
    if ea_items_added or ea_items_updated:
        transformations.append(("entity_alternative", (ea_items_added, ea_items_updated, ())))
    if pval_ids_removed:
        transformations.append(("parameter_value", ((), (), pval_ids_removed)))
    if ea_items_added or ea_items_updated or pval_ids_removed:
        info.append("Convert entity activity control using tool/feature/method into entity_alternative")
    return transformations, info
