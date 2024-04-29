"""Replace values with reference to list values

Revision ID: 989fccf80441
Revises: 0c7d199ae915
Create Date: 2022-03-14 11:33:13.777028

"""
from alembic import op
from sqlalchemy import MetaData
from sqlalchemy.sql.expression import bindparam
from sqlalchemy.orm import sessionmaker
from spinedb_api.parameter_value import dump_db_value, from_database, ParameterValueFormatError
from spinedb_api.helpers import group_concat
from spinedb_api.exception import SpineIntegrityError


# revision identifiers, used by Alembic.
revision = "989fccf80441"
down_revision = "0c7d199ae915"
branch_labels = None
depends_on = None


def upgrade():
    conn = op.get_bind()
    Session = sessionmaker(bind=conn)
    session = Session()
    meta = MetaData(conn)
    meta.reflect()
    list_value = meta.tables["list_value"]
    parameter_definition = meta.tables["parameter_definition"]
    parameter_value = meta.tables["parameter_value"]
    parameter_definitions = {x.id: x._asdict() for x in session.query(parameter_definition)}
    parameter_values = {x.id: x._asdict() for x in session.query(parameter_value)}
    parameter_value_lists = {
        x.parameter_value_list_id: x.value_id_list
        for x in session.query(
            list_value.c.parameter_value_list_id,
            group_concat(list_value.c.id, list_value.c.index).label("value_id_list"),
        ).group_by(list_value.c.parameter_value_list_id)
    }
    list_values = {x.id: from_database(x.value, x.type) for x in session.query(list_value)}
    pdefs = []
    for pdef in parameter_definitions.values():
        try:
            if replace_default_values_with_list_references(pdef, parameter_value_lists, list_values):
                pdefs.append(pdef)
        except SpineIntegrityError:
            pass
    pvals = []
    for pval in parameter_values.values():
        try:
            if replace_parameter_values_with_list_references(
                pval, parameter_definitions, parameter_value_lists, list_values
            ):
                pvals.append(pval)
        except SpineIntegrityError:
            pass
    for table, items in ((parameter_definition, pdefs), (parameter_value, pvals)):
        if not items:
            continue
        upd = table.update()
        upd = upd.where(table.c.id == bindparam("id"))
        upd = upd.values({key: bindparam(key) for key in table.columns.keys()})
        conn.execute(upd, items)


def downgrade():
    pass


def replace_default_values_with_list_references(item, parameter_value_lists, list_values):
    parameter_value_list_id = item.get("parameter_value_list_id")
    return _replace_values_with_list_references(
        "parameter_definition", item, parameter_value_list_id, parameter_value_lists, list_values
    )


def replace_parameter_values_with_list_references(item, parameter_definitions, parameter_value_lists, list_values):
    parameter_definition_id = item["parameter_definition_id"]
    parameter_definition = parameter_definitions[parameter_definition_id]
    parameter_value_list_id = parameter_definition["parameter_value_list_id"]
    return _replace_values_with_list_references(
        "parameter_value", item, parameter_value_list_id, parameter_value_lists, list_values
    )


def _replace_values_with_list_references(item_type, item, parameter_value_list_id, parameter_value_lists, list_values):
    if parameter_value_list_id is None:
        return False
    if parameter_value_list_id not in parameter_value_lists:
        raise SpineIntegrityError("Parameter value list not found.")
    value_id_list = parameter_value_lists[parameter_value_list_id]
    if value_id_list is None:
        raise SpineIntegrityError("Parameter value list is empty!")
    value_key, type_key = {
        "parameter_value": ("value", "type"),
        "parameter_definition": ("default_value", "default_type"),
    }[item_type]
    value = dict.get(item, value_key)
    value_type = dict.get(item, type_key)
    try:
        parsed_value = from_database(value, value_type)
    except ParameterValueFormatError as err:
        raise SpineIntegrityError(f"Invalid {value_key} '{value}': {err}") from None
    if parsed_value is None:
        return False
    list_value_id = next((id_ for id_ in value_id_list if list_values.get(id_) == parsed_value), None)
    if list_value_id is None:
        valid_values = ", ".join(f"{dump_db_value(list_values.get(id_))[0].decode('utf8')!r}" for id_ in value_id_list)
        raise SpineIntegrityError(
            f"Invalid {value_key} '{parsed_value}' - it should be one from the parameter value list: {valid_values}."
        )
    item[value_key] = str(list_value_id).encode("UTF8")
    item[type_key] = "list_value_ref"
    item["list_value_id"] = list_value_id
    return True
