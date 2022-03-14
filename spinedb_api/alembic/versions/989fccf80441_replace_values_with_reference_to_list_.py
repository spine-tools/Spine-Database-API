"""Replace values with reference to list values

Revision ID: 989fccf80441
Revises: 0c7d199ae915
Create Date: 2022-03-14 11:33:13.777028

"""
from alembic import op
from sqlalchemy import MetaData
from sqlalchemy.sql.expression import bindparam
from sqlalchemy.orm import sessionmaker
from spinedb_api.check_functions import (
    replace_default_values_with_list_references,
    replace_parameter_values_with_list_references,
)
from spinedb_api.parameter_value import from_database
from spinedb_api.helpers import group_concat
from spinedb_api.exception import SpineIntegrityError


# revision identifiers, used by Alembic.
revision = '989fccf80441'
down_revision = '0c7d199ae915'
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
