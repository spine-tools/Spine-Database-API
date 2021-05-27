"""Separate type from value

Revision ID: 1e4997105288
Revises: fbb540efbf15
Create Date: 2021-05-26 16:00:49.244440

"""
import json
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '1e4997105288'
down_revision = 'fbb540efbf15'
branch_labels = None
depends_on = None

LONGTEXT_LENGTH = 2 ** 32 - 1


def upgrade():
    conn = op.get_bind()
    m = sa.MetaData(conn)
    m.reflect()
    # Get items to update
    pd_items = _get_items(m, conn, "parameter_definition")
    pv_items = _get_items(m, conn, "parameter_value")
    # Alter tables
    with op.batch_alter_table("parameter_definition") as batch_op:
        batch_op.alter_column("default_value", sa.LargeBinary(LONGTEXT_LENGTH), server_default=sa.null())
        batch_op.add_column(sa.Column('default_type', sa.String(length=255), nullable=True))
        batch_op.drop_column('data_type')
    with op.batch_alter_table("parameter_value") as batch_op:
        batch_op.alter_column("value", sa.LargeBinary(LONGTEXT_LENGTH), server_default=sa.null())
        batch_op.add_column(sa.Column('type', sa.String(length=255), nullable=True))
    # Do update items
    _update_table(m, conn, "parameter_definition", pd_items)
    _update_table(m, conn, "parameter_value", pv_items)
    # TODO: update values in value lists


def _get_table_and_fields(m, tablename):
    value_field, type_field = {
        "parameter_definition": ("default_value", "default_type"),
        "parameter_value": ("value", "type"),
    }[tablename]
    table = m.tables[tablename]
    return table, value_field, type_field


def _get_items(m, conn, tablename):
    table, value_field, type_field = _get_table_and_fields(m, tablename)
    items = []
    for row in conn.execute(table.select()):
        value = getattr(row, value_field, None)
        if value is None:
            continue
        parsed_value = json.loads(value)
        if not isinstance(parsed_value, dict):
            continue
        type_ = parsed_value.pop("type", None)
        if type_ is None:
            continue
        value = bytes(json.dumps(parsed_value), "UTF8")
        items.append({"b_id": row.id, value_field: value, type_field: type_})
    return items


def _update_table(m, conn, tablename, items):
    if not items:
        return
    table, value_field, type_field = _get_table_and_fields(m, tablename)
    upd = (
        table.update()
        .where(table.c.id == sa.bindparam('b_id'))
        .values(**{value_field: sa.bindparam(value_field), type_field: sa.bindparam(type_field)})
    )
    conn.execute(upd, items)


def downgrade():
    pass
