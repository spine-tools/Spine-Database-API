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


def upgrade():
    with op.batch_alter_table("parameter_definition") as batch_op:
        batch_op.add_column(sa.Column('default_type', sa.String(length=255), nullable=True))
        batch_op.drop_column('data_type')
    with op.batch_alter_table("parameter_value") as batch_op:
        batch_op.add_column(sa.Column('type', sa.String(length=255), nullable=True))
    # Populate type and default_type columns
    conn = op.get_bind()
    m = sa.MetaData(conn)
    m.reflect()
    _update_table("parameter_definition", m, conn)
    _update_table("parameter_value", m, conn)


def _update_table(tablename, m, conn):
    value_field, type_field = {
        "parameter_definition": ("default_value", "default_type"),
        "parameter_value": ("value", "type"),
    }[tablename]
    table = m.tables[tablename]
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
        value = json.dumps(parsed_value)
        items.append({"b_id": row.id, value_field: value, type_field: type_})
    if not items:
        return
    upd = (
        table.update()
        .where(table.c.id == sa.bindparam('b_id'))
        .values(**{value_field: sa.bindparam(value_field), type_field: sa.bindparam(type_field)})
    )
    conn.execute(upd, items)


def downgrade():
    pass
