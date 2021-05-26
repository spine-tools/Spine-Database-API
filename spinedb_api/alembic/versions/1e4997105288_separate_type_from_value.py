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
    for row in conn.execute("SELECT id, value FROM parameter_value"):
        val = json.loads(row.value)
        if not isinstance(val, dict):
            continue
        type_ = val.get("type")
        if type_ is None:
            continue
        conn.execute("UPDATE parameter_value SET type = :type WHERE id = :id", type=type_, id=row.id)
    for row in conn.execute("SELECT id, default_value FROM parameter_definition"):
        val = json.loads(row.default_value)
        if not isinstance(val, dict):
            continue
        type_ = val.get("type")
        if type_ is None:
            continue
        conn.execute("UPDATE parameter_definition SET default_type = :type WHERE id = :id", type=type_, id=row.id)


def downgrade():
    pass
