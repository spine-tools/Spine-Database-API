"""create parameter group table

Revision ID: 6260fa2e3248
Revises: e9f2c2330cf8
Create Date: 2026-02-09 11:11:30.123731

"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "6260fa2e3248"
down_revision = "e9f2c2330cf8"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "parameter_group",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.String(155), nullable=False),
        sa.Column("color", sa.String(6), nullable=False),
        sa.Column("priority", sa.Integer, nullable=False),
        sa.UniqueConstraint("name"),
    )
    with op.batch_alter_table("parameter_definition") as batch_op:
        batch_op.add_column(sa.Column("parameter_group_id", sa.Integer, sa.ForeignKey("parameter_group.id")))


def downgrade():
    pass
