"""add parameter type table

Revision ID: c55527151b29
Revises: ca7a13da8ff6
Create Date: 2024-07-08 08:57:42.268563

"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "c55527151b29"
down_revision = "ca7a13da8ff6"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "parameter_type",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column(
            "parameter_definition_id",
            sa.Integer,
            sa.ForeignKey("parameter_definition.id", onupdate="CASCADE", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("rank", sa.Integer, nullable=False),
        sa.Column("type", sa.String(255), nullable=False),
        sa.UniqueConstraint("parameter_definition_id", "type", "rank"),
    )


def downgrade():
    pass
