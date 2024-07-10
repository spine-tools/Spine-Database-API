"""create entity_class_display_mode table

Revision ID: 14708c6a710f
Revises: e010777927a5
Create Date: 2024-07-09 15:19:31.510752

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "14708c6a710f"
down_revision = "e010777927a5"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "entity_class_display_mode",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.Unicode(255), nullable=False),
        sa.Column("description", sa.Text, nullable=True),
        sa.UniqueConstraint("name"),
    )


def downgrade():
    pass
