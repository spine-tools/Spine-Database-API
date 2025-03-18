"""create entity_location table

Revision ID: 91f1f55aa972
Revises: 7e2e66ae0f8f
Create Date: 2025-03-18 15:20:36.616427

"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "91f1f55aa972"
down_revision = "7e2e66ae0f8f"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "entity_location",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column(
            "entity_id", sa.Integer, sa.ForeignKey("entity.id", onupdate="CASCADE", ondelete="CASCADE"), nullable=False
        ),
        sa.Column("lat", sa.Float),
        sa.Column("lon", sa.Float),
        sa.Column("alt", sa.Float),
        sa.Column("shape_name", sa.String(155)),
        sa.Column("shape_blob", sa.Text, server_default=sa.null()),
        sa.UniqueConstraint("entity_id"),
    )


def downgrade():
    pass
