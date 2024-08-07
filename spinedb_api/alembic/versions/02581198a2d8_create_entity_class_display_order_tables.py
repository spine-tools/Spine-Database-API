"""create entity class display order tables

Revision ID: 02581198a2d8
Revises: ca7a13da8ff6
Create Date: 2024-07-10 12:13:10.690801

"""

from alembic import op
import sqlalchemy as sa
from spinedb_api.helpers import DisplayStatus

# revision identifiers, used by Alembic.
revision = "02581198a2d8"
down_revision = "c55527151b29"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "entity_class_display_mode",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.String(255), nullable=False, unique=True),
        sa.Column("description", sa.Text(), server_default=sa.null()),
    )
    op.create_table(
        "display_mode__entity_class",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("display_mode_id", sa.Integer, nullable=False),
        sa.Column("entity_class_id", sa.Integer, nullable=False),
        sa.Column("display_order", sa.Integer, nullable=False),
        sa.Column(
            "display_status",
            sa.Enum(*DisplayStatus.values(), name="display_status_enum"),
            server_default=DisplayStatus.VISIBLE,
            nullable=False,
        ),
        sa.Column("display_font_color", sa.BigInteger, server_default=sa.null()),
        sa.Column("display_background_color", sa.BigInteger, server_default=sa.null()),
        sa.ForeignKeyConstraint(
            ["display_mode_id"],
            ["entity_class_display_mode.id"],
            name=op.f("fk_display_mode_display_mode"),
            onupdate="CASCADE",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["entity_class_id"],
            ["entity_class.id"],
            name=op.f("fk_display_mode_entity_class"),
            onupdate="CASCADE",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint(
            "display_mode_id", "entity_class_id", "display_order", name=op.f("uq_display_mode_class_order")
        ),
    )


def downgrade():
    pass
