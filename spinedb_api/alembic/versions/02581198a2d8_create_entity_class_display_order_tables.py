"""create entity class display order tables

Revision ID: 02581198a2d8
Revises: ca7a13da8ff6
Create Date: 2024-07-10 12:13:10.690801

"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "02581198a2d8"
down_revision = "ca7a13da8ff6"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "entity_class_display_mode",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.Unicode(255), nullable=False, unique=True),
        sa.Column("description", sa.Text, nullable=True),
    )
    op.create_table(
        "display_mode__entity_class",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("display_mode_id", sa.Integer, nullable=False),
        sa.Column("entity_class_id", sa.Integer, nullable=False),
        sa.Column("display_order", sa.Integer, nullable=False),
        sa.Column("display_status", sa.Text, server_default=sa.null()),
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
        sa.UniqueConstraint("display_mode_id", "entity_class_id", name=op.f("uq_display_mode_entity_class")),
    )


def downgrade():
    pass
