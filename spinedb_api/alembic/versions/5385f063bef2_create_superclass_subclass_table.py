"""create superclass_subclass table

Revision ID: 5385f063bef2
Revises: ce9faa82ed59
Create Date: 2023-10-30 17:11:23.316879

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "5385f063bef2"
down_revision = "ce9faa82ed59"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "superclass_subclass",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("superclass_id", sa.Integer(), nullable=False),
        sa.Column("subclass_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["subclass_id"],
            ["entity_class.id"],
            name=op.f("fk_superclass_subclass_subclass_id_entity_class"),
            onupdate="CASCADE",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["superclass_id"],
            ["entity_class.id"],
            name=op.f("fk_superclass_subclass_superclass_id_entity_class"),
            onupdate="CASCADE",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_superclass_subclass")),
        sa.UniqueConstraint("subclass_id", name=op.f("uq_superclass_subclass_subclass_id")),
    )


def downgrade():
    pass
