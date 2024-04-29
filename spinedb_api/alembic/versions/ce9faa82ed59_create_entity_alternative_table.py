"""create entity_alternative table

Revision ID: ce9faa82ed59
Revises: 6b7c994c1c61
Create Date: 2023-09-21 14:35:28.867803

"""
from alembic import op
import sqlalchemy as sa
from spinedb_api.compatibility import convert_tool_feature_method_to_entity_alternative


# revision identifiers, used by Alembic.
revision = "ce9faa82ed59"
down_revision = "6b7c994c1c61"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "entity_alternative",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("entity_id", sa.Integer(), nullable=False),
        sa.Column("alternative_id", sa.Integer(), nullable=False),
        sa.Column("active", sa.Boolean(name="active"), server_default=sa.text("1"), nullable=False),
        sa.Column("commit_id", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(
            ["alternative_id"],
            ["alternative.id"],
            name=op.f("fk_entity_alternative_alternative_id_alternative"),
            onupdate="CASCADE",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(["commit_id"], ["commit.id"], name=op.f("fk_entity_alternative_commit_id_commit")),
        sa.ForeignKeyConstraint(
            ["entity_id"],
            ["entity.id"],
            name=op.f("fk_entity_alternative_entity_id_entity"),
            onupdate="CASCADE",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_entity_alternative")),
        sa.UniqueConstraint("entity_id", "alternative_id", name=op.f("uq_entity_alternative_entity_idalternative_id")),
    )
    try:
        op.drop_table("next_id")
    except sa.exc.OperationalError:
        pass
    convert_tool_feature_method_to_entity_alternative(op.get_bind(), use_existing_tool_feature_method=True, apply=True)


def downgrade():
    pass
