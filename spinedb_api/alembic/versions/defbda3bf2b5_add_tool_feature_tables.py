"""add tool feature tables

Revision ID: defbda3bf2b5
Revises: 39e860a11b05
Create Date: 2020-09-01 20:12:57.300147

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "defbda3bf2b5"
down_revision = "39e860a11b05"
branch_labels = None
depends_on = None


def upgrade():
    m = sa.MetaData(op.get_bind())
    m.reflect()
    if "next_id" in m.tables:
        with op.batch_alter_table("next_id") as batch_op:
            batch_op.add_column(sa.Column("tool_id", sa.Integer, server_default=sa.null()))
            batch_op.add_column(sa.Column("feature_id", sa.Integer, server_default=sa.null()))
            batch_op.add_column(sa.Column("tool_feature_id", sa.Integer, server_default=sa.null()))
            batch_op.add_column(sa.Column("tool_feature_method_id", sa.Integer, server_default=sa.null()))
    op.create_table(
        "tool",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.String(155), nullable=False),
        sa.Column("description", sa.String(255), server_default=sa.null()),
        sa.Column("commit_id", sa.Integer, sa.ForeignKey("commit.id")),
    )
    op.create_table(
        "feature",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("parameter_definition_id", sa.Integer, nullable=False),
        sa.Column("parameter_value_list_id", sa.Integer, nullable=False),
        sa.Column("description", sa.String(255), server_default=sa.null()),
        sa.Column("commit_id", sa.Integer, sa.ForeignKey("commit.id")),
        sa.UniqueConstraint("parameter_definition_id", "parameter_value_list_id"),
        sa.ForeignKeyConstraint(
            ("parameter_definition_id", "parameter_value_list_id"),
            ("parameter_definition.id", "parameter_definition.parameter_value_list_id"),
            onupdate="CASCADE",
            ondelete="CASCADE",
        ),
    )
    op.create_table(
        "tool_feature",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("tool_id", sa.Integer, sa.ForeignKey("tool.id")),
        sa.Column("feature_id", sa.Integer, nullable=False),
        sa.Column("parameter_value_list_id", sa.Integer, nullable=False),
        sa.Column("required", sa.Boolean(name="required"), server_default=sa.false(), nullable=False),
        sa.Column("commit_id", sa.Integer, sa.ForeignKey("commit.id")),
        sa.UniqueConstraint("tool_id", "feature_id"),
        sa.ForeignKeyConstraint(
            ("feature_id", "parameter_value_list_id"),
            ("feature.id", "feature.parameter_value_list_id"),
            onupdate="CASCADE",
            ondelete="CASCADE",
        ),
    )
    op.create_table(
        "tool_feature_method",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("tool_feature_id", sa.Integer, nullable=False),
        sa.Column("parameter_value_list_id", sa.Integer, nullable=False),
        sa.Column("method_index", sa.Integer),
        sa.Column("commit_id", sa.Integer, sa.ForeignKey("commit.id")),
        sa.UniqueConstraint("tool_feature_id", "method_index"),
        sa.ForeignKeyConstraint(
            ("tool_feature_id", "parameter_value_list_id"),
            ("tool_feature.id", "tool_feature.parameter_value_list_id"),
            onupdate="CASCADE",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ("parameter_value_list_id", "method_index"),
            ("parameter_value_list.id", "parameter_value_list.value_index"),
            onupdate="CASCADE",
            ondelete="CASCADE",
        ),
    )


def downgrade():
    pass
