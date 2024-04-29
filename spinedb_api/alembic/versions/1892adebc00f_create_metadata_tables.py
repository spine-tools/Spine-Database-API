"""create metadata tables

Revision ID: 1892adebc00f
Revises: defbda3bf2b5
Create Date: 2020-10-05 14:51:13.787685

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "1892adebc00f"
down_revision = "defbda3bf2b5"
branch_labels = None
depends_on = None


def upgrade():
    m = sa.MetaData(op.get_bind())
    m.reflect()
    if "next_id" in m.tables:
        with op.batch_alter_table("next_id") as batch_op:
            batch_op.add_column(sa.Column("metadata_id", sa.Integer, server_default=sa.null()))
            batch_op.add_column(sa.Column("parameter_value_metadata_id", sa.Integer, server_default=sa.null()))
            batch_op.add_column(sa.Column("entity_metadata_id", sa.Integer, server_default=sa.null()))
    op.create_table(
        "metadata",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.String(155), nullable=False),
        sa.Column("value", sa.String(255), nullable=False),
        sa.Column("commit_id", sa.Integer, sa.ForeignKey("commit.id")),
        sa.UniqueConstraint("name", "value"),
    )
    op.create_table(
        "parameter_value_metadata",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column(
            "parameter_value_id",
            sa.Integer,
            sa.ForeignKey("parameter_value.id", onupdate="CASCADE", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "metadata_id",
            sa.Integer,
            sa.ForeignKey("metadata.id", onupdate="CASCADE", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("commit_id", sa.Integer, sa.ForeignKey("commit.id")),
        sa.UniqueConstraint("parameter_value_id", "metadata_id"),
    )
    op.create_table(
        "entity_metadata",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column(
            "entity_id", sa.Integer, sa.ForeignKey("entity.id", onupdate="CASCADE", ondelete="CASCADE"), nullable=False
        ),
        sa.Column(
            "metadata_id",
            sa.Integer,
            sa.ForeignKey("metadata.id", onupdate="CASCADE", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("commit_id", sa.Integer, sa.ForeignKey("commit.id")),
        sa.UniqueConstraint("entity_id", "metadata_id"),
    )


def downgrade():
    pass
