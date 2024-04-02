"""create entity_group_table

Revision ID: 9da58d2def22
Revises: 070a0eb89e88
Create Date: 2020-06-09 21:31:07.912724

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "9da58d2def22"
down_revision = "070a0eb89e88"
branch_labels = None
depends_on = None


def upgrade():
    m = sa.MetaData(op.get_bind())
    m.reflect()
    if "next_id" in m.tables:
        with op.batch_alter_table("next_id") as batch_op:
            batch_op.add_column(sa.Column("entity_group_id", sa.Integer))
    op.create_table(
        "entity_group",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("entity_id", sa.Integer, nullable=False),
        sa.Column("entity_class_id", sa.Integer, nullable=False),
        sa.Column("member_id", sa.Integer, nullable=False),
        sa.UniqueConstraint("entity_id", "member_id"),
        sa.ForeignKeyConstraint(
            ("entity_id", "entity_class_id"), ("entity.id", "entity.class_id"), onupdate="CASCADE", ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ("member_id", "entity_class_id"), ("entity.id", "entity.class_id"), onupdate="CASCADE", ondelete="CASCADE"
        ),
    )


def downgrade():
    pass
