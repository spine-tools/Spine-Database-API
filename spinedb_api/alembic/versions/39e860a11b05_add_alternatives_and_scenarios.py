"""add alternatives and scenarios

Revision ID: 39e860a11b05
Revises: 9da58d2def22
Create Date: 2020-03-05 14:04:00.854936

"""
from alembic import op
import sqlalchemy as sa
from datetime import datetime


# revision identifiers, used by Alembic.
revision = '39e860a11b05'
down_revision = '9da58d2def22'
branch_labels = None
depends_on = None


def create_new_tables():
    op.create_table(
        "alternative",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.Text, nullable=False),
        sa.Column("description", sa.Text, nullable=True),
        sa.Column("commit_id", sa.Integer, sa.ForeignKey("commit.id")),
        sa.UniqueConstraint("name"),
    )
    op.create_table(
        "scenario",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.Text, nullable=False),
        sa.Column("description", sa.Text, nullable=True),
        sa.Column("commit_id", sa.Integer, sa.ForeignKey("commit.id")),
        sa.UniqueConstraint("name"),
    )
    op.create_table(
        "scenario_alternative",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("scenario_id", sa.Integer, nullable=False),
        sa.Column("alternative_id", sa.Integer, nullable=False),
        sa.Column("rank", sa.Integer, nullable=False),
        sa.Column("commit_id", sa.Integer, sa.ForeignKey("commit.id")),
        sa.UniqueConstraint("scenario_id", "rank"),
        sa.UniqueConstraint("scenario_id", "alternative_id"),
        sa.ForeignKeyConstraint(("scenario_id",), ("scenario.id",), onupdate="CASCADE", ondelete="CASCADE"),
        sa.ForeignKeyConstraint(("alternative_id",), ("alternative.id",), onupdate="CASCADE", ondelete="CASCADE"),
    )


def add_base_alternative():
    op.execute("""INSERT INTO alternative (id, name, description) VALUES (1, "Base", "Base alternative, null")""")


def alter_tables_after_update():
    with op.batch_alter_table("parameter_value") as batch_op:
        batch_op.add_column(sa.Column("alternative_id", sa.Integer, nullable=True))
        batch_op.create_unique_constraint(None, ["parameter_definition_id", "entity_id", "alternative_id"])
        batch_op.drop_constraint("uq_parameter_value_parameter_definition_identity_id")

    op.execute("UPDATE parameter_value SET alternative_id = 1")
    with op.batch_alter_table("parameter_value") as batch_op:
        batch_op.alter_column('alternative_id', nullable=False)
        batch_op.create_foreign_key(
            None, "alternative", ("alternative_id",), ("id",), onupdate="CASCADE", ondelete="CASCADE"
        )

    m = sa.MetaData(op.get_bind())
    m.reflect()
    if "next_id" in m.tables:
        with op.batch_alter_table("next_id") as batch_op:
            batch_op.add_column(sa.Column("alternative_id", sa.Integer, server_default=sa.null()))
            batch_op.add_column(sa.Column("scenario_id", sa.Integer, server_default=sa.null()))
            batch_op.add_column(sa.Column("scenario_alternative_id", sa.Integer, server_default=sa.null()))
        user = "alembic"
        date = datetime.utcnow()
        conn = op.get_bind()
        conn.execute(
            """
            UPDATE next_id
            SET
                user = :user,
                date = :date,
                alternative_id = 2,
                scenario_id = 1,
                scenario_alternative_id = 1
            """,
            user=user,
            date=date,
        )


def upgrade():
    create_new_tables()
    add_base_alternative()
    alter_tables_after_update()


def downgrade():
    with op.batch_alter_table("parameter_value") as batch_op:
        batch_op.drop_column("alternative_id")
    op.delete_table("scenario_alternative")
    op.delete_table("scenario")
    op.delete_table("alternative")
    m = sa.MetaData(op.get_bind())
    m.reflect()
    if "next_id" in m.tables:
        with op.batch_alter_table("next_id") as batch_op:
            batch_op.drop_column("alternative_id")
            batch_op.drop_column("scenario_id")
            batch_op.drop_column("scenario_alternative_id")
