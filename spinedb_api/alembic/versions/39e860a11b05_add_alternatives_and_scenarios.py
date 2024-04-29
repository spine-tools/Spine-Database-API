"""add alternatives and scenarios

Revision ID: 39e860a11b05
Revises: 9da58d2def22
Create Date: 2020-03-05 14:04:00.854936

"""
from datetime import datetime, timezone
from alembic import op
import sqlalchemy as sa
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker


# revision identifiers, used by Alembic.
revision = "39e860a11b05"
down_revision = "9da58d2def22"
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
        sa.Column("active", sa.Boolean(name="active"), server_default=sa.false(), nullable=False),
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


def add_upgrade_comment_to_commits(session, Base):
    commit = Base.classes.commit(
        comment="Upgrade database: add scenarios and alternatives.", user="alembic", date=datetime.now(timezone.utc)
    )
    session.add(commit)
    session.commit()
    return commit.id


def add_base_alternative(commit_id, session, Base):
    alternative = Base.classes.alternative(name="Base", description="Base alternative", commit_id=commit_id)
    session.add(alternative)
    session.commit()


def commit_ids_for_types(upgrade_commit_id, session, Base):
    for entity_type in session.query(Base.classes.entity_type).all():
        entity_type.commit_id = upgrade_commit_id
    for entity_class_type in session.query(Base.classes.entity_class_type).all():
        entity_class_type.commit_id = upgrade_commit_id
    session.commit()


def alter_tables_after_update():
    inspector = sa.inspect(op.get_bind())
    parameter_value_uq_names = [x["name"] for x in inspector.get_unique_constraints("parameter_value")]
    with op.batch_alter_table("parameter_value") as batch_op:
        batch_op.add_column(sa.Column("alternative_id", sa.Integer, nullable=True))
        batch_op.create_unique_constraint(None, ["parameter_definition_id", "entity_id", "alternative_id"])
        if "uq_parameter_value_parameter_definition_identity_id" in parameter_value_uq_names:
            batch_op.drop_constraint("uq_parameter_value_parameter_definition_identity_id")

    op.execute("UPDATE parameter_value SET alternative_id = 1")
    with op.batch_alter_table("parameter_value") as batch_op:
        batch_op.alter_column("alternative_id", nullable=False)
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

    with op.batch_alter_table("entity_type") as batch_op:
        batch_op.alter_column("commit_id", nullable=False)
    with op.batch_alter_table("entity_class_type") as batch_op:
        batch_op.alter_column("commit_id", nullable=False)


def upgrade():
    create_new_tables()
    Session = sessionmaker(bind=op.get_bind())
    session = Session()
    Base = automap_base()
    Base.prepare(op.get_bind(), reflect=True)
    upgrade_commit_id = add_upgrade_comment_to_commits(session, Base)
    add_base_alternative(upgrade_commit_id, session, Base)
    commit_ids_for_types(upgrade_commit_id, session, Base)
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
    with op.batch_alter_table("entity_type") as batch_op:
        batch_op.alter_column("commit_id", nullable=True)
    with op.batch_alter_table("entity_class_type") as batch_op:
        batch_op.alter_column("commit_id", nullable=True)
