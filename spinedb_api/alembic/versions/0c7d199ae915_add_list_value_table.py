"""Add list_value table

Revision ID: 0c7d199ae915
Revises: 7d0b467f2f4e
Create Date: 2022-03-07 14:55:32.802765

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from spinedb_api.helpers import LONGTEXT_LENGTH, naming_convention

# revision identifiers, used by Alembic.
revision = '0c7d199ae915'
down_revision = '7d0b467f2f4e'
branch_labels = None
depends_on = None


def upgrade():
    conn = op.get_bind()
    Session = sessionmaker(bind=conn)
    session = Session()
    Base = automap_base()
    Base.prepare(conn, reflect=True)
    pvl = session.query(Base.classes.parameter_value_list).all()
    tfm = session.query(Base.classes.tool_feature_method).all()
    session.query(Base.classes.parameter_value_list).delete()
    m = sa.MetaData(op.get_bind())
    m.reflect()
    if "next_id" in m.tables:
        with op.batch_alter_table("next_id") as batch_op:
            batch_op.add_column(sa.Column("list_value_id", sa.Integer, server_default=sa.null()))
    op.create_table(
        'list_value',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('parameter_value_list_id', sa.Integer, sa.ForeignKey("parameter_value_list.id"), nullable=False),
        sa.Column('index', sa.Integer, nullable=False),
        sa.Column('type', sa.String(255)),
        sa.Column('value', sa.LargeBinary(LONGTEXT_LENGTH), server_default=sa.null()),
        sa.Column('commit_id', sa.Integer, sa.ForeignKey("commit.id")),
        sa.UniqueConstraint('parameter_value_list_id', 'index'),
    )
    op.drop_table("tool_feature_method")
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
            ("list_value.parameter_value_list_id", "list_value.index"),
            onupdate="CASCADE",
            ondelete="CASCADE",
            name="tool_feature_method_parameter_value_list_id",
        ),
    )
    with op.batch_alter_table("parameter_value_list", naming_convention=naming_convention) as batch_op:
        batch_op.drop_column('value_index')
        batch_op.drop_column('value')
    # Add
    pvl_items = list({x.id: {"id": x.id, "name": x.name, "commit_id": x.commit_id} for x in pvl}.values())
    lv_items = [{"parameter_value_list_id": x.id, "index": x.value_index, "value": x.value, "type": None} for x in pvl]
    tfm_items = [
        {c: getattr(x, c) for c in ("id", "tool_feature_id", "parameter_value_list_id", "method_index", "commit_id")}
        for x in tfm
    ]
    NewBase = automap_base()
    NewBase.prepare(conn, reflect=True)
    session.bulk_insert_mappings(NewBase.classes.parameter_value_list, pvl_items)
    session.bulk_insert_mappings(NewBase.classes.list_value, lv_items)
    session.bulk_insert_mappings(NewBase.classes.tool_feature_method, tfm_items)


def downgrade():
    pass
