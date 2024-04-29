"""drop_object_and_relationship_tables

Revision ID: 6b7c994c1c61
Revises: 989fccf80441
Create Date: 2023-02-09 06:48:46.585108

"""
from alembic import op
import sqlalchemy as sa
from spinedb_api.helpers import naming_convention

# revision identifiers, used by Alembic.
revision = "6b7c994c1c61"
down_revision = "989fccf80441"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "entity_class_dimension",
        sa.Column("entity_class_id", sa.Integer(), nullable=False),
        sa.Column("dimension_id", sa.Integer(), nullable=False),
        sa.Column("position", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["dimension_id"],
            ["entity_class.id"],
            name=op.f("fk_entity_class_dimension_dimension_id_entity_class"),
            onupdate="CASCADE",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["entity_class_id"],
            ["entity_class.id"],
            name=op.f("fk_entity_class_dimension_entity_class_id_entity_class"),
            onupdate="CASCADE",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("entity_class_id", "dimension_id", "position", name=op.f("pk_entity_class_dimension")),
        sa.UniqueConstraint("entity_class_id", "dimension_id", "position", name="uq_entity_class_dimension"),
    )
    op.create_table(
        "entity_element",
        sa.Column("entity_id", sa.Integer(), nullable=False),
        sa.Column("entity_class_id", sa.Integer(), nullable=False),
        sa.Column("element_id", sa.Integer(), nullable=False),
        sa.Column("dimension_id", sa.Integer(), nullable=False),
        sa.Column("position", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["element_id", "dimension_id"],
            ["entity.id", "entity.class_id"],
            name=op.f("fk_entity_element_element_id_entity"),
            onupdate="CASCADE",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["entity_class_id", "dimension_id", "position"],
            [
                "entity_class_dimension.entity_class_id",
                "entity_class_dimension.dimension_id",
                "entity_class_dimension.position",
            ],
            name=op.f("fk_entity_element_entity_class_id_entity_class_dimension"),
            onupdate="CASCADE",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["entity_id", "entity_class_id"],
            ["entity.id", "entity.class_id"],
            name=op.f("fk_entity_element_entity_id_entity"),
            onupdate="CASCADE",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("entity_id", "position", name=op.f("pk_entity_element")),
    )
    _persist_data()
    # NOTE: some constraints are only created by the create_new_spine_database() function,
    # not by the corresponding migration script. Thus we need to check before removing those constraints.
    # We should avoid this in the future.
    entity_class_constraints, entity_constraints = _get_constraints()
    with op.batch_alter_table("entity", naming_convention=naming_convention) as batch_op:
        for cname in ("uq_entity_idclass_id", "uq_entity_idtype_idclass_id"):
            if cname in entity_constraints:
                batch_op.drop_constraint(cname, type_="unique")
        batch_op.drop_constraint("fk_entity_type_id_entity_type", type_="foreignkey")
        batch_op.drop_column("type_id")
    with op.batch_alter_table("entity_class", naming_convention=naming_convention) as batch_op:
        for cname in ("uq_entity_class_idtype_id", "uq_entity_class_type_idname"):
            if cname in entity_class_constraints:
                batch_op.drop_constraint(cname, type_="unique")
        batch_op.drop_constraint("fk_entity_class_type_id_entity_class_type", type_="foreignkey")
        batch_op.drop_constraint("fk_entity_class_commit_id_commit", type_="foreignkey")
        batch_op.drop_column("commit_id")
        batch_op.drop_column("type_id")
    op.drop_table("object_class")
    op.drop_table("entity_class_type")
    # op.drop_table('next_id')
    op.drop_table("object")
    op.drop_table("relationship_entity_class")
    op.drop_table("relationship")
    op.drop_table("entity_type")
    op.drop_table("relationship_class")
    op.drop_table("relationship_entity")


def _get_constraints():
    conn = op.get_bind()
    meta = sa.MetaData(conn)
    meta.reflect()
    return [[c.name for c in meta.tables[tname].constraints] for tname in ["entity_class", "entity"]]


def _persist_data():
    conn = op.get_bind()
    meta = sa.MetaData(conn)
    meta.reflect()
    ecd_items = [
        {"entity_class_id": x["entity_class_id"], "dimension_id": x["member_class_id"], "position": x["dimension"]}
        for x in conn.execute("SELECT * FROM relationship_entity_class")
    ]
    ee_items = [
        {
            "entity_id": x["entity_id"],
            "entity_class_id": x["entity_class_id"],
            "element_id": x["member_id"],
            "dimension_id": x["member_class_id"],
            "position": x["dimension"],
        }
        for x in conn.execute("SELECT * FROM relationship_entity")
    ]
    op.bulk_insert(meta.tables["entity_class_dimension"], ecd_items)
    op.bulk_insert(meta.tables["entity_element"], ee_items)


def downgrade():
    pass
