"""fix foreign key constraints in entity classes

Revision ID: 7d0b467f2f4e
Revises: fd542cebf699
Create Date: 2022-02-15 15:41:06.794006

"""
from alembic import op
from spinedb_api.helpers import naming_convention


# revision identifiers, used by Alembic.
revision = "7d0b467f2f4e"
down_revision = "fd542cebf699"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("object_class", naming_convention=naming_convention) as batch_op:
        batch_op.drop_constraint("fk_object_class_entity_class_id_entity_class", type_="foreignkey")
        batch_op.create_foreign_key(
            op.f("fk_object_class_entity_class_id_entity_class"),
            "entity_class",
            ["entity_class_id", "type_id"],
            ["id", "type_id"],
            ondelete="CASCADE",
        )
    with op.batch_alter_table("relationship_class", naming_convention=naming_convention) as batch_op:
        batch_op.drop_constraint("fk_relationship_class_entity_class_id_entity_class", type_="foreignkey")
        batch_op.create_foreign_key(
            op.f("fk_relationship_class_entity_class_id_entity_class"),
            "entity_class",
            ["entity_class_id", "type_id"],
            ["id", "type_id"],
            ondelete="CASCADE",
        )


def downgrade():
    pass
