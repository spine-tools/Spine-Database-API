"""fix foreign key constraints in object and relationship tables

Revision ID: 738d494a08ac
Revises: 1e4997105288
Create Date: 2022-01-05 09:18:48.858784

"""
from alembic import op
from spinedb_api.helpers import naming_convention


# revision identifiers, used by Alembic.
revision = "738d494a08ac"
down_revision = "1e4997105288"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("object", naming_convention=naming_convention) as batch_op:
        batch_op.drop_constraint("fk_object_entity_id_entity", type_="foreignkey")
        batch_op.create_foreign_key(
            op.f("fk_object_entity_id_entity"),
            "entity",
            ["entity_id", "type_id"],
            ["id", "type_id"],
            onupdate="CASCADE",
            ondelete="CASCADE",
        )
    with op.batch_alter_table("relationship", naming_convention=naming_convention) as batch_op:
        batch_op.drop_constraint("fk_relationship_entity_id_entity", type_="foreignkey")
        batch_op.create_foreign_key(
            op.f("fk_relationship_entity_id_entity"),
            "entity",
            ["entity_id", "type_id"],
            ["id", "type_id"],
            onupdate="CASCADE",
            ondelete="CASCADE",
        )


def downgrade():
    pass
