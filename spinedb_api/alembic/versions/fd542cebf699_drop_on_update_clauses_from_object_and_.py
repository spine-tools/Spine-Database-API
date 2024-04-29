"""drop on update clauses from object and relationship foreign key constraints

Revision ID: fd542cebf699
Revises: 738d494a08ac
Create Date: 2022-01-05 11:00:31.154790

"""
from alembic import op
from spinedb_api.helpers import naming_convention


# revision identifiers, used by Alembic.
revision = "fd542cebf699"
down_revision = "738d494a08ac"
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
            ondelete="CASCADE",
        )
    with op.batch_alter_table("relationship", naming_convention=naming_convention) as batch_op:
        batch_op.drop_constraint("fk_relationship_entity_id_entity", type_="foreignkey")
        batch_op.create_foreign_key(
            op.f("fk_relationship_entity_id_entity"),
            "entity",
            ["entity_id", "type_id"],
            ["id", "type_id"],
            ondelete="CASCADE",
        )


def downgrade():
    pass
