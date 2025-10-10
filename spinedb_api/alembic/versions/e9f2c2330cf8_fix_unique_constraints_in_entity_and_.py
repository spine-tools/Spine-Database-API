"""fix unique constraints in entity and entity_class tables

Revision ID: e9f2c2330cf8
Revises: 91f1f55aa972
Create Date: 2025-10-06 10:11:27.100719

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "e9f2c2330cf8"
down_revision = "91f1f55aa972"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("entity_class") as batch_op:
        batch_op.create_unique_constraint(None, ["name"])
    with op.batch_alter_table("entity") as batch_op:
        batch_op.create_unique_constraint(None, ["id", "class_id"])


def downgrade():
    pass
