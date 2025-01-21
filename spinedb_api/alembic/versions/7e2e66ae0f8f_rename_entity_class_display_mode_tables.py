"""rename entity class display mode tables

Revision ID: 7e2e66ae0f8f
Revises: 02581198a2d8
Create Date: 2024-08-21 09:25:22.928566

"""

from alembic import op
import sqlalchemy as sa
from spinedb_api import naming_convention

# revision identifiers, used by Alembic.
revision = "7e2e66ae0f8f"
down_revision = "02581198a2d8"
branch_labels = None
depends_on = None


def upgrade():
    m = sa.MetaData()
    m.reflect(op.get_bind())
    with op.batch_alter_table("entity_class_display_mode") as batch_op:
        batch_op.drop_constraint("pk_entity_class_display_mode", type_="primary")
        batch_op.create_primary_key("pk_display_mode", ["id"])
        batch_op.drop_constraint("uq_entity_class_display_mode_name", type_="unique")
        batch_op.create_unique_constraint("uq_display_mode_name", ["name"])
    with op.batch_alter_table("display_mode__entity_class") as batch_op:
        batch_op.alter_column("entity_class_display_mode_id", new_column_name="display_mode_id")
        batch_op.drop_constraint("pk_display_mode__entity_class", type_="primary")
        batch_op.create_primary_key("pk_entity_class_display_mode", ["id"])
        batch_op.drop_constraint("fk_display_mode__entity_class_entity_class_id_entity_class", type_="foreignkey")
        batch_op.create_foreign_key(
            "fk_entity_class_display_mode_entity_class_id_entity_class",
            "entity_class",
            ["entity_class_id"],
            ["id"],
            ondelete="CASCADE",
            onupdate="CASCADE",
        )
        batch_op.drop_constraint(
            "fk_display_mode__entity_class_entity_class_display_mode_id_entity_class_display_mode", type_="foreignkey"
        )
        batch_op.drop_constraint("display_status_enum", type_="check")
    op.rename_table("entity_class_display_mode", "display_mode")
    op.rename_table("display_mode__entity_class", "entity_class_display_mode")
    with op.batch_alter_table("entity_class_display_mode") as batch_op:
        batch_op.create_foreign_key(
            "fk_entity_class_display_mode_display_mode_id_display_mode",
            "display_mode",
            ["display_mode_id"],
            ["id"],
            ondelete="CASCADE",
            onupdate="CASCADE",
        )
        batch_op.drop_constraint(
            "uq_display_mode__entity_class_entity_class_display_mode_identity_class_id", type_="unique"
        )
        batch_op.create_unique_constraint(
            "uq_entity_class_display_mode_display_mode_identity_class_id", ["display_mode_id", "entity_class_id"]
        )
        batch_op.create_check_constraint("display_status_enum", "display_status IN ('visible', 'hidden', 'greyed_out')")


def downgrade():
    pass
