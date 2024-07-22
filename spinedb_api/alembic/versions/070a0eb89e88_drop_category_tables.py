"""drop category tables

Revision ID: 070a0eb89e88
Revises: bba1e2ef5153
Create Date: 2019-09-20 13:04:52.423483

"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "070a0eb89e88"
down_revision = "bba1e2ef5153"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("object_class") as batch_op:
        batch_op.drop_column("category_id")
        batch_op.create_check_constraint("type_id", "`type_id` = 1")
    with op.batch_alter_table("object") as batch_op:
        batch_op.drop_column("category_id")
        batch_op.create_check_constraint("type_id", "`type_id` = 1")
    op.drop_table("object_class_category")
    op.drop_table("object_category")
    # Sneak some patches in:
    # 1. DEFAULT NULL for parameter_tag.description
    with op.batch_alter_table("parameter_tag") as batch_op:
        batch_op.alter_column("description", server_default=sa.null())
    # 2. Rename parameter_definition constraints. Basically remove and readd them so they get the correct name
    insp = sa.inspect(op.get_bind())
    parameter_definition_pk_name = insp.get_pk_constraint("parameter_definition")["name"]
    parameter_definition_fk_names = [x["name"] for x in insp.get_foreign_keys("parameter_definition")]
    with op.batch_alter_table("parameter_definition") as batch_op:
        if parameter_definition_pk_name == "pk_parameter":
            batch_op.drop_constraint("pk_parameter", type_="primary")
            batch_op.create_primary_key(None, ["id"])
        if "fk_parameter_commit_id_commit" in parameter_definition_fk_names:
            batch_op.drop_constraint("fk_parameter_commit_id_commit", type_="foreignkey")
            batch_op.create_foreign_key(None, "commit", ["commit_id"], ["id"])


def downgrade():
    pass
