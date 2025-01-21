"""rename parameter to parameter_definition

Revision ID: 8c19c53d5701
Revises:
Create Date: 2019-01-24 16:47:21.493240

"""

from alembic import op
import sqlalchemy as sa

naming_convention = {
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "uq": "uq_%(table_name)s_%(column_0N_name)s",
}

# revision identifiers, used by Alembic.
revision = "8c19c53d5701"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    m = sa.MetaData()
    m.reflect(op.get_bind())
    if "next_id" in m.tables:
        with op.batch_alter_table("next_id") as batch_op:
            batch_op.alter_column("parameter_id", new_column_name="parameter_definition_id", type_=sa.Integer)
    with op.batch_alter_table("parameter_value", naming_convention=naming_convention) as batch_op:
        batch_op.alter_column("parameter_id", new_column_name="parameter_definition_id", type_=sa.Integer)
        batch_op.drop_constraint("fk_parameter_value_parameter_id_parameter", type_="foreignkey")
    with op.batch_alter_table("parameter", naming_convention=naming_convention) as batch_op:
        batch_op.drop_constraint("uq_parameter_name", type_="unique")
        batch_op.create_unique_constraint("uq_parameter_definition_name", ["name"])
    op.rename_table("parameter", "parameter_definition")
    with op.batch_alter_table("parameter_value", naming_convention=naming_convention) as batch_op:
        batch_op.create_foreign_key(
            "fk_parameter_value_parameter_definition_id_parameter_definition",
            "parameter_definition",
            ["parameter_definition_id"],
            ["id"],
        )


def downgrade():
    m = sa.MetaData(op.get_bind())
    m.reflect()
    if "next_id" in m.tables:
        with op.batch_alter_table("next_id") as batch_op:
            batch_op.alter_column("parameter_definition_id", new_column_name="parameter_id")
    with op.batch_alter_table("parameter_value") as batch_op:
        batch_op.alter_column("parameter_definition_id", new_column_name="parameter_id", type_=sa.Integer)
        batch_op.drop_constraint("fk_parameter_value_parameter_definition_id_parameter_definition", type_="foreignkey")
    op.rename_table("parameter_definition", "parameter")
    with op.batch_alter_table("parameter_value", naming_convention=naming_convention) as batch_op:
        batch_op.create_foreign_key("fk_parameter_value_parameter_id_parameter", "parameter", ["parameter_id"], ["id"])
