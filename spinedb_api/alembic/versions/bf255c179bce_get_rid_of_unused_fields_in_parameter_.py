"""get rid of unused fields in parameter tables and update unique constraints

Revision ID: bf255c179bce
Revises: 51fd7b69acf7
Create Date: 2019-03-26 15:34:26.550171

"""

from alembic import op
import sqlalchemy as sa

naming_convention = {
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "uq": "uq_%(table_name)s_%(column_0N_name)s",
}


# revision identifiers, used by Alembic.
revision = "bf255c179bce"
down_revision = "51fd7b69acf7"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("parameter_definition") as batch_op:
        batch_op.drop_column("can_have_time_series", type_=sa.Integer)
        batch_op.drop_column("can_have_time_pattern", type_=sa.Integer)
        batch_op.drop_column("can_be_stochastic", type_=sa.Integer)
        batch_op.drop_column("is_mandatory", type_=sa.Integer)
        batch_op.drop_column("precision", type_=sa.Integer)
        batch_op.drop_column("unit", type_=sa.String(155))
        batch_op.drop_column("minimum_value", type_=sa.Float)
        batch_op.drop_column("maximum_value", type_=sa.Float)
    # Move value to json
    op.execute("UPDATE parameter_value SET json = value WHERE json IS NULL and value IS NOT NULL")
    with op.batch_alter_table("parameter_value") as batch_op:
        batch_op.drop_column("index", type_=sa.Integer)
        batch_op.drop_column("value", type_=sa.String(155))
        batch_op.drop_column("expression", type_=sa.String(155))
        batch_op.drop_column("time_pattern", type_=sa.String(155))
        batch_op.drop_column("time_series_id", type_=sa.String(155))
        batch_op.drop_column("stochastic_model_id", type_=sa.String(155))
        batch_op.alter_column("json", new_column_name="value", type_=sa.String(155))
    # Update primary keys
    with op.batch_alter_table("object", naming_convention=naming_convention) as batch_op:
        batch_op.drop_constraint("uq_object_name", type_="unique")
        batch_op.create_unique_constraint("uq_object_name_class_id", ["name", "class_id"])
    with op.batch_alter_table("relationship", naming_convention=naming_convention) as batch_op:
        batch_op.create_unique_constraint("uq_relationship_name_class_id_dimension", ["name", "class_id", "dimension"])
    with op.batch_alter_table("parameter_definition", naming_convention=naming_convention) as batch_op:
        batch_op.drop_constraint("uq_parameter_definition_name", type_="unique")
        batch_op.create_unique_constraint(
            "uq_parameter_definition_name_class_id", ["name", "object_class_id", "relationship_class_id"]
        )


def downgrade():
    with op.batch_alter_table("parameter_definition") as batch_op:
        batch_op.add_column(sa.Column("can_have_time_series", sa.Integer, default=0))
        batch_op.add_column(sa.Column("can_have_time_pattern", sa.Integer, default=1))
        batch_op.add_column(sa.Column("can_be_stochastic", sa.Integer, default=0))
        batch_op.add_column(sa.Column("is_mandatory", sa.Integer, default=0))
        batch_op.add_column(sa.Column("precision", sa.Integer, default=2))
        batch_op.add_column(sa.Column("unit", sa.String(155)))
        batch_op.add_column(sa.Column("minimum_value", sa.Float))
        batch_op.add_column(sa.Column("maximum_value", sa.Float))
    with op.batch_alter_table("parameter_value") as batch_op:
        batch_op.add_column(sa.Column("index", sa.Integer, default=1))
        batch_op.add_column(sa.Column("json", sa.String(255)))
        batch_op.add_column(sa.Column("expression", sa.String(255)))
        batch_op.add_column(sa.Column("time_pattern", sa.String(155)))
        batch_op.add_column(sa.Column("time_series_id", sa.Integer))
        batch_op.add_column(sa.Column("stochastic_model_id", sa.Integer))
    # Update primary keys
    with op.batch_alter_table("object") as batch_op:
        batch_op.drop_constraint("uq_object_name_class_id")
        batch_op.create_unique_constraint("uq_object_name", ["name"])
    with op.batch_alter_table("relationship") as batch_op:
        batch_op.drop_constraint("uq_relationship_name_class_id_dimension")
    with op.batch_alter_table("parameter_definition") as batch_op:
        batch_op.drop_constraint("uq_parameter_definition_name_class_id")
        batch_op.create_unique_constraint("uq_parameter_definition_name", ["name"])
