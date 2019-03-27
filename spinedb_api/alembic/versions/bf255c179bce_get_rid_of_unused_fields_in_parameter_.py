"""get rid of unused fields in parameter tables and update unique constraints

Revision ID: bf255c179bce
Revises: 51fd7b69acf7
Create Date: 2019-03-26 15:34:26.550171

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'bf255c179bce'
down_revision = '51fd7b69acf7'
branch_labels = None
depends_on = None

naming_convention = {
    "uq": "uq_%(table_name)s_%(column_0_N_name)s"
}

def upgrade():
    with op.batch_alter_table("parameter_definition") as batch_op:
        batch_op.drop_column('can_have_time_series')
        batch_op.drop_column('can_have_time_pattern')
        batch_op.drop_column('can_be_stochastic')
        batch_op.drop_column('is_mandatory')
        batch_op.drop_column('precision')
        batch_op.drop_column('unit')
        batch_op.drop_column('minimum_value')
        batch_op.drop_column('maximum_value')
    # Move value to json
    op.execute("UPDATE parameter_value SET json = value WHERE json IS NULL and value IS NOT NULL")
    with op.batch_alter_table("parameter_value") as batch_op:
        batch_op.drop_column('index')
        batch_op.drop_column('value')
        batch_op.drop_column('expression')
        batch_op.drop_column('time_pattern')
        batch_op.drop_column('time_series_id')
        batch_op.drop_column('stochastic_model_id')
        batch_op.alter_column('json', new_column_name='value')
    # Update primary keys
    with op.batch_alter_table("object", naming_convention=naming_convention) as batch_op:
        batch_op.drop_constraint('uq_object_name')
        batch_op.create_unique_constraint("uq_object_name_class_id", ["name", "class_id"])
    with op.batch_alter_table("relationship", naming_convention=naming_convention) as batch_op:
        batch_op.create_unique_constraint("uq_relationship_name_class_id_dimension", ["name", "class_id", "dimension"])
    with op.batch_alter_table("parameter_definition", naming_convention=naming_convention) as batch_op:
        batch_op.drop_constraint('uq_parameter_definition_name')
        batch_op.create_unique_constraint(
            "uq_parameter_definition_name_class_id", ["name", "object_class_id", "relationship_class_id"])

def downgrade():
    with op.batch_alter_table("parameter_definition") as batch_op:
        batch_op.add_column(sa.Column('can_have_time_series', sa.Integer, default=0))
        batch_op.add_column(sa.Column('can_have_time_pattern', sa.Integer, default=1))
        batch_op.add_column(sa.Column('can_be_stochastic', sa.Integer, default=0))
        batch_op.add_column(sa.Column('is_mandatory', sa.Integer, default=0))
        batch_op.add_column(sa.Column('precision', sa.Integer, default=2))
        batch_op.add_column(sa.Column('unit', sa.String(155)))
        batch_op.add_column(sa.Column('minimum_value', sa.Float))
        batch_op.add_column(sa.Column('maximum_value', sa.Float))
    with op.batch_alter_table("parameter_value") as batch_op:
        batch_op.add_column(sa.Column('index', sa.Integer, default=1))
        batch_op.add_column(sa.Column('json', sa.String(255)))
        batch_op.add_column(sa.Column('expression', sa.String(255)))
        batch_op.add_column(sa.Column('time_pattern', sa.String(155)))
        batch_op.add_column(sa.Column('time_series_id', sa.Integer))
        batch_op.add_column(sa.Column('stochastic_model_id', sa.Integer))
    # Update primary keys
    with op.batch_alter_table("object") as batch_op:
        batch_op.drop_constraint('uq_object_name_class_id')
        batch_op.create_unique_constraint("uq_object_name", ["name"])
    with op.batch_alter_table("relationship") as batch_op:
        batch_op.drop_constraint('uq_relationship_name_class_id_dimension')
    with op.batch_alter_table("parameter_definition") as batch_op:
        batch_op.drop_constraint('uq_parameter_definition_name_class_id')
        batch_op.create_unique_constraint("uq_parameter_definition_name", ["name"])
