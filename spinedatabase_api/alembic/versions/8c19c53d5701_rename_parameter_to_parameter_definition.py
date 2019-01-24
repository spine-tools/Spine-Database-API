"""rename parameter to parameter_definition

Revision ID: 8c19c53d5701
Revises:
Create Date: 2019-01-24 16:47:21.493240

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '8c19c53d5701'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("parameter_value") as batch_op:
        batch_op.alter_column('parameter_id', new_column_name='parameter_definition_id')
    op.rename_table('parameter', 'parameter_definition')

def downgrade():
    op.rename_table('parameter_definition', 'parameter')
    with op.batch_alter_table("parameter_value") as batch_op:
        batch_op.alter_column('parameter_definition_id', new_column_name='parameter_id')
