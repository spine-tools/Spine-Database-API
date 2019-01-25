"""Add parameter_class and parameter_definition_enum

Revision ID: 51fd7b69acf7
Revises: 8c19c53d5701
Create Date: 2019-01-25 15:47:05.100028

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '51fd7b69acf7'
down_revision = '8c19c53d5701'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'parameter_class',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('name', sa.String(155), nullable=False),
        sa.Column('description', sa.Unicode(255)),
        sa.Column('flags', sa.Unicode(255)),
        sa.Column('commit_id', sa.INTEGER, sa.ForeignKey('commit.id'), nullable=False)
    )
    op.create_table(
        'parameter_definition_enum',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('parameter_definition_id', sa.Integer, sa.ForeignKey('parameter_definition.id'), nullable=False),
        sa.Column('symbol', sa.Unicode(255), nullable=False),
        sa.Column('value', sa.Unicode(255)),
        sa.Column('commit_id', sa.Integer, sa.ForeignKey('commit.id'), nullable=False)
    )
    with op.batch_alter_table("parameter_definition") as batch_op:
        batch_op.add_column(sa.Column('class_id', sa.INTEGER, sa.ForeignKey(
            'parameter_class.id', name='fk_parameter_definition_class_id_parameter_class')))


def downgrade():
    with op.batch_alter_table("parameter_definition") as batch_op:
        batch_op.drop_column('class_id')
    op.drop_table('parameter_definition_enum')
    op.drop_table('parameter_class')
