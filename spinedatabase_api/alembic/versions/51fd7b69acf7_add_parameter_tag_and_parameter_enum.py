"""Add parameter_tag, parameter_definition_tag, and parameter_enum

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
        'parameter_tag',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('tag', sa.String(155), nullable=False, unique=True),
        sa.Column('description', sa.Unicode(255)),
        sa.Column('commit_id', sa.Integer, sa.ForeignKey('commit.id'), nullable=False)
    )
    op.create_table(
        'parameter_definition_tag',
        sa.Column('id', sa.Integer, nullable=False, primary_key=True),
        sa.Column('parameter_definition_id', sa.Integer, sa.ForeignKey('parameter_definition.id'), nullable=False),
        sa.Column('parameter_tag_id', sa.Integer, sa.ForeignKey('parameter_tag.id'), nullable=False),
        sa.Column('commit_id', sa.Integer, sa.ForeignKey('commit.id'), nullable=False)
    )
    op.create_table(
        'parameter_enum',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('symbol', sa.Unicode(255), primary_key=True, nullable=False),
        sa.Column('value', sa.Unicode(255)),
        sa.Column('commit_id', sa.Integer, sa.ForeignKey('commit.id'), nullable=False)
    )
    with op.batch_alter_table("parameter_definition") as batch_op:
        batch_op.add_column(sa.Column('enum_id', sa.Integer, sa.ForeignKey(
            'parameter_enum.id', name='fk_parameter_definition_enum_id_parameter_enum')))


def downgrade():
    with op.batch_alter_table("parameter_definition") as batch_op:
        batch_op.drop_column('enum_id')
    op.drop_table('parameter_enum')
    op.drop_table('parameter_definition_tag')
    op.drop_table('parameter_tag')
