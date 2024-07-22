"""change active_by_default server default to true

Revision ID: e010777927a5
Revises: 8b0eff478bcb
Create Date: 2024-05-13 13:00:28.059409

"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "e010777927a5"
down_revision = "8b0eff478bcb"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("entity_class") as batch_op:
        batch_op.alter_column("active_by_default", server_default=sa.true())


def downgrade():
    pass
