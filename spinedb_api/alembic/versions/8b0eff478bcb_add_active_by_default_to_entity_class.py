"""add active_by_default to entity_class

Revision ID: 8b0eff478bcb
Revises: 5385f063bef2
Create Date: 2024-01-12 09:55:08.934574

"""
from alembic import op
import sqlalchemy as sa
import sqlalchemy.orm


# revision identifiers, used by Alembic.
revision = '8b0eff478bcb'
down_revision = '5385f063bef2'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("entity_class") as batch_op:
        batch_op.add_column(
            sa.Column(
                "active_by_default", sa.Boolean(name="active_by_default"), server_default=sa.false(), nullable=False
            ),
        )
    conn = op.get_bind()
    session = sa.orm.sessionmaker(bind=conn)()
    metadata = sa.MetaData()
    metadata.reflect(bind=conn)
    dimension_table = metadata.tables["entity_class_dimension"]
    dimensional_class_ids = {row.entity_class_id for row in session.query(dimension_table)}
    metadata.reflect(bind=conn)
    class_table = metadata.tables["entity_class"]
    update_statement = (
        class_table.update().where(class_table.c.id == sa.bindparam("target_id")).values(active_by_default=True)
    )
    conn.execute(update_statement, [{"target_id": class_id} for class_id in dimensional_class_ids])


def downgrade():
    raise NotImplementedError()
