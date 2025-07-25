"""reencode parameter_values

Revision ID: a973ab537da2
Revises: 91f1f55aa972
Create Date: 2025-05-21 12:49:16.861670

"""

from alembic import op
import sqlalchemy as sa
from spinedb_api.arrow_value import TABLE_TYPE
from spinedb_api.compat.data_transition import transition_data

# revision identifiers, used by Alembic.
revision = "a973ab537da2"
down_revision = "91f1f55aa972"
branch_labels = None
depends_on = None


TYPES_TO_REENCODE = {"time_pattern", "time_series", "array", "map"}


def upgrade():
    conn = op.get_bind()
    metadata = sa.MetaData(bind=conn)
    metadata.reflect(bind=conn)
    _upgrade_table_types(metadata.tables["parameter_definition"], "default_value", "default_type", conn)
    _upgrade_table_types(metadata.tables["parameter_value"], "value", "type", conn)
    _upgrade_table_types(metadata.tables["list_value"], "value", "type", conn)


def downgrade():
    pass


def _upgrade_table_types(table, value_label, type_label, connection):
    value_column = getattr(table.c, value_label)
    type_column = getattr(table.c, type_label)
    batch_data = []
    for id_, old_blob in connection.execute(
        sa.select(table.c.id, value_column).where(type_column.in_(TYPES_TO_REENCODE))
    ):
        new_blob = transition_data(old_blob)
        batch_data.append({"id": id_, type_label: TABLE_TYPE, value_label: new_blob})
    if batch_data:
        update_statement = (
            table.update()
            .where(table.c.id == sa.bindparam("id"))
            .values(
                {
                    "id": sa.bindparam("id"),
                    type_label: sa.bindparam(value_label),
                    value_label: sa.bindparam(value_label),
                }
            )
        )
        connection.execute(update_statement, batch_data)
