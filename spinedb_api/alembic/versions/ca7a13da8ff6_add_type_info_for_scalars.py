"""add_type_info_for_scalars

Revision ID: ca7a13da8ff6
Revises: e010777927a5
Create Date: 2024-06-26 11:55:40.006129

"""

import json
from alembic import op
import sqlalchemy as sa
from spinedb_api.parameter_value import type_for_scalar

# revision identifiers, used by Alembic.
revision = "ca7a13da8ff6"
down_revision = "e010777927a5"
branch_labels = None
depends_on = None


def upgrade():
    connection = op.get_bind()
    metadata = sa.MetaData()
    metadata.reflect(bind=connection)
    _update_scalar_type_info(metadata.tables["parameter_definition"], "default_value", "default_type", connection)
    _update_scalar_type_info(metadata.tables["parameter_value"], "value", "type", connection)
    _update_scalar_type_info(metadata.tables["list_value"], "value", "type", connection)


def downgrade():
    pass


def _update_scalar_type_info(table, value_label, type_label, connection):
    value_by_id = _get_scalar_values_by_id(table, value_label, type_label, connection)
    update_statement = table.update()
    for id_, value in value_by_id.items():
        if value is None:
            continue
        parsed_value = json.loads(value)
        if parsed_value is None:
            continue
        value_type = type_for_scalar(parsed_value)
        connection.execute(update_statement.where(table.c.id == id_), {type_label: value_type})


def _get_scalar_values_by_id(table, value_label, type_label, connection):
    value_column = getattr(table.c, value_label)
    type_column = getattr(table.c, type_label)
    return {
        row.id: row._mapping[value_label]
        for row in connection.execute(sa.select(table.c.id, value_column).where(type_column == None))
    }
