"""reencode parameter_values

Revision ID: a973ab537da2
Revises: e9f2c2330cf8
Create Date: 2025-05-21 12:49:16.861670

"""

from typing import Any, Optional, SupportsFloat
from alembic import op
import sqlalchemy as sa
from spinedb_api.compat.converters import parse_duration
from spinedb_api.parameter_value import DateTime, Duration, from_dict, to_database
from spinedb_api.value_support import load_db_value

# revision identifiers, used by Alembic.
revision = "a973ab537da2"
down_revision = "e9f2c2330cf8"
branch_labels = None
depends_on = None


TYPES_TO_REENCODE = {"duration", "date_time", "time_pattern", "time_series", "array", "map"}


def upgrade():
    conn = op.get_bind()
    metadata = sa.MetaData()
    metadata.reflect(bind=conn)
    _upgrade_table_types(metadata.tables["parameter_definition"], "default_value", "default_type", conn)
    _upgrade_table_types(metadata.tables["parameter_value"], "value", "type", conn)
    _upgrade_table_types(metadata.tables["list_value"], "value", "type", conn)


def downgrade():
    pass


def _upgrade_table_types(table, value_label, type_label, connection):
    value_column = getattr(table.c, value_label)
    type_column = getattr(table.c, type_label)
    update_statement = (
        table.update()
        .where(table.c.id == sa.bindparam("id"))
        .values(
            {
                "id": sa.bindparam("id"),
                value_label: sa.bindparam(value_label),
            }
        )
    )
    batch_data = []
    for id_, type_, old_blob in connection.execute(
        sa.select(table.c.id, type_column, value_column).where(type_column.in_(TYPES_TO_REENCODE))
    ):
        legacy_value = _from_database_legacy(old_blob, type_)
        new_blob, _ = to_database(legacy_value)
        batch_data.append({"id": id_, value_label: new_blob})
        if len(batch_data) == 100:
            connection.execute(update_statement, batch_data)
            batch_data.clear()
    if batch_data:
        connection.execute(update_statement, batch_data)


def _from_database_legacy(value: bytes, type_: Optional[str]) -> Optional[Any]:
    parsed = load_db_value(value)
    if isinstance(parsed, dict):
        return from_dict(parsed, type_)
    if type_ == DateTime.TYPE:
        return DateTime(parsed)
    if type_ == Duration.TYPE:
        return Duration(parse_duration(parsed))
    raise RuntimeError(f"migration for {type_} missing")
