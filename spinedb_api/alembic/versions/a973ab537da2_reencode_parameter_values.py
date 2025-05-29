"""reencode parameter_values

Revision ID: a973ab537da2
Revises: 91f1f55aa972
Create Date: 2025-05-21 12:49:16.861670

"""

from alembic import op
import sqlalchemy as sa
from spinedb_api.compat.reencode_for_data_transition import transition_data


# revision identifiers, used by Alembic.
revision = "a973ab537da2"
down_revision = "91f1f55aa972"
branch_labels = None
depends_on = None


def upgrade():
    # Reflect table definition
    conn = op.get_bind()
    metadata = sa.MetaData()
    metadata.bind = conn

    # Define a lightweight representation of the table
    my_table = sa.Table(
        "parameter_value",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("value", sa.BINARY),
        sa.Column("type", sa.String),  # TODO do we need the type?
    )

    # Read current data
    results = conn.execute(sa.select(my_table.c.id, my_table.c.value, my_table.c.type)).fetchall()

    # NOTE: maybe this should be derived from `models.ValueTypeNames`,
    # but we don't want non-JSON values like integer, number, boolean;
    # also some names differ by `-` <-> `_`
    convertible = ("date_time", "duration", "time_pattern", "time_series", "array", "map")

    # Apply transformation
    for row in results:
        if row.type not in convertible:
            continue
        old_value = row.value
        new_value = transition_data(old_value)

        # FIXME:
        # - `type` also needs translation; from the `convertible`
        #   list, ("time_series", "array", "map") -> "table"
        # - can the insertions be queued?
        # Update the row
        conn.execute(my_table.update().where(my_table.c.id == row.id).values(value=new_value))


def downgrade():
    pass
