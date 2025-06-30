"""reencode parameter_values

Revision ID: a973ab537da2
Revises: 91f1f55aa972
Create Date: 2025-05-21 12:49:16.861670

"""

from alembic import op
import sqlalchemy as sa
from spinedb_api.arrow_value import TABLE_TYPE
from spinedb_api.compat.data_transition import transition_data

# # Override transition_data for testing. All to-be-transitioned fields
# # are prepended by b"transitioned".
# def transition_data(old):
#     return b"transitioned" + old

# revision identifiers, used by Alembic.
revision = "a973ab537da2"
down_revision = "91f1f55aa972"
branch_labels = None
depends_on = None


# Data of these types need to be converted
TARGET_TYPES = ("date_time", "duration", "time_pattern", "time_series", "array", "map")

# These types need to be changed to `table`
TYPES_TO_TABLE = ("time_series", "array", "map")


def upgrade():
    # Reflect table definition
    conn = op.get_bind()
    metadata = sa.MetaData(bind=conn)

    # Define a lightweight representation of the table
    param_value_table = sa.Table(
        "parameter_value",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("value", sa.BINARY),
        sa.Column("type", sa.String),
    )

    # Select rows to process
    select_statement = sa.select(param_value_table.c.id, param_value_table.c.value, param_value_table.c.type).where(
        param_value_table.c.type.in_(TARGET_TYPES)
    )
    results = conn.execute(select_statement).fetchall()

    # Prepare batch update data
    batch_data = []
    for row in results:
        old_value, new_type = row.value, row.type
        if row.type in TARGET_TYPES:
            new_value = transition_data(old_value)
            if row.type in TYPES_TO_TABLE:
                new_type = TABLE_TYPE
        else:
            new_value = old_value
        batch_data.append({"batch_id": row.id, "new_value": new_value, "new_type": new_type})

    # Apply updates
    update_statement = (
        param_value_table.update()
        .where(param_value_table.c.id == sa.bindparam("batch_id"))
        .values(value=sa.bindparam("new_value"), type=sa.bindparam("new_type"))
    )
    with conn.begin():
        if batch_data:
            conn.execute(update_statement, batch_data)


def downgrade():
    pass
