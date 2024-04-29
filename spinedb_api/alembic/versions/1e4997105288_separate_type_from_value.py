"""Separate type from value

Revision ID: 1e4997105288
Revises: fbb540efbf15
Create Date: 2021-05-26 16:00:49.244440

"""

import json
from alembic import op
import sqlalchemy as sa
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker


# revision identifiers, used by Alembic.
revision = "1e4997105288"
down_revision = "fbb540efbf15"
branch_labels = None
depends_on = None

LONGTEXT_LENGTH = 2 ** 32 - 1


def upgrade():
    conn = op.get_bind()
    Session = sessionmaker(bind=conn)
    session = Session()
    Base = automap_base()
    Base.prepare(conn, reflect=True)
    # Get items to update
    pd_items = _get_items(session, Base, "parameter_definition")
    pv_items = _get_items(session, Base, "parameter_value")
    pvl_items = _get_pvl_items(session, Base)
    # Alter tables
    with op.batch_alter_table("parameter_definition") as batch_op:
        batch_op.drop_column("data_type")
        batch_op.drop_column("default_value")
        batch_op.add_column(sa.Column("default_value", sa.LargeBinary(LONGTEXT_LENGTH), server_default=sa.null()))
        batch_op.add_column(sa.Column("default_type", sa.String(length=255), nullable=True))
    with op.batch_alter_table("parameter_value") as batch_op:
        batch_op.drop_column("value")
        batch_op.add_column(sa.Column("value", sa.LargeBinary(LONGTEXT_LENGTH), server_default=sa.null()))
        batch_op.add_column(sa.Column("type", sa.String(length=255), nullable=True))
    with op.batch_alter_table("parameter_value_list") as batch_op:
        batch_op.drop_column("value")
        batch_op.add_column(sa.Column("value", sa.LargeBinary(LONGTEXT_LENGTH), server_default=sa.null()))
    # Do update items
    Base = automap_base()
    Base.prepare(conn, reflect=True)
    session.bulk_update_mappings(Base.classes.parameter_definition, pd_items)
    session.bulk_update_mappings(Base.classes.parameter_value, pv_items)
    session.bulk_update_mappings(Base.classes.parameter_value_list, pvl_items)
    session.commit()


def _get_items(session, Base, tablename):
    fields = {"parameter_definition": ("default_value", "default_type"), "parameter_value": ("value", "type")}[
        tablename
    ]
    items = []
    for row in session.query(getattr(Base.classes, tablename)):
        value = getattr(row, fields[0], None)
        if value is None:
            continue
        parsed_value = json.loads(value)
        type_ = parsed_value.pop("type", None) if isinstance(parsed_value, dict) else None
        value = bytes(json.dumps(parsed_value), "UTF8")
        item = dict(zip(fields, (value, type_)))
        item["id"] = row.id
        items.append(item)
    return items


def _get_pvl_items(session, Base):
    return [
        {"id": row.id, "value_index": row.value_index, "value": bytes(row.value, "UTF8")}
        for row in session.query(Base.classes.parameter_value_list)
    ]


def downgrade():
    pass
