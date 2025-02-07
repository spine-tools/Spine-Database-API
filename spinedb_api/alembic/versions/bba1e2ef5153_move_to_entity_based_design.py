"""move to entity based design

Revision ID: bba1e2ef5153
Revises: bf255c179bce
Create Date: 2019-09-17 13:38:53.437119

"""

from datetime import datetime
from alembic import op
import sqlalchemy as sa
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = "bba1e2ef5153"
down_revision = "bf255c179bce"
branch_labels = None
depends_on = None


def create_new_tables():
    op.create_table(
        "entity_class_type",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.Unicode(255), nullable=False, unique=True),
        sa.Column("commit_id", sa.Integer, sa.ForeignKey("commit.id")),
    )
    op.create_table(
        "entity_class",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column(
            "type_id",
            sa.Integer,
            sa.ForeignKey("entity_class_type.id", onupdate="CASCADE", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("name", sa.Unicode(255), nullable=False, unique=True),
        sa.Column("description", sa.Unicode(255), server_default=sa.null()),
        sa.Column("display_order", sa.Integer, server_default="99"),
        sa.Column("display_icon", sa.BigInteger, server_default=sa.null()),
        sa.Column("hidden", sa.Integer, server_default="0"),
        sa.Column("commit_id", sa.Integer, sa.ForeignKey("commit.id")),
    )
    op.create_table(
        "temp_relationship_class",
        sa.Column("entity_class_id", sa.Integer),
        sa.Column("type_id", sa.Integer, nullable=False),
    )
    op.create_table(
        "relationship_entity_class",
        sa.Column("entity_class_id", sa.Integer, primary_key=True),
        sa.Column("dimension", sa.Integer, primary_key=True),
        sa.Column("member_class_id", sa.Integer, nullable=False),
        sa.Column("member_class_type_id", sa.Integer, nullable=False),
        sa.ForeignKeyConstraint(
            ("member_class_id", "member_class_type_id"),
            ("entity_class.id", "entity_class.type_id"),
            onupdate="CASCADE",
            ondelete="CASCADE",
        ),
    )
    op.create_table(
        "entity_type",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.Unicode(255), nullable=False, unique=True),
        sa.Column("commit_id", sa.Integer, sa.ForeignKey("commit.id")),
    )
    op.create_table(
        "entity",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("type_id", sa.Integer, sa.ForeignKey("entity_type.id", onupdate="CASCADE", ondelete="CASCADE")),
        sa.Column("class_id", sa.Integer, sa.ForeignKey("entity_class.id", onupdate="CASCADE", ondelete="CASCADE")),
        sa.Column("name", sa.Unicode(255), nullable=False),
        sa.Column("description", sa.String(255), server_default=sa.null()),
        sa.Column("commit_id", sa.Integer, sa.ForeignKey("commit.id")),
        sa.UniqueConstraint("class_id", "name"),
    )
    op.create_table(
        "temp_relationship",
        sa.Column("entity_id", sa.Integer),
        sa.Column("entity_class_id", sa.Integer, nullable=False),
        sa.Column("type_id", sa.Integer, nullable=False),
    )
    op.create_table(
        "relationship_entity",
        sa.Column("entity_id", sa.Integer, primary_key=True),
        sa.Column("entity_class_id", sa.Integer, nullable=False),
        sa.Column("dimension", sa.Integer, primary_key=True),
        sa.Column("member_id", sa.Integer, nullable=False),
        sa.Column("member_class_id", sa.Integer, nullable=False),
        sa.ForeignKeyConstraint(
            ("member_id", "member_class_id"), ("entity.id", "entity.class_id"), onupdate="CASCADE", ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ("entity_class_id", "dimension", "member_class_id"),
            (
                "relationship_entity_class.entity_class_id",
                "relationship_entity_class.dimension",
                "relationship_entity_class.member_class_id",
            ),
            onupdate="CASCADE",
            ondelete="CASCADE",
        ),
    )


def insert_into_new_tables():
    # Easy ones
    op.execute("""INSERT INTO entity_class_type (id, name) VALUES (1, "object")""")
    op.execute("""INSERT INTO entity_class_type (id, name) VALUES (2, "relationship")""")
    op.execute("""INSERT INTO entity_type (id, name) VALUES (1, "object")""")
    op.execute("""INSERT INTO entity_type (id, name) VALUES (2, "relationship")""")
    # More difficult ones
    conn = op.get_bind()
    meta = sa.MetaData()
    meta.reflect(conn)
    # entity class level
    entity_classes = [
        {
            "type_id": 1,
            "name": r.name,
            "description": r.description,
            "display_order": r.display_order,
            "display_icon": r.display_icon,
            "hidden": r.hidden,
            "commit_id": r.commit_id,
        }
        for r in conn.execute(
            text("SELECT name, description, display_order, display_icon, hidden, commit_id FROM object_class")
        )
    ] + [
        {
            "type_id": 2,
            "name": r.name,
            "description": None,
            "display_order": None,
            "display_icon": None,
            "hidden": r.hidden,
            "commit_id": r.commit_id,
        }
        for r in conn.execute(text("SELECT name, hidden, commit_id FROM relationship_class GROUP BY name"))
    ]
    op.bulk_insert(meta.tables["entity_class"], entity_classes)
    # Id mappings
    obj_cls_to_ent_cls = {
        r.object_class_id: r.entity_class_id
        for r in conn.execute(
            text(
                """
            SELECT object_class.id AS object_class_id, entity_class.id AS entity_class_id
            FROM object_class, entity_class
            WHERE entity_class.type_id = 1
            AND object_class.name = entity_class.name
            """
            )
        )
    }
    rel_cls_to_ent_cls = {
        r.relationship_class_id: r.entity_class_id
        for r in conn.execute(
            text(
                """
            SELECT relationship_class.id AS relationship_class_id, entity_class.id AS entity_class_id
            FROM relationship_class, entity_class
            WHERE entity_class.type_id = 2
            AND relationship_class.name = entity_class.name
            GROUP BY relationship_class_id, entity_class_id
            """
            )
        )
    }
    temp_relationship_classes = [
        {"entity_class_id": r.id, "type_id": 2, "commit_id": r.commit_id}
        for r in conn.execute(text("SELECT id, commit_id FROM entity_class WHERE type_id = 2"))
    ]
    op.bulk_insert(meta.tables["temp_relationship_class"], temp_relationship_classes)
    relationship_entity_classes = [
        {
            "entity_class_id": rel_cls_to_ent_cls[r.id],
            "dimension": r.dimension,
            "member_class_id": obj_cls_to_ent_cls[r.object_class_id],
            "member_class_type_id": 1,
            "commit_id": r.commit_id,
        }
        for r in conn.execute(text("SELECT id, dimension, object_class_id, commit_id FROM relationship_class"))
    ]
    op.bulk_insert(meta.tables["relationship_entity_class"], relationship_entity_classes)
    # entity level
    entities = [
        {"type_id": 1, "class_id": obj_cls_to_ent_cls[r.class_id], "name": r.name, "commit_id": r.commit_id}
        for r in conn.execute(text("SELECT class_id, name, commit_id FROM object"))
    ] + [
        {"type_id": 2, "class_id": rel_cls_to_ent_cls[r.class_id], "name": r.name, "commit_id": r.commit_id}
        for r in conn.execute(text("SELECT class_id, name, commit_id FROM relationship GROUP BY class_id, name"))
    ]
    op.bulk_insert(meta.tables["entity"], entities)
    # Id mappings
    obj_to_ent = {
        r.object_id: r.entity_id
        for r in conn.execute(
            text(
                """
            SELECT object.id AS object_id, entity.id AS entity_id
            FROM object, entity
            WHERE entity.type_id = 1
            AND object.name = entity.name
            """
            )
        )
    }
    rel_to_ent = {
        r.relationship_id: r.entity_id
        for r in conn.execute(
            text(
                """
            SELECT relationship.id AS relationship_id, entity.id AS entity_id
            FROM relationship, entity
            WHERE entity.type_id = 2
            AND relationship.name = entity.name
            GROUP BY relationship_id, entity_id
            """
            )
        )
    }
    temp_relationships = [
        {"entity_id": r.id, "entity_class_id": r.class_id, "type_id": 2, "commit_id": r.commit_id}
        for r in conn.execute(text("SELECT id, class_id, commit_id FROM entity WHERE type_id = 2"))
    ]
    op.bulk_insert(meta.tables["temp_relationship"], temp_relationships)
    relationship_entities = [
        {
            "entity_id": rel_to_ent[r.id],
            "entity_class_id": rel_cls_to_ent_cls[r.class_id],
            "dimension": r.dimension,
            "member_id": obj_to_ent[r.object_id],
            "member_class_id": obj_cls_to_ent_cls[r.object_class_id],
            "commit_id": r.commit_id,
        }
        for r in conn.execute(
            text(
                """
            SELECT r.id, r.class_id, r.dimension, o.class_id AS object_class_id, r.object_id, r.commit_id
            FROM relationship AS r, object AS o
            WHERE r.object_id = o.id
            """
            )
        )
    ]
    op.bulk_insert(meta.tables["relationship_entity"], relationship_entities)
    # Return metadata and id mappings
    return (meta, obj_cls_to_ent_cls, rel_cls_to_ent_cls, obj_to_ent, rel_to_ent)


def alter_tables_before_update(meta):
    with op.batch_alter_table("object_class") as batch_op:
        batch_op.add_column(sa.Column("entity_class_id", sa.Integer))
        batch_op.add_column(sa.Column("type_id", sa.Integer))
        batch_op.create_foreign_key(
            None,
            "entity_class",
            ["entity_class_id", "type_id"],
            ["id", "type_id"],
            onupdate="CASCADE",
            ondelete="CASCADE",
        )
    with op.batch_alter_table("object") as batch_op:
        batch_op.add_column(sa.Column("entity_id", sa.Integer))
        batch_op.add_column(sa.Column("type_id", sa.Integer))
        batch_op.create_foreign_key(
            None, "entity", ["entity_id", "type_id"], ["id", "type_id"], onupdate="CASCADE", ondelete="CASCADE"
        )
    with op.batch_alter_table("parameter_definition") as batch_op:
        batch_op.add_column(sa.Column("entity_class_id", sa.Integer))
        batch_op.drop_constraint("uq_parameter_definition_name_class_id", type_="unique")
        batch_op.create_foreign_key(
            None, "entity_class", ["entity_class_id"], ["id"], onupdate="CASCADE", ondelete="CASCADE"
        )
        batch_op.create_unique_constraint(None, ["name", "entity_class_id"])
    with op.batch_alter_table("parameter_value") as batch_op:
        batch_op.alter_column("parameter_definition_id", nullable=False)
        batch_op.add_column(sa.Column("entity_id", sa.Integer))
        batch_op.add_column(sa.Column("entity_class_id", sa.Integer))
        batch_op.create_unique_constraint(None, ["parameter_definition_id", "entity_id"])
        batch_op.create_foreign_key(
            None, "entity", ["entity_id", "entity_class_id"], ["id", "class_id"], onupdate="CASCADE", ondelete="CASCADE"
        )
    # Can you believe some dbs still have the `parameter` table after revision 8c19c53d5701 ???
    if "parameter" in sa.inspect(op.get_bind()).get_table_names():
        op.drop_table("parameter")
    if "next_id" not in meta.tables:
        return
    with op.batch_alter_table("next_id") as batch_op:
        batch_op.drop_column("object_class_id")
        batch_op.drop_column("object_id")
        batch_op.drop_column("relationship_class_id")
        batch_op.drop_column("relationship_id")
        batch_op.add_column(sa.Column("entity_class_type_id", sa.Integer, server_default=sa.null()))
        batch_op.add_column(sa.Column("entity_class_id", sa.Integer, server_default=sa.null()))
        batch_op.add_column(sa.Column("entity_type_id", sa.Integer, server_default=sa.null()))
        batch_op.add_column(sa.Column("entity_id", sa.Integer, server_default=sa.null()))


def update_tables(meta, obj_cls_to_ent_cls, rel_cls_to_ent_cls, obj_to_ent, rel_to_ent):
    conn = op.get_bind()
    ent_to_ent_cls = {r.id: r.class_id for r in conn.execute(text("SELECT id, class_id FROM entity"))}
    for object_class_id, entity_class_id in obj_cls_to_ent_cls.items():
        conn.execute(
            text("UPDATE object_class SET entity_class_id = :entity_class_id, type_id = 1 WHERE id = :object_class_id"),
            {"entity_class_id": entity_class_id, "object_class_id": object_class_id},
        )
        conn.execute(
            text(
                """
            UPDATE parameter_definition SET entity_class_id = :entity_class_id
            WHERE object_class_id = :object_class_id
            """
            ),
            {
                "entity_class_id": entity_class_id,
                "object_class_id": object_class_id,
            },
        )
    for relationship_class_id, entity_class_id in rel_cls_to_ent_cls.items():
        conn.execute(
            text(
                """
            UPDATE parameter_definition SET entity_class_id = :entity_class_id
            WHERE relationship_class_id = :relationship_class_id
            """
            ),
            {
                "entity_class_id": entity_class_id,
                "relationship_class_id": relationship_class_id,
            },
        )
    for object_id, entity_id in obj_to_ent.items():
        conn.execute(
            text("UPDATE object SET entity_id = :entity_id, type_id = 1 WHERE id = :object_id"),
            {
                "entity_id": entity_id,
                "object_id": object_id,
            },
        )
        entity_class_id = ent_to_ent_cls[entity_id]
        conn.execute(
            text(
                """
            UPDATE parameter_value SET entity_id = :entity_id, entity_class_id = :entity_class_id
            WHERE object_id = :object_id
            """
            ),
            {
                "entity_id": entity_id,
                "entity_class_id": entity_class_id,
                "object_id": object_id,
            },
        )
    for relationship_id, entity_id in rel_to_ent.items():
        entity_class_id = ent_to_ent_cls[entity_id]
        conn.execute(
            text(
                """
            UPDATE parameter_value SET entity_id = :entity_id, entity_class_id = :entity_class_id
            WHERE relationship_id = :relationship_id
            """
            ),
            {
                "entity_id": entity_id,
                "entity_class_id": entity_class_id,
                "relationship_id": relationship_id,
            },
        )
    # Clean our potential mess.
    # E.g., I've seen parameter definitions with an invalid relationship_class_id for some reason...!
    conn.execute(text("DELETE FROM parameter_definition WHERE entity_class_id IS NULL"))
    conn.execute(text("DELETE FROM parameter_value WHERE entity_class_id IS NULL OR entity_id IS NULL"))
    if "next_id" not in meta.tables:
        return
    row = conn.execute(text("SELECT MAX(id) FROM entity_class")).fetchone()
    entity_class_id = row[0] + 1 if row else 1
    row = conn.execute(text("SELECT MAX(id) FROM entity")).fetchone()
    entity_id = row[0] + 1 if row else 1
    user = "alembic"
    date = datetime.utcnow()
    conn.execute(
        text(
            """
        UPDATE next_id
        SET
            user = :user,
            date = :date,
            entity_class_type_id = 3,
            entity_type_id = 3,
            entity_class_id = :entity_class_id,
            entity_id = :entity_id
        """
        ),
        {
            "user": user,
            "date": date,
            "entity_class_id": entity_class_id,
            "entity_id": entity_id,
        },
    )


def alter_tables_after_update(meta):
    with op.batch_alter_table("object_class") as batch_op:
        batch_op.drop_column("id")
        batch_op.drop_column("name")
        batch_op.drop_column("description")
        batch_op.drop_column("display_order")
        batch_op.drop_column("display_icon")
        batch_op.drop_column("hidden")
        batch_op.drop_column("commit_id")
        batch_op.alter_column("type_id", nullable=False)
        batch_op.create_check_constraint("type_id", "`type_id` = 1")
        batch_op.create_primary_key(None, ["entity_class_id"])
    with op.batch_alter_table("object") as batch_op:
        batch_op.drop_column("class_id")
        batch_op.drop_column("id")
        batch_op.drop_column("name")
        batch_op.drop_column("description")
        batch_op.drop_column("commit_id")
        batch_op.alter_column("type_id", nullable=False)
        batch_op.create_check_constraint("type_id", "`type_id` = 1")
        batch_op.create_primary_key(None, ["entity_id"])
    op.drop_table("relationship_class")
    op.drop_table("relationship")
    op.rename_table("temp_relationship_class", "relationship_class")
    op.rename_table("temp_relationship", "relationship")
    with op.batch_alter_table("relationship_class") as batch_op:
        batch_op.create_check_constraint("type_id", "`type_id` = 2")
        batch_op.create_primary_key(None, ["entity_class_id"])
        batch_op.create_foreign_key(
            None,
            "entity_class",
            ("entity_class_id", "type_id"),
            ("id", "type_id"),
            onupdate="CASCADE",
            ondelete="CASCADE",
        )
    with op.batch_alter_table("relationship") as batch_op:
        batch_op.create_check_constraint("type_id", "`type_id` = 2")
        batch_op.create_primary_key(None, ["entity_id"])
        batch_op.create_foreign_key(
            None, "entity", ("entity_id", "type_id"), ("id", "type_id"), onupdate="CASCADE", ondelete="CASCADE"
        )
    with op.batch_alter_table("relationship_entity_class") as batch_op:
        batch_op.create_foreign_key(
            None, "relationship_class", ["entity_class_id"], ["entity_class_id"], onupdate="CASCADE", ondelete="CASCADE"
        )
        batch_op.create_check_constraint("member_class_type_id", "`member_class_type_id` != 2")
    with op.batch_alter_table("relationship_entity") as batch_op:
        batch_op.create_foreign_key(
            None,
            "relationship",
            ["entity_id", "entity_class_id"],
            ["entity_id", "entity_class_id"],
            onupdate="CASCADE",
            ondelete="CASCADE",
        )
    with op.batch_alter_table("parameter_definition") as batch_op:
        batch_op.drop_constraint("ck_parameter_obj_or_rel_class_id_is_not_null")
        batch_op.drop_column("object_class_id")
        batch_op.drop_column("relationship_class_id")
        batch_op.alter_column("entity_class_id", nullable=False)
        dummy_relationship_class = next(
            (x.name for x in meta.tables["parameter_definition"].c if x.name.startswith("dummy_relationship_class")),
            None,
        )
        if dummy_relationship_class:
            batch_op.drop_column(dummy_relationship_class)
    with op.batch_alter_table("parameter_value") as batch_op:
        batch_op.drop_constraint("ck_parameter_value_obj_or_rel_id_is_not_null")
        batch_op.drop_column("object_id")
        batch_op.drop_column("relationship_id")
        batch_op.alter_column("entity_class_id", nullable=False)
        batch_op.alter_column("entity_id", nullable=False)
        dummy_relationship = next(
            (x.name for x in meta.tables["parameter_value"].c if x.name.startswith("dummy_relationship")), None
        )
        if dummy_relationship:
            batch_op.drop_column(dummy_relationship)
        batch_op.create_foreign_key(
            None,
            "parameter_definition",
            ["parameter_definition_id", "entity_class_id"],
            ["id", "entity_class_id"],
            onupdate="CASCADE",
            ondelete="CASCADE",
        )


def upgrade():
    create_new_tables()
    meta, obj_cls_to_ent_cls, rel_cls_to_ent_cls, obj_to_ent, rel_to_ent = insert_into_new_tables()
    alter_tables_before_update(meta)
    update_tables(meta, obj_cls_to_ent_cls, rel_cls_to_ent_cls, obj_to_ent, rel_to_ent)
    alter_tables_after_update(meta)


def downgrade():
    # TODO: try and do this???
    pass
