"""move to entity based design

Revision ID: bba1e2ef5153
Revises: bf255c179bce
Create Date: 2019-09-17 13:38:53.437119

"""
from alembic import op
import sqlalchemy as sa
from spinedb_api import naming_convention


# revision identifiers, used by Alembic.
revision = "bba1e2ef5153"
down_revision = "bf255c179bce"
branch_labels = None
depends_on = None

# TODO: What about `next_id`???


def create_new_tables():
    op.create_table(
        "entity_class_type",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.Unicode(255), nullable=False, unique=True),
        sa.Column("commit_id", sa.Integer, sa.ForeignKey("commit.id", onupdate="CASCADE", ondelete="CASCADE")),
    )
    op.create_table(
        "entity_class",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("type_id", sa.Integer, sa.ForeignKey("entity_class_type.id", onupdate="CASCADE", ondelete="CASCADE")),
        sa.Column("name", sa.Unicode(255), nullable=False, unique=True),
        sa.Column("description", sa.Unicode(255), server_default=sa.null()),
        sa.Column("hidden", sa.Integer, server_default="0"),
        sa.Column("commit_id", sa.Integer, sa.ForeignKey("commit.id", onupdate="CASCADE", ondelete="CASCADE")),
    )
    op.create_table(
        "temp_relationship_class",
        sa.Column("entity_class_id", sa.Integer),
        sa.Column("type_id", sa.Integer, sa.CheckConstraint("type_id=2")),
        sa.Column("commit_id", sa.Integer, sa.ForeignKey("commit.id", onupdate="CASCADE", ondelete="CASCADE")),
        sa.ForeignKeyConstraint(
            ("entity_class_id", "type_id"),
            ("entity_class.id", "entity_class.type_id"),
            name="fk_relationship_class_entity_class_id_entity_class",
            onupdate="CASCADE",
            ondelete="CASCADE",
        ),
    )
    op.create_table(
        "relationship_entity_class",
        sa.Column(
            "entity_class_id",
            sa.Integer,
            sa.ForeignKey("temp_relationship_class.entity_class_id", onupdate="CASCADE", ondelete="CASCADE"),
        ),
        sa.Column("dimension", sa.Integer),
        sa.Column("member_class_id", sa.Integer),
        sa.Column("member_class_type_id", sa.Integer, sa.CheckConstraint("member_class_type_id=1")),
        sa.Column("commit_id", sa.Integer, sa.ForeignKey("commit.id", onupdate="CASCADE", ondelete="CASCADE")),
        sa.ForeignKeyConstraint(
            ("member_class_id", "member_class_type_id"),
            ("entity_class.id", "entity_class.type_id"),
            name="fk_relationship_entity_class_member_class_id_entity_class",
            onupdate="CASCADE",
            ondelete="CASCADE",
        ),
    )
    op.create_table(
        "entity_type",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.Unicode(255), nullable=False, unique=True),
        sa.Column("commit_id", sa.Integer, sa.ForeignKey("commit.id", onupdate="CASCADE", ondelete="CASCADE")),
    )
    op.create_table(
        "entity",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("type_id", sa.Integer, sa.ForeignKey("entity_type.id", onupdate="CASCADE", ondelete="CASCADE")),
        sa.Column("class_id", sa.Integer, sa.ForeignKey("entity_class.id", onupdate="CASCADE", ondelete="CASCADE")),
        sa.Column("name", sa.Unicode(255), nullable=False),
        sa.Column("commit_id", sa.Integer, sa.ForeignKey("commit.id", onupdate="CASCADE", ondelete="CASCADE")),
        sa.UniqueConstraint("class_id", "name"),
    )
    op.create_table(
        "temp_relationship",
        sa.Column("entity_id", sa.Integer),
        sa.Column("entity_class_id", sa.Integer),
        sa.Column("type_id", sa.Integer, sa.CheckConstraint("type_id=2")),
        sa.Column("commit_id", sa.Integer, sa.ForeignKey("commit.id", onupdate="CASCADE", ondelete="CASCADE")),
        sa.ForeignKeyConstraint(
            ("entity_id", "entity_class_id", "type_id"),
            ("entity.id", "entity.class_id", "entity.type_id"),
            name="fk_relationship_entity_id_entity",
            onupdate="CASCADE",
            ondelete="CASCADE",
        ),
    )
    op.create_table(
        "relationship_entity",
        sa.Column("entity_id", sa.Integer),
        sa.Column("entity_class_id", sa.Integer),
        sa.Column("dimension", sa.Integer),
        sa.Column("member_id", sa.Integer),
        sa.Column("member_class_id", sa.Integer),
        sa.Column("commit_id", sa.Integer, sa.ForeignKey("commit.id", onupdate="CASCADE", ondelete="CASCADE")),
        sa.ForeignKeyConstraint(
            ("entity_id", "entity_class_id"),
            ("temp_relationship.entity_id", "temp_relationship.entity_class_id"),
            name="fk_relationship_entity_id_relationship",
            onupdate="CASCADE",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ("member_id", "member_class_id"),
            ("entity.id", "entity.class_id"),
            name="fk_relationship_entity_member_id_entity",
            onupdate="CASCADE",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ("entity_class_id", "dimension", "member_class_id"),
            (
                "relationship_entity_class.entity_class_id",
                "relationship_entity_class.dimension",
                "relationship_entity_class.member_class_id",
            ),
            name="fk_relationship_entity_entity_class_id_relationship_entity_class",
            onupdate="CASCADE",
            ondelete="CASCADE",
        ),
    )


def insert_into_new_tables():
    # Easy ones
    op.execute("""INSERT INTO entity_class_type (id, name) VALUES (1, "object_class")""")
    op.execute("""INSERT INTO entity_class_type (id, name) VALUES (2, "relationship_class")""")
    op.execute("""INSERT INTO entity_type (id, name) VALUES (1, "object")""")
    op.execute("""INSERT INTO entity_type (id, name) VALUES (2, "relationship")""")
    # More difficult ones
    conn = op.get_bind()
    meta = sa.MetaData(conn)
    meta.reflect()
    # entity class level
    entity_classes = [
        {
            "type_id": 1,
            "name": r["name"],
            "description": r["description"],
            "hidden": r["hidden"],
            "commit_id": r["commit_id"],
        }
        for r in conn.execute("SELECT name, description, hidden, commit_id FROM object_class")
    ] + [
        {"type_id": 2, "name": r["name"], "description": None, "hidden": r["hidden"], "commit_id": r["commit_id"]}
        for r in conn.execute("SELECT name, hidden, commit_id FROM relationship_class GROUP BY name")
    ]
    op.bulk_insert(meta.tables["entity_class"], entity_classes)
    # Id mappings
    obj_cls_to_ent_cls = {
        r["object_class_id"]: r["entity_class_id"]
        for r in conn.execute(
            """
            SELECT object_class.id AS object_class_id, entity_class.id AS entity_class_id
            FROM object_class, entity_class
            WHERE entity_class.type_id = 1
            AND object_class.name = entity_class.name
            """
        )
    }
    rel_cls_to_ent_cls = {
        r["relationship_class_id"]: r["entity_class_id"]
        for r in conn.execute(
            """
            SELECT relationship_class.id AS relationship_class_id, entity_class.id AS entity_class_id
            FROM relationship_class, entity_class
            WHERE entity_class.type_id = 2
            AND relationship_class.name = entity_class.name
			GROUP BY relationship_class_id, entity_class_id
            """
        )
    }
    temp_relationship_classes = [
        {"entity_class_id": r["id"], "type_id": 2, "commit_id": r["commit_id"]}
        for r in conn.execute("SELECT id, commit_id FROM entity_class WHERE type_id = 2")
    ]
    op.bulk_insert(meta.tables["temp_relationship_class"], temp_relationship_classes)
    relationship_entity_classes = [
        {
            "entity_class_id": rel_cls_to_ent_cls[r["id"]],
            "dimension": r["dimension"],
            "member_class_id": obj_cls_to_ent_cls[r["object_class_id"]],
            "member_class_type_id": 1,
            "commit_id": r["commit_id"],
        }
        for r in conn.execute("SELECT id, dimension, object_class_id, commit_id FROM relationship_class")
    ]
    op.bulk_insert(meta.tables["relationship_entity_class"], relationship_entity_classes)
    # entity level
    entities = [
        {"type_id": 1, "class_id": obj_cls_to_ent_cls[r["class_id"]], "name": r["name"], "commit_id": r["commit_id"]}
        for r in conn.execute("SELECT class_id, name, commit_id FROM object")
    ] + [
        {"type_id": 2, "class_id": rel_cls_to_ent_cls[r["class_id"]], "name": r["name"], "commit_id": r["commit_id"]}
        for r in conn.execute("SELECT class_id, name, commit_id FROM relationship GROUP BY class_id, name")
    ]
    op.bulk_insert(meta.tables["entity"], entities)
    # Id mappings
    obj_to_ent = {
        r["object_id"]: r["entity_id"]
        for r in conn.execute(
            """
            SELECT object.id AS object_id, entity.id AS entity_id
            FROM object, entity
            WHERE entity.type_id = 1
            AND object.name = entity.name
            """
        )
    }
    rel_to_ent = {
        r["relationship_id"]: r["entity_id"]
        for r in conn.execute(
            """
            SELECT relationship.id AS relationship_id, entity.id AS entity_id
            FROM relationship, entity
            WHERE entity.type_id = 2
            AND relationship.name = entity.name
			GROUP BY relationship_id, entity_id
            """
        )
    }
    temp_relationships = [
        {"entity_id": r["id"], "entity_class_id": r["class_id"], "type_id": 2, "commit_id": r["commit_id"]}
        for r in conn.execute("SELECT id, class_id, commit_id FROM entity WHERE type_id = 2")
    ]
    op.bulk_insert(meta.tables["temp_relationship"], temp_relationships)
    relationship_entities = [
        {
            "entity_id": rel_to_ent[r["id"]],
            "entity_class_id": rel_cls_to_ent_cls[r["class_id"]],
            "dimension": r["dimension"],
            "member_id": obj_to_ent[r["object_id"]],
            "member_class_id": obj_cls_to_ent_cls[r["object_class_id"]],
            "commit_id": r["commit_id"],
        }
        for r in conn.execute(
            """
            SELECT r.id, r.class_id, r.dimension, o.class_id AS object_class_id, r.object_id, r.commit_id
            FROM relationship AS r, object AS o
            WHERE r.object_id = o.id
            """
        )
    ]
    op.bulk_insert(meta.tables["relationship_entity"], relationship_entities)
    # Return metadata and id mappings
    return (meta, obj_cls_to_ent_cls, rel_cls_to_ent_cls, obj_to_ent, rel_to_ent)


def alter_old_tables_before_update():
    with op.batch_alter_table("object_class") as batch_op:
        batch_op.add_column(sa.Column("entity_class_id", sa.Integer))
        batch_op.add_column(sa.Column("type_id", sa.Integer))
        batch_op.create_foreign_key(
            "fk_object_class_entity_class_id_entity_class",
            "entity_class",
            ["entity_class_id", "type_id"],
            ["id", "type_id"],
        )
    with op.batch_alter_table("object", naming_convention=naming_convention) as batch_op:
        batch_op.add_column(sa.Column("entity_id", sa.Integer))
        batch_op.add_column(sa.Column("type_id", sa.Integer))
        batch_op.create_foreign_key("fk_object_entity_id_entity", "entity", ["entity_id", "type_id"], ["id", "type_id"])
    with op.batch_alter_table("parameter_definition", naming_convention=naming_convention) as batch_op:
        batch_op.add_column(sa.Column("entity_class_id", sa.Integer))
        batch_op.drop_constraint("uq_parameter_definition_name_class_id", type_="unique")
        batch_op.create_foreign_key(
            "fk_parameter_definition_entity_class_id_entity_class", "entity_class", ["entity_class_id"], ["id"]
        )
        batch_op.create_unique_constraint("uq_parameter_definition_name_entity_class_id", ["name", "entity_class_id"])
    with op.batch_alter_table("parameter_value", naming_convention=naming_convention) as batch_op:
        batch_op.add_column(sa.Column("entity_id", sa.Integer))
        batch_op.add_column(sa.Column("entity_class_id", sa.Integer))
        batch_op.create_foreign_key(
            "fk_parameter_value_entity_id_entity", "entity", ["entity_id", "entity_class_id"], ["id", "class_id"]
        )


def update_old_tables(obj_cls_to_ent_cls, rel_cls_to_ent_cls, obj_to_ent, rel_to_ent):
    conn = op.get_bind()
    ent_to_ent_cls = {r["id"]: r["class_id"] for r in conn.execute("SELECT id, class_id FROM entity")}
    for object_class_id, entity_class_id in obj_cls_to_ent_cls.items():
        conn.execute(
            "UPDATE object_class SET entity_class_id = :entity_class_id, type_id = 1 WHERE id = :object_class_id",
            entity_class_id=entity_class_id,
            object_class_id=object_class_id,
        )
        conn.execute(
            """
            UPDATE parameter_definition SET entity_class_id = :entity_class_id
            WHERE object_class_id = :object_class_id
            """,
            entity_class_id=entity_class_id,
            object_class_id=object_class_id,
        )
    for relationship_class_id, entity_class_id in rel_cls_to_ent_cls.items():
        conn.execute(
            """
            UPDATE parameter_definition SET entity_class_id = :entity_class_id
            WHERE relationship_class_id = :relationship_class_id
            """,
            entity_class_id=entity_class_id,
            relationship_class_id=relationship_class_id,
        )
    for object_id, entity_id in obj_to_ent.items():
        conn.execute(
            "UPDATE object SET entity_id = :entity_id, type_id = 1 WHERE id = :object_id",
            entity_id=entity_id,
            object_id=object_id,
        )
        entity_class_id = ent_to_ent_cls[entity_id]
        conn.execute(
            """
            UPDATE parameter_value SET entity_id = :entity_id, entity_class_id = :entity_class_id
            WHERE object_id = :object_id
            """,
            entity_id=entity_id,
            entity_class_id=entity_class_id,
            object_id=object_id,
        )

    for relationship_id, entity_id in obj_to_ent.items():
        entity_class_id = ent_to_ent_cls[entity_id]
        conn.execute(
            """
            UPDATE parameter_value SET entity_id = :entity_id, entity_class_id = :entity_class_id
            WHERE relationship_id = :relationship_id
            """,
            entity_id=entity_id,
            entity_class_id=entity_class_id,
            relationship_id=relationship_id,
        )


def alter_old_tables_after_update(meta):
    with op.batch_alter_table("object_class") as batch_op:
        batch_op.drop_column("id")
        batch_op.drop_column("name")
        batch_op.drop_column("description")
        batch_op.drop_column("hidden")
        batch_op.create_check_constraint("ck_object_class_entity_class_type", "type_id = 1")
    with op.batch_alter_table("object") as batch_op:
        batch_op.drop_column("class_id")
        batch_op.drop_column("id")
        batch_op.drop_column("name")
        batch_op.drop_column("description")
        batch_op.create_check_constraint("ck_object_entity_type", "type_id = 1")
    op.drop_table("relationship_class")
    op.drop_table("relationship")
    op.rename_table("temp_relationship_class", "relationship_class")
    op.rename_table("temp_relationship", "relationship")
    par_def_col_names = [x.name for x in meta.tables["parameter_definition"].c]
    with op.batch_alter_table("parameter_definition") as batch_op:
        batch_op.drop_column("object_class_id")
        batch_op.drop_column("relationship_class_id")
        batch_op.drop_column(next(x for x in par_def_col_names if x.startswith("dummy_relationship_class")))
    par_val_col_names = [x.name for x in meta.tables["parameter_value"].c]
    with op.batch_alter_table("parameter_value") as batch_op:
        batch_op.drop_column("object_id")
        batch_op.drop_column("relationship_id")
        batch_op.drop_column(next(x for x in par_val_col_names if x.startswith("dummy_relationship")))


def upgrade():
    create_new_tables()
    meta, obj_cls_to_ent_cls, rel_cls_to_ent_cls, obj_to_ent, rel_to_ent = insert_into_new_tables()
    alter_old_tables_before_update()
    update_old_tables(obj_cls_to_ent_cls, rel_cls_to_ent_cls, obj_to_ent, rel_to_ent)
    alter_old_tables_after_update(meta)


def downgrade():
    # TODO: try and do this???
    pass
