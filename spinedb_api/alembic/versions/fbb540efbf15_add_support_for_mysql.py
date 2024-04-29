"""add support for mysql

Revision ID: fbb540efbf15
Revises: 1892adebc00f
Create Date: 2021-01-05 10:40:16.720937

"""
from alembic import op
from sqlalchemy import Text
from spinedb_api.helpers import LONGTEXT_LENGTH, naming_convention


# revision identifiers, used by Alembic.
revision = "fbb540efbf15"
down_revision = "1892adebc00f"
branch_labels = None
depends_on = None


def upgrade():
    # 1. Drop referential actions in foreign keys with columns also having a check contraint
    with op.batch_alter_table("object_class", naming_convention=naming_convention) as batch_op:
        batch_op.drop_constraint("fk_object_class_entity_class_id_entity_class", type_="foreignkey")
        batch_op.create_foreign_key(
            op.f("fk_object_class_entity_class_id_entity_class"),
            "entity_class",
            ["entity_class_id", "type_id"],
            ["id", "type_id"],
        )
    with op.batch_alter_table("relationship_class", naming_convention=naming_convention) as batch_op:
        batch_op.drop_constraint("fk_relationship_class_entity_class_id_entity_class", type_="foreignkey")
        batch_op.create_foreign_key(
            op.f("fk_relationship_class_entity_class_id_entity_class"),
            "entity_class",
            ["entity_class_id", "type_id"],
            ["id", "type_id"],
        )
    with op.batch_alter_table("relationship_entity_class", naming_convention=naming_convention) as batch_op:
        batch_op.drop_constraint("fk_relationship_entity_class_member_class_id_entity_class", type_="foreignkey")
        batch_op.create_foreign_key(
            op.f("fk_relationship_entity_class_member_class_id_entity_class"),
            "entity_class",
            ["member_class_id", "member_class_type_id"],
            ["id", "type_id"],
        )
    with op.batch_alter_table("object", naming_convention=naming_convention) as batch_op:
        batch_op.drop_constraint("fk_object_entity_id_entity", type_="foreignkey")
        batch_op.create_foreign_key(
            op.f("fk_object_entity_id_entity"), "entity", ["entity_id", "type_id"], ["id", "type_id"]
        )
    with op.batch_alter_table("relationship", naming_convention=naming_convention) as batch_op:
        batch_op.drop_constraint("fk_relationship_entity_id_entity", type_="foreignkey")
        batch_op.create_foreign_key(
            op.f("fk_relationship_entity_id_entity"), "entity", ["entity_id", "type_id"], ["id", "type_id"]
        )
    # 2. Add new unique constraints required to make some foreign keys work
    with op.batch_alter_table("relationship_entity_class", naming_convention=naming_convention) as batch_op:
        batch_op.create_unique_constraint(
            "uq_relationship_entity_class", ["entity_class_id", "dimension", "member_class_id"]
        )
    with op.batch_alter_table("relationship", naming_convention=naming_convention) as batch_op:
        batch_op.create_unique_constraint(
            op.f("uq_relationship_entity_identity_class_id"), ["entity_id", "entity_class_id"]
        )
    with op.batch_alter_table("parameter_definition", naming_convention=naming_convention) as batch_op:
        batch_op.create_unique_constraint(
            op.f("uq_parameter_definition_idparameter_value_list_id"), ["id", "parameter_value_list_id"]
        )
    with op.batch_alter_table("feature", naming_convention=naming_convention) as batch_op:
        batch_op.create_unique_constraint(
            op.f("uq_feature_idparameter_value_list_id"), ["id", "parameter_value_list_id"]
        )
    with op.batch_alter_table("tool_feature", naming_convention=naming_convention) as batch_op:
        batch_op.create_unique_constraint(
            op.f("uq_tool_feature_idparameter_value_list_id"), ["id", "parameter_value_list_id"]
        )
    # 3. Rename constraints having too long name
    with op.batch_alter_table("parameter_definition_tag", naming_convention=naming_convention) as batch_op:
        batch_op.create_unique_constraint(
            "uq_parameter_definition_tag", ["parameter_definition_id", "parameter_tag_id"]
        )
        batch_op.drop_constraint("uq_parameter_definition_tag_parameter_definition_idparameter_tag_id", type_="unique")
    with op.batch_alter_table("parameter_value", naming_convention=naming_convention) as batch_op:
        batch_op.create_unique_constraint(
            "uq_parameter_value", ["parameter_definition_id", "entity_id", "alternative_id"]
        )
        batch_op.drop_constraint("uq_parameter_value_parameter_definition_identity_idalternative_id", type_="unique")
    # 4. Extend length of fields holding parameter values
    with op.batch_alter_table("parameter_definition") as batch_op:
        batch_op.alter_column("default_value", type_=Text(LONGTEXT_LENGTH))
    with op.batch_alter_table("parameter_value") as batch_op:
        batch_op.alter_column("value", type_=Text(LONGTEXT_LENGTH))
    with op.batch_alter_table("parameter_value_list") as batch_op:
        batch_op.alter_column("value", type_=Text(LONGTEXT_LENGTH))
    # 5. Extend length of description fields
    with op.batch_alter_table("alternative") as batch_op:
        batch_op.alter_column("description", type_=Text())
    with op.batch_alter_table("scenario") as batch_op:
        batch_op.alter_column("description", type_=Text())
    with op.batch_alter_table("entity_class") as batch_op:
        batch_op.alter_column("description", type_=Text())
    with op.batch_alter_table("entity") as batch_op:
        batch_op.alter_column("description", type_=Text())
    with op.batch_alter_table("parameter_definition") as batch_op:
        batch_op.alter_column("description", type_=Text())
    with op.batch_alter_table("parameter_tag") as batch_op:
        batch_op.alter_column("description", type_=Text())
    with op.batch_alter_table("tool") as batch_op:
        batch_op.alter_column("description", type_=Text())
    with op.batch_alter_table("feature") as batch_op:
        batch_op.alter_column("description", type_=Text())


def downgrade():
    pass
