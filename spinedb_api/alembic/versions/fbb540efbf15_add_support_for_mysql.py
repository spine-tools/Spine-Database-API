"""add support for mysql

Revision ID: fbb540efbf15
Revises: 1892adebc00f
Create Date: 2021-01-05 10:40:16.720937

"""
from alembic import op
from sqlalchemy import Text
from spinedb_api import naming_convention
from spinedb_api.helpers import LONGTEXT_LENGTH


# revision identifiers, used by Alembic.
revision = 'fbb540efbf15'
down_revision = '1892adebc00f'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("object_class", naming_convention=naming_convention) as batch_op:
        # Drop referential actions in foreign key with columns also having a check contraint
        batch_op.drop_constraint('fk_object_class_entity_class_id_entity_class', type_='foreignkey')
        batch_op.create_foreign_key(
            op.f('fk_object_class_entity_class_id_entity_class'),
            'entity_class',
            ['entity_class_id', 'type_id'],
            ['id', 'type_id'],
        )
    with op.batch_alter_table("relationship_class") as batch_op:
        # Drop referential actions in foreign key with columns also having a check contraint
        batch_op.drop_constraint('fk_relationship_class_entity_class_id_entity_class', type_='foreignkey')
        batch_op.create_foreign_key(
            op.f('fk_relationship_class_entity_class_id_entity_class'),
            'entity_class',
            ['entity_class_id', 'type_id'],
            ['id', 'type_id'],
        )
    with op.batch_alter_table("relationship_entity_class") as batch_op:
        # Drop referential actions in foreign key with columns also having a check contraint
        batch_op.drop_constraint('fk_relationship_entity_class_member_class_id_entity_class', type_='foreignkey')
        batch_op.create_foreign_key(
            op.f('fk_relationship_entity_class_member_class_id_entity_class'),
            'entity_class',
            ['member_class_id', 'member_class_type_id'],
            ['id', 'type_id'],
        )
        # New unique constraint required to make a foreign key work
        batch_op.create_unique_constraint(
            'uq_relationship_entity_class', ['entity_class_id', 'dimension', 'member_class_id'],
        )
    with op.batch_alter_table("object") as batch_op:
        # Drop referential actions in foreign key with columns also having a check contraint
        batch_op.drop_constraint('fk_object_entity_id_entity', type_='foreignkey')
        batch_op.create_foreign_key(
            op.f('fk_object_entity_id_entity'), 'entity', ['entity_id', 'type_id'], ['id', 'type_id']
        )
    with op.batch_alter_table("relationship") as batch_op:
        # Drop referential actions in foreign key with columns also having a check contraint
        batch_op.drop_constraint('fk_relationship_entity_id_entity', type_='foreignkey')
        batch_op.create_foreign_key(
            op.f('fk_relationship_entity_id_entity'), 'entity', ['entity_id', 'type_id'], ['id', 'type_id'],
        )
        # New unique constraint required to make a foreign key work
        batch_op.create_unique_constraint(
            op.f('uq_relationship_entity_identity_class_id'), ['entity_id', 'entity_class_id']
        )
    with op.batch_alter_table("parameter_definition") as batch_op:
        # New unique constraint required to make a foreign key work
        batch_op.create_unique_constraint(
            op.f('uq_parameter_definition_idparameter_value_list_id'), ['id', 'parameter_value_list_id'],
        )
        # Extend length of fields holding parameter values
        batch_op.alter_column('default_value', type_=Text(LONGTEXT_LENGTH))
    with op.batch_alter_table("feature") as batch_op:
        # New unique constraint required to make a foreign key work
        batch_op.create_unique_constraint(
            op.f('uq_feature_idparameter_value_list_id'), ['id', 'parameter_value_list_id']
        )
    with op.batch_alter_table("tool_feature") as batch_op:
        # New unique constraint required to make a foreign key work
        batch_op.create_unique_constraint(
            op.f('uq_tool_feature_idparameter_value_list_id'), ['id', 'parameter_value_list_id']
        )
    with op.batch_alter_table("parameter_definition_tag") as batch_op:
        # Rename constraint having too long name
        batch_op.create_unique_constraint(
            'uq_parameter_definition_tag', ['parameter_definition_id', 'parameter_tag_id']
        )
        batch_op.drop_constraint(
            'uq_parameter_definition_tag_parameter_definition_idparameter_tag_id', type_='unique',
        )
    with op.batch_alter_table("parameter_value") as batch_op:
        # Rename constraint having too long name
        batch_op.create_unique_constraint(
            'uq_parameter_value', ['parameter_definition_id', 'entity_id', 'alternative_id']
        )
        batch_op.drop_constraint('uq_parameter_value_parameter_definition_identity_idalternative_id', type_='unique')
        # Extend length of fields holding parameter values
        batch_op.alter_column('value', type_=Text(LONGTEXT_LENGTH))
    with op.batch_alter_table("parameter_value_list") as batch_op:
        # Extend length of fields holding parameter values
        batch_op.alter_column('value', type_=Text(LONGTEXT_LENGTH))


def downgrade():
    pass
