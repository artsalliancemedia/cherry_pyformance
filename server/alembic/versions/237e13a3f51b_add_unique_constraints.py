"""add_unique_constraints

Revision ID: 237e13a3f51b
Revises: 564fa521b76a
Create Date: 2013-12-05 12:16:08.906000

"""

# revision identifiers, used by Alembic.
revision = '237e13a3f51b'
down_revision = '564fa521b76a'

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.create_unique_constraint("_call_stack_name_uc", "call_stack_names", ['module_name', 'class_name', 'fn_name'])
    op.create_unique_constraint("_sql_strings_uc", "sql_strings", ["sql"])
    op.create_unique_constraint("_sql_stack_item_uc", "sql_stack_items", ["module", "function"])
    op.create_unique_constraint("_sql_arguments_uc", "sql_arguments", ["value"])
    op.create_unique_constraint("_file_names_uc", "file_names", ["filename"])
    op.create_unique_constraint("_metadata_item_uc", "metadata_items", ['key', 'value'])

def downgrade():
    drop_constraint("_call_stack_name_uc", "call_stack_names")
    drop_constraint("_sql_strings_uc", "sql_strings")
    drop_constraint("_sql_stack_item_uc", "sql_stack_items")
    drop_constraint("_sql_arguments_uc", "sql_arguments")
    drop_constraint("_file_names_uc", "file_names")
    drop_constraint("_metadata_item_uc", "metadata_items")