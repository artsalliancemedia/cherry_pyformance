"""add sql arg key

Revision ID: 25606b7db808
Revises: 237e13a3f51b
Create Date: 2014-01-13 12:05:37.214000

"""

# revision identifiers, used by Alembic.
revision = '25606b7db808'
down_revision = '237e13a3f51b'

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.add_column('sql_arguments', sa.Column('key', sa.String))
    op.drop_constraint("sql_arguments_value_key", "sql_arguments")


def downgrade():
    op.drop_column('sql_arguments', 'key')
    op.create_unique_constraint("sql_arguments_value_key", "sql_arguments", ["value"])
