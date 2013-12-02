"""add_sql_arg_index

Revision ID: 2c1b8e9ae0aa
Revises: 1c62c3da4ecb
Create Date: 2013-12-02 15:46:57.115000
 
"""

# revision identifiers, used by Alembic.
revision = '2c1b8e9ae0aa'
down_revision = '1c62c3da4ecb'

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.add_column('sql_arguements', sa.Column('index', sa.Integer))


def downgrade():
    op.drop_column('sql_arguements', 'index')
