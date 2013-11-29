"""add sql args tables

Revision ID: 1c62c3da4ecb
Revises: None
Create Date: 2013-11-29 18:12:30.086000

"""

# revision identifiers, used by Alembic.
revision = '1c62c3da4ecb'
down_revision = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.create_table(
                    'sql_statement_argument_association',
                    sa.Column('sql_statement_id', sa.Integer, sa.ForeignKey('sql_statements.id'), primary_key=True), 
                    sa.Column('argument_id', sa.Integer, sa.ForeignKey('sql_arguements.id'), primary_key=True)
                    )
    op.create_table(
                    'sql_arguements',
                    sa.Column('id', sa.Integer, primary_key=True),
                    sa.Column('value',sa.String)
                    )

def downgrade():
    op.drop_table('sql_statement_argument_association')
    op.drop_table('sql_arguements')
