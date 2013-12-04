"""adding stack association table

Revision ID: 564fa521b76a
Revises: 2c1b8e9ae0aa
Create Date: 2013-12-02 16:42:07.882000

"""

# revision identifiers, used by Alembic.
revision = '564fa521b76a'
down_revision = '2c1b8e9ae0aa'

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.create_table(
                    'call_stack_names',
                    sa.Column('id', sa.Integer, primary_key=True),
                    sa.Column('module_name', sa.String),
                    sa.Column('class_name', sa.String),
                    sa.Column('fn_name', sa.String)
                    )
    op.add_column('call_stacks', sa.Column('call_stack_name_id', sa.Integer, sa.ForeignKey('call_stack_names.id')))


    op.drop_table('sql_statement_argument_association')
    op.drop_table('sql_arguements')
    op.create_table(
                    'sql_arguments',
                    sa.Column('id', sa.Integer, primary_key=True),
                    sa.Column('value', sa.String)
                    )
    op.create_table(
                    'sql_arguments_association',
                    sa.Column('sql_statement_id', sa.Integer, sa.ForeignKey('sql_statements.id'), primary_key=True), 
                    sa.Column('sql_argument_id', sa.Integer, sa.ForeignKey('sql_arguments.id'), primary_key=True),
                    sa.Column('index', sa.Integer, primary_key=True)
                    )
    op.create_table(
                    'sql_strings',
                    sa.Column('id', sa.Integer, primary_key=True),
                    sa.Column('sql', sa.String)
                    )
    op.drop_column('sql_stack_items', 'sql_statement_id')
    op.create_table(
                    'sql_stack_association',
                    sa.Column('sql_statement_id', sa.Integer, sa.ForeignKey('sql_statements.id'), primary_key=True), 
                    sa.Column('sql_stack_item_id', sa.Integer, sa.ForeignKey('sql_stack_items.id'), primary_key=True),
                    sa.Column('index', sa.Integer, primary_key=True)
                    )
    op.add_column('sql_statements', sa.Column('sql_string_id', sa.Integer, sa.ForeignKey('sql_strings.id')))

    op.add_column('file_accesses', sa.Column('mode', sa.String,))
    op.create_table(
                    'file_names',
                    sa.Column('id', sa.Integer, primary_key=True),
                    sa.Column('filename', sa.String)
                    )
    op.add_column('file_accesses', sa.Column('file_name_id', sa.Integer, sa.ForeignKey('file_names.id')))

    

def downgrade():
    op.drop_column('file_accesses', 'file_name_id')
    op.drop_table('file_names')
    op.drop_column('file_accesses', 'mode')
    
    op.drop_table('sql_arguments_association')
    op.drop_table('sql_arguments')
    op.create_table(
                    'sql_arguements',
                    sa.Column('id', sa.Integer, primary_key=True),
                    sa.Column('value', sa.String)
                    )
    op.create_table(
                    'sql_statement_argument_association',
                    sa.Column('sql_statement_id', sa.Integer, sa.ForeignKey('sql_statements.id'), primary_key=True), 
                    sa.Column('argument_id', sa.Integer, sa.ForeignKey('sql_arguements.id'), primary_key=True)
                    )
    op.drop_column('sql_statements', 'sql_string_id')
    op.drop_table('sql_stack_association')
    op.add_column('sql_stack_items', sa.Column('sql_statement_id', sa.Integer, sa.ForeignKey('sql_statements.id')))
    op.drop_table('sql_strings')
    
    op.drop_column('call_stacks', 'call_stack_name_id')
    op.drop_table('call_stack_names')
