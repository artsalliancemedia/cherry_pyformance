import sqlalchemy
from sqlalchemy import Table, Column, Integer, String, Float, ForeignKey
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
from threading import Thread
from sqlparse import tokens as sql_tokens, parse as parse_sql
import os
from collections import defaultdict
import pstats

Base = declarative_base()

call_stack_metadata_association_table = Table('call_stack_metadata_association', Base.metadata,
    Column('call_stack_id', Integer, ForeignKey('call_stacks.id'), primary_key=True), 
    Column('metadata_id', Integer, ForeignKey('metadata_items.id'), primary_key=True)
)

sql_statement_metadata_association_table = Table('sql_statement_metadata_association', Base.metadata,
    Column('sql_statement_id', Integer, ForeignKey('sql_statements.id'), primary_key=True), 
    Column('metadata_id', Integer, ForeignKey('metadata_items.id'), primary_key=True)
)
sql_statement_argument_association_table = Table('sql_statement_argument_association', Base.metadata,
    Column('sql_statement_id', Integer, ForeignKey('sql_statements.id'), primary_key=True), 
    Column('argument_id', Integer, ForeignKey('sql_arguements.id'), primary_key=True)
)

file_access_metadata_association_table = Table('file_access_metadata_association', Base.metadata,
    Column('file_access_id', Integer, ForeignKey('file_accesses.id'), primary_key=True), 
    Column('metadata_id', Integer, ForeignKey('metadata_items.id'), primary_key=True)
)

class CallStack(Base):
    __tablename__ = 'call_stacks'
    id = Column(Integer, primary_key=True)
    datetime = Column(Float)
    duration = Column(Float)
    pstat_uuid = Column(String)

    metadata_items = relationship('MetaData', secondary=call_stack_metadata_association_table, cascade='all', backref='call_stacks')

    def __init__(self, profile):
        self.datetime = profile['datetime']
        self.duration = profile['duration']
        self.pstat_uuid = profile['pstat_uuid']

    def _to_dict(self):
        response = {'id':self.id,
                    'datetime':self.datetime,
                    'duration':self.duration,
                    'pstat_uuid':self.pstat_uuid}
        return dict(response.items() + self._metadata().items())
    
    def _stats(self):
        return pstats.Stats(os.path.join(os.getcwd(),'pstats',self.pstat_uuid))
                
    def _metadata(self):
        list_dict = defaultdict(list)
        for key, value in [meta._to_tuple() for meta in self.metadata_items]:
            list_dict[key].append(value)
        return list_dict

    def __repr__(self):
        return 'Callstack({0}, {1!s})'.format(self._metadata()['full_name'],int(self.datetime))


class SQLStatement(Base):
    __tablename__ = 'sql_statements'
    id = Column(Integer, primary_key=True)
    datetime = Column(Float)
    duration = Column(Float)

    sql_stack_items = relationship('SQLStackItem', cascade='all', backref='sql_statements')
    arguments = relationship('SQLArg', secondary=sql_statement_argument_association_table, cascade='all', backref='sql_statements')
    metadata_items = relationship('MetaData', secondary=sql_statement_metadata_association_table, cascade='all', backref='sql_statements')

    def __init__(self, profile):
        self.datetime = profile['datetime']
        self.duration = profile['duration']

    def _to_dict(self):
        response = {'id':self.id,
                    'datetime':self.datetime,
                    'duration':self.duration,
                    'args':self._args()}
        return dict(response.items() + self._metadata().items())

    def _metadata(self):
        list_dict = defaultdict(list)
        for key, value in [meta._to_tuple() for meta in self.metadata_items]:
            list_dict[key].append(value)
        return list_dict
    
    def _stack(self):
        return [stack._to_dict() for stack in self.sql_stack_items]

    def _args(self):
        return [arg.value for arg in self.arguments]

    def __repr__(self):
        sql = self._metadata()['sql_string']
        if len(sql)>20:
            sql = sql[0:17]+'...'
        return 'SqlStatement({0}, {1!s})'.format(sql,int(self.datetime))


class FileAccess(Base):
    __tablename__ = 'file_accesses'
    id = Column(Integer, primary_key=True)
    time_to_open = Column(Float)
    datetime = Column(Float)
    duration = Column(Float)
    data_written = Column(Integer)
    
    metadata_items = relationship('MetaData', secondary=file_access_metadata_association_table, cascade='all', backref='file_accesses')
  
    def __init__(self, profile):
        self.datetime = profile['datetime']
        self.time_to_open = profile['time_to_open']
        self.duration = profile['duration']
        self.data_written = profile['data_written']
      
    def _to_dict(self):
        response = {'id':self.id,
                    'datetime':self.datetime,
                    'duration':self.duration,
                    'data_written':self.data_written}
        return dict(response.items() + self._metadata().items())
                
    def _metadata(self):
        list_dict = defaultdict(list)
        for key, value in [meta._to_tuple() for meta in self.metadata_items]:
            list_dict[key].append(value)
        return list_dict
    
    def __repr__(self):
        return 'FileAccess({0}, {1!s})'.format(self._metadata()['filename'],int(self.datetime))

#========================================#


class SQLStackItem(Base):
    __tablename__ = 'sql_stack_items'
    id = Column(Integer, primary_key=True)
    sql_statement_id = Column(Integer, ForeignKey('sql_statements.id'))
    function = Column(String)
    module = Column(String)

    def __init__(self, stat):
        self.function = stat['function']
        self.module = stat['module']

    def _to_dict(self):
        return {'module':self.module,
                'function':self.function}

    def _metadata(self):
        return dict([meta._to_tuple() for meta in self.metadata_items])

    def __repr__(self):
        return 'SQLStackItem({0})'.format(self.id)


class SQLArg(Base):
    __tablename__ = 'sql_arguements'
    id = Column(Integer, primary_key=True)
    value = Column(String)
    index = Column(Integer)

    def __init__(self, value, index):
        self.value = value
        self.index = index

    def __repr__(self):
        return 'SQLArg({0})'.format(self.value)
    


#========================================#

class MetaData(Base):
    __tablename__ = 'metadata_items'
    id = Column(Integer, primary_key=True)
    key = Column(String)
    value = Column(String)

    def __init__(self, key, value):
        self.key = key
        self.value = value

    def _to_tuple(self):
        return (self.key,self.value)

    def __repr__(self):
        return 'MetaData({0}={1})'.format(self.key,self.value)

#========================================#

def create_db_and_connect(postgres_string):
    database = sqlalchemy.create_engine(postgres_string + '/profile_stats')
    database.connect()
    return database

session = None

def setup_profile_database(username, password):
    postgres_string = 'postgresql://' + username + ':' + password + '@localhost'
    try:
        #print os.system('alembic upgrade head')
        db = create_db_and_connect(postgres_string)
    except:
        postgres = sqlalchemy.create_engine(postgres_string + '/postgres')
        conn = postgres.connect()
        conn.execute('commit')
        conn.execute('create database profile_stats')
        conn.close()
        db = create_db_and_connect(postgres_string)
        Base.metadata.create_all(db)
        
        # Stamp table with current version for Alembic upgrades
        from alembic.config import Config
        from alembic import command
        alembic_cfg = Config("alembic.ini")
        command.stamp(alembic_cfg, "head")
    global Session
    global session
    Session = sessionmaker(bind=db)
    session = Session()

