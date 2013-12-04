import sqlalchemy
from sqlalchemy import Table, Column, Integer, String, Float, ForeignKey
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
from threading import Thread
from sqlparse import tokens as sql_tokens, parse as parse_sql
import os
from collections import defaultdict
from operator import attrgetter

Base = declarative_base()



call_stack_metadata_association_table = Table('call_stack_metadata_association', Base.metadata,
    Column('call_stack_id', Integer, ForeignKey('call_stacks.id'), primary_key=True), 
    Column('metadata_id', Integer, ForeignKey('metadata_items.id'), primary_key=True)
)

class CallStack(Base):
    __tablename__ = 'call_stacks'
    id = Column(Integer, primary_key=True)
    call_stack_name_id = Column(Integer, ForeignKey('call_stack_names.id'))
    datetime = Column(Float)
    duration = Column(Float)
    pstat_uuid = Column(String)

    name = relationship('CallStackName', cascade='all', backref='call_stacks')
    metadata_items = relationship('MetaData', secondary=call_stack_metadata_association_table, cascade='all', backref='call_stacks')

    def __init__(self, profile):
        self.datetime = profile['datetime']
        self.duration = profile['duration']
        self.pstat_uuid = profile['pstat_uuid']


    def to_dict(self):
        name = self.name
        response = {'id':self.id,
                    'name': name.full_name(),
                    'datetime':self.datetime,
                    'duration':self.duration,
                    'pstat_uuid':self.pstat_uuid}
        return dict(response.items() + self._metadata().items())

    def _metadata(self):
        list_dict = defaultdict(list)
        for key, value in [meta._to_tuple() for meta in self.metadata_items]:
            list_dict[key].append(value)
        # if list only one item, set to that one item
        list_dict = dict(list_dict)
        for k,v in list_dict.items():
            if len(v)==1:
                list_dict[k] = v[0]
        return list_dict

    # def __repr__(self):
    #     return 'Callstack({0}, {1!s})'.format(self._metadata()['full_name'],int(self.datetime))

class CallStackName(Base):
    __tablename__ = 'call_stack_names'
    id = Column(Integer, primary_key=True)
    module_name = Column(String)
    class_name = Column(String)
    fn_name = Column(String)

    def __init__(self, name_dict):
        self.module_name = name_dict['module_name']
        self.class_name = name_dict['class_name']
        self.fn_name = name_dict['fn_name']
        
    def full_name(self):
        if self.class_name:
            return '{0}.{1}: {2}'.format(self.module_name, self.class_name, self.fn_name)
        else:
            return '{0}.{1}'.format(self.module_name, self.fn_name)



#========================================#


sql_statement_metadata_association_table = Table('sql_statement_metadata_association', Base.metadata,
    Column('sql_statement_id', Integer, ForeignKey('sql_statements.id'), primary_key=True), 
    Column('metadata_id', Integer, ForeignKey('metadata_items.id'), primary_key=True)
)

class SQLStatement(Base):
    __tablename__ = 'sql_statements'
    id = Column(Integer, primary_key=True)
    sql_string_id = Column(Integer, ForeignKey('sql_strings.id'))
    datetime = Column(Float)
    duration = Column(Float)

    sql_string = relationship('SQLString', cascade='all', backref='sql_statements')
    sql_stack_items = relationship('SQLStackAssociation', cascade='all', backref='sql_statements')
    arguments = relationship('SQLArgAssociation', cascade='all', backref='sql_statements')
    metadata_items = relationship('MetaData', secondary=sql_statement_metadata_association_table, cascade='all', backref='sql_statements')

    def __init__(self, profile):
        self.datetime = profile['datetime']
        self.duration = profile['duration']

    def to_dict(self):
        sql = self.sql_string.sql
        response = {'id':self.id,
                    'sql':sql,
                    'datetime':self.datetime,
                    'duration':self.duration,
                    'args':self._args()}
        return dict(response.items() + self._metadata().items())

    def _metadata(self):
        list_dict = defaultdict(list)
        for key, value in [meta._to_tuple() for meta in self.metadata_items]:
            list_dict[key].append(value)
        # if list only one item, set to that one item
        list_dict = dict(list_dict)
        for k,v in list_dict.items():
            if len(v)==1:
                list_dict[k] = v[0]
        return list_dict
    
    def _stack(self):
        self.sql_stack_items.sort(key=attrgetter('index'))
        return [stack_item.stack_item.to_dict() for stack_item in self.sql_stack_items]

    def _args(self):
        self.arguments.sort(key=attrgetter('index'))
        return [arg.arg.value for arg in self.arguments]

    def __repr__(self):
        sql = self._metadata()['sql_string']
        if len(sql)>20:
            sql = sql[0:17]+'...'
        return 'SqlStatement({0}, {1!s})'.format(sql,int(self.datetime))


class SQLString(Base):
    __tablename__ = 'sql_strings'
    id = Column(Integer, primary_key=True)
    sql = Column(String)

    def __init__(self, sql):
        if type(sql)==dict:
            self.sql = sql['sql']
        else:
            self.sql = sql

    def __repr__(self):
        truncated_sql = self.sql[:17]+'...' if len(self.sql)>20 else self.sql
        return 'SQLString({0})'.format(truncated_sql)


class SQLStackAssociation(Base):
    __tablename__ = 'sql_stack_association'
    sql_statement_id = Column(Integer, ForeignKey('sql_statements.id'), primary_key=True)
    sql_stack_item_id = Column(Integer, ForeignKey('sql_stack_items.id'), primary_key=True)
    index = Column(Integer, primary_key=True)

    stack_item = relationship("SQLStackItem", cascade='all', backref="sql_association")


class SQLStackItem(Base):
    __tablename__ = 'sql_stack_items'
    id = Column(Integer, primary_key=True)
    module = Column(String)
    function = Column(String)

    def __init__(self, stat):
        self.function = stat['function']
        self.module = stat['module']

    def to_dict(self):
        return {'module':self.module,
                'function':self.function}

    def __repr__(self):
        return 'SQLStackItem({0})'.format(self.id)


class SQLArgAssociation(Base):
    __tablename__ = 'sql_arguments_association'
    sql_statement_id = Column(Integer, ForeignKey('sql_statements.id'), primary_key=True)
    sql_argument_id = Column(Integer, ForeignKey('sql_arguments.id'), primary_key=True)
    index = Column(Integer, primary_key=True)

    arg = relationship("SQLArg", cascade='all', backref="sql_association")



class SQLArg(Base):
    __tablename__ = 'sql_arguments'
    id = Column(Integer, primary_key=True)
    value = Column(String)

    def __init__(self, value):
        if type(value)==dict:
            self.value = value['value']
        else:
            self.value = value
        
    def __repr__(self):
        return 'SQLArg({0})'.format(self.id)
    


#========================================#


file_access_metadata_association_table = Table('file_access_metadata_association', Base.metadata,
    Column('file_access_id', Integer, ForeignKey('file_accesses.id'), primary_key=True), 
    Column('metadata_id', Integer, ForeignKey('metadata_items.id'), primary_key=True)
)

class FileAccess(Base):
    __tablename__ = 'file_accesses'
    id = Column(Integer, primary_key=True)
    file_name_id = Column(Integer, ForeignKey('file_names.id'))
    time_to_open = Column(Float)
    datetime = Column(Float)
    duration = Column(Float)
    data_written = Column(Integer)
    mode = Column(String)
    
    filename = relationship('FileName', cascade='all', backref='file_accesses')
    metadata_items = relationship('MetaData', secondary=file_access_metadata_association_table, cascade='all', backref='file_accesses')
  
    def __init__(self, profile):
        self.datetime = profile['datetime']
        self.time_to_open = profile['time_to_open']
        self.duration = profile['duration']
        self.data_written = profile['data_written']
        self.mode = profile['mode']
      
    def to_dict(self):
        filename = self.filename.filename
        response = {'id':self.id,
                    'filename':filename,
                    'mode':self.mode,
                    'datetime':self.datetime,
                    'duration':self.duration,
                    'data_written':self.data_written}
        return dict(response.items() + self._metadata().items())
                
    def _metadata(self):
        list_dict = defaultdict(list)
        for key, value in [meta._to_tuple() for meta in self.metadata_items]:
            list_dict[key].append(value)
        # if list only one item, set to that one item
        list_dict = dict(list_dict)
        for k,v in list_dict.items():
            if len(v)==1:
                list_dict[k] = v[0]
        return list_dict
    
    def __repr__(self):
        return 'FileAccess({0}, {1!s})'.format(self._metadata()['filename'],int(self.datetime))


class FileName(Base):
    __tablename__ = 'file_names'
    id = Column(Integer, primary_key=True)
    filename = Column(String)

    def __init__(self, filename):
        if type(filename)==dict:
            self.filename = filename['filename']
        else:
            self.filename = filename





#========================================#

class MetaData(Base):
    __tablename__ = 'metadata_items'
    id = Column(Integer, primary_key=True)
    key = Column(String)
    value = Column(String)

    def __init__(self, meta_dict):
        length = len(meta_dict.items())
        if length==1:
            key,value = meta_dict.items()[0]
        elif length==2:
            key,value = meta_dict['key'], meta_dict['value']
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

