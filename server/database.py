import sqlalchemy
from sqlalchemy import Table, Column, Integer, String, Float, ForeignKey
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
from threading import Thread
from sqlparse import tokens as sql_tokens, parse as parse_sql

Base = declarative_base()

call_stack_metadata_association_table = Table('call_stack_metadata_association', Base.metadata,
    Column('call_stack_id', Integer, ForeignKey('call_stacks.id'), primary_key=True), 
    Column('metadata_id', Integer, ForeignKey('metadata_items.id'), primary_key=True)
)

sql_statement_metadata_association_table = Table('sql_statement_metadata_association', Base.metadata,
    Column('sql_statement_id', Integer, ForeignKey('sql_statements.id'), primary_key=True), 
    Column('metadata_id', Integer, ForeignKey('metadata_items.id'), primary_key=True)
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

    call_stack_items = relationship("CallStackItem", cascade="all", backref='call_stacks')
    metadata_items = relationship('MetaData', secondary=call_stack_metadata_association_table, cascade='all', backref='call_stacks')

    def __init__(self, profile_stats):
        self.datetime = profile_stats['datetime']
        self.duration = profile_stats['total_time']

    def _to_dict(self):
        response = {'id':self.id,
                    'datetime':self.datetime,
                    'duration':self.duration}
        return dict(response.items() + self._metadata().items())
                
    def _metadata(self):
        return dict([meta._to_tuple() for meta in self.metadata_items])
    
    def _stack(self):
        return [stack._to_dict() for stack in self.call_stack_items]


    def __repr__(self):
        return '<callstack %d>'%(self.id)


class SQLStatement(Base):
    __tablename__ = 'sql_statements'
    id = Column(Integer, primary_key=True)
    datetime = Column(Float)
    duration = Column(Float)

    sql_stack_items = relationship('SQLStackItem', cascade='all', backref='sql_statements')
    metadata_items = relationship('MetaData', secondary=sql_statement_metadata_association_table, cascade='all', backref='sql_statements')

    def __init__(self, profile_stats):
        self.datetime = profile_stats['datetime']
        self.duration = profile_stats['duration']

    def _to_dict(self):
        response = {'id':self.id,
                    'datetime':self.datetime,
                    'duration':self.duration}
        return dict(response.items() + self._metadata().items())

    def _metadata(self):
        return dict([meta._to_tuple() for meta in self.metadata_items])
    
    def _stack(self):
        return [stack._to_dict() for stack in self.sql_stack_items]

    def __repr__(self):
        return '<sql statement %d>'%(self.id)


class FileAccess(Base):
    __tablename__ = 'file_accesses'
    id = Column(Integer, primary_key=True)
    time_to_open = Column(Float)
    datetime = Column(Float)
    duration = Column(Float)
    data_written = Column(Integer)
    
    metadata_items = relationship('MetaData', secondary=file_access_metadata_association_table, cascade='all', backref='file_accesses')
  
    def __init__(self, profile_stats):
        self.datetime = profile_stats['datetime']
        self.time_to_open = profile_stats['time_to_open']
        self.duration = profile_stats['duration_open']
        self.data_written = profile_stats['data_written']
      
    def _to_dict(self):
        response = {'id':self.id,
                    'datetime':self.datetime,
                    'duration':self.duration,
                    'data_written':self.data_written}
        return dict(response.items() + self._metadata().items())
                
    def _metadata(self):
        return dict([meta._to_tuple() for meta in self.metadata_items])
    
    def __repr__(self):
        return '<file access %d>'%(self.id)

#========================================#

class CallStackItem(Base):
    __tablename__ = 'call_stack_items'
    id = Column(Integer, primary_key=True)
    call_stack_id = Column(Integer, ForeignKey('call_stacks.id'))
    function = Column(String)
    line = Column(Integer)
    module = Column(String)
    total_calls = Column(Integer)
    native_calls = Column(Integer)
    cumulative_time = Column(Float)
    total_time = Column(Float)

    def __init__(self, stats):
        self.function = stats['function']['name']
        self.line = stats['function']['line']
        self.module = stats['function']['module']
        self.total_calls = stats['total_calls']
        self.native_calls = stats['native_calls']
        self.cumulative_time = stats['cumulative']
        self.total_time = stats['time']
      
    def _to_dict(self):
        return {'total_calls': self.total_calls,
                'native_calls': self.native_calls,
                'cumulative': self.cumulative_time,
                'time': self.total_time,
                'module':self.module,
                'line':self.line,
                'function':self.function}

    def _metadata(self):
        return dict([meta._to_tuple() for meta in self.metadata_items])

    def __repr__(self):
        return '<callstack item %d>'%(self.id)


class SQLStackItem(Base):
    __tablename__ = 'sql_stack_items'
    id = Column(Integer, primary_key=True)
    sql_statement_id = Column(Integer, ForeignKey('sql_statements.id'))
    function = Column(String)
    module = Column(String)

    def __init__(self, stats):
        self.function = stats['function']
        self.module = stats['module']

    def _to_dict(self):
        return {'module':self.module,
                'function':self.function}

    def _metadata(self):
        return dict([meta._to_tuple() for meta in self.metadata_items])
    


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
    global Session
    global session
    Session = sessionmaker(bind=db)
    session = Session()

def get_metadata_list(metadata_dictionary, db_session):
    metadata_list = []
    for metadata_key in metadata_dictionary.keys():
        if not isinstance(metadata_dictionary[metadata_key], list):
            metadata_dictionary[metadata_key] = [metadata_dictionary[metadata_key]]
        for dict_value in metadata_dictionary[metadata_key]:
            metadata_query = db_session.query(MetaData).filter_by(key=metadata_key, value=dict_value)
            if metadata_query.count() == 0:
                # Add new metadata if does not exist
                metadata = MetaData(metadata_key, dict_value)
                metadata_list.append(metadata)
                db_session.add(metadata)
                db_session.commit()
            else:
                metadata_list.append(metadata_query.first())
    return list(set(metadata_list))

def push_fn_stats_new_thread(stats_packet):
    db_session = Session()
    
    # Get flush metadata
    flush_metadata_list = get_metadata_list(stats_packet['flush_metadata'], db_session)
    
    for profile_stats in stats_packet['profile']:
        # Get function metadata
        function_metadata_list = get_metadata_list(profile_stats['metadata_buffer'], db_session)
        metadata_list = flush_metadata_list + function_metadata_list
        
        # Create call stack items
        call_stack_item_list = []
        for stats in profile_stats['stats_buffer']['pstats']:
            call_stack_item = CallStackItem(stats)
            call_stack_item_list.append(call_stack_item)

        # Add call stack
        call_stack = CallStack(profile_stats['stats_buffer'])
        call_stack.call_stack_items = call_stack_item_list
        call_stack.metadata_items = metadata_list
        db_session.add(call_stack)
        db_session.commit()

def push_fn_stats(stats_packet):
    Thread(target=push_fn_stats_new_thread, args=(stats_packet,)).start()
   

def push_sql_stats_new_thread(stats_packet):
    db_session = Session()
                    
    # Get flush metadata
    flush_metadata_list = get_metadata_list(stats_packet['flush_metadata'], db_session)
    
    for profile_stats in stats_packet['profile']:
        # Parse SQL statement
        parsed_sql = parse_sql(profile_stats['metadata_buffer']['sql_string'])[0]
        sql_identifiers = []
        for token in parsed_sql.tokens:
            for item in token.flatten():
                if item.ttype == sql_tokens.Name:
                    sql_identifiers.append(item.value)
        profile_stats['metadata_buffer']['statement_identifiers'] = sql_identifiers
                    
        # Get SQL metadata
        sql_metadata_list = get_metadata_list(profile_stats['metadata_buffer'], db_session)
        metadata_list = flush_metadata_list + sql_metadata_list
        
        # Create sql stack items
        sql_stack_item_list = []
        for sql_stack_stat in profile_stats['stats_buffer']['stack']:
            sql_stack_item = SQLStackItem(sql_stack_stat)
            sql_stack_item_list.append(sql_stack_item)

        # Add sql statement
        sql_statement = SQLStatement(profile_stats['stats_buffer'])
        sql_statement.metadata_items = metadata_list
        sql_statement.sql_stack_items = sql_stack_item_list
        db_session.add(sql_statement)
        db_session.commit()

def push_sql_stats(stats_packet):
    Thread(target=push_sql_stats_new_thread, args=(stats_packet,)).start()


def push_file_stats_new_thread(stats_packet):
    db_session = Session()
                    
    # Get flush metadata
    flush_metadata_list = get_metadata_list(stats_packet['flush_metadata'], db_session)
    
    for profile_stats in stats_packet['profile']:
        # Get file metadata
        file_metadata_list = get_metadata_list(profile_stats['metadata_buffer'], db_session)
        metadata_list = flush_metadata_list + file_metadata_list

        # Add file access row
        file_access = FileAccess(profile_stats['stats_buffer'])
        file_access.metadata_items = metadata_list
        db_session.add(file_access)
        db_session.commit()
    
def push_file_stats(stats_packet):
    Thread(target=push_file_stats_new_thread, args=(stats_packet,)).start()
