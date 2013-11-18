import sqlalchemy
from sqlalchemy import Column, Integer, String, Float, ForeignKey
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
from threading import Thread

Base = declarative_base()

class CallStack(Base):
    __tablename__ = 'call_stacks'
    id = Column(Integer, primary_key=True)
    datetime = Column(Float)
    total_time = Column(Float)
    
    call_stack_items = relationship("CallStackItem", cascade="all", backref='call_stacks')
  
    def __init__(self, profile_stats):
        self.datetime = profile_stats['datetime']
        self.total_time = profile_stats['total_time']
      
    def __repr__(self):
        return str(self.id)

class CallStackItem(Base):
    __tablename__ = 'call_stack_items'
    id = Column(Integer, primary_key=True)
    call_stack_id = Column(None, ForeignKey('call_stacks.id'))
    function_name = Column(String)
    line_number = Column(Integer)
    module = Column(String)
    total_calls = Column(Integer)
    native_calls = Column(Integer)
    cumulative_time = Column(Float)
    total_time = Column(Float)
  
    def __init__(self, call_stack_id, stats):
        self.call_stack_id = call_stack_id
        self.function_name = stats['function']['name']
        self.line_number = stats['function']['line']
        self.module = stats['function']['module']
        self.total_calls = stats['total_calls']
        self.native_calls = stats['native_calls']
        self.cumulative_time = stats['cumulative']
        self.total_time = stats['time']
      
    def __repr__(self):
        return ""

class SQLStatement(Base):
    __tablename__ = 'sql_statements'
    id = Column(Integer, primary_key=True)
    sql_string = Column(String)
    datetime = Column(Float)
    duration = Column(Float)
  
    def __init__(self, profile_stats):
        self.sql_string = profile_stats['sql']
        self.datetime = profile_stats['datetime']
        self.duration = profile_stats['duration']
      
    def __repr__(self):
        return self.sql_string

class MetaData(Base):
    __tablename__ = 'metadata'
    id = Column(Integer, primary_key=True)
    key = Column(String)
    value = Column(String)
  
    def __init__(self, key, value):
        self.key = key
        self.value = value
        
class CallStackMetadata(Base):
    __tablename__ = 'call_stack_metadata'
    id = Column(Integer, primary_key=True)
    call_stack_id = Column(None, ForeignKey('call_stacks.id'))
    metadata_id = Column(None, ForeignKey('metadata.id'))
  
    def __init__(self, call_stack_id, metadata_id):
        self.call_stack_id = call_stack_id
        self.metadata_id = metadata_id
        
class SQLStatementMetadata(Base):
    __tablename__ = 'sql_statement_metadata'
    id = Column(Integer, primary_key=True)
    sql_statement_id = Column(None, ForeignKey('sql_statements.id'))
    metadata_id = Column(None, ForeignKey('metadata.id'))
  
    def __init__(self, sql_statement_id, metadata_id):
        self.sql_statement_id = sql_statement_id
        self.metadata_id = metadata_id

class SQLStack(Base):
    __tablename__ = 'sql_stack_items'
    id = Column(Integer, primary_key=True)
    sql_statement_id = Column(None, ForeignKey('sql_statements.id'))
    module = Column(String)
    function = Column(String)

    def __init__(self, sql_statement_id, stack_item):
        self.sql_statement_id = sql_statement_id
        self.module = stack_item['module']
        self.function = stack_item['function']


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
    return metadata_list

def push_fn_stats_new_thread(stats_packet):
    db_session = Session()
    # Add flush metadata
    flush_metadata_list = get_metadata_list(stats_packet['flush_metadata'], db_session)
    for flush_metadata in flush_metadata_list:
        db_session.add(flush_metadata)
    
    for profile_stats in stats_packet['profile']:
        # Add function metadata
        function_metadata_list = get_metadata_list(profile_stats['metadata_buffer'], db_session)
        for function_metadata in function_metadata_list:
            db_session.add(function_metadata)
            
        # Add call stack
        call_stack = CallStack(profile_stats['stats_buffer'])
        db_session.add(call_stack)
        db_session.commit()
        
        # Add call stack/metadata relationships
        for flush_metadata in flush_metadata_list:
            db_session.add(CallStackMetadata(call_stack.id, flush_metadata.id))
        for function_metadata in function_metadata_list:
            db_session.add(CallStackMetadata(call_stack.id, function_metadata.id))
        
        # Add call stack items
        pstats = profile_stats['stats_buffer']['pstats']
        for stats in pstats:
            db_session.add(CallStackItem(call_stack.id, stats))
    db_session.commit()

def push_fn_stats(stats_packet):
    Thread(target=push_fn_stats_new_thread, args=(stats_packet,)).start()
    
def push_sql_stats_new_thread(stats_packet):
    db_session = Session()
    # Add flush metadata
    flush_metadata_list = get_metadata_list(stats_packet['flush_metadata'], db_session)
    for flush_metadata in flush_metadata_list:
        db_session.add(flush_metadata)
    
    for profile_stats in stats_packet['profile']:
        # Add SQL metadata
        sql_metadata_list = get_metadata_list(profile_stats['metadata_buffer'], db_session)
        for sql_metadata in sql_metadata_list:
            db_session.add(sql_metadata)

        # Add sql statement
        sql_statement = SQLStatement(profile_stats['stats_buffer'])
        db_session.add(sql_statement)
        db_session.commit()
        
        sql_stack_list = profile_stats['stats_buffer']['stack']
        
        # Add sql statement/metadata relationships
        for flush_metadata in flush_metadata_list:
            db_session.add(SQLStatementMetadata(sql_statement.id, flush_metadata.id))
        for sql_metadata in sql_metadata_list:
            db_session.add(SQLStatementMetadata(sql_statement.id, sql_metadata.id))
        for sql_stack_item in sql_stack_list:
            db_session.add(SQLStack(sql_statement.id, sql_stack_item))
    db_session.commit()


def push_sql_stats(stats_packet):
    Thread(target=push_sql_stats_new_thread, args=(stats_packet,)).start()
