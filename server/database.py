import sqlalchemy
from sqlalchemy import Column, Integer, String, Float, ForeignKey
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
from threading import Thread
from sqlparse import tokens as sql_tokens, parse as parse_sql

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
        return '<callstack %d>'%(self.id)


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
        return '<sql statement %d>'%(self.id)


class FileAccess(Base):
    __tablename__ = 'file_accesses'
    id = Column(Integer, primary_key=True)
    time_to_open = Column(Float)
    datetime = Column(Float)
    duration_open = Column(Float)
    data_written = Column(Integer)
  
    def __init__(self, profile_stats):
        self.datetime = profile_stats['datetime']
        self.time_to_open = profile_stats['time_to_open']
        self.duration_open = profile_stats['duration_open']
        self.data_written = profile_stats['data_written']
      
    def __repr__(self):
        return '<file access %d>'%(self.id)

#========================================#

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
        return '<callstack item %d>'%(self.id)


class SQLStackItem(Base):
    __tablename__ = 'sql_stack_items'
    id = Column(Integer, primary_key=True)
    sql_statement_id = Column(None, ForeignKey('sql_statements.id'))
    module = Column(String)
    function = Column(String)

    def __init__(self, sql_statement_id, stack_item):
        self.sql_statement_id = sql_statement_id
        self.module = stack_item['module']
        self.function = stack_item['function']

#========================================#

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


class FileAccessMetadata(Base):
    __tablename__ = 'file_access_metadata'
    id = Column(Integer, primary_key=True)
    file_access_id = Column(None, ForeignKey('file_accesses.id'))
    metadata_id = Column(None, ForeignKey('metadata.id'))
  
    def __init__(self, file_access_id, metadata_id):
        self.file_access_id = file_access_id
        self.metadata_id = metadata_id


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
        for flush_metadata in set(flush_metadata_list):
            db_session.add(CallStackMetadata(call_stack.id, flush_metadata.id))
        for function_metadata in set(function_metadata_list):
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
        # Parse SQL statement
        parsed_sql = parse_sql(profile_stats['stats_buffer']['sql'])[0]
        sql_keywords = []
        sql_identifiers = []
        for token in parsed_sql.tokens:
            for item in token.flatten():
                if item.ttype == sql_tokens.Keyword:
                    sql_keywords.append(item.value)
                elif item.ttype == sql_tokens.Name:
                    sql_identifiers.append(item.value)
                    
        profile_stats['metadata_buffer']['statement_keywords'] = sql_keywords
        profile_stats['metadata_buffer']['statement_identifiers'] = sql_identifiers
                    
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
        for flush_metadata in set(flush_metadata_list):
            db_session.add(SQLStatementMetadata(sql_statement.id, flush_metadata.id))
        for sql_metadata in set(sql_metadata_list):
            db_session.add(SQLStatementMetadata(sql_statement.id, sql_metadata.id))
        
        # Add sql stack items
        for sql_stack_item in sql_stack_list:
            db_session.add(SQLStackItem(sql_statement.id, sql_stack_item))
    db_session.commit()

def push_sql_stats(stats_packet):
    Thread(target=push_sql_stats_new_thread, args=(stats_packet,)).start()


def push_file_stats_new_thread(stats_packet):
    db_session = Session()
                    
    # Add flush metadata
    flush_metadata_list = get_metadata_list(stats_packet['flush_metadata'], db_session)
    for flush_metadata in flush_metadata_list:
        db_session.add(flush_metadata)
    
    for profile_stats in stats_packet['profile']:
        # Add file metadata
        file_metadata_list = get_metadata_list(profile_stats['metadata_buffer'], db_session)
        for file_access_metadata in file_metadata_list:
            db_session.add(file_access_metadata)

        # Add file access obj
        file_access = FileAccess(profile_stats['stats_buffer'])
        db_session.add(file_access)
        db_session.commit()
        
        # Add file access/metadata relationships
        for flush_metadata in set(flush_metadata_list):
            db_session.add(FileAccessMetadata(file_access.id, flush_metadata.id))
        for file_access_metadata in set(file_metadata_list):
            db_session.add(FileAccessMetadata(file_access.id, file_access_metadata.id))

    db_session.commit()

def push_file_stats(stats_packet):
    Thread(target=push_file_stats_new_thread, args=(stats_packet,)).start()
