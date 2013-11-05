import sqlalchemy
    
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

from sqlalchemy import Column, Integer, String, Float, ForeignKey
from sqlalchemy.orm import sessionmaker, relationship

class Profile(Base):
    __tablename__ = 'profiles'
    id = Column(Integer, primary_key=True)
    module = Column(String)
    request_method = Column(String)
    time = Column(Float)
    path = Column(String)
    profile_id = Column(Integer, unique=True)
    
    call_stack = relationship("CallStackItem", cascade="all", backref='profiles')
    profile_metadata = relationship("ProfileMetadata", cascade="all", backref='profiles')
  
    def __init__(self, profile_stats):
        self.module = profile_stats['module']
        self.request_method = profile_stats['request_method']
        self.time = profile_stats['time']
        self.path = profile_stats['path']
      
    def __repr__(self):
        return ""

class CallStackItem(Base):
    __tablename__ = 'call_stack_items'
    id = Column(Integer, primary_key=True)
    profile_id = Column(None, ForeignKey('profiles.id'))
    function_name = Column(String)
    line_number = Column(Integer)
    module = Column(String)
    total_calls = Column(Integer)
    native_calls = Column(Integer)
    cumulative_time = Column(Float)
    total_time = Column(Float)
  
    def __init__(self, profile_id, stats):
        self.profile_id = profile_id
        self.function_name = stats['function']['name']
        self.line_number = stats['function']['line']
        self.module = stats['function']['module']
        self.total_calls = stats['total_calls']
        self.native_calls = stats['native_calls']
        self.cumulative_time = stats['cumulative']
        self.total_time = stats['time']
      
    def __repr__(self):
        return ""

class ProfileMetadata(Base):
    __tablename__ = 'profile_metadata'
    id = Column(Integer, primary_key=True)
    profile_id = Column(None, ForeignKey('profiles.id'))
  
    def __init__(self, profile_id):
        self.profile_id = profile_id
      
    def __repr__(self):
        return ""

def create_db_and_connect():
    database = sqlalchemy.create_engine('postgresql://postgres:my_password@localhost/profile_stats', echo=True)
    database.connect()
    return database

db = None
session = None

def setup_profile_database():
    global db, session
    try:
        db = create_db_and_connect()
    except:
        postgres = sqlalchemy.create_engine("postgresql://postgres:my_password@localhost/postgres")
        conn = postgres.connect()
        conn.execute("commit")
        conn.execute("create database profile_stats")
        conn.close()
        db = create_db_and_connect()
    Base.metadata.create_all(db)
    Session = sessionmaker(bind=db)
    session = Session()

def push_stats_buffer(stats_buffer):
    for profile_stats in stats_buffer:
        profile = Profile(profile_stats)
        session.add(profile)
        session.commit()
        session.add(ProfileMetadata(profile.id))
        pstats = profile_stats['pstats']
        for stats in pstats:
            session.add(CallStackItem(profile.id, stats))
    session.commit()
        