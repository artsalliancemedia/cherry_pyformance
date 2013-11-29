import cherrypy
from cherrypy._cpcompat import ntou, json_decode
import zlib
import database as db
import os
import cPickle
import pstats
import uuid
from threading import Thread
from sqlparse import tokens as sql_tokens, parse as parse_sql
from sqlalchemy import and_


allowed_content_types = [ntou('application/json'),
                         ntou('text/javascript'),
                         ntou('application/gzip')]

def decompress_json(entity):
    """Try decompressing json before parsing, incase compressed
    content was sent to the server"""

    if not entity.headers.get(ntou("Content-Length"), ntou("")):
        raise cherrypy.HTTPError(411)
    
    body = entity.fp.read()
    # decompress if gzip content type
    if entity.headers.get(ntou("Content-Type")) == ntou("application/gzip"):
        try:
            body = zlib.decompress(body)
        except:
            raise cherrypy.HTTPError(500, 'Invalid gzip data')
    try:
        cherrypy.serving.request.json = json_decode(body.decode('utf-8'))
    except ValueError:
        raise cherrypy.HTTPError(400, 'Invalid JSON document')


class StatHandler(object):
    '''
    A base stat handler for incoming stats. By initialising with a given push function
    the various handlers can be created for different stat types.
    '''
    exposed = True

    def __init__(self, push_fn):
        self.push_fn = push_fn

    @cherrypy.tools.json_in(content_type=allowed_content_types, processor=decompress_json)
    def POST(self):
        # Add sender's ip to flush metadata
        cherrypy.serving.request.json['metadata']['ip_address'] = cherrypy.request.remote.ip
        self.push_fn(cherrypy.serving.request.json)
        return 'Hello, World.'


class BogusStats(object):
    '''
    A bogus class to put the stats into, this object can be used
    to initilise a pstats object
    '''
    def __init__(self, stats):
        self.stats = stats

    def create_stats(self):
       '''A phantom method to trick pstats into accepting its stats''' 
       pass


def parse_fn_packet(packet):
    db_session = db.Session()
    
    # Get global metadata
    global_metadata_list = get_metadata_list(packet['metadata'], db_session)
    
    for profile in packet['stats']:
        # pull and unpickle pstats
        stats = cPickle.loads(str(profile['profile']))
        # need to make it a bogus stats object for it to initialise
        # (needs a create_stats method and stats attr)
        stats = BogusStats(stats)
        stats = pstats.Stats(stats)
        profile['duration'] = stats.total_tt
        _id = str(uuid.uuid4())
        while os.path.isfile(os.path.join(os.getcwd(),'pstats',_id)):
            _id = str(uuid.uuid4())
        profile['pstat_uuid'] = _id
        stats.dump_stats('pstats\\'+_id)

        # Get function metadata
        function_metadata_list = get_metadata_list(profile['metadata'], db_session)
        metadata_list = global_metadata_list + function_metadata_list

        # Add call stack
        call_stack = db.CallStack(profile)
        call_stack.metadata_items = metadata_list
        db_session.add(call_stack)
    db_session.commit()

def push_fn_stats(packet):
    Thread(target=parse_fn_packet, args=(packet,)).start()
   

def parse_sql_packet(packet):
    db_session = db.Session()
                    
    # Get flush metadata
    global_metadata_list = get_metadata_list(packet['metadata'], db_session)
    
    for profile in packet['stats']:
        # Parse SQL statement
        parsed_sql = parse_sql(profile['metadata']['sql_string'])[0]
        sql_identifiers = []
        for token in parsed_sql.tokens:
            for item in token.flatten():
                if item.ttype == sql_tokens.Name:
                    sql_identifiers.append(item.value)
        profile['metadata']['statement_identifiers'] = sql_identifiers
                    
        # Get SQL metadata
        sql_metadata_list = get_metadata_list(profile['metadata'], db_session)
        metadata_list = global_metadata_list + sql_metadata_list
        
        # Create sql stack items
        sql_stack_item_list = get_stack_list(profile['stack'], db_session)

        # Create arguments list
        sql_arguments_list = get_arg_list(profile['args'], db_session)

        # Add sql statement
        sql_statement = db.SQLStatement(profile)
        sql_statement.metadata_items = metadata_list
        sql_statement.sql_stack_items = sql_stack_item_list
        sql_statement.arguments = sql_arguments_list

        db_session.add(sql_statement)
    db_session.commit()

def push_sql_stats(packet):
    Thread(target=parse_sql_packet, args=(packet,)).start()


def parse_file_packet(packet):
    db_session = db.Session()
                    
    # Get flush metadata
    global_metadata_list = get_metadata_list(packet['metadata'], db_session)
    
    for profile in packet['stats']:
        # Get file metadata
        file_metadata_list = get_metadata_list(profile['metadata'], db_session)
        metadata_list = global_metadata_list + file_metadata_list

        # Add file access row
        file_access = db.FileAccess(profile)
        file_access.metadata_items = metadata_list
        db_session.add(file_access)
    db_session.commit()
    
def push_file_stats(packet):
    Thread(target=parse_file_packet, args=(packet,)).start()



def get_metadata_list(metadata_dictionary, db_session):
    metadata_list = []
    for metadata_key in metadata_dictionary.keys():
        if not isinstance(metadata_dictionary[metadata_key], list):
            metadata_dictionary[metadata_key] = [metadata_dictionary[metadata_key]]
        for dict_value in metadata_dictionary[metadata_key]:
            metadata_query = db_session.query(db.MetaData).filter_by(key=metadata_key, value=dict_value)
            if metadata_query.count() == 0:
                # Add new metadata if does not exist
                metadata = db.MetaData(metadata_key, dict_value)
                metadata_list.append(metadata)
                db_session.add(metadata)
                db_session.commit()
            else:
                metadata_list.append(metadata_query.first())
    return list(set(metadata_list))


def get_arg_list(args, db_session):
    arg_list = []
    for arg in args:
        arg_query = db_session.query(db.SQLArg).filter(db.SQLArg.value==arg)
        if arg_query.count()==0:
            arg_obj = db.SQLArg(arg)
            arg_list.append(arg_obj)
            db_session.add(arg_obj)
            db_session.commit()
        else:
            arg_list.append(arg_query.first())
    return single_instance_list(arg_list)

def get_stack_list(stack, db_session):
    stack_list = []
    for stack_item in stack:
        query = db_session.query(db.SQLStackItem).filter(and_(db.SQLStackItem.function==stack_item['function'],
                                                              db.SQLStackItem.module==stack_item['module']))
        if query.count()==0:
            stack_item_obj = db.SQLStackItem(stack_item)
            stack_list.append(stack_item_obj)
            db_session.add(stack_item_obj)
            db_session.commit()
        else:
            stack_list.append(query.first())
    return single_instance_list(stack_list)


def single_instance_list(in_list):
    out_list = []
    for item in in_list:
        if not item in out_list:
            out_list.append(item)
    return out_list


function_stat_handler = StatHandler(push_fn_stats)
handler_stat_handler = StatHandler(push_fn_stats)
sql_stat_handler = StatHandler(push_sql_stats)
file_stat_handler = StatHandler(push_file_stats)
