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
from operator import attrgetter


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

    def __init__(self, parse_fn):
        self.parse_fn = parse_fn

    @cherrypy.tools.json_in(content_type=allowed_content_types, processor=decompress_json)
    def POST(self):
        # Add sender's ip to flush metadata
        cherrypy.serving.request.json['metadata']['ip_address'] = cherrypy.request.remote.ip
        Thread(target=self.parse_fn, args=(cherrypy.serving.request.json,)).start()
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
    metadata_list = get_metadata_list(packet['metadata'], db_session)
    
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

        # callstack metadata
        call_stack_name = get_or_create(db_session,
                                       db.CallStackMetadata,
                                       module_name = profile['module'],
                                       class_name = profile['class'],
                                       fn_name = profile['function']))

        # Add call stack
        call_stack = db.CallStack(profile)
        call_stack.name = call_stack_name
        call_stack.metadata_items = metadata_list
        db_session.add(call_stack)
    db_session.commit()
 

def parse_sql_packet(packet):
    db_session = db.Session()
                    
    # Get flush metadata
    global_metadata_list = get_metadata_list(packet['metadata'], db_session)
    
    for profile in packet['stats']:
        # Parse SQL statement
        parsed_sql = parse_sql(profile['sql_string'])[0]
        sql_identifiers = []
        for token in parsed_sql.tokens:
            for item in token.flatten():
                if item.ttype == sql_tokens.Name:
                    sql_identifiers.append(item.value)
        statement_type = profile['sql_string'].split()[0]
                            
        # Get SQL metadata
        sql_identifiers = get_metadata_list({'statement_identifiers':sql_identifiers
                                             'statement_type':statement_type},
                                            db_session)
        metadata_list = global_metadata_list + sql_identifiers
        
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


def parse_file_packet(packet):
    db_session = db.Session()
                    
    # Get flush metadata
    metadata_list = get_metadata_list(packet['metadata'], db_session)
    
    for profile in packet['stats']:
        # Add file access row
        file_access = db.FileAccess(profile)
        file_access.metadata_items = metadata_list
        db_session.add(file_access)
    db_session.commit()
    

def get_metadata_list(metadata_dictionary, db_session):
    metadata_list = []
    for metadata_key in metadata_dictionary.keys():
        # make each value in the dictionary a list, even if only one value
        if not isinstance(metadata_dictionary[metadata_key], list):
            metadata_dictionary[metadata_key] = [metadata_dictionary[metadata_key]]
        for dict_value in metadata_dictionary[metadata_key]:
            metadata_list.append(get_or_create(db_session,
                                                db.MetaData,
                                                key=metadata_key,
                                                value=dict_value))
    return list(set(metadata_list))


def get_arg_list(args, db_session):
    arg_list = []
    for i in range(len(args)):
        arg = args[i]
        arg_list.append(get_or_create(db_session,
                                      db.SQLArg,
                                      value=arg,
                                      index=i))
    arg_list.sort(key=attrgetter(index))
    return arg_list

def get_stack_list(stack, db_session):
    stack_list = []
    for stack_item in stack:
        stack_list.append(get_or_create(db_session,
                                        db.SQLStackItem,
                                        function=stack_item['function'],
                                        module=stack_item['module']))
    return stack_list



def get_or_create(session, model, **kwargs):
    instance = session.query(model).filter_by(**kwargs).first()
    if not instance:
        instance = model(**kwargs)
        session.add(instance)
    return instance


def single_instance_list(in_list):
    out_list = []
    for item in in_list:
        if not item in out_list:
            out_list.append(item)
    return out_list


function_stat_handler = StatHandler(parse_fn_packet)
handler_stat_handler = StatHandler(parse_fn_packet)
sql_stat_handler = StatHandler(parse_sql_packet)
file_stat_handler = StatHandler(parse_file_packet)
