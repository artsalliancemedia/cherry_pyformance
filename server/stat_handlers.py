import cherrypy
from cherrypy._cpcompat import ntou, json_decode
import zlib
import database as db
import os
import cPickle
import pstats
import uuid
from threading import Thread
from Queue import Queue
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

stat_handler_queue = Queue()
        
def worker():
    while True:
        item = stat_handler_queue.get()
        fn = item[0]
        fn(item[1])
        stat_handler_queue.task_done()
        
worker_thread = Thread(target=worker)
worker_thread.daemon = True
worker_thread.start()

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
        # Add sender's details to the metadata
        cherrypy.serving.request.json['metadata']['ip_address'] = cherrypy.request.remote.ip
        if cherrypy.request.remote.name:
            cherrypy.serving.request.json['metadata']['hostname'] = cherrypy.request.remote.name

        stat_handler_queue.put([self.parse_fn, cherrypy.serving.request.json])

        cherrypy.response.status = 202 # Send back Accepted so they know it's successfully into the processing queue.
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
    db_session = db.session
    
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

        # callstack names
        call_stack_name = get_or_create(db_session,
                                       db.CallStackName,
                                       module_name = profile['module'],
                                       class_name = profile['class'],
                                       fn_name = profile['function'])
        
        # Add call stack
        call_stack = db.CallStack(profile)
        call_stack.name = call_stack_name
        call_stack.metadata_items = metadata_list
        # add to session
        db_session.add(call_stack)

    db_session.commit()
 

def parse_sql_packet(packet):
    db_session = db.session
                    
    # Get flush metadata
    global_metadata_list = get_metadata_list(packet['metadata'], db_session)
    
    for profile in packet['stats']:
        
        # get-or-set all arguments (do not map relationship yet)
        sql_arg_list = get_arg_list(db_session, profile['args'])

        # get-or-set all stack items (do not map relationship yet)
        sql_stack_item_list = get_stack_list(db_session, profile['stack'])

        # Parse SQL string
        parsed_sql = parse_sql(profile['sql_string'])[0]
        sql_identifiers = []
        for token in parsed_sql.tokens:
            for item in token.flatten():
                if item.ttype == sql_tokens.Name:
                    sql_identifiers.append(item.value)
        statement_type = profile['sql_string'].split()[0]

        # get-or-set the metadata
        sql_identifiers = get_metadata_list({'statement_identifiers':sql_identifiers,
                                             'statement_type':statement_type},
                                            db_session)
        metadata_list = global_metadata_list + sql_identifiers

        # get-or-set the sql string
        sql_string = get_or_create(db_session,
                                   db.SQLString,
                                   sql=profile['sql_string'])
        
        # create the statement object
        sql_statement = db.SQLStatement(profile)

        # add the arg asssociatons
        for i, arg in enumerate(sql_arg_list):
            sql_arg_assoc = db.SQLArgAssociation(index=i)
            sql_arg_assoc.arg = arg
            sql_statement.arguments.append(sql_arg_assoc)

        # add the stack asssociatons
        for i, stack_item in enumerate(sql_stack_item_list):
            sql_stack_item_assoc = db.SQLStackAssociation(index=i)
            sql_stack_item_assoc.stack_item = stack_item
            sql_statement.sql_stack_items.append(sql_stack_item_assoc)

        # add the metadata
        sql_statement.metadata_items = metadata_list

        # add the sql string
        sql_statement.sql_string = sql_string

        # Add sql statement to session
        db_session.add(sql_statement)
    
    db_session.commit()


def parse_file_packet(packet):
    db_session = db.session
                    
    # Get flush metadata
    metadata_list = get_metadata_list(packet['metadata'], db_session)
    
    for profile in packet['stats']:
        # Add filename
        filename = get_or_create(db_session,
                                 db.FileName,
                                 filename=profile['filename'])
        # Add file access row
        file_access = db.FileAccess(profile)
        file_access.filename = filename
        file_access.metadata_items = metadata_list
        # add to session
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


def get_arg_list(db_session, args):
    arg_list = []
    for arg in args:
        sql_arg = get_or_create(db_session,
                                db.SQLArg,
                                value=arg)
        arg_list.append(sql_arg)
    return arg_list

def get_stack_list(db_session, stack):
    sql_stack_item_list = []
    for stack_item in stack:
        sql_stack_item = get_or_create(db_session,
                                       db.SQLStackItem,
                                       function=stack_item['function'],
                                       module=stack_item['module'])
        sql_stack_item_list.append(sql_stack_item)
    return sql_stack_item_list

def get_or_create(session, model, **kwargs):
    instance = session.query(model).filter_by(**kwargs).first()
    if not instance:
        instance = model(kwargs)
        session.add(instance)
    return instance


function_stat_handler = StatHandler(parse_fn_packet)
handler_stat_handler = StatHandler(parse_fn_packet)
sql_stat_handler = StatHandler(parse_sql_packet)
file_stat_handler = StatHandler(parse_file_packet)
