import cherrypy
from cherrypy._cpcompat import ntou, json_decode
import zlib
import database as db
import cPickle
import uuid
from threading import Thread
from sqlparse import tokens as sql_tokens, parse as parse_sql


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
        stats = cPickle.loads(profile['profile'])
        # need to make it a bogus stats object for it to initialise
        # (needs a create_stats method and stats attr)
        stats = BogusStats(stats)
        stats = pstats.Stats(stats)
        packet['duration'] = stats.total_tt
        _id = str(uuid.uuid4())
        while not os.path.isfile(os.path.join(os.getcwd(),'pstats',_id)):
            packet['uuid'] = _id
            stats.dump_stats('pstats\\'+_id)
            break

        # Get function metadata
        function_metadata_list = get_metadata_list(profile['metadata'], db_session)
        metadata_list = flush_metadata_list + function_metadata_list

        # Add call stack
        call_stack = CallStack(profile)
        call_stack.metadata_items = metadata_list
        db_session.add(call_stack)
    db_session.commit()

def push_fn_stats(packet):
    Thread(target=parse_fn_packet, args=(packet,)).start()
   

def parse_sql_packet(packet):
    db_session = db.Session()
                    
    # Get flush metadata
    flush_metadata_list = get_metadata_list(packet['metadata'], db_session)
    
    for profile in packet['profile']:
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
        metadata_list = flush_metadata_list + sql_metadata_list
        
        # Create sql stack items
        sql_stack_item_list = []
        for sql_stack_stat in profile['stack']:
            sql_stack_item = SQLStackItem(sql_stack_stat)
            sql_stack_item_list.append(sql_stack_item)

        # Add sql statement
        sql_statement = SQLStatement(profile)
        sql_statement.metadata_items = metadata_list
        sql_statement.sql_stack_items = sql_stack_item_list
        db_session.add(sql_statement)
    db_session.commit()

def push_sql_stats(packet):
    Thread(target=parse_sql_packet, args=(packet,)).start()


def parse_file_stats(packet):
    db_session = db.Session()
                    
    # Get flush metadata
    flush_metadata_list = get_metadata_list(packet['metadata'], db_session)
    
    for profile in packet['profile']:
        # Get file metadata
        file_metadata_list = get_metadata_list(profile['metadata'], db_session)
        metadata_list = flush_metadata_list + file_metadata_list

        # Add file access row
        file_access = FileAccess(profile)
        file_access.metadata_items = metadata_list
        db_session.add(file_access)
    db_session.commit()
    
def push_file_stats(packet):
    Thread(target=parse_file_stats, args=(packet,)).start()



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



function_stat_handler = StatHandler(push_fn_stats)
handler_stat_handler = StatHandler(push_fn_stats)
sql_stat_handler = StatHandler(push_sql_stats)
file_stat_handler = StatHandler(push_file_stats)
