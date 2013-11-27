import json
import cherrypy
from cherrypy._cpcompat import ntou, json_decode
import sys
import database as db
import zlib
import os
import mako.template

from json_ui import JSONAPI
from table_ui import Tables
from aggregate_json_ui import AggregateAPI
from aggregate_table_ui import AggregatePages

# add gzip to allowed content types for decompressing JSON if compressed.
allowed_content_types = [ntou('application/json'),
                         ntou('text/javascript'),
                         ntou('application/gzip')]

def handle_error():
    cherrypy.response.status = 500
    cherrypy.response.body = mako.template.Template(filename=os.path.join(os.getcwd(),'static','templates','500.html'))\
                                          .render(error_str=cherrypy._cperror.format_exc())


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
        cherrypy.serving.request.json['flush_metadata']['ip_address'] = cherrypy.request.remote.ip
        self.push_fn(cherrypy.serving.request.json)
        return 'Hello, World.'

def start_cherrypy(host, port):
    cherrypy.server.socket_host = host
    cherrypy.server.socket_port = int(port)
    cherrypy.log('Mounting the handlers')
    method_dispatch_cfg = {'/': {'request.dispatch': cherrypy.dispatch.MethodDispatcher()} }
    front_end_config = {'/': {'tools.staticdir.on': True,
                              'tools.staticdir.dir': os.path.join(os.getcwd(), 'static'),
                              'tools.staticdir.content_types': {'js': 'application/javascript',
                                                                'css': 'text/css',
                                                                'images': 'image/png'}
                              }
                        }

    cherrypy.config.update({'request.error_response': handle_error})
    cherrypy.config.update({'error_page.404': os.path.join(os.getcwd(),'static','templates','404.html')})


    function_stat_handler = StatHandler(db.push_fn_stats)
    handler_stat_handler = StatHandler(db.push_fn_stats)
    sql_stat_handler = StatHandler(db.push_sql_stats)
    file_stat_handler = StatHandler(db.push_file_stats)

    cherrypy.tree.mount( function_stat_handler, '/function',   method_dispatch_cfg )
    cherrypy.tree.mount( handler_stat_handler,  '/handler',    method_dispatch_cfg )
    cherrypy.tree.mount( sql_stat_handler,      '/database',   method_dispatch_cfg )
    cherrypy.tree.mount( file_stat_handler,     '/file',       method_dispatch_cfg )

    cherrypy.tree.mount( Tables(),              '/tables',     front_end_config )
    cherrypy.tree.mount( JSONAPI(),             '/tables/api', {'/':{}} )
    cherrypy.tree.mount( AggregatePages(),      '/',           front_end_config )
    cherrypy.tree.mount( AggregateAPI(),        '/api',        {'/':{}} )

    # Attach the signal handlers to detect keyboard interrupt
    if hasattr(cherrypy.engine, 'signal_handler'):
        cherrypy.engine.signal_handler.subscribe()
    if hasattr(cherrypy.engine, 'console_control_handler'):
        cherrypy.engine.console_control_handler.subscribe()
    
    cherrypy.log('Starting CherryPy')
    try:
        cherrypy.engine.start()
    except IOError:
        logging.error('Unable to bind to address (%s, %d)' % (cfg.cherry_host(), cfg.cherry_port()))
        sys.exit(1)

    cherrypy.engine.wait(cherrypy.process.wspbus.states.STARTED)
    cherrypy.log('CherryPy started')
    cherrypy.engine.block()

def load_config():
    try:
        with open('server_config.json') as cfg_file:
            cfg = json.load(cfg_file)
            return cfg
    except:
        print 'Failed to load config file (server_config.json)'
        sys.exit(1)

if __name__ == '__main__':
    cfg = load_config()
        
    username = cfg['database_username']
    password = cfg['database_password']
    host = cfg['server_host']
    port = cfg['server_port']
    
    try:
        db.setup_profile_database(username, password)
        start_cherrypy(host, port)
    except Exception, ex:
        print str(ex)
