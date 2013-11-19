import json
import cherrypy
from cherrypy._cpcompat import ntou, json_decode
import sys
import database as db
import zlib
import os

# add gzip to allowed content types for decompressing JSON if compressed.
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
        cherrypy.serving.request.json['flush_metadata']['ip_address'] = cherrypy.request.remote.ip
        self.push_fn(cherrypy.serving.request.json)
        return 'Hello, World.'

def start_cherrypy(host, port):
    cherrypy.server.socket_host = host
    cherrypy.server.socket_port = int(port)
    cherrypy.log('Mounting the handlers')
    method_dispatch_cfg = {'/': {'request.dispatch': cherrypy.dispatch.MethodDispatcher()} }
    front_end_config = {'/': {'request.dispatch': cherrypy.dispatch.MethodDispatcher(),
                              'tools.staticdir.on': True,
                              'tools.staticdir.dir': os.path.join(os.getcwd(), 'static'),
                              'tools.staticdir.content_types': {'js': 'application/javascript',
                                                                'css': 'text/css',
                                                                'images': 'image/png'}
                              }
                        }


    function_stat_handler = StatHandler(db.push_fn_stats)
    handler_stat_handler = StatHandler(db.push_fn_stats)
    sql_stat_handler = StatHandler(db.push_sql_stats)

    from rest_ui import Root

    cherrypy.tree.mount( function_stat_handler, '/function', method_dispatch_cfg )
    cherrypy.tree.mount( handler_stat_handler,  '/handler',  method_dispatch_cfg )
    cherrypy.tree.mount( sql_stat_handler,      '/database', method_dispatch_cfg )
    cherrypy.tree.mount( Root(),                '/',         front_end_config )
    
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
    global cfg
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
