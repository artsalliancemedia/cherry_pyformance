import os
import sys
import logging
import ConfigParser
from optparse import OptionParser

# Put the external python libraries on the path
# Note: Make sure that the first you import is the serv module (import serv)
#       in the main startup script (like in CinemaServices.py, CircuitServices.py,
#       CircuitServicesSync.py, CircuitServicesWsw.py) whenever you create another one.
#       This guarantees that the libraries are on the path no matter
#       in which order modules are imported later.
lib_dir = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'lib'))
sys.path.insert(0, lib_dir)

#deal with awesome eggs!
for path in os.listdir(lib_dir):
    if path.endswith('egg'):
        sys.path.insert(0, os.path.join(lib_dir,path))


import cherrypy
import sys
import database as db
import os
import mako.template

from json_ui import JSONAPI
from table_ui import Tables
from aggregate_json_ui import AggregateAPI
from aggregate_table_ui import AggregatePages

from stat_handlers import function_stat_handler, handler_stat_handler, sql_stat_handler, file_stat_handler


# add gzip to allowed content types for decompressing JSON if compressed.

def handle_error():
    cherrypy.response.status = 500
    cherrypy.response.body = mako.template.Template(
                                filename=os.path.join(os.getcwd(), 'static', 'templates', '500.html')
                            ).render(error_str=cherrypy._cperror.format_exc())


def start_cherrypy(host, port):
    cherrypy.server.socket_host = host
    cherrypy.server.socket_port = int(port)
    cherrypy.log.screen = False
    cherrypy.log('Mounting the handlers')
    method_dispatch_cfg = {'/': {'request.dispatch': cherrypy.dispatch.MethodDispatcher()} }
    front_end_config = {
            '/static': {
                'tools.staticdir.on': True,
                'tools.staticdir.dir': os.path.join(os.getcwd(), 'static'),
                'tools.staticdir.content_types': {
                    'js': 'application/javascript',
                    'css': 'text/css',
                    'images': 'image/png'
                }
            }
        }

    cherrypy.config.update({'request.error_response': handle_error})
    cherrypy.config.update({'error_page.404': os.path.join(os.getcwd(),'static','templates','404.html')})



    cherrypy.tree.mount(function_stat_handler, '/function',   method_dispatch_cfg )
    cherrypy.tree.mount(handler_stat_handler,  '/handler',    method_dispatch_cfg )
    cherrypy.tree.mount(sql_stat_handler,      '/database',   method_dispatch_cfg )
    cherrypy.tree.mount(file_stat_handler,     '/file',       method_dispatch_cfg )

    cherrypy.tree.mount(Tables(),              '/tables')
    cherrypy.tree.mount(JSONAPI(),             '/tables/api')
    cherrypy.tree.mount(AggregatePages(),      '/',           front_end_config)
    cherrypy.tree.mount(AggregateAPI(),        '/api')

    # Attach the signal handlers to detect keyboard interrupt
    if hasattr(cherrypy.engine, 'signal_handler'):
        cherrypy.engine.signal_handler.subscribe()
    if hasattr(cherrypy.engine, 'console_control_handler'):
        cherrypy.engine.console_control_handler.subscribe()

    cherrypy.log('Starting CherryPy')
    try:
        cherrypy.engine.start()
    except IOError:
        logging.error('Unable to bind to address ({0}, {1})',format(cfg['server_host'], cfg['server_port']))
        sys.exit(1)

    cherrypy.engine.wait(cherrypy.process.wspbus.states.STARTED)
    cherrypy.log('CherryPy started')
    cherrypy.engine.block()

def _parse_options():
    parser = OptionParser()
    parser.add_option(
        '-r',
        '--reset',
        dest='reset_db',
        action='store_true',
        default=False,
        help='Reset the profile_stats database prior to launching service'
    )
    options, args = parser.parse_args()
    return options, args

def load_config():
    config = ConfigParser.ConfigParser()

    config.read('server_config.cfg')
    if config.sections() == []:
        print 'Failed to load config file (server_config.cfg)'
        sys.exit(1)

    config_dict = config._sections['global']
    config_dict.pop('__name__')
    return config_dict

if __name__ == '__main__':
    cfg = load_config()
    options, args = _parse_options()
    print 'hi'
    try:
        # Set up the initialise database config
        db.setup(cfg['database_username'], cfg['database_password'], reset_db=options.reset_db)

        # Ensure we have a pstats directory to write into.
        if not os.path.exists('pstats'):
            os.makedirs('pstats')

        start_cherrypy(cfg['server_host'], cfg['server_port'])
    except Exception, ex:
        print str(ex)
