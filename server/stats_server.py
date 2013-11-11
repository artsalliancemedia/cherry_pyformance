import cherrypy
import sys
import database as db

def print_help_string():
    print 'Use as follows:\n\n' \
          'python stats_server.py username password host [port]\n'
    
def print_arg_error():
    print 'Arguments incorrect!'
    print_help_string()

class Root(object):

class Function_Stat_Handler(object):
    exposed = True

    def GET(self):
        return 'Hello, World.'

    @cherrypy.tools.json_in()
    def POST(self):
        db.push_fn_stats(cherrypy.serving.request.json, cherrypy.request.remote.ip)
        return 'Hello, World.'


class Handler_Stat_Handler(object):
    exposed = True

    def GET(self):
        return 'Hello, World.'

    @cherrypy.tools.json_in()
    def POST(self):
        db.push_fn_stats(cherrypy.serving.request.json, cherrypy.request.remote.ip)
        return 'Hello, World.'


class SQL_Stat_Handler(object):
    exposed = True

    def GET(self):
        return 'Hello, World.'

    @cherrypy.tools.json_in()
    def POST(self):
        db.push_sql_stats(cherrypy.serving.request.json, cherrypy.request.remote.ip)
        return 'Hello, World.'



def start_cherrypy():
    cherrypy.config.update({'server.socket_port': 8888})
    cherrypy.log('Mounting the handlers')
    method_dispatch_cfg = {'/': {'request.dispatch': cherrypy.dispatch.MethodDispatcher()} }
    cherrypy.tree.mount( Function_Stat_Handler(), '/function', method_dispatch_cfg )
    cherrypy.tree.mount( Handler_Stat_Handler(),  '/handler',  method_dispatch_cfg )
    cherrypy.tree.mount( SQL_Stat_Handler(),      '/database', method_dispatch_cfg )
    cherrypy.log('Starting CherryPy')
    try:
        cherrypy.engine.start()
    except IOError:
        logging.error('Unable to bind to address (%s, %d)' % (cfg.cherry_host(), cfg.cherry_port()))
        sys.exit(1)

    cherrypy.engine.wait(cherrypy.process.wspbus.states.STARTED)
    cherrypy.log('CherryPy started')
    cherrypy.engine.block()


if __name__ == '__main__':
    if '--help' in sys.argv:
        print_help_string()
        sys.exit(1)
    if not (len(sys.argv) == 4 or len(sys.argv) == 5):
        print_arg_error()
        sys.exit(1)
        
    username = sys.argv[1]
    password = sys.argv[2]
    host = sys.argv[3]
    port = None
    if len(sys.argv) == 5:
        port = sys.argv[4]
    
    try:
        db.setup_profile_database(username, password, host, port)
        start_cherrypy()
    except Exception, ex:
        print str(ex)