import cherrypy
import sys
import database as db


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
    try:
        db.setup_profile_database()
        start_cherrypy()
    except Exception, ex:
        print str(ex)