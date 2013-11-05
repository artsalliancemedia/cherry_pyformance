import cherrypy
import sys
import json
import database as db

class Root(object):
    exposed = True

    def GET(self):
        return 'Hello, World.'

    @cherrypy.tools.json_in()
    def POST(self):
        db.push_stats_buffer(cherrypy.serving.request.json)
        return 'Hello, World.'

def start_cherrypy():
    cherrypy.config.update({'server.socket_port': 8888})
    cherrypy.log('Mounting the app')
    cherrypy.tree.mount(Root(), '/',
        {'/':
            {'request.dispatch': cherrypy.dispatch.MethodDispatcher()}
        }
    )
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