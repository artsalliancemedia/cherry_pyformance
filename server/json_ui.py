# import sqlalchemy
import database as db
import cherrypy
# from sqlalchemy import or_, and_
from cgi import escape as html_escape


class JSONAPI(object):

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def callstacks(self, id=None, **kwargs):
        if id:
            item = db.session.query(db.CallStack).get(id)
            if item:
                response = item._to_dict()
                response['stack'] = item._stack()
                return response
            else:
                raise cherrypy.NotFound
        else:
            results = db.session.query(db.CallStack)
            return [item._to_dict() for item in results.all()]


    @cherrypy.expose
    @cherrypy.tools.json_out()
    def callstackitems(self, id=None, **kwargs):
        if id:
            return db.session.query(db.CallStackItem).get(id)._to_dict()
        else:
            results = db.session.query(db.CallStackItem)
            return [item._to_dict() for item in results.all()]

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def sqlstatements(self, id=None, **kwargs):
        if id:
            item = db.session.query(db.SQLStatement).get(id)
            if item:
                response = item._to_dict()
                response['stack'] = item._stack()
                return response
            else:
                raise cherrypy.NotFound
        else:
            results = db.session.query(db.SQLStatement)
            return [item._to_dict() for item in results.all()]

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def sqlstackitems(self, id=None, **kwargs):
        if id:
            return db.session.query(db.SQLStackItem).get(id)._to_dict()
        else:
            results = db.session.query(db.SQLStackItem)
            return [item._to_dict() for item in results.all()]

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def fileaccesses(self, id=None, **kwargs):
        if id:
            return db.session.query(db.FileAccess).get(id)._to_dict()
        else:
            results = db.session.query(db.FileAccess)
            return [item._to_dict() for item in results.all()]

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def metadata(self, kv=None, **kwargs):
        if kv and kv=='keys':
            return sorted([html_escape(str(item[0])) for item in db.session.query(db.MetaData.key).distinct().filter_by(**kwargs).all()])
        if kv and kv=='values':
            return sorted([html_escape(str(item[0])) for item in db.session.query(db.MetaData.value).filter_by(**kwargs).all()])

