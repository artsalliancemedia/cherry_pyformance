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
                return item._to_dict()
            else:
                raise cherrypy.NotFound
        else:
            results = db.session.query(db.CallStack)
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
            
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def metadata(self, id=None, **kwargs):
        results_list = []
        if 'get_keys' in kwargs:
            key_list_dicts = db.session.query(db.MetaData.key).distinct().all()
            results_list = [key_dict[0] for key_dict in key_list_dicts]
        else:
            metadata_list = db.session.query(db.MetaData).filter_by(**kwargs).all()
            results_list = [metadata.__dict__['value'] for metadata in metadata_list]
        
        results_list.sort(key=unicode.lower)
        return results_list
