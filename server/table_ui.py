from mako.template import Template
from mako.lookup import TemplateLookup
import sqlalchemy
import database as db
import cherrypy
import os
from urllib import urlencode

class Tables(object):
    
    templates_dir = os.path.join(os.getcwd(),'static','templates')
    template_lookup = TemplateLookup(directories=[templates_dir,])

    @cherrypy.expose
    def callstacks(self, id=None, **kwargs):
        if id:
            call_stack = db.session.query(db.CallStack).get(id)
            if call_stack == None:
                raise cherrypy.HTTPError(404)
            metadata_id = call_stack.name.id
            mytemplate = Template(filename=os.path.join(self.templates_dir,'callstack.html'), lookup=self.template_lookup)
            return mytemplate.render(call_stack=call_stack, metadata_id=metadata_id, encoded_kwargs=urlencode(kwargs))
        else:
            mytemplate = Template(filename=os.path.join(self.templates_dir,'callstacks.html'), lookup=self.template_lookup)
            return mytemplate.render(encoded_kwargs=urlencode(kwargs))


    @cherrypy.expose
    def sqlstatements(self, id=None, **kwargs):
        if id:
            sql_statement = db.session.query(db.SQLStatement).get(id)
            if sql_statement == None:
                raise cherrypy.HTTPError(404)
            metadata_id = sql_statement.sql_string.id
            mytemplate = Template(filename=os.path.join(self.templates_dir,'sqlstatement.html'), lookup=self.template_lookup)
            return mytemplate.render(sql_statement=sql_statement, metadata_id=metadata_id, encoded_kwargs=urlencode(kwargs))
        else:
            mytemplate = Template(filename=os.path.join(self.templates_dir,'sqlstatements.html'), lookup=self.template_lookup)
            return mytemplate.render(encoded_kwargs=urlencode(kwargs))

    @cherrypy.expose
    def fileaccesses(self, id=None, **kwargs):
        if id:
            file_access = db.session.query(db.FileAccess).get(id)
            if file_access == None:
                raise cherrypy.HTTPError(404)
            metadata_id = file_access.filename.id
            mytemplate = Template(filename=os.path.join(self.templates_dir,'fileaccess.html'), lookup=self.template_lookup)
            return mytemplate.render(file_access=file_access, metadata_id=metadata_id, encoded_kwargs=urlencode(kwargs))
        else:
            mytemplate = Template(filename=os.path.join(self.templates_dir,'fileaccesses.html'), lookup=self.template_lookup)
            return mytemplate.render(encoded_kwargs=urlencode(kwargs))
