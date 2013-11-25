import mako.template
import sqlalchemy
import database as db
import cherrypy
import os
from urllib import urlencode

class CallStacks(object):
    exposed = True

    def GET(self, id=None, **kwargs):
        if id:
            call_stack = db.session.query(db.CallStack).get(id)
            if call_stack == None:
                raise cherrypy.HTTPError(404)
            mytemplate = mako.template.Template(filename=os.path.join(os.getcwd(),'static','templates','callstack.html'))
            return mytemplate.render(callstack=call_stack, encoded_kwargs=urlencode(kwargs))
        else:
            mytemplate = mako.template.Template(filename=os.path.join(os.getcwd(),'static','templates','callstacks.html'))
            return mytemplate.render(encoded_kwargs=urlencode(kwargs))

class SQLStatements(object):
    exposed = True

    def GET(self, id=None, **kwargs):
        if id:
            sql_statement = db.session.query(db.SQLStatement).get(id)
            if sql_statement == None:
                raise cherrypy.HTTPError(404)
            mytemplate = mako.template.Template(filename=os.path.join(os.getcwd(),'static','templates','sqlstatement.html'))
            return mytemplate.render(sql_statement=sql_statement, encoded_kwargs=urlencode(kwargs))
        else:
            mytemplate = mako.template.Template(filename=os.path.join(os.getcwd(),'static','templates','sqlstatements.html'))
            return mytemplate.render(encoded_kwargs=urlencode(kwargs))

class FileAccesses(object):
    exposed = True

    def GET(self, id=None, **kwargs):
        if id:
            file_access = db.session.query(db.FileAccess).get(id)
            if file_access == None:
                raise cherrypy.HTTPError(404)
            mytemplate = mako.template.Template(filename=os.path.join(os.getcwd(),'static','templates','fileaccess.html'))
            return mytemplate.render(file_access=file_access, encoded_kwargs=urlencode(kwargs))
        else:
            mytemplate = mako.template.Template(filename=os.path.join(os.getcwd(),'static','templates','fileaccesses.html'))
            return mytemplate.render(encoded_kwargs=urlencode(kwargs))