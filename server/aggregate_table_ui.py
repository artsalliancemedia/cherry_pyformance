import mako.template
import cherrypy
from aggregate_json_ui import json_aggregate, parse_kwargs
import os
from urllib import urlencode
import database as db


class AggregatePages(object):
    @cherrypy.expose
    def index(self):
        return self.callstacks()

    @cherrypy.expose
    def callstacks(self, id=None, **kwargs):
        if id:
            table_kwargs, filter_kwargs = parse_kwargs(kwargs)
            call_stack, total, filtered = json_aggregate(id, table_kwargs, filter_kwargs, db.CallStack)
            if call_stack == None:
                raise cherrypy.HTTPError(404)
            call_stack[1]=str(call_stack[1]) #unicode throws off template when casting dict as js obj
            call_stack[2]=int(call_stack[2]) #convert long to int
            mytemplate = mako.template.Template(filename=os.path.join(os.getcwd(),'static','templates','aggregatecallstack.html'))
            if 'id' in filter_kwargs: filter_kwargs.pop('id')
            return mytemplate.render(call_stack=call_stack, encoded_kwargs=urlencode(filter_kwargs))
        else:
            mytemplate = mako.template.Template(filename=os.path.join(os.getcwd(),'static','templates','aggregatecallstacks.html'))
            return mytemplate.render()

    @cherrypy.expose
    def sqlstatements(self, id=None, **kwargs):
        if id:
            table_kwargs, filter_kwargs = parse_kwargs(kwargs)
            statement, total, filtered = json_aggregate(id, table_kwargs, filter_kwargs, db.SQLStatement)
            if statement == None:
                raise cherrypy.HTTPError(404)
            statement[1]=str(statement[1]) #unicode throws off template when casting dict as js obj
            statement[2]=int(statement[2]) #convert long to int
            mytemplate = mako.template.Template(filename=os.path.join(os.getcwd(),'static','templates','aggregatesql.html'))
            if 'id' in filter_kwargs: filter_kwargs.pop('id')
            return mytemplate.render(statement=statement, encoded_kwargs=urlencode(filter_kwargs))
        else:
            mytemplate = mako.template.Template(filename=os.path.join(os.getcwd(),'static','templates','aggregatesqls.html'))
            return mytemplate.render()

    @cherrypy.expose
    def fileaccesses(self, id=None, **kwargs):
        if id:
            table_kwargs, filter_kwargs = parse_kwargs(kwargs)
            file_access, total, filtered = json_aggregate(id, table_kwargs, filter_kwargs, db.FileAccess)
            if file_access == None:
                raise cherrypy.HTTPError(404)
            file_access[1]=str(file_access[1]) #unicode throws off template when casting dict as js obj
            file_access[2]=int(file_access[2]) #convert long to int
            mytemplate = mako.template.Template(filename=os.path.join(os.getcwd(),'static','templates','aggregatefileaccess.html'))
            if 'id' in filter_kwargs: filter_kwargs.pop('id')
            return mytemplate.render(file_access=file_access, encoded_kwargs=urlencode(filter_kwargs))
        else:
            mytemplate = mako.template.Template(filename=os.path.join(os.getcwd(),'static','templates','aggregatefileaccesses.html'))
            return mytemplate.render()
