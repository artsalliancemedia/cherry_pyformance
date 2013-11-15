import database as db
import cherrypy
import mako.template
import os
from urllib import urlencode
from cgi import escape as html_escape


column_order = {'CallStack':['id','total_time','datetime'],
                'CallStackItem':['id','call_stack_id','function_name','line_number','module','total_calls','native_calls','cumulative_time','total_time'],
                'SQLStatement':['id','sql_string','duration','datetime'],
                'MetaData':['id','key','value']}

def json_get(table_class, id=None, **kwargs):
    if id:
        kwargs['id'] = id
    kwargs.pop('_', None)
    items = db.session.query(table_class).filter_by(**kwargs).all()
    data = []
    for item in items:
        record = [html_escape(str(item.__dict__[x])) for x in column_order[table_class.__name__]]
        data.append(record)
    return {'aaData':data}


class JSONCallStacks(object):
    exposed = True
    @cherrypy.tools.json_out()
    def GET(self, id=None, **kwargs):
        return json_get(db.CallStack, id, **kwargs)

class JSONCallStackItems(object):
    exposed = True
    @cherrypy.tools.json_out()
    def GET(self, id=None, **kwargs):
        return json_get(db.CallStackItem, id, **kwargs)

class JSONSQLStatements(object):
    exposed = True
    @cherrypy.tools.json_out()
    def GET(self, id=None, **kwargs):
        return json_get(db.SQLStatement, id, **kwargs)

class JSONMetadata(object):
    exposed = True
    @cherrypy.tools.json_out()
    def GET(self, id=None, **kwargs):
        call_stack_metadata = db.session.query(db.CallStackMetadata).filter_by(call_stack_id=kwargs['call_stack_id']).all()
        metadata_ids = [item.metadata_id for item in call_stack_metadata]
        metadata_list = db.session.query(db.MetaData).filter(db.MetaData.id.in_(metadata_ids)).all()
        data = []
        for metadata in metadata_list:
            record = [html_escape(str(metadata.__dict__[x])) for x in column_order['MetaData']]
            data.append(record)
        return {'aaData':data}


class CallStacks(object):
    exposed = True

    def GET(self, id=None, **kwargs):
        if id:
            call_stack = db.session.query(db.CallStack).get(id)
            mytemplate = mako.template.Template(filename=os.path.join(os.getcwd(),'static','templates','callstack.html'))
            return mytemplate.render(callstack=call_stack,
                                     encoded_kwargs='call_stack_id='+str(call_stack.id)+'&'+urlencode(kwargs))
        else:
            mytemplate = mako.template.Template(filename=os.path.join(os.getcwd(),'static','templates','callstacks.html'))
            return mytemplate.render(encoded_kwargs=urlencode(kwargs))

class SQLStatements(object):
    exposed = True

    def GET(self, **kwargs):
        mytemplate = mako.template.Template(filename=os.path.join(os.getcwd(),'static','templates','sqlstatements.html'))
        return mytemplate.render(encoded_kwargs=urlencode(kwargs))


class Root(object):
    exposed = True

    def GET(self):
        return 'Hello, world.'

    callstacks = CallStacks()
    sqlstatements = SQLStatements()

    _callstacks = JSONCallStacks()
    _callstackitems = JSONCallStackItems()
    _sqlstatements = JSONSQLStatements()
    _metadata = JSONMetadata()
