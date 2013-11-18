import database as db
import cherrypy
import mako.template
import os
from urllib import urlencode
from cgi import escape as html_escape
import time


column_order = {'CallStack':['id','total_time','datetime'],
                'CallStackItem':['id','call_stack_id','function_name','line_number','module','total_calls','native_calls','cumulative_time','total_time'],
                'SQLStatement':['id','sql_string','duration','datetime'],
                'SQLStack':['id','sql_statement_id','module','function'],
                'MetaData':['id','key','value']}

def json_get(table_class, id=None, **kwargs):
    if id:
        kwargs['id'] = id
    kwargs.pop('_', None)
    items = db.session.query(table_class).filter_by(**kwargs).all()
    data = []
    for item in items:
        record = []
        for column in column_order[table_class.__name__]:
            datum = item.__dict__[column]
            if column == 'datetime':
                datum = time.strftime('%a, %d %b %Y %H:%M:%S', time.localtime(datum))
            if type(datum) == float:
                datum = "%f"%datum
            record.append(html_escape(str(datum)))
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
        metadata_relations = []
        if 'call_stack_id' in kwargs:
            metadata_relations = db.session.query(db.CallStackMetadata).filter_by(call_stack_id=kwargs['call_stack_id']).all()
        elif 'sql_statement_id' in kwargs:
            metadata_relations = db.session.query(db.SQLStatementMetadata).filter_by(sql_statement_id=kwargs['sql_statement_id']).all()
        metadata_ids = [item.metadata_id for item in metadata_relations]
        metadata_list = db.session.query(db.MetaData).filter(db.MetaData.id.in_(metadata_ids)).all()
        data = []
        for metadata in metadata_list:
            record = [html_escape(str(metadata.__dict__[x])) for x in column_order['MetaData']]
            data.append(record)
        return {'aaData':data}


class JSONSQLStacks(object):
    exposed = True
    @cherrypy.tools.json_out()
    def GET(self, id=None, **kwargs):
        return json_get(db.SQLStack, id, **kwargs)




class CallStacks(object):
    exposed = True

    def GET(self, id=None, **kwargs):
        if id:
            call_stack = db.session.query(db.CallStack).get(id)
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
            mytemplate = mako.template.Template(filename=os.path.join(os.getcwd(),'static','templates','sqlstatement.html'))
            return mytemplate.render(sql_statement=sql_statement, encoded_kwargs=urlencode(kwargs))
        else:
            mytemplate = mako.template.Template(filename=os.path.join(os.getcwd(),'static','templates','sqlstatements.html'))
            return mytemplate.render(encoded_kwargs=urlencode(kwargs))


class Root(object):
    exposed = True

    def GET(self):
        return mako.template.Template(filename=os.path.join(os.getcwd(),'static','templates','index.html')).render()

    callstacks = CallStacks()
    sqlstatements = SQLStatements()

    _callstacks = JSONCallStacks()
    _callstackitems = JSONCallStackItems()
    _sqlstatements = JSONSQLStatements()
    _sqlstacks = JSONSQLStacks()
    _metadata = JSONMetadata()
