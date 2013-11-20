import database as db
import cherrypy
import mako.template
import os
from urllib import urlencode
from cgi import escape as html_escape
import time
from stats_server import load_config

cfg = load_config()

column_order = {'CallStack':['id','total_time','datetime'],
                'CallStackItem':['id','call_stack_id','function_name','line_number','module','total_calls','native_calls','cumulative_time','total_time'],
                'SQLStatement':['id','sql_string','duration','datetime'],
                'SQLStackItem':['id','sql_statement_id','module','function'],
                'FileAccess':['id','time_to_open','duration_open','data_written','datetime'],
                'MetaData':['id','key','value']}

def json_get(table_class, id=None, **kwargs):
    if id:
        kwargs['id'] = id
    kwargs.pop('_', None)
    
    filtered_query = db.session.query(table_class).filter_by(**kwargs)
    num_items = filtered_query.count()
    # Might set start/length using keyword args at some point in the future
    length = int(cfg['max_table_items'])
    start = max(0, num_items - length)
        
    items = filtered_query.offset(start).limit(length).all()
    data = []
    for item in items:
        record = []
        for column in column_order[table_class.__name__]:
            datum = item.__dict__[column]
            if type(datum) == float:
                datum = "%f"%datum
            if column == 'sql_string':
                datum = datum.replace('\\n', '<br>')
            else:
                datum = html_escape(str(datum))
            record.append(datum)
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
        elif 'file_access_id' in kwargs:
            metadata_relations = db.session.query(db.FileAccessMetadata).filter_by(file_access_id=kwargs['file_access_id']).all()
        metadata_ids = [item.metadata_id for item in metadata_relations]
        metadata_list = db.session.query(db.MetaData).filter(db.MetaData.id.in_(metadata_ids)).all()
        data = []
        for metadata in metadata_list:
            record = [html_escape(str(metadata.__dict__[x])) for x in column_order['MetaData']]
            data.append(record)
        return {'aaData':data}

class JSONSQLStackItems(object):
    exposed = True
    @cherrypy.tools.json_out()
    def GET(self, id=None, **kwargs):
        return json_get(db.SQLStackItem, id, **kwargs)


class JSONFileAccesses(object):
    exposed = True
    @cherrypy.tools.json_out()
    def GET(self, id=None, **kwargs):
        return json_get(db.FileAccess, id, **kwargs)


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

class FileAccesses(object):
    exposed = True

    def GET(self, id=None, **kwargs):
        if id:
            file_access = db.session.query(db.FileAccess).get(id)
            mytemplate = mako.template.Template(filename=os.path.join(os.getcwd(),'static','templates','fileaccess.html'))
            return mytemplate.render(file_access=file_access, encoded_kwargs=urlencode(kwargs))
        else:
            mytemplate = mako.template.Template(filename=os.path.join(os.getcwd(),'static','templates','fileaccesses.html'))
            return mytemplate.render(encoded_kwargs=urlencode(kwargs))





class Root(object):
    exposed = True

    def GET(self):
        return mako.template.Template(filename=os.path.join(os.getcwd(),'static','templates','index.html')).render()

    callstacks = CallStacks()
    sqlstatements = SQLStatements()
    fileaccesses = FileAccesses()

    _callstacks = JSONCallStacks()
    _callstackitems = JSONCallStackItems()
    _sqlstatements = JSONSQLStatements()
    _sqlstackitems = JSONSQLStackItems()
    _fileaccesses = JSONFileAccesses()
    _metadata = JSONMetadata()
