import database as db
import cherrypy
import mako.template
import os


def json_get(table_class, id=None, **kwargs):
    if id:
        kwargs['id'] = id
    if '_' in kwargs:
        del(kwargs['_'])
    items = db.session.query(table_class).filter_by(**kwargs).all()
    data = []
    for item in items:
        del(item.__dict__['_sa_instance_state'])
        data.append(item.__dict__)
    return {'aaData':data}



class JSONSenders(object):
    exposed = True
    @cherrypy.tools.json_out()
    def GET(self, id=None, **kwargs):
        return json_get(db.Sender, id, **kwargs)

class JSONMethodCalls(object):
    exposed = True
    @cherrypy.tools.json_out()
    def GET(self, id=None, **kwargs):
        return json_get(db.MethodCall, id, **kwargs)

class JSONCallStacks(object):
    exposed = True
    @cherrypy.tools.json_out()
    def GET(self, id=None, **kwargs):
        return json_get(db.CallStack, id, **kwargs)

class JSONSQLStatements(object):
    exposed = True
    @cherrypy.tools.json_out()
    def GET(self, id=None, **kwargs):
        return json_get(db.SQLStatement, id, **kwargs)








class Senders(object):
    exposed = True

    @cherrypy.tools.json_out()
    def GET(self, sender_id=None, **kwargs):
        if sender_id:
            kwargs['id'] = sender_id
        senders = db.session.query(db.Sender).filter_by(**kwargs).all()

        data = []
        for sender in senders:
            del(sender.__dict__['_sa_instance_state'])
            data.append(sender.__dict__)

        return data

class MethodCalls(object):
    exposed = True

    def GET(self, method_id=None, **kwargs):
        if method_id:
            callstacks = db.session.query(db.CallStack).filter_by(method_call_id=method_id, **kwargs).all()
            data = []
            for callstack in callstacks:
                del(callstack.__dict__['_sa_instance_state'])
                data.append(callstack.__dict__)

            mytemplate = mako.template.Template(filename=os.path.join(os.getcwd(),'static','templates','index.html'))
            return mytemplate.render(data=data)
        
        else:
            """
            methods = db.session.query(db.MethodCall).filter_by(**kwargs).all()

            data = []
            for method in methods:
                del(method.__dict__['_sa_instance_state'])
                data.append(method.__dict__)
            """

            mytemplate = mako.template.Template(filename=os.path.join(os.getcwd(),'static','templates','index.html'))
            return mytemplate.render(url='/_methodcalls')# , column_list=[{'sName':''},{'sName':''},{'sName':''},{'sName':''}]

class CallStacks(object):
    exposed = True

    def GET(self, callstack_id=None, **kwargs):
        if callstack_id:
            kwargs['id'] = callstack_id
        callstacks = db.session.query(db.CallStack).filter_by(**kwargs).all()

        data = []
        for callstack in callstacks:
            del(callstack.__dict__['_sa_instance_state'])
            data.append(callstack.__dict__)

        mytemplate = mako.template.Template(filename=os.path.join(os.getcwd(),'static','templates','index.html'))
        return mytemplate.render(data=data)

class SQLStatements(object):
    exposed = True

    def GET(self, statement_id=None, **kwargs):
        if statement_id:
            kwargs['id'] = statement_id
        statements = db.session.query(db.SQLStatement).filter_by(**kwargs).all()

        data = []
        for statement in statements:
            del(statement.__dict__['_sa_instance_state'])
            data.append(statement.__dict__)

        mytemplate = mako.template.Template(filename=os.path.join(os.getcwd(),'static','templates','index.html'))
        return mytemplate.render(data=data)

class Root(object):
    exposed = True

    def GET(self):
        return 'Hello, world.'

    # senders = Senders()
    methodcalls = MethodCalls()
    callstacks = CallStacks()
    sqlstatements = SQLStatements()

    _senders = JSONSenders()
    _methodcalls = JSONMethodCalls()
    _callstacks = JSONCallStacks()
    _sqlstatements = JSONSQLStatements()
