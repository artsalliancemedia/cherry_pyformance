import cherrypy
import os
import mako.template
import table_ui
import json_ui
import aggregate_table_ui
import aggregate_json_ui

def handle_error():
    cherrypy.response.status = 500
    cherrypy.response.body = mako.template.Template(filename=os.path.join(os.getcwd(),'static','templates','500.html'))\
                                          .render(error_str=cherrypy._cperror.format_exc())

class Root(object):
    exposed = True
    
    def __init__(self):
        cherrypy.config.update({'request.error_response': handle_error})
        cherrypy.config.update({'error_page.404': os.path.join(os.getcwd(),'static','templates','404.html')})

    def GET(self):
        return mako.template.Template(filename=os.path.join(os.getcwd(),'static','templates','index.html')).render()
    
    callstacks = table_ui.CallStacks()
    sqlstatements = table_ui.SQLStatements()
    fileaccesses = table_ui.FileAccesses()
    aggregatecallstacks = aggregate_table_ui.AggregateCallStacks()
    aggregatesql = aggregate_table_ui.AggregateSQL()
    aggregatefileaccesses = aggregate_table_ui.AggregateFileAccesses()
    
    _callstacks = json_ui.JSONCallStacks()
    _callstackitems = json_ui.JSONCallStackItems()
    _sqlstatements = json_ui.JSONSQLStatements()
    _sqlstackitems = json_ui.JSONSQLStackItems()
    _fileaccesses = json_ui.JSONFileAccesses()
    _metadata = aggregate_json_ui.JSONMetadata()
    _aggregatecallstacks = aggregate_json_ui.JSONAggregateCallStacks()
    _aggregatesql = aggregate_json_ui.JSONAggregateSQL()
    _aggregatefileaccesses = aggregate_json_ui.JSONAggregateFileAccesses()

