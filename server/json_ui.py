# import sqlalchemy
import database as db
import cherrypy
# from sqlalchemy import or_, and_
from cgi import escape as html_escape
import re
import os.path
import json
import analyse_stats as a

def retrieve_pstat(uuid):
    if not os.path.isfile(os.path.join('pstats',uuid+'.json')):
        stats = a.load(uuid)
        callees = a.keys_to_str(stats.all_callees)
        total_tt = stats.total_tt
        stats = a.keys_to_str(stats.stats)
        response = {'stats':stats,'callees':callees,'total_tt':total_tt}
        a.write_json(response, uuid)
        return response
    else:
        with open(os.path.join('pstats',str(uuid)+'.json')) as f:
            return json.load(f)



class JSONAPI(object):

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def callstacks(self, id=None, **kwargs):
        if id:
            item = db.session.query(db.CallStack).get(id)
            if item:
                response = item.to_dict()
                stats_object = item._stats()
                stats = stats_object.stats
                response['stats_keys'] = [str(key) for key in stats.keys()]
                response['stats_values'] = [str(val) for val in stats.values()]
                return response
            else:
                raise cherrypy.NotFound
        else:
            results = db.session.query(db.CallStack)
            return [item.to_dict() for item in results.all()]

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def callstackitems(self, callstack_id):
        callstack = db.session.query(db.CallStack).get(callstack_id)
        if not callstack:
            raise cherrypy.NotFound
        uuid = callstack.pstat_uuid
        return retrieve_pstat(uuid)


    @cherrypy.expose
    @cherrypy.tools.json_out()
    def sqlstatements(self, id=None, **kwargs):
        if id:
            item = db.session.query(db.SQLStatement).get(id)
            if item:
                response = item.to_dict()
                response['stack'] = item._stack()
                # substitute args
                sql_string = str(response['sql'])
                for key,val in response['args']:
                    # Protect old database args (before key was added to sql args)
                    if key == None or val == None:
                        continue
                    # If postgres arg, surround with %(arg)s to replace properly
                    if key != '?':
                        key = '\%\(' + key + '\)s'
                    print key,val
                    sql_string = re.sub(key, val, sql_string, 1)
                # Just send values to html template
                response['args'] = [kv_pair[1] for kv_pair in response['args']]
                response['sql'] = sql_string
                return response
            else:
                raise cherrypy.NotFound
        else:
            results = db.session.query(db.SQLStatement)
            return [item.to_dict() for item in results.all()]

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def sqlstackitems(self, id=None, **kwargs):
        if id:
            return db.session.query(db.SQLStackItem).get(id).to_dict()
        else:
            results = db.session.query(db.SQLStackItem)
            return [item.to_dict() for item in results.all()]

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def fileaccesses(self, id=None, **kwargs):
        if id:
            return db.session.query(db.FileAccess).get(id).to_dict()
        else:
            results = db.session.query(db.FileAccess)
            return [item.to_dict() for item in results.all()]
        
    table_metadata_keys_dict = {'callstacks':[['module','class','method'],['statement_identifiers','statement_type']],
                                'sqlstatements':[[],[]],
                                'fileaccesses':[[],['statement_identifiers','statement_type']]}
    
    call_stack_metadata_dict = {'module': ['module_name', db.CallStackName.module_name],
                                'class':  ['class_name', db.CallStackName.class_name],
                                'method': ['fn_name', db.CallStackName.fn_name]}
    
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def metadata(self, id=None, **kwargs):
        results_list = []
        if 'get_keys' in kwargs:
            table_name = kwargs['get_keys']
            table_metadata_keys = self.table_metadata_keys_dict[table_name]
            extra_keys = table_metadata_keys[0]
            remove_keys = table_metadata_keys[1]
            
            key_list_dicts = db.session.query(db.MetaData.key).distinct().all()
            results_list = [key_dict[0] for key_dict in key_list_dicts]
            results_list += extra_keys
            for key in remove_keys:
                if key in results_list:
                    results_list.remove(key)
        else:
            results_list = []
            for key in kwargs:
                if kwargs[key] in self.call_stack_metadata_dict:
                    call_stack_attr_name = self.call_stack_metadata_dict[kwargs[key]][0]
                    call_stack_attr = self.call_stack_metadata_dict[kwargs[key]][1]
                    call_stack_name_list = db.session.query(db.CallStackName).all()
                    results_list += list(set([getattr(call_stack_name, call_stack_attr_name) for call_stack_name in call_stack_name_list]))
            
            metadata_list = db.session.query(db.MetaData).filter_by(**kwargs).all()
            results_list += [metadata.__dict__['value'] for metadata in metadata_list]
        
        for result in results_list:
            if result == None:
                results_list.remove(result)
        results_list = [str(result) for result in results_list]
        results_list.sort(key=str.lower)
        return results_list
