import database as db
import sqlalchemy
from sqlalchemy import or_, func, and_
import cherrypy
import mako.template
import os
from urllib import urlencode
from cgi import escape as html_escape
import time
import math
from operator import itemgetter

column_order = {'CallStack':['id','total_time','datetime'],
                'CallStackItem':['id','call_stack_id','function_name','line_number','module','total_calls','native_calls','cumulative_time','total_time'],
                'SQLStatement':['id','duration','datetime'],
                'SQLStackItem':['id','sql_statement_id','module','function'],
                'FileAccess':['id','time_to_open','duration_open','data_written','datetime'],
                'MetaData':['id','key','value']}

datatables_keys = ['sEcho','iColumns','sColumns','iDisplayStart','iDisplayLength','sSearch','bRegex','iSortingCols',
                   'mDataProp_','sSearch_','bRegex_','iSortCol_','sSortDir_','bSortable_']


def search_filter(query, table_class, search_string):
    if search_string:
        string_clauses = []
        for class_attr in column_order[table_class.__name__]:
            attr = getattr(table_class, class_attr)
            if type(attr.type) == sqlalchemy.types.String:
                string_clauses.append(attr.like('%' + search_string + '%'))
        if string_clauses:
            return query.filter(or_(*string_clauses))
    return query
    
def sort_filter(query, table_class, sorted_columns, sort_directions):
    if sorted_columns and sort_directions:
        for i in range(0, len(sorted_columns)):
            attr = getattr(table_class, column_order[table_class.__name__][int(sorted_columns[i])])
            if sort_directions[i] == 'asc':
                query = query.order_by(attr.asc())
            elif sort_directions[i] == 'desc':
                query = query.order_by(attr.desc())
    return query

def json_get(table_class, id=None, **kwargs):
    if id:
        kwargs['id'] = id
    kwargs.pop('_', None)
    
    # Get filter keyword args (id, total_time, etc)
    keyword_args = {}
    metadata_ids = []
    for keyword in kwargs.keys():
        if keyword in column_order[table_class.__name__]:
            keyword_args[keyword] = kwargs[keyword]
        query = db.session.query(db.MetaData).filter_by(key=keyword, value=kwargs[keyword])
        if query.count() > 0:
            metadata_ids.append(query.first().id) # Should only be one entry for every key/value pair
    
    total_query = db.session.query(table_class)
    total_num_items = total_query.count()
    
    # Filter using keyword arguments
    filtered_query = total_query.filter_by(**keyword_args)
    
    # Filter using metadata
    metadata_clauses = []
    for metadata_id in metadata_ids:
        metadata_clauses.append(table_class.metadata_items.any(id=metadata_id))
    filtered_query = filtered_query.filter(and_(*metadata_clauses))
    
    # Apply search
    filtered_query = search_filter(filtered_query, table_class, kwargs['sSearch'])
    
    # Apply sort
    sorted_columns = []
    sort_directions = []
    for i in range(0, int(kwargs['iSortingCols'])):
        sorted_columns.append(kwargs['iSortCol_' + str(i)])
        sort_directions.append(kwargs['sSortDir_' + str(i)])
    filtered_query = sort_filter(filtered_query, table_class, sorted_columns, sort_directions)
    
    filtered_num_items = filtered_query.count()
    
    # Apply offset/length
    filtered_query = filtered_query.offset(kwargs['iDisplayStart']).limit(kwargs['iDisplayLength'])
    
    items = filtered_query.all()
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
    return {'aaData':data,
            "sEcho": int(kwargs['sEcho']),
            "iTotalRecords": total_num_items,
            "iTotalDisplayRecords": filtered_num_items
           }

 
     
def is_datatables_key(key):
    for database_key in datatables_keys:
        if database_key in key:
            return True
    return False


def datatables(query_func):
    def dt_wrapped(id, datatables, start_date, end_date, sort, start, limit, **kwargs):
        print id
        if datatables == 'true':
            # move datatables keys to another dict
            table_kwargs ={}
            for key in kwargs.keys():
                if is_datatables_key(key):
                    table_kwargs[key] = kwargs.pop(key)

            # parse datatables kwargs
            search = table_kwargs['sSearch']
            sort = []
            cols = ('id','sql','count','avg','min','max')
            for i in range(int(table_kwargs['iSortingCols'])):
                sort_col = cols[int(table_kwargs['iSortCol_' + str(i)])]
                sort_dir = 'DESC' if table_kwargs['sSortDir_' + str(i)]=='desc' else 'ASC'
                sort.append((sort_col,sort_dir))
            start = int(table_kwargs['iDisplayStart'])
            limit = int(table_kwargs['iDisplayLength'])
            results, total_num_items, filtered_num_items = query_func(id=id,
                                                                      search=search,
                                                                      start_date=start_date,
                                                                      end_date=end_date,
                                                                      sort=sort,
                                                                      start=start,
                                                                      limit=limit)
            return {'aaData':results,
                    "sEcho": int(table_kwargs['sEcho']),
                    "iTotalRecords": total_num_items,
                    "iTotalDisplayRecords": filtered_num_items}
        else:
            results, total_num_items, filtered_num_items = query_func(id=id,
                                                                      search=None,
                                                                      start_date=start_date,
                                                                      end_date=end_date,
                                                                      sort=[],
                                                                      start=start,
                                                                      limit=limit)
            return results
    return dt_wrapped


@datatables
def json_aggregate_sql(id, search, start_date, end_date, sort, start, limit):
    if id:
        total_num_items = 1
    else:
        total_num_items = db.session.query(db.MetaData).filter(db.MetaData.key=='sql_string').count()

    query = db.session.query(db.MetaData.id,
                             db.MetaData.value.label('sql'),
                             func.count(db.MetaData.id).label('count'),
                             func.avg(db.SQLStatement.duration).label('avg'),
                             func.min(db.SQLStatement.duration).label('min'),
                             func.max(db.SQLStatement.duration).label('max'))
    query = query.filter(db.MetaData.key=='sql_string')
    query = query.join(db.SQLStatement.metadata_items)
    query = query.group_by(db.MetaData.id)
    # if id:          query = query.filter(db.MetaData.id==id)
    if start_date:  query = query.filter(db.SQLStatement.datetime>start_date)
    if end_date:    query = query.filter(db.SQLStatement.datetime<end_date)
    if search:      query = query.filter(db.SQLStatement.sql_string.like('%%%s%%'%search))
    for sorter in sort:
        query = query.order_by('%s %s'%sorter)
    filtered_num_items = query.count()
    if start:       query = query.offset(start)
    if limit:       query = query.limit(limit)
    results = query.all()
    if id: results = results[0]

    return results, total_num_items, filtered_num_items



class JSONAggregateSQL(object):
    exposed = True
    @cherrypy.tools.json_out()
    def GET(self, id=None, datatables=False, start_date=None, end_date=None, **kwargs):
        return json_aggregate_sql(id=id, datatables=datatables, start_date=start_date, end_date=end_date,
                                  sort=[('avg','ASC')], start=None, limit=None, **kwargs)


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
        main_table_item = None
        if 'call_stack_id' in kwargs:
            main_table_item = db.session.query(db.CallStack).get(kwargs['call_stack_id'])
        elif 'sql_statement_id' in kwargs:
            main_table_item = db.session.query(db.SQLStatement).get(kwargs['sql_statement_id'])
        elif 'file_access_id' in kwargs:
            main_table_item = db.session.query(db.FileAccess).get(kwargs['file_access_id'])
        
        data = []
        if main_table_item:
            for metadata in main_table_item.metadata_items:
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

class AggregateSQL(object):
    exposed = True
    def GET(self, id=None, start_date=None, end_date=None, **kwargs):
        if id:
            statement, total, filtered = json_aggregate_sql(id=id, datatables=False, start_date=start_date, end_date=end_date,
                                                            sort=[], start=None, limit=None, **kwargs)
            statement[1]=str(statement[1]) #unicode throws off template when casting dict as js obj
            mytemplate = mako.template.Template(filename=os.path.join(os.getcwd(),'static','templates','aggregatesql.html'))
            return mytemplate.render(statement=statement)
        else:
            mytemplate = mako.template.Template(filename=os.path.join(os.getcwd(),'static','templates','aggregatesqls.html'))
            return mytemplate.render()



def handle_error():
    cherrypy.response.status = 500
    cherrypy.response.body = mako.template.Template(filename=os.path.join(os.getcwd(),'static','templates','500.html')).render()


class Root(object):
    exposed = True
    
    def __init__(self):
        cherrypy.config.update({'request.error_response': handle_error})
        cherrypy.config.update({'error_page.404': os.path.join(os.getcwd(),'static','templates','404.html')})

    def GET(self):
        return mako.template.Template(filename=os.path.join(os.getcwd(),'static','templates','index.html')).render()

    callstacks = CallStacks()
    sqlstatements = SQLStatements()
    fileaccesses = FileAccesses()
    aggregatesql = AggregateSQL()

    _callstacks = JSONCallStacks()
    _callstackitems = JSONCallStackItems()
    _sqlstatements = JSONSQLStatements()
    _sqlstackitems = JSONSQLStackItems()
    _fileaccesses = JSONFileAccesses()
    _metadata = JSONMetadata()
    _aggregatesql = JSONAggregateSQL()

