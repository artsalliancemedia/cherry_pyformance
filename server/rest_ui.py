import database as db
import sqlalchemy
from sqlalchemy import or_, and_
import cherrypy
import mako.template
import os
from urllib import urlencode
from cgi import escape as html_escape
import time
import math
from operator import itemgetter
import sqlparse

column_order = {'CallStack':['id','total_time','datetime'],
                'CallStackItem':['id','call_stack_id','function_name','line_number','module','total_calls','native_calls','cumulative_time','total_time'],
                'SQLStatement':['id','duration','datetime'],
                'SQLStackItem':['id','sql_statement_id','module','function'],
                'FileAccess':['id','time_to_open','duration_open','data_written','datetime'],
                'MetaData':['id','key','value']}

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


def meta_id(meta_key,meta_value):
    try:
        return db.session.query(db.MetaData).filter(db.MetaData.key==meta_key, db.MetaData.value==meta_value).first().id
    except:
        return None
    
datatables_keys = ['sEcho','iColumns','sColumns','iDisplayStart','iDisplayLength','sSearch','bRegex','iSortingCols',
                   'mDataProp_','sSearch_','bRegex_','iSortCol_','sSortDir_','bSortable_']
     
def is_datatables_key(key):
    for database_key in datatables_keys:
        if database_key in key:
            return True
    return False

def json_aggregate_sql(id=None, search=None, start_date=None, end_date=None,
                       sort=[('avg',True)], start=0, limit=20):
    if id:
        sql_strings = [db.session.query(db.MetaData).get(id)]
        total_num_items = 1
        if sql_strings[0].key != 'sql_string':
            return {}, 0, 0
    else:
        # get all sql strings
        sql_string_query = db.session.query(db.MetaData).filter(db.MetaData.key=='sql_string')
        # get total count
        total_num_items = sql_string_query.count()
        # filter query by table search and get results
        if search:
            sql_string_query = sql_string_query.filter(db.MetaData.value.like('%%%s%%'%search))
        sql_strings = sql_string_query.all()

    results = []
    for sql_string in sql_strings:
        times = []
        datetimes = []
        # get items from sqlstatementmetadata table for each sql query
        sql_statement_links = db.session.query(db.SQLStatementMetadata).filter(db.SQLStatementMetadata.metadata_id==sql_string.id)
        for sql_statement_link in sql_statement_links.all():
            # get the individual calls for that statement
            sql_statements = db.session.query(db.SQLStatement).filter(db.SQLStatement.id==sql_statement_link.main_table_id)
            # filter by date
            if start_date:
                sql_statements = sql_statements.filter(db.SQLStatement.datetime>start_date)
            if end_date:
                sql_statements = sql_statements.filter(db.SQLStatement.datetime<end_date)
            # get data from resultant set
            for sql_statement in sql_statements.all():
                times.append(sql_statement.duration)
                if id:
                    datetimes.append(sql_statement.datetime)
        count = len(times)
        results.append({'id': sql_string.id,
                        'sql': sql_string.value,
                        'count': count,
                        'min': min(times),
                        'max': max(times),
                        'avg': (math.fsum(times)/count)})
        # if id exists, add the times to the only result
        if id:
            results[0]['calls'] = [{'duration':times[i],'datetime':datetimes[i]} for i in range(len(times))]
            results[0]['calls'] = sorted(results[0]['calls'],key=itemgetter('datetime'))
            return results[0], 1, 1

    # Apply sort
    for sort_item in sort:
        results = sorted(results, key=itemgetter(sort_item[0]), reverse=sort_item[1])
    # get filtered count
    filtered_num_items = len(results)

    # apply start and limit
    results = results[start:start+limit]

    return results, total_num_items, filtered_num_items





class JSONAggregateSQL(object):
    exposed = True

    @cherrypy.tools.json_out()
    def GET(self, id=None, datatables=False, start_date=None, end_date=None, **kwargs):
        if datatables == 'true':
            # move datatables keys to another dict
            table_kwargs ={}
            for key in kwargs.keys():
                if is_datatables_key(key):
                    table_kwargs[key] = kwargs.pop(key)

            # parse datatables kwargs
            search = table_kwargs['sSearch']
            sort = []
            cols = ('id','sql','count','min','max','avg')
            for i in range(int(table_kwargs['iSortingCols'])):
                sort_col = cols[int(table_kwargs['iSortCol_' + str(i)])]
                sort_rev = True if table_kwargs['sSortDir_' + str(i)]=='desc' else False
                sort.append((sort_col,sort_rev))
            start = int(table_kwargs['iDisplayStart'])
            limit = int(table_kwargs['iDisplayLength'])

            results, total_num_items, filtered_num_items = json_aggregate_sql(id=None,
                                                                              search=search,
                                                                              start_date=start_date,
                                                                              end_date=end_date,
                                                                              sort=sort,
                                                                              start=start,
                                                                              limit=limit)
            for irow in range(len(results)):
                results[irow] = [results[irow][col_name] for col_name in cols]
            return {'aaData':results,
                    "sEcho": int(table_kwargs['sEcho']),
                    "iTotalRecords": total_num_items,
                    "iTotalDisplayRecords": filtered_num_items}
        else:
            return json_aggregate_sql(id=id)[0]


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
    def GET(self, id=None):
        if id:
            statement, total, filered = json_aggregate_sql(id=id, start_date=time.time()-6000)
            statement['sql']=str(statement['sql']) #unicode throws off template when casting dict as js obj
            mytemplate = mako.template.Template(filename=os.path.join(os.getcwd(),'static','templates','aggregatesql.html'))
            return mytemplate.render(statement=statement)
        else:            
            mytemplate = mako.template.Template(filename=os.path.join(os.getcwd(),'static','templates','aggregatesqls.html'))
            return mytemplate.render()


class Test(object):
    exposed=True
    def GET(self):
        q = db.session.query(db.MetaData.value,
                             db.SQLStatementMetadata,
                             db.SQLStatement.duration,
                             db.SQLStatement.duration,
                             db.SQLStatement.duration,
                             db.SQLStatement.datetime)
        q.filter()
        

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
