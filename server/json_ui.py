import sqlalchemy
import database as db
import cherrypy
from sqlalchemy import or_, and_
from cgi import escape as html_escape

column_order = {'CallStack':['id','total_time','datetime'],
                'CallStackItem':['id','call_stack_id','function_name','line_number','module','total_calls','native_calls','cumulative_time','total_time'],
                'SQLStatement':['id','duration','datetime'],
                'SQLStackItem':['id','sql_statement_id','module','function'],
                'FileAccess':['id','time_to_open','duration_open','data_written','datetime']}
                
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