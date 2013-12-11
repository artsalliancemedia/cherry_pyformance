import database as db
import sqlalchemy
from sqlalchemy import func, and_, or_
import cherrypy
from cgi import escape as html_escape
from operator import itemgetter

datatables_keys = ['sEcho','iColumns','sColumns','iDisplayStart','iDisplayLength','sSearch','bRegex','iSortingCols',
                   'mDataProp_','sSearch_','bRegex_','iSortCol_','sSortDir_','bSortable_']

def is_datatables_key(key):
    for database_key in datatables_keys:
        if database_key in key:
            return True
    return False

def parse_kwargs(kwargs):
    # move datatables keys to another dict
    table_kwargs = {}
    for key in kwargs.keys():
        if is_datatables_key(key):
            table_kwargs[key] = kwargs.pop(key)

    # remove any empty kwargs MUST BE AFTER TABLE KWARGS
    for k,v in kwargs.items():
        if v == '':
            del(kwargs[k])

    # move filters to another dict
    filter_kwargs = kwargs
    for key in ['start_date','end_date','start','limit']:
        if key in filter_kwargs:
            filter_kwargs[key] = int(filter_kwargs[key])

    if 'sort' in filter_kwargs:
        if type(filter_kwargs['sort'])==unicode:
            filter_kwargs['sort'] = [(str(filter_kwargs['sort']),'DESC')]
        elif type(filter_kwargs['sort'])==list:
            filter_kwargs['sort'] = filter_kwargs['sort']

    return table_kwargs, filter_kwargs

column_name_dict = {db.CallStack: 'full_name',
                    db.SQLStatement: 'sql_string',
                    db.FileAccess: 'filename'}

def datatables(query_func):
    def dt_wrapped(id, table_kwargs, filter_kwargs, table_class):
        if table_kwargs:
            # parse datatables kwargs
            search = table_kwargs['sSearch']
            sort = []
            cols = ('id',column_name_dict[table_class],'count','total','avg','min','max')
            for i in range(int(table_kwargs['iSortingCols'])):
                sort_col = cols[int(table_kwargs['iSortCol_' + str(i)])]
                sort_dir = 'DESC' if table_kwargs['sSortDir_' + str(i)]=='desc' else 'ASC'
                sort.append((sort_col,sort_dir))
            start = int(table_kwargs['iDisplayStart'])
            limit = int(table_kwargs['iDisplayLength'])
            start_date = filter_kwargs.get('start_date',None)
            end_date = filter_kwargs.get('end_date',None)

            data, total_num_items, filtered_num_items = query_func(table_class,
                                                                      id=id,
                                                                      filter_kwargs=filter_kwargs,
                                                                      search=search,
                                                                      start_date=start_date,
                                                                      end_date=end_date,
                                                                      sort=sort,
                                                                      start=start,
                                                                      limit=limit)
            return {'aaData':data,
                    "sEcho": int(table_kwargs['sEcho']),
                    "iTotalRecords": total_num_items,
                    "iTotalDisplayRecords": filtered_num_items}
        else:
            sort = filter_kwargs.get('sort', [('avg','DESC')])
            limit = filter_kwargs.get('limit',None)
            start_date = filter_kwargs.get('start_date',None)
            end_date = filter_kwargs.get('end_date',None)
            return query_func(table_class, id=id, filter_kwargs=filter_kwargs, sort=sort, limit=limit, start_date=start_date, end_date=end_date)
    return dt_wrapped

call_stack_metadata_dict = {'module': db.CallStackName.module_name,
                            'class':  db.CallStackName.class_name,
                            'method': db.CallStackName.fn_name}

def filter_query(query, filter_kwargs, table_class):
    for k in filter_kwargs:
        if 'key_' in k:
            v = k.replace('key','value')
            if filter_kwargs[k] in call_stack_metadata_dict:
                if filter_kwargs[k] == '':
                    filter_kwargs[k] = None
                call_stack_attr = call_stack_metadata_dict[filter_kwargs[k]]
                query = query.filter(call_stack_attr==filter_kwargs[v])
            else:
                query = query.filter(table_class.metadata_items.any(and_(db.MetaData.key==filter_kwargs[k], db.MetaData.value==filter_kwargs[v])))
    return query

metadata_table_dict = {db.CallStack: [db.CallStackName,
                                      func.concat(db.CallStackName.module_name,
                                                  db.CallStackName.class_name,
                                                  db.CallStackName.fn_name),
                                      db.CallStack.name],
                       db.SQLStatement: [db.SQLString,
                                         db.SQLString.sql,
                                         db.SQLStatement.sql_string],
                       db.FileAccess: [db.FileName,
                                       db.FileName.filename,
                                       db.FileAccess.filename]}

searchable_columns_dict = {db.CallStack: [db.CallStackName.module_name, db.CallStackName.class_name, db.CallStackName.fn_name],
                           db.SQLStatement: [db.SQLString.sql],
                           db.FileAccess: [db.FileName.filename]}

@datatables
def json_aggregate(table_class, id=None, filter_kwargs=None, search=None, start_date=None, end_date=None, sort=[('avg','DESC')], start=None, limit=None):
    column_name = column_name_dict[table_class]
    metadata_table = metadata_table_dict[table_class][0]
    metadata_value = metadata_table_dict[table_class][1]
    table_class_column = metadata_table_dict[table_class][2]
    if id: # Grabbing information for a specific method, sql statement or filename
        total_num_items = 1

        times_query = db.session.query(metadata_table.id,
                                       table_class.id,
                                       table_class.duration,
                                       table_class.datetime)
        times_query = times_query.join(table_class_column)
        
        times_query = filter_query(times_query, filter_kwargs, table_class)
        
        times_query = times_query.filter(metadata_table.id==id)
        if start_date:  times_query = times_query.filter(table_class.datetime>start_date)
        if end_date:    times_query = times_query.filter(table_class.datetime<end_date)
        times = times_query.all()
        times = [(time[2],time[3],time[1]) for time in times]
        times = sorted(times, key=itemgetter(1))
    else: # Grab all known information
        total_num_items = db.session.query(metadata_table).count()
    
    query = db.session.query(metadata_table.id,
                             metadata_value.label(column_name),
                             func.count(table_class.id).label('count'),
                             func.sum(table_class.duration).label('total'),
                             func.avg(table_class.duration).label('avg'),
                             func.min(table_class.duration).label('min'),
                             func.max(table_class.duration).label('max'))
    # Only get information for current tab (e.g. Call Stacks)
    query = query.join(table_class_column)
    
    # Filter data based on the key/value pairs picked in the side bar
    query = filter_query(query, filter_kwargs, table_class)
    
    query = query.group_by(metadata_table.id)
    if id:          query = query.filter(metadata_table.id==id)
    if start_date:  query = query.filter(table_class.datetime>start_date)
    if end_date:    query = query.filter(table_class.datetime<end_date)
    if search:
        search_clauses = []
        for column in searchable_columns_dict[table_class]:
            search_clauses.append(column.ilike(('%%%s%%'%search)))
        query = query.filter(or_(*search_clauses))
    for sorter in sort:
        query = query.order_by('%s %s'%sorter)
    filtered_num_items = query.count()
    if start:       query = query.offset(start)
    if limit:       query = query.limit(limit)
    if id:
        try:
            result = list(query.first())
            # Convert call stack name object to string
            result[1] = str(result[1])
            result.append(times)
            return result,1,1
        except:
            return [],0,0
    else:
        # Convert to lists from keyedTuples
        results = [list(result) for result in query.all()]
        # Convert call stack name objects to strings
        for result in results:
            result[1] = str(result[1])
        return results, total_num_items, filtered_num_items

class AggregateAPI(object):
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def callstacks(self, id=None, **kwargs):
        table_kwargs, filter_kwargs = parse_kwargs(kwargs)
        return json_aggregate(id, table_kwargs, filter_kwargs, db.CallStack)

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def sqlstatements(self, id=None, **kwargs):
        table_kwargs, filter_kwargs = parse_kwargs(kwargs)
        return json_aggregate(id, table_kwargs, filter_kwargs, db.SQLStatement)
    
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def fileaccesses(self, id=None, **kwargs):
        table_kwargs, filter_kwargs = parse_kwargs(kwargs)
        return json_aggregate(id, table_kwargs, filter_kwargs, db.FileAccess)



