import database as db
import sqlalchemy
from sqlalchemy import func, and_
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
    # remove _
    # kwargs.pop('_')

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
    filter_kwargs = {}
    for key in ['start_date','end_date','start','limit']:
        if key in kwargs:
            filter_kwargs[key] = int(kwargs.pop(key))

    if 'sort' in kwargs:
        if type(kwargs['sort'])==unicode:
            filter_kwargs['sort']=[(str(kwargs.pop('sort')),'DESC')]
        elif type(kwargs['sort'])==list:
            filter_kwargs['sort']=kwargs.pop('sort')

    return table_kwargs, filter_kwargs

column_name_dict = {db.CallStack: 'full_method',
                    db.SQLStatement: 'sql_string',
                    db.FileAccess: 'filename'}

def datatables(query_func):
    def dt_wrapped(id, table_kwargs, filter_kwargs, table_class):
        if table_kwargs:
            # parse datatables kwargs
            search = table_kwargs['sSearch']
            sort = []
            cols = ('id',column_name_dict[table_class],'count','avg','min','max')
            for i in range(int(table_kwargs['iSortingCols'])):
                sort_col = cols[int(table_kwargs['iSortCol_' + str(i)])]
                sort_dir = 'DESC' if table_kwargs['sSortDir_' + str(i)]=='desc' else 'ASC'
                sort.append((sort_col,sort_dir))
            start = int(table_kwargs['iDisplayStart'])
            limit = int(table_kwargs['iDisplayLength'])
            start_date = filter_kwargs.get('start_date',None)
            end_date = filter_kwargs.get('end_date',None)

            results, total_num_items, filtered_num_items = query_func(table_class,
                                                                      id=id,
                                                                      search=search,
                                                                      start_date=start_date,
                                                                      end_date=end_date,
                                                                      sort=sort,
                                                                      start=start,
                                                                      limit=limit)
            data = [list(result) for result in results] # convert to lists from keyedTuples
            return {'aaData':data,
                    "sEcho": int(table_kwargs['sEcho']),
                    "iTotalRecords": total_num_items,
                    "iTotalDisplayRecords": filtered_num_items}
        else:
            filter_kwargs['id']=id
            result, total_num_items, filtered_num_items = query_func(table_class, **filter_kwargs)
            return list(result), total_num_items, filtered_num_items
    return dt_wrapped

@datatables
def json_aggregate(table_class, id=None, search=None, start_date=None, end_date=None, sort=[('avg','DESC')], start=None, limit=None):
    column_name = column_name_dict[table_class]
    if id:
        total_num_items = 1

        times_query = db.session.query(db.MetaData.id,
                                       table_class.id,
                                       table_class.duration,
                                       table_class.datetime)
        times_query = times_query.filter(db.MetaData.id==id)
        times_query = times_query.join(table_class.metadata_items)
        if start_date:  times_query = times_query.filter(table_class.datetime>start_date)
        if end_date:    times_query = times_query.filter(table_class.datetime<end_date)
        times = times_query.all()
        times = [(time[2],time[3],time[1]) for time in times]
        times = sorted(times, key=itemgetter(1))
    else:
        total_num_items = db.session.query(db.MetaData).filter(db.MetaData.key==column_name).count()
    
    query = db.session.query(db.MetaData.id,
                             db.MetaData.value.label(column_name),
                             func.count(db.MetaData.id).label('count'),
                             func.avg(table_class.duration).label('avg'),
                             func.min(table_class.duration).label('min'),
                             func.max(table_class.duration).label('max'))
    query = query.filter(db.MetaData.key==column_name)
    query = query.join(table_class.metadata_items)
    query = query.group_by(db.MetaData.id)
    if id:          query = query.filter(db.MetaData.id==id)
    if start_date:  query = query.filter(table_class.datetime>start_date)
    if end_date:    query = query.filter(table_class.datetime<end_date)
    if search:      query = query.filter(and_(db.MetaData.key==column_name, db.MetaData.value.like('%%%s%%'%search)))
    for sorter in sort:
        query = query.order_by('%s %s'%sorter)
    filtered_num_items = query.count()
    if start:       query = query.offset(start)
    if limit:       query = query.limit(limit)
    if id:
        try:
            result = list(query.first())
            result.append(times)
            return result,1,1
        except:
            raise cherrypy.HTTPError(404)

    else:
        return query.all(), total_num_items, filtered_num_items


class JSONAggregateCallStacks(object):
    exposed = True
    @cherrypy.tools.json_out()
    def GET(self, id=None, **kwargs):
        table_kwargs, filter_kwargs = parse_kwargs(kwargs)
        return json_aggregate(id, table_kwargs, filter_kwargs, db.CallStack)
    
class JSONAggregateSQL(object):
    exposed = True
    @cherrypy.tools.json_out()
    def GET(self, id=None, **kwargs):
        table_kwargs, filter_kwargs = parse_kwargs(kwargs)
        return json_aggregate(id, table_kwargs, filter_kwargs, db.SQLStatement)
    
class JSONAggregateFileAccesses(object):
    exposed = True
    @cherrypy.tools.json_out()
    def GET(self, id=None, **kwargs):
        table_kwargs, filter_kwargs = parse_kwargs(kwargs)
        return json_aggregate(id, table_kwargs, filter_kwargs, db.FileAccess)

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
                record = [html_escape(str(metadata.__dict__[x])) for x in ['id','key','value']]
                data.append(record)
        return {'aaData':data}