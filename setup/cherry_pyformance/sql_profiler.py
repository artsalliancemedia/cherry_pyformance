import time

from cherry_pyformance import cfg, stat_logger, sql_stats_buffer

###============================================================###


#####
##### WORK IN PROGRESS
#####

# class Psycopg2CursorWrapper(object):

#     def __init__(self, cursor, connect_params=None, cursor_params=None):
#         object.__setattr__(self, '_cursor', cursor)
#         object.__setattr__(self, '_connect_params', connect_params)
#         object.__setattr__(self, '_cursor_params', cursor_params)
#         object.__setattr__(self, 'fetchone', cursor.fetchone)
#         object.__setattr__(self, 'fetchmany', cursor.fetchmany)
#         object.__setattr__(self, 'fetchall', cursor.fetchall)

#     def __setattr__(self, name, value):
#         setattr(self._cursor, name, value)

#     def __getattr__(self, name):
#         return getattr(self._cursor, name)

#     def __iter__(self):
#         return iter(self._cursor)

#     def execute(self, sql, *args, **kwargs):
#         start_time = time.time()
#         output = self._cursor.execute(sql, *args, **kwargs)
#         end_time = time.time()
#         global sql_stats_buffer
#         sql_stats_buffer.append({'sql':sql,
#                              'datetime':start_time,
#                              'duration':end_time-start_time
#                             })
#         return output

#     def executemany(self, sql, *args, **kwargs):
#         start_time = time.time()
#         output = self._cursor.executemany(sql, *args, **kwargs)
#         end_time = time.time()
#         global sql_stats_buffer
#         sql_stats_buffer.append({'sql':sql,
#                              'datetime':start_time,
#                              'duration':end_time-start_time
#                             })
#         return output

#     def callproc(self, procname, *args, **kwargs):
#         return self._cursor.callproc(procname, *args, **kwargs)

# class Psycopg2ConnectionWrapper(object):

#     def __init__(self, connection, connect_params=None):
#         self._connection = connection
#         self._connect_params = connect_params

#     def cursor(self, *args, **kwargs):
#         return Psycopg2CursorWrapper(self._connection.cursor(*args, **kwargs), self._connect_params, (args, kwargs))

#     def commit(self):
#         return self._connection.commit()

#     def rollback(self):
#         return self._connection.rollback()

# class Psycopg2ConnectionFactory(object):
#     def __init__(self, connect):
#         self.__connect = connect

#     def __call__(self, *args, **kwargs):
#         return Psycopg2ConnectionWrapper(self.__connect(*args, **kwargs), (args, kwargs))

###============================================================###

class SqliteCursorWrapper(object):

    def __init__(self, cursor):
        object.__setattr__(self, '_cursor', cursor)
        object.__setattr__(self, 'fetchone', cursor.fetchone)
        object.__setattr__(self, 'fetchmany', cursor.fetchmany)
        object.__setattr__(self, 'fetchall', cursor.fetchall)

    def __setattr__(self, name, value):
        setattr(self._cursor, name, value)

    def __getattr__(self, name):
        return getattr(self._cursor, name)

    def __iter__(self):
        return iter(self._cursor)

    def execute(self, sql, *args, **kwargs):
        return profile_sql(self._cursor.execute, sql, *args, **kwargs)

    def executemany(self, sql, *args, **kwargs):
        return profile_sql(self._cursor.executemany, sql, *args, **kwargs)

    def executescript(self, script, *args, **kwargs):
        return self._cursor.executescript(script, *args, **kwargs)

class SqliteConnectionWrapper(object):

    def __init__(self, connection):
        self._connection = connection

    def __enter__(self):
        self._connection.__enter__()
        return self

    def __exit__(self, *args, **kwargs):
        return self._connection.__exit__(*args, **kwargs)

    def cursor(self, *args, **kwargs):
        return SqliteCursorWrapper(self._connection.cursor(*args, **kwargs))

    def commit(self):
        return self._connection.commit()

    def rollback(self):
        return self._connection.rollback()

    def execute(self, sql, *args, **kwargs):
        return profile_sql(self._connection.execute, sql, *args, **kwargs)
        

    def executemany(self, sql, *args, **kwargs):
        return profile_sql(self._connection.executemany, sql, *args, **kwargs)
        

    def executescript(self, script, *args, **kwargs):
        return self._cursor.executescript(script, *args, **kwargs)

class SqliteConnectionFactory(object):

    def __init__(self, connect):
        self.__connect = connect

    def __call__(self, *args, **kwargs):
        return SqliteConnectionWrapper(self.__connect(*args, **kwargs))

###============================================================###

def profile_sql(action, sql, *args, **kwargs):
    start_time = time.time()
    output = action(sql, *args, **kwargs)
    end_time = time.time()
    time_diff = end_time-start_time
    if time_diff > 0:
        _id = id(sql+str(end_time))
        global sql_stats_buffer
        sql_stats_buffer[_id] = {'sql':sql.replace('\n','\\n'),
                              'datetime':start_time,
                              'duration':time_diff
                             }
    return output

def decorate_connections():
    if cfg['database'] == u'sqlite':
        import sqlite3
        setattr(sqlite3.dbapi2,'connect', SqliteConnectionFactory(sqlite3.dbapi2.connect))
    # if cfg['database'] == u'postgres':
    #     import psycopg2
    #     setattr(psycopg2,'connect', Psycopg2ConnectionFactory(psycopg2.connect))

