import copy
from cherry_pyformance import stat_logger, push_stats, stats_package_template, cfg
from cherry_pyformance import handler_stats_buffer, function_stats_buffer, sql_stats_buffer

def flush_function_handler_stats(stats_buffer, stat_type):
    """
    If there are items on the stats_buffer, their stats tuples are
    parsed to a dictionary and the records are pushed to whichever output
    is configured in the config file. (Currently json dump or push to server)
    """
    stat_logger.info('Flushing %s stats buffer.'%stat_type)
    # initialise a package of stats to push, not all stats may be ready to be pushed
    stats_to_push = []
    for _id in stats_buffer.keys():
        # test if there is a pstats key
        if 'pstats' in stats_buffer[_id]['stats_buffer']:
            pstats_buffer = stats_buffer[_id]['stats_buffer']['pstats']
            parsed_stats = []
            # convert all stats tuples to dictionaries
            for stat in pstats_buffer:
                parsed_stats.append({'function':{'module':stat[0][0],
                                                 'line':stat[0][1],
                                                 'name':stat[0][2]},
                                     'native_calls':stat[1][0],
                                     'total_calls':stat[1][1],
                                     'time':stat[1][2],
                                     'cumulative':stat[1][3] })
            stats_buffer[_id]['stats_buffer']['pstats'] = parsed_stats
            # put a deep copy on the stats_to_push list
            stats_to_push.append(copy.deepcopy(stats_buffer[_id]))
            # remove parsed stats, keeping transient stats
            del stats_buffer[_id]
    length = len(stats_to_push)
    if length != 0:
        stats_package = copy.deepcopy(stats_package_template)
        stats_package['profile'] = stats_to_push
        stats_package['type'] = stat_type
        push_stats(stats_package)
        stat_logger.info('Flushed %d stats from the %s buffer' % (length,stat_type))
    else:
        stat_logger.info('No stats on the function buffer to flush.')

def flush_sql_stats(stats_buffer, stat_type):
    """
    If there are items on the sql_stats_buffer, they are pushed to whichever output
    is configured in the config file. (Currently json dump or push to server)
    """
    stat_logger.info('Flushing SQL stats buffer.')
    # initialise a package of stats to push, not all stats may be ready to be pushed
    stats_to_push = []
    for _id in stats_buffer.keys():
        stats_to_push.append(copy.deepcopy(stats_buffer[_id]))
        del stats_buffer[_id]
    length = len(stats_to_push)
    if length != 0:
        stats_package = copy.deepcopy(stats_package_template)
        stats_package['profile'] = stats_to_push
        stats_package['type'] = stat_type
        push_stats(stats_package)
        stat_logger.info('Flushed %d stats from the SQL buffer' % length)
    else:
        stat_logger.info('No stats on the SQL buffer to flush.')

def flush_stats():
    if cfg['handlers']:
        flush_function_handler_stats(handler_stats_buffer, stat_type='handler')
    if cfg['functions']:
        flush_function_handler_stats(function_stats_buffer, stat_type='function')
    if cfg['database']:
        flush_sql_stats(sql_stats_buffer, stat_type='database')
