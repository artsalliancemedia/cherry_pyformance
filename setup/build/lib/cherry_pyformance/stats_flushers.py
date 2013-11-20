import copy
from cherry_pyformance import stat_logger, push_stats, stats_package_template, cfg
from cherry_pyformance import handler_stats_buffer, function_stats_buffer, sql_stats_buffer, file_stats_buffer


def fn_and_hdlr_pre_push(stats_buffer, _id):
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
    else:
        # raise a keyerror, this allows the stat to be igored for now
        raise KeyError


def _flush_stats(stats_buffer, stat_type, pre_push_fn=lambda x,y:None):
    stat_logger.info('Flushing %s stats buffer.'%stat_type)
    # initialise a package of stats to push, not all stats may be ready to be pushed
    stats_to_push = []
    for _id in stats_buffer.keys():
        # sometimes stat has already gone by this point.
        try:
            pre_push_fn(stats_buffer, _id)
            stats_to_push.append(copy.deepcopy(stats_buffer[_id]))
            del stats_buffer[_id]
        except KeyError:
            # if does not exist, assume another flusher on another thread has taken care of it
            # OR if keyerror is passed from pre_push_fn then we shall assume the item pstats are
            # not ready and will not proceed with that item
            pass
    length = len(stats_to_push)
    if length != 0:
        stats_package = copy.deepcopy(stats_package_template)
        stats_package['profile'] = stats_to_push
        stats_package['type'] = stat_type
        push_stats(stats_package)
        stat_logger.info('Flushed %d stats from the %s buffer' % (length,stat_type))
    else:
        stat_logger.info('No stats on the %s buffer to flush.'%stat_type)


def flush_stats():
    if cfg['handlers']:
        _flush_stats(handler_stats_buffer, 'handler', pre_push_fn=fn_and_hdlr_pre_push)
    if cfg['functions']:
        _flush_stats(function_stats_buffer, 'function', pre_push_fn=fn_and_hdlr_pre_push)
    if cfg['database']:
        _flush_stats(sql_stats_buffer, 'database')
    if cfg['files']:
        _flush_stats(file_stats_buffer, 'file')
