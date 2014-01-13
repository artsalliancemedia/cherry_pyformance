from cherry_pyformance import stat_logger, push_stats, stats_package_template, cfg
from handler_profiler import handler_stats_buffer
from function_profiler import function_stats_buffer
from sql_profiler import sql_stats_buffer
from file_profiler import file_stats_buffer


def _flush_stats(stats_buffer, stat_type):
    stat_logger.info('Flushing {0} stats buffer.'.format(stat_type))
    # initialise a package of stats to push, not all stats may be ready to be pushed
    stats_to_push = []
    for _id in stats_buffer.keys():
        # sometimes stat has already gone by this point.
        try:
            if stat_type in ('function','handler'):
                # only push pickled items from the buffer
                if type(stats_buffer[_id]['profile'])==str:
                    stats_to_push.append(stats_buffer[_id])
                    del stats_buffer[_id] 
            elif stat_type == 'database':
                # convert all args to strings, allows for easier filtering when single instancing
                # having a mix of numbers and strings is not ideal
                # if you have a better solution please let me know!
                if isinstance(stats_buffer[_id]['args'], dict): # for sql args (they have keys used for sql string insertion)
                    for key in stats_buffer[_id]['args']:
                        stats_buffer[_id]['args'][key] = str(stats_buffer[_id]['args'][key])
                else:
                    stats_buffer[_id]['args'] = [str(arg) for arg in stats_buffer[_id]['args']]
                stats_to_push.append(stats_buffer[_id])
                del stats_buffer[_id]
            else:
                stats_to_push.append(stats_buffer[_id])
                del stats_buffer[_id]
        except KeyError:
            # if does not exist, assume a flusher on another thread has taken care of it
            pass
    length = len(stats_to_push)
    if length != 0:
        stats_package = stats_package_template.copy()
        stats_package['stats'] = stats_to_push
        stats_package['type'] = stat_type
        push_stats(stats_package)
        stat_logger.info('Flushed {0} stats from the {1} buffer'.format(length,stat_type))
    else:
        stat_logger.info('No stats on the {0} buffer to flush.'.format(stat_type))


def flush_stats():
    if cfg['handlers']:
        _flush_stats(handler_stats_buffer, 'handler')
    if cfg['functions']:
        _flush_stats(function_stats_buffer, 'function')
    if cfg['sql']['database']:
        _flush_stats(sql_stats_buffer, 'database')
    if cfg['files']['files_enabled']:
        _flush_stats(file_stats_buffer, 'file')
