import json
import os.path
import sys
import time
import logging
import copy
import inspect
from urllib2 import urlopen, Request
from shutil import copyfile
import cherrypy
from cherrypy.process.plugins import Monitor

# initialise 3 buffers
function_stats_buffer = {}
handler_stats_buffer = {}
sql_stats_buffer = {}
file_stats_buffer = {}

def get_stat(item, stat):
    """
    Returns the value of an item's stat based on the stat name, not the tuple index
    """
    f = ('file','line','name')
    s = ('native_calls','total_calls','time','cumulative')
    if stat in f:
        return item[0][f.index(stat)]
    elif stat in s:
        return item[1][s.index(stat)]
    else:
        return 0


def create_output_fn():
    """
    Creates an output function for dealing with the stats_buffer on flush.
    Uses the configuration to determine the method (write or POST) and
    location to push the data to and constructs a function based on this.
    """
    try:
        output_type = cfg['output']['type']
        location = str(cfg['output']['location'])
        compress = cfg['output']['compress']
        if output_type == 'disk':
            stat_logger.info('Writing collected stats to %s' % location)
            if compress:
                import gzip
                def push_stats_fn(stats, location=location):
                    """A function to write the compressed json to disk"""
                    filename = os.path.join(location, 'tms_%s_stats_%s.json.gz'%( stats['type'], str(int(time.time())) ) )
                    f = gzip.open(filename,'wb')
                    f.write(json.dumps(stats, indent=4, separators=(',', ': ')))
                    f.close()

            else:
                def push_stats_fn(stats, location=location):
                    """A function to write the json to disk"""
                    filename = os.path.join(location, 'tms_%s_stats_%s.json'%( stats['type'], str(int(time.time())) ) )
                    with open(filename,'w') as json_file:
                        json.dump(stats, json_file, indent=4, separators=(',', ': '))

        elif output_type == 'server':
            if compress:
                import zlib
            address = location if location.startswith('http://') else 'http://'+location
            stat_logger.info('Sending collected stats to %s' % address)

            def push_stats_fn(stats, location=location):
                """A function to push json to server"""
                output = json.dumps(stats, indent=4, separators=(',', ': '))
                headers = {'Content-Type':'application/json'}
                if compress:
                    output = zlib.compress(output)
                    headers = {'Content-Type':'application/gzip'}

                ############# TODO MAKE THIS HTTPS
                urlopen(Request('%s/%s'%(address,stats['type']), output, headers=headers))

        else:
            # if no valid method found, raise a KeyError to be caught
            stat_logger.warning('Invalid stats output param given, use "disk" or "server"')
            raise KeyError
    except KeyError:
        # could not ascertain output method, do nothing with stats
        stat_logger.info('Could not ascertain output method, defaulting to "pass". Check the profile_stats_config.json is valid.')
        def push_stats_fn(stats):
            pass
    return push_stats_fn


def load_config(config_file_path=None):
    if config_file_path is None:
        config_file_path = os.path.join(os.path.dirname(inspect.stack()[-1][1]), "cherrypyformance_config.json")

    try:
        with open(config_file_path) as cfg_file:
            return json.load(cfg_file)
    except:
        #stat_logger.info('Failed to load stats profiling configuration. Creating from default.')
        default_config_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), "default_config.json")
        copyfile(default_config_file, config_file_path)

        with open(config_file_path) as cfg_file:
            return json.load(cfg_file)

def setup_logging():
    '''
    Sets up the stats logger.
    '''
    stat_logger = logging.getLogger('stats')
    stats_log_handler = logging.Handler(level='INFO')
    log_format = '%(asctime)s::%(levelname)s::[%(module)s:%(lineno)d]::[%(threadName)s] %(message)s'
    stats_log_handler.setFormatter(log_format)
    stat_logger.addHandler(stats_log_handler)
    return stat_logger


def initialise(config_file_path=None):
    global cfg
    cfg = load_config(config_file_path)

    global stat_logger
    stat_logger = setup_logging()
    
    global push_stats
    push_stats = create_output_fn()
    
    global stats_package_template
    stats_package_template = {'flush_metadata': cfg['metadata'],
                              'type': 'default_type',
                              'profile': []}

    if cfg['functions']:
        from function_profiler import decorate_functions
        # call this now and later, that way if imports overwrite our wraps
        # then we re-wrap them again at engine start.
        decorate_functions()
        cherrypy.engine.subscribe('start', decorate_functions, 0)

    if cfg['handlers']:
        from handler_profiler import decorate_handlers
        # no point wrapping these now as they won't be active before
        # engine start.
        cherrypy.engine.subscribe('start', decorate_handlers, 0)

    if cfg['database']:
        from sql_profiler import decorate_connections
        # call this now and later, that way if imports overwrite our wraps
        # then we re-wrap them again at engine start.
        decorate_connections()
        cherrypy.engine.subscribe('start', decorate_connections, 0)

    if cfg['files']:
        from file_profiler import decorate_open
        # this is very unlikely to be overwriten, call asap.
        decorate_open()

    from stats_flushers import flush_stats

    # create a monitor to periodically flush the stats buffers at the flush_interval
    Monitor(cherrypy.engine, flush_stats,
        frequency=cfg['flush_interval'],
        name='Flush stats buffers').subscribe()

    # when the engine stops, flush any stats.
    # cherrypy.engine.subscribe('stop', flush_stats)
