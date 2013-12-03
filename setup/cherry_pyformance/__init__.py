import json
import ConfigParser
import os.path
import sys
import time
import logging
import copy
import inspect
from urllib2 import urlopen, Request, URLError
from shutil import copyfile
import cherrypy
from cherrypy.process.plugins import Monitor


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
    location = str(cfg['output']['location'])
    # Need to change to getBool
    address = location if location.startswith('http://') else 'http://'+location
    compress == True if cfg['output']['compress']=='true' else False
    if compress:
        import zlib
    stat_logger.info('Sending collected stats to {0}{1}'.format(address,' (compressed)'*compress))

    def push_stats_fn(stats, address=address):
        """A function to push json to server"""
        output = json.dumps(stats)
        headers = {'Content-Type':'application/json'}
        if compress:
            output = zlib.compress(output)
            headers = {'Content-Type':'application/gzip'}
        ############# TODO MAKE THIS HTTPS
        stat_logger.info('Sending collected stats to %s' % address)
        try:
            urlopen(Request('%s/%s'%(address, stats['type']), output, headers=headers))
        except URLError as e:
            print e
    return push_stats_fn


def load_config(config_file_path=None):
    if config_file_path is None:
        # Get the directory where the executing script lives in and copy the default config in there if nothing is supplied.
        config_file_path = os.path.join(os.path.dirname(inspect.stack()[-1][1]), "cherrypyformance_config.cfg")
    config = ConfigParser.ConfigParser()
    
    config.read(config_file_path)
    if config.sections() == []:
        #stat_logger.info('Failed to load stats profiling configuration. Creating from default.')
        default_config_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), "default_config.cfg")
        copyfile(default_config_file, config_file_path)
        config.read(config_file_path)
    
    config_dict = config._sections
    for section in config_dict.values():
        section.pop('__name__')
    return config_dict

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
    stats_package_template = {'metadata': cfg['metadata'],
                              'type': 'default_type',
                              'stats': []}

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

    if cfg['sql']['sql_enabled']:
        from sql_profiler import decorate_connections
        # call this now and later, that way if imports overwrite our wraps
        # then we re-wrap them again at engine start.
        decorate_connections()
        cherrypy.engine.subscribe('start', decorate_connections, 0)

    if cfg['files']['files_enabled']:
        from file_profiler import decorate_open
        # this is very unlikely to be overwritten, call asap.
        decorate_open()

    from stats_flushers import flush_stats

    # create a monitor to periodically flush the stats buffers at the flush_interval
    Monitor(cherrypy.engine, flush_stats,
        frequency=int(cfg['output']['flush_interval']),
        name='Flush stats buffers').subscribe()

    # when the engine stops, flush any stats.
    # cherrypy.engine.subscribe('stop', flush_stats)
