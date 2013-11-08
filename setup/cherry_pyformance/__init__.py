import json
import os.path
import sys
import time
import logging
from urllib2 import urlopen, Request
from shutil import copyfile

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

stat_logger = None
cfg = None
push_stats = None

def create_output_fn():
    """
    Creates an output function for dealing with the stats_buffer on flush.
    Uses the configuration to determine the method (write or POST) and
    location to push the data to and constructs a function based on this.
    """
    global push_stats
    try:
        if cfg['output']['type'] == 'disk':
            filename = str(cfg['output']['location'])
            stat_logger.info('Writing collected stats to %s' % filename)
            def push_stats_fn(stats, filename=filename):
                filename = os.path.join(filename, 'tms_stats_'+str(int(time.time()))+'.json')
                """A function to write the json to disk"""
                with open(filename,'w') as json_file:
                    json.dump(stats, json_file, indent=4, separators=(',', ': '))
        elif cfg['output']['type'] == 'server':
            address = str(cfg['output']['location'])
            address = address if address.startswith('http://') else 'http://'+address
            stat_logger.info('Sending collected stats to %s' % address)
            def push_stats_fn(stats, address=address):
                output = json.dumps(stats, indent=4, separators=(',', ': '))
                """A function to push json to server"""
                urlopen(Request(address, output, headers={'Content-Type':'application/json'}))############# TODO MAKE THIS HTTPS
        else:
            # if no valid method found, raise a KeyError to be caught
            stat_logger.warning('Invalid stats output param given, use "disk" or "server"')
            raise KeyError
    except KeyError:
        # could not ascertain output method, do nothing with stats
        stat_logger.info('Could not ascertain output method, defaulting to "pass". Check the profile_stats_config.json is valid.')
        def push_stats_fn(stats):
            pass
    push_stats = push_stats_fn

from handler_profiler import initialise as handler_profiler_init
from function_profiler import initialise as function_profiler_init

def initialise(config_file_path):
    global cfg
    global stat_logger

    stat_logger = logging.getLogger('stats')
    stats_log_handler = logging.Handler(level='INFO')
    log_format = '%(asctime)s::%(levelname)s::[%(module)s:%(lineno)d]::[%(threadName)s] %(message)s'
    stats_log_handler.setFormatter(log_format)
    stat_logger.addHandler(stats_log_handler)
    
    try:
        with open(config_file_path) as cfg_file:
            cfg = json.load(cfg_file)
    except:
        try:
            stat_logger.info('Failed to load stats profiling configuration. Creating from default.')
            default_config_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), "default_config.json")
            copyfile(default_config_file, config_file_path)
            with open(config_file_path) as cfg_file:
                cfg = json.load(cfg_file)
        except:
            stat_logger.error('Failed to create config file from default')
            sys.exit(1)
    
    create_output_fn()
    
    if cfg['handlers']:
        handler_profiler_init(cfg, stat_logger, push_stats)
        
    if cfg['functions']:
        function_profiler_init(cfg, stat_logger, push_stats)
