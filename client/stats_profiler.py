"""
A stats profiling tool.
Import into CinemaServices.py after the main imports.

Configure profile_stats_config.json with all the functions
and cherrypy handlers to profile. The profile data is
parsed to json and put on a buffer. This is periodically
flushed out to json on the filesystem or pushed to the 
stats server where the data will be analysed and displayed.
"""
import cherrypy
from cherrypy.process.plugins import Monitor
import cProfile
import inspect
import json
import copy
import time
import sys
import os.path
from urllib2 import urlopen, Request

#=====================================================#

# A global stats buffer, all profile data is pushed to
# this and periodically flushed by via a Monitor hooked
# onto the cherrypy bus
stats_buffer = {}

#=====================================================#

class StatsTool(cherrypy.Tool):
    """
    A cherrypy tool which wraps handlers, collecting
    profile stats from them. After the repsonse has
    been sent, the stats are sorted and put on the buffer.
    """

    def __init__(self, sort='time', num_results=10):
        self._name = 'profile'
        self._point = 'before_handler'
        self._priority = 80
        self._setargs()
        self.sort = sort
        self.num_results = num_results

    def _setup(self):
        # Hooks the profile wrapper onto self._point
        # then also hooks self.record_stop after the response has been dealt
        cherrypy.Tool._setup(self)
        cherrypy.serving.request.hooks.attach('on_end_request', self.record_stop)

    def callable(self):
        """
        This is the handler wrapper. It initialises a stat record
        on the stats_buffer based on request metadata, then fires the handler
        while collecting its profile infomation.
        """
        global stats_buffer
        request = cherrypy.serving.request
        # Take the id of the request, this guarantees no cross-contamination
        # of stats as each record is tied to the id of an instance of a request.
        # These are guaranteed to be unique, even if two of the same request are
        # fired at the same time.
        req_id = id(request)
        stats_buffer[req_id] = {'id': req_id,
                                'function': request.path_info,
                                'class': request.app.root.__class__.__name__,
                                'module': inspect.getmodule(request.app.root.__class__).__name__,
                                'datetime': time.time(),
                                'total_time': 0,
                                'profile': cProfile.Profile()}
        # At this point the profile key of the object on the stats buffer has no
        # profile stats in it. It needs to be put in the buffer now as multiple
        # handler calls could be occuring simultaneously during the lifetime of
        # the tool instance.
        handler = cherrypy.serving.request.handler
        def wrapper(*args, **kwargs):
            # profile the handler
            return stats_buffer[req_id]['profile'].runcall(handler, *args, **kwargs)
        cherrypy.serving.request.handler = wrapper

    def record_stop(self):
        """
        This method is called once the response has been sent. Now the stats for
        are this request are called from the buffer, sorted, trimmed and put back
        on the buffer in dict form, removing the profile data. The result should
        be json serialisable.
        """
        global stats_buffer
        req_id = id(cherrypy.serving.request)
        try:
            stats_buffer[req_id]['profile'].snapshot_stats()
            # add total time taken
            stats_buffer[req_id]['total_time'] = stats_buffer[req_id]['profile'].totall_tt
            # sort the stats in decending order of the sorting stat, then trim
            # to num_results, there will be a lot of negligable stats we can ignore
            stats = sorted(stats_buffer[req_id]['profile'].stats.items(),
                           key=lambda x: get_stat(x,self.sort),
                           reverse=True )[:self.num_results]
            stats_buffer[req_id]['pstats'] = copy.deepcopy(stats)
            # remove the profile data
            del stats_buffer[req_id]['profile']
        except KeyError:
            # If TMS UI is open when TMS is starting started it tries to recall
            # a non-exsistent item from the buffer. 
            stat_logger.warning('The request id %d is not in the stats_buffer.' % req_id)


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

#=====================================================#

class StatWrapper(object):
    """
    A wrapper class which takes profile data of the wrapped function when called.
    
    If an inner_func is supplied (implying the function is actually a decorated
    version of inner_func), the inner_func's details (module name etc.) are used
    on the stat record, but the decorated version is used for profiling.

    Currently the _after method (which adds stats to the buffer) is called before
    the functions output is returned. TODO: Put this in another thread so the result
    is seamlessly returned without waiting on the push to the buffer.
    """

    def __init__(self, function, inner_func=None, sort='time', num_results=10):
        self._function = function
        self._profile = cProfile.Profile()
        self._sort = sort
        self._num_results = num_results

        # If there is an inner_func, get some metadata from there
        # otherwise, use function's metadata
        self._inner_func = inner_func if inner_func else function
        self._name = self._inner_func.__name__
        self._module_name = inspect.getmodule(self._inner_func).__name__
        self._class_name = self._inner_func.__class__.__name__

    def __call__(self, *args, **kwargs):
        output = self._profile.runcall(self._function, *args, **kwargs)
        self._after()
        return output

    def _after(self):
        """
        Pushes the stats collected to the buffer.
        """
        global stats_buffer
        self._profile.snapshot_stats()
        stats = sorted(self._profile.stats.items(), key=lambda x: get_stat(x,self._sort), reverse=True)[:self._num_results]
        # Take the id of the current time as a unique identifier.
        # This is inkeeping with the request ids seen on the stat records seen on the tool.
        _id = id(time.time())
        stats_buffer[_id] = {'id': _id,
                            'function': self._name,
                            'class': self._class_name,
                            'module': self._module_name,
                            'datetime': time.time(),
                            'total_time': self._profile.totall_tt,
                            'pstats': stats}
        # reset the profiler for new function calls.
        ################### TODO TEST IF MULTIPLE FUNCTION CALLS OVERLAP. 
        ###### Currently only one profiler exists for each function instance
        ###### This could cause problems if the function is called on multiple
        ###### threads. Would need to implement a system similar to the tool
        ###### where the stats record is initialised and a placeholder is put
        ###### on the buffer before the stats are recorded.
        self._profile = cProfile.Profile()

def decorate_function(path):
    """
    Takes the string of a module, i.e. "serv.core.some_module.some_function"
    and acquires the actual function (or method) object. It does this by splitting
    the string, importing the root module, recursively uses getattr to acquire
    the submodules. Finally it replaces the object with an instance of StatWrapper
    which wraps that function.

    If the function in question is wrapped with a decorator, the decorator function's
    closure items are scanned for the raw function. This is also used in generating
    the StatWrapper object so that the original, unwrapped functions name and module
    name can be used on the stat record.

    How the function in the argument is imported will affect when this function must be
    called (or rather where this module is imported). If the function is imported like so:
    "import a.b.c"
    then decorate_function can be called at anytime before a.b.c is called, however if it
    is imported like so:
    "from a.b import c"
    then decorate_function must be called after the import, as the import will overwrite
    our instance. Lastly when doing:
    "import a.b.c as d"
    calling decorate_function on d will not work, only a.b.c will work.
    """
    # split on the '.'
    path_string = path
    path = path.split('.')
    module = path[0]
    attribute = path[1]

    try:
        # import the root
        __import__(module)
        module = sys.modules[module]

        parent = module
        original = getattr(parent, attribute)
        # loop through the submodules to get their instances
        for attribute in path[2:]:
            parent = original
            original = getattr(original, attribute)

        # at this point we need to test if the function is wrapped
        outer_func = inner_func = original

        # recursively look through the func_closure items and look for callables.
        while inner_func.func_closure is not None:
            for item in inner_func.func_closure:
                if hasattr(item.cell_contents,'__call__'):
                    inner_func = item.cell_contents
                    # break the for loop if one is found. Not a perfect solution, but will work 99% of cases
                    # seldom will decorators have multiple callables in the closure items - I hope
                    break

        # if nothing changes, don't bother passing both functions to the StatWrapper __init__
        if outer_func == inner_func:
            inner_func = None

        # replace the function instance with a wrapped one.
        setattr(parent, attribute, StatWrapper(outer_func, inner_func=inner_func, sort='time', num_results=10))
    except Exception as e:
        stat_logger.warning('Failed to wrap function %s for stats profiling. Check configuration and importation method. The function will not be profiled.' % path_string)
        print e

#=====================================================#

def flush_stats():
    """
    If there are items on the stats_buffer, their stats tuples are
    parsed to a dictionary and the records are pushed to whichever output
    is configured in the config file. (Currently json dump or push to server)
    """
    stat_logger.info('Flushing stats buffer.')
    global stats_buffer
    # initialise a package of stats to push, not all stats may be ready to be pushed
    stats_to_push = []
    for _id in stats_buffer.keys():
        # test if there is a pstats key
        if 'pstats' in stats_buffer[_id]:
            pstats = stats_buffer[_id]['pstats']
            parsed_stats = []
            # convert all stats tuples to dictionaries
            for stat in pstats:
                parsed_stats.append({'function':{'module':stat[0][0],
                                                 'line':stat[0][1],
                                                 'name':stat[0][2]},
                                     'native_calls':stat[1][0],
                                     'total_calls':stat[1][1],
                                     'time':stat[1][2],
                                     'cumulative':stat[1][3] })
            stats_buffer[_id]['pstats'] = parsed_stats
            # put a deep copy on the stats_to_push list
            stats_to_push.append(copy.deepcopy(stats_buffer[_id]))
            # remove parsed stats, keeping transient stats
            del stats_buffer[_id]
    length = len(stats_to_push)
    if length != 0:
        stats_package = copy.deepcopy(stats_package_template)
        stats_package[stats] = stats_to_push
        push_stats(stats_package)
        stat_logger.info('Flushed %d stats from the buffer' % length)
    else:
        stat_logger.info('No stats on the buffer to flush.')
 
#=====================================================#

def create_output_fn():
    """
    Creates an output function for dealing with the stats_buffer on flush.
    Uses the configuration to determine the method (write or POST) and
    location to push the data to and constructs a function based on this.
    """
    try:
        if cfg['output']['type'] == 'disk':
            filename = str(cfg['output']['location'])
            stat_logger.info('Writing collected stats to %s' % filename)
            def push_stats(stats, filename=filename):
                filename = os.path.join(filename, 'tms_stats_'+str(int(time.time()))+'.json')
                """A function to write the json to disk"""
                with open(filename,'w') as file:
                    json.dump(stats, file, indent=4, separators=(',', ': '))
            return push_stats
        elif cfg['output']['type'] == 'server':
            address = str(cfg['output']['location'])
            address = address if address.startswith('http://') else 'http://'+address
            stat_logger.info('Sending collected stats to %s' % address)
            def push_stats(stats, address=address):
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
        def push_stats(stats):
            pass
    return push_stats



import logging
stat_logger = logging.getLogger('stats')
stats_log_handler = logging.Handler(level='INFO')
log_format = '%(asctime)s::%(levelname)s::[%(module)s:%(lineno)d]::[%(threadName)s] %(message)s'
stats_log_handler.setFormatter(log_format)
stat_logger.addHandler(stats_log_handler)

# read config
cfg = None
try:
    with open('client\\stats_profiler_config.json') as cfg_file:
        cfg = json.load(cfg_file)
except Exception:
    stat_logger.error('Failed to load stats profiling configuration. Check config exists.')
    sys.exit(1)

# initialise what to do with stats on flush
push_stats = create_output_fn()

# put tool in toolbox
cherrypy.tools.stats = StatsTool( sort=cfg['sort_on'], num_results=cfg['num_results'] )

stats_package_template = {'exhibitor_chain': cfg['exhibitor_chain'],
                          'exhibitor_branch': cfg['exhibitor_branch'],
                          'product': cfg['product'],
                          'version': cfg['version'],
                          'stats': []}
}


def decorate_functions_and_handlers():
    """
    A function to apply the StatsTool to handlers given in the config and
    StatWrapper to other functions in the config.
    
    This function must be called after the apps have been mounted to the
    tree, otherwise there will be no config to alter. However normally
    this module is imported before the engine is setup, so that initialising
    functions can be profiled. Therefore this function is hooked to the
    'start' call on the cherrypy bus with a high priority.
    """
    # decorate all handlers supplied in config
    stat_logger.info('Wrapping cherrypy handers for stats gathering.')
    try:
        for root in cfg['handlers'].keys():
            for handler in cfg['handlers'][root]:
                cherrypy.tree.apps[str(root)].merge({str(handler):{'tools.stats.on':True}})
    except KeyError:
        stat_logger.warning('Stats configuation incorrect. Could not obtain handlers to wrap.')
    except Exception as e:
        stat_logger.warning('Failed to wrap cherrypy handler for stats profiling.')
        print e

    # deorate all functions supplied in config
    stat_logger.info('Wrapping functions for stats gathering')
    for function in cfg['functions']:
        decorate_function(function)

# subscibe the function which decorates the handlers
cherrypy.engine.subscribe('start', decorate_functions_and_handlers, 0)
# create a monitor to periodically flush the stats_buffer at the flush_interval
Monitor(cherrypy.engine, flush_stats,
    frequency=cfg['flush_interval'],
    name='Flush profile stats buffer').subscribe()
# when the engine stops, flush any stats.
cherrypy.engine.subscribe('stop', flush_stats)
