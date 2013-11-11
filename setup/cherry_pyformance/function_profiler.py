"""
A stats profiling tool.
Import into CinemaServices.py after the main imports.

Configure profile_stats_config.json with all the
functions to profile. The profile data is parsed
to json and put on a buffer. This is periodically
flushed out to json on the filesystem or pushed to the 
stats server where the data will be analysed and displayed.
"""
import cProfile
import pstats
import inspect
import copy
import time
import sys
import logging

from cherry_pyformance import cfg, get_stat, stat_logger, function_stats_buffer

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
        global function_stats_buffer
        self._profile = pstats.Stats(self._profile)
        stats = sorted(self._profile.stats.items(), key=lambda x: get_stat(x,self._sort), reverse=True)[:self._num_results]
        # Take the id of the current time as a unique identifier.
        # This is inkeeping with the request ids seen on the stat records seen on the tool.
        _id = id(time.time())
        function_stats_buffer[_id] = {'id': _id,
                            'function': self._name,
                            'class': self._class_name,
                            'module': self._module_name,
                            'datetime': time.time(),
                            'total_time': self._profile.total_tt,
                            'pstats': stats}
        # reset the profiler for new function calls.
        ################### TODO TEST IF MULTIPLE FUNCTION CALLS OVERLAP. 
        ###### Currently only one profiler exists for each function instance
        ###### This could cause problems if the function is called on multiple
        ###### threads. Would need to implement a system similar to the tool
        ###### where the stats record is initialised and a placeholder is put
        ###### on the buffer before the stats are recorded.
        self._profile = cProfile.Profile()

#=====================================================#

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

#=====================================================#

def decorate_functions():
    """
    A function to apply the StatWrapper to other functions in the config.
    
    This function must be called after the apps have been mounted to the
    tree, otherwise there will be no config to alter. However normally
    this module is imported before the engine is setup, so that initialising
    functions can be profiled. Therefore this function is hooked to the
    'start' call on the cherrypy bus with a high priority.
    """

    # decorate all functions supplied in config
    stat_logger.info('Wrapping functions for stats gathering')
    for function in cfg['functions']:
        decorate_function(function)
        
<<<<<<< HEAD
def initialise(config, logger, push_stats_fn):
    global cfg
    global stat_logger
    global push_stats
    cfg = config
    stat_logger = logger
    push_stats = push_stats_fn
    
    # subscribe the function which decorates the handlers
    cherrypy.engine.subscribe('start', decorate_functions, 0)
    
    # create a monitor to periodically flush the stats_buffer at the flush_interval
    Monitor(cherrypy.engine, flush_stats,
        frequency=cfg['flush_interval'],
        name='Flush profile stats buffer').subscribe()
        
    # when the engine stops, flush any stats.
    cherrypy.engine.subscribe('stop', flush_stats)
=======
>>>>>>> upstream/master
