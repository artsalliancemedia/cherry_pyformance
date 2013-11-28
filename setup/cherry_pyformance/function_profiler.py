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
import inspect
import time
import sys
from threading import Thread

from cherry_pyformance import cfg, get_stat, stat_logger, function_stats_buffer

#=====================================================#

class StatWrapper(object):
    """
    A wrapper class which takes profile data of the wrapped function when called.
    
    If an inner_func is supplied (implying the function is actually a decorated
    version of inner_func), the inner_func's details (module name etc.) are used
    on the stat record, but the decorated version is used for profiling.
    """
    def __init__(self, function, inner_func=None):
        self._function = function
        # If there is an inner_func, get some metadata from there
        # otherwise, use function's metadata
        self._inner_func = inner_func if inner_func else function

        self._name = self._inner_func.__name__
        self._module_name = inspect.getmodule(self._inner_func).__name__
        self._class_name = self._inner_func.__class__.__name__

    def __call__(self, *args, **kwargs):
        _id = id(time.time())
        # initialise the item on the buffer
        function_stats_buffer[_id] = {'datetime': float(time.time()),
                                      'profile': cProfile.Profile()}
        output = function_stats_buffer[_id]['profile'].runcall(self._function, *args, **kwargs)
        Thread(target=self._after(), args=(_id,)).start()
        return output

    def _after(self, _id):
        """
        Pushes the stats collected to the buffer.
        """
        if _id in function_stats_buffer:
            function_stats_buffer[_id]['metadata'] = {'function': _name,
                                                      'class': _class_name,
                                                      'module': _module_name}
            stats = function_stats_buffer[req_id]['profile']
            stats.create_stats()
            # pickle stats and put back on the buffer for flushing
            pickled_stats = cPickle.dumps(stats.stats)
            function_stats_buffer[req_id]['profile'] = pickled_stats

#=====================================================#

def get_importer():
    module = sys._getframe(1).f_globals.get('__name__')
    if module != '__main__':
        return module
    else:
        return os.path.basename(sys.argv[0])[:-3]


def get_wrapped(function):
    # at this point we need to test if the function is wrapped
    inner_func = function
    # recursively look through the func_closure items and look for callables.
    while inner_func.func_closure is not None:
        for item in inner_func.func_closure:
            if hasattr(item.cell_contents,'__call__'):
                inner_func = item.cell_contents
                # break the for loop if one is found. Not a perfect solution, but will work 99% of cases
                # seldom will decorators have multiple callables in the closure items - I hope
                break

    # if nothing changes, don't bother passing both functions to the StatWrapper __init__
    if inner_func is function:
        inner_func = None
    return function, inner_func


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
    full_string = path
    try:
        split_string = path.rsplit('.', 1)
        if len(split_string)==1:
            importer = get_importer()
            __import__(importer)
            importer = sys.modules[importer]



        module = __import__()
        while not inspect.ismodule(path[module_index]):
            module_index -= 1
            
        module = path[0:module_index+1]
        attribute = path[module_index+1:]
    except:
        raise Exception('There was an issue finding module and function {0}'.format(path_string))

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


        outer_func, inner_func = get_wrapped(original)
        # replace the function instance with a wrapped one.
        setattr(parent, attribute, StatWrapper(outer_func, inner_func))
        print '\nWRAPPED\n'
    except Exception as e:
        raise e
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
