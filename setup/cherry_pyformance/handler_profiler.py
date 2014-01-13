"""
A stats profiling tool.
Import into CinemaServices.py after the main imports.

Configure profile_stats_config.json with all the 
cherrypy handlers to profile. The profile data is
parsed to json and put on a buffer. This is periodically
flushed out to json on the filesystem or pushed to the 
stats server where the data will be analysed and displayed.
"""
import cherrypy
import cProfile
import inspect
import time
import cPickle
import traceback

from cherry_pyformance import cfg, stat_logger

handler_stats_buffer = {}

#=====================================================#

class StatsTool(cherrypy.Tool):
    """
    A cherrypy tool which wraps handlers, collecting
    profile stats from them. After the response has
    been sent, the stats are sorted and put on the buffer.
    """

    def __init__(self):
        self._name = 'profile'
        self._point = 'before_handler'
        self._priority = 80
        self._setargs()

    def _setup(self):
        # Hooks the profile wrapper onto self._point
        # then also hooks self.record_stop after the response has been dealt
        cherrypy.Tool._setup(self)
        cherrypy.serving.request.hooks.attach('on_end_request', self.record_stop)

    def callable(self):
        """
        This is the handler wrapper. It initialises a stat record
        on the handler_stats_buffer based on request metadata, then fires the handler
        while collecting its profile information.
        """
        request = cherrypy.serving.request
        handler = request.handler
        # Check if handler exists (might not for static requests)
        if handler:
            # Take the id of the request, this guarantees no cross-contamination
            # of stats as each record is tied to the id of an instance of a request.
            # These are guaranteed to be unique, even if two of the same request are
            # fired at the same time.
            req_id = id(request)
            # initialise the item on the buffer
            handler_stats_buffer[req_id] = {'datetime': float(time.time()),
                                            'profile': cProfile.Profile()}
            # At this point the profile key of the object on the stats buffer has no
            # profile stats in it. It needs to be put in the buffer now as multiple
            # handler calls could be occuring simultaneously during the lifetime of
            # the tool instance.
            def wrapper(*args, **kwargs):
                # profile the handler
                return handler_stats_buffer[req_id]['profile'].runcall(handler, *args, **kwargs)
            cherrypy.serving.request.handler = wrapper

    def record_stop(self):
        """
        This method is called once the response has been sent. Now the stats for
        are this request are called from the buffer, pickled and put back
        on the buffer. The result should be json serialisable.
        """
        request = cherrypy.serving.request
        req_id = id(request)
        if req_id in handler_stats_buffer:
            _module = inspect.getmodule(request.app.root.__class__).__name__
            _class = request.app.root.__class__.__name__
            _method = request.path_info
            handler_stats_buffer[req_id]['module'] = _module
            handler_stats_buffer[req_id]['class'] = _class
            handler_stats_buffer[req_id]['function'] = _method
            
            stats = handler_stats_buffer[req_id]['profile']
            stats.create_stats()
            # pickle stats and put back on the buffer for flushing
            pickled_stats = cPickle.dumps(stats.stats)
            handler_stats_buffer[req_id]['profile'] = pickled_stats

#=====================================================#

def decorate_handlers():
    """
    A function to apply the StatsTool to handlers given in the config
    
    This function must be called after the apps have been mounted to the
    tree, otherwise there will be no config to alter. However normally
    this module is imported before the engine is setup, so that initialising
    functions can be profiled. Therefore this function is hooked to the
    'start' call on the cherrypy bus with a high priority.
    """

    cherrypy.tools.stats = StatsTool()

    # decorate all handlers supplied in config
    stat_logger.info('Wrapping cherrypy handers for stats gathering.')
    try:
        for root in cfg['handlers'].keys():
            if cfg['handlers'][root]:
                for handler in cfg['handlers'][root].split(','):
                    if str(root) == '/': # Can't have empty key in config file
                        root = ''
                    cherrypy.tree.apps[str(root)].merge({str(handler):{'tools.stats.on':True}})
        for root in cfg['ignored_handlers'].keys():
            if cfg['ignored_handlers'][root]:
                for handler in cfg['ignored_handlers'][root].split(','):
                    cherrypy.tree.apps[str(root)].merge({str(handler):{'tools.stats.on':False}})
    except KeyError:
        stat_logger.warning('Stats configuration incorrect. Could not obtain handlers to wrap.')
    except Exception:
        stat_logger.warning('Failed to wrap cherrypy handler for stats profiling.')
