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
import pstats
import inspect
import copy
import time

from cherry_pyformance import cfg, get_stat, stat_logger, handler_stats_buffer

#=====================================================#

class StatsTool(cherrypy.Tool):
    """
    A cherrypy tool which wraps handlers, collecting
    profile stats from them. After the repsonse has
    been sent, the stats are sorted and put on the buffer.
    """

    def __init__(self, sort, num_results):
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
        on the handler_stats_buffer based on request metadata, then fires the handler
        while collecting its profile infomation.
        """
        global handler_stats_buffer
        request = cherrypy.serving.request
        # Take the id of the request, this guarantees no cross-contamination
        # of stats as each record is tied to the id of an instance of a request.
        # These are guaranteed to be unique, even if two of the same request are
        # fired at the same time.
        req_id = id(request)
        
        _method = request.path_info
        _class = request.app.root.__class__.__name__
        _module = inspect.getmodule(request.app.root.__class__).__name__
        full_method_name = _module + ' - ' + _class + ' - ' + _method
        
        handler_stats_buffer[req_id] = {'stats_buffer': {'id': req_id,
                                                         'datetime': time.time(),
                                                         'total_time': 0,
                                                         'profile': cProfile.Profile()},
                                        'metadata_buffer': {'function': _method,
                                                            'class': _class,
                                                            'module': _module,
                                                            'full_method': full_method_name}
                                       }
        # At this point the profile key of the object on the stats buffer has no
        # profile stats in it. It needs to be put in the buffer now as multiple
        # handler calls could be occuring simultaneously during the lifetime of
        # the tool instance.
        handler = cherrypy.serving.request.handler
        def wrapper(*args, **kwargs):
            # profile the handler
            return handler_stats_buffer[req_id]['stats_buffer']['profile'].runcall(handler, *args, **kwargs)
        cherrypy.serving.request.handler = wrapper

    def record_stop(self):
        """
        This method is called once the response has been sent. Now the stats for
        are this request are called from the buffer, sorted, trimmed and put back
        on the buffer in dict form, removing the profile data. The result should
        be json serialisable.
        """
        global handler_stats_buffer
        req_id = id(cherrypy.serving.request)
        try:
            handler_stats_buffer[req_id]['stats_buffer']['profile'] = pstats.Stats(handler_stats_buffer[req_id]['stats_buffer']['profile'])
            # add total time taken
            handler_stats_buffer[req_id]['stats_buffer']['total_time'] = handler_stats_buffer[req_id]['stats_buffer']['profile'].total_tt
            # sort the stats in decending order of the sorting stat, then trim
            # to num_results, there will be a lot of negligable stats we can ignore
            stats = sorted(handler_stats_buffer[req_id]['stats_buffer']['profile'].stats.items(),
                           key=lambda x: get_stat(x,self.sort),
                           reverse=True )[:self.num_results]
            handler_stats_buffer[req_id]['stats_buffer']['pstats'] = copy.deepcopy(stats)
            # remove the profile data
            del handler_stats_buffer[req_id]['stats_buffer']['profile']
        except KeyError:
            # If TMS UI is open when TMS is starting started it tries to recall
            # a non-exsistent item from the buffer. 
            stat_logger.warning('The request id %d is not in the handler_stats_buffer.' % req_id)

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

    cherrypy.tools.stats = StatsTool(sort=cfg['global']['sort_on'], num_results=int(cfg['global']['num_results']))

    # decorate all handlers supplied in config
    stat_logger.info('Wrapping cherrypy handers for stats gathering.')
    try:
        for root in cfg['handlers'].keys():
            if cfg['handlers'][root]:
                for handler in cfg['handlers'][root].split(','):
                    cherrypy.tree.apps[str(root)].merge({str(handler):{'tools.stats.on':True}})
        for root in cfg['ignored_handlers'].keys():
            if cfg['ignored_handlers'][root]:
                for handler in cfg['ignored_handlers'][root].split(','):
                    cherrypy.tree.apps[str(root)].merge({str(handler):{'tools.stats.on':False}})
    except KeyError:
        stat_logger.warning('Stats configuration incorrect. Could not obtain handlers to wrap.')
    except Exception as e:
        stat_logger.warning('Failed to wrap cherrypy handler for stats profiling.')
