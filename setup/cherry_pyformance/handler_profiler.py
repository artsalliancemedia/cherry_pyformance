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
from cherrypy.process.plugins import Monitor
import cProfile
import pstats
import inspect
import copy
import time

from cherry_pyformance import get_stat

stat_logger = None
cfg = None
push_stats = None

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
            stats_buffer[req_id]['profile'] = pstats.Stats(stats_buffer[req_id]['profile'])
            # add total time taken
            stats_buffer[req_id]['total_time'] = stats_buffer[req_id]['profile'].total_tt
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
            pstats_buffer = stats_buffer[_id]['pstats']
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
            stats_buffer[_id]['pstats'] = parsed_stats
            # put a deep copy on the stats_to_push list
            stats_to_push.append(copy.deepcopy(stats_buffer[_id]))
            # remove parsed stats, keeping transient stats
            del stats_buffer[_id]
    length = len(stats_to_push)
    if length != 0:
        stats_package_template = {'exhibitor_chain': cfg['exhibitor_chain'],
                                  'exhibitor_branch': cfg['exhibitor_branch'],
                                  'product': cfg['product'],
                                  'version': cfg['version'],
                                  'stats': []}
        stats_package = copy.deepcopy(stats_package_template)
        stats_package['stats'] = stats_to_push
        push_stats(stats_package)
        stat_logger.info('Flushed %d stats from the buffer' % length)
    else:
        stat_logger.info('No stats on the buffer to flush.')
 
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
    # decorate all handlers supplied in config
    stat_logger.info('Wrapping cherrypy handers for stats gathering.')
    try:
        for root in cfg['handlers'].keys():
            for handler in cfg['handlers'][root]:
                cherrypy.tree.apps[str(root)].merge({str(handler):{'tools.stats.on':True}})
        for root in cfg['ignored_handlers'].keys():
            for handler in cfg['ignored_handlers'][root]:
                cherrypy.tree.apps[str(root)].merge({str(handler):{'tools.stats.on':False}})
    except KeyError:
        stat_logger.warning('Stats configuration incorrect. Could not obtain handlers to wrap.')
    except Exception as e:
        stat_logger.warning('Failed to wrap cherrypy handler for stats profiling.')
        
def initialise(config, logger, push_stats_fn):
    global cfg
    global stat_logger
    global push_stats
    cfg = config
    stat_logger = logger
    push_stats = push_stats_fn

    # put tool in toolbox
    cherrypy.tools.stats = StatsTool( sort=cfg['sort_on'], num_results=cfg['num_results'] )

    # subscribe the function which decorates the handlers
    cherrypy.engine.subscribe('start', decorate_handlers, 0)
    
    # create a monitor to periodically flush the stats_buffer at the flush_interval
    Monitor(cherrypy.engine, flush_stats,
        frequency=cfg['flush_interval'],
        name='Flush profile stats buffer').subscribe()
        
    # when the engine stops, flush any stats.
    cherrypy.engine.subscribe('stop', flush_stats)
