"""
A stats profiling decorator. .

Wrap the functions that you want to monitor and prosper. 
"""
import cProfile
import inspect
import time
from threading import Thread
import cPickle
from cherry_pyformance import cfg


decorator_stats_buffer = {}

def _after(_id):
    """
    Pushes the stats collected to the buffer.
    """
    if _id in decorator_stats_buffer:
        print 'creating stats'
        stats = decorator_stats_buffer[_id]['profile']
        stats.create_stats()
        # pickle stats and put back on the buffer for flushing
        pickled_stats = cPickle.dumps(stats.stats)
        decorator_stats_buffer[_id]['profile'] = pickled_stats

def stat_wrapped(func):
    """
    A wrapper function which takes profile data of the wrapped function when called.
    """
    if cfg.get('active',False):
        def inner(*args, **kwargs):
            _id = id(time.time())
            decorator_stats_buffer[_id] = {
                'datetime': float(time.time()),
                'profile': cProfile.Profile(),
                'module': inspect.getmodule(func).__name__,
                'class': func.__class__.__name__,
                'function': func.__name__
            }
            out = decorator_stats_buffer[_id]['profile'].runcall(func, *args, **kwargs)
            Thread(target=_after, args=(_id,)).start()
            return out
        
        inner.__doc__ = func.__doc__
        inner.__name__ = func.__name__
        return inner
    else:
        return func