import time

class TimeIt(object):
    """Time a Task method and write the results to metadata
    
    Warning: the method being timed must be an instance method (self must be the first argument).
    """
    def __init__(self, func):
        self._func = func
        self._timeName = "%s_time" % (func.__name__,)
    def __call__(self, funcself, *args, **keyArgs):
        t1 = time.time()
        res = self._func(funcself, *arg,**kw)
        t2 = time.time()
    
        funcself.addMetadata(self._timeName, t2-t1)
        return res
