import time
import atexit
import sys

class Stats:
    instances = []
    def __init__(self, types=[]):
        self.__class__.instances.append(self)
        self.types = set(types)
        self._stats = {}

    def set(self, name, elapsed=0, count=1):
        self.types.add(name)
        cnt = name+'_cnt'
        avg = name+'_avg'
        tot = name+'_tot'
        for x in (cnt, tot):
            if x not in self._stats:
                self._stats[x] = 0
        self._stats[cnt] += count
        self._stats[tot] += elapsed

        if avg in self._stats:
            self._stats[avg] = (self._stats[avg] + elapsed) / 2
        else:
            self._stats[avg] = elapsed

    def text(self):
        out = []
        out.append("%-20s %10s %13s %13s %10s" % (
            'Name','Count','Total','Avg','Ops/sec'))
        for name in self.types:
            cnt = self._stats[name+'_cnt']
            avg = self._stats[name+'_avg']
            tot = self._stats[name+'_tot']
            ops = 0
            if tot > 0:
                ops = cnt / tot
            out.append("%-20.20s %10d %10.2f ms %10.2f ms %10d" % (
                name, cnt, tot * 1000, avg * 1000, ops
            ))


        return '\n'.join(out)

    def print_stats(self):
        print(self.text())

    def timeit(self, method):
        def timed(*args, **kw):
            ts = time.time()
            result = method(*args, **kw)
            te = time.time()
            self.set(method.__name__, (te - ts)) # * 1000
            return result
        return timed


def get_size(obj, seen=None):
    """Recursively finds size of objects"""
    size = sys.getsizeof(obj)
    if seen is None:
        seen = set()
    obj_id = id(obj)
    if obj_id in seen:
        return 0
    # Important mark as seen *before* entering recursion to gracefully handle
    # self-referential objects
    seen.add(obj_id)
    if isinstance(obj, dict):
        size += sum([get_size(v, seen) for v in obj.values()])
        size += sum([get_size(k, seen) for k in obj.keys()])
    elif hasattr(obj, '__dict__'):
        size += get_size(obj.__dict__, seen)
    elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes, bytearray)):
        size += sum([get_size(i, seen) for i in obj])
    return size


def sizefmt(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)

def dumpstats():
    for x, i in enumerate(Stats.instances):
        print('*'*78)
        print('Stats for instance',x)
        print(i.text())

atexit.register(dumpstats)
