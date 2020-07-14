# DirQueue - Message queue using a directory of files
import errno
import fcntl
import json
import os
from pathlib import Path
import shortuuid
import sys
import threading
import time



HARD_LIMIT = 5000 # Defines entire sequence range
SOFT_LIMIT = 2000 # Messages are rejected above this limit


class Stats:
    def __init__(self):
        self.last_time = time.time()
        self.types = ('put','get','get_queue','done','get_size','reject')
        for i in self.types:
            for j in ('cnt','ttime','avg'):
                setattr(self, i+'_'+ j, 0)
                if j == 'avg':
                    setattr(self, i+'_'+ j, None)

    def set(self, i, elapsed=0):
        cnt = i+'_cnt'
        avg = i+'_avg'
        tot = i+'_ttime'
        setattr(self, cnt, getattr(self,cnt) + 1)
        setattr(self, tot, getattr(self,tot) + elapsed)

        if getattr(self, avg) != None:
            setattr(self, avg, (getattr(self, avg) + elapsed) / 2)
        else:
            setattr(self, avg, elapsed)

    def text(self):
        out = []
        out.append("stats:")
        for t in TS.types:
            total = getattr(TS, t+'_ttime')
            avg = getattr(TS, t+'_avg') or 0
            cnt = getattr(TS, t+'_cnt')
            ops = 0
            if total > 0:
                ops = cnt / total
            out.append("%-10s cnt:%8d avg:%8.4fs tot:%8.4fs ops:%8d /s" % (
                t, cnt, avg, total, ops
            ))


        return '\n'.join(out)

    def print_stats(self):
        print(self.text())

    def write_stats(self, fname):
        if time.time() - self.last_time > 10:
            with open(fname, 'w') as f:
                f.write(self.text())
                f.flush()
                self.last_time = time.time()

TS = Stats()


def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        TS.set(method.__name__, (te - ts)) # * 1000
        return result
    return timed




# Thread safe counter
class Counter():
    def __init__(self, dname, value, limit=True):
        self.dname = dname
        self.count = value
        self.limit = limit

        self._lock = threading.Lock()

    def current(self):
        return self.count

    def next(self):
        limit = self.limit

        with self._lock:
            count = self.count + 1

            # Wrap at hard limit
            if count > HARD_LIMIT:
                count = count - HARD_LIMIT

            # Existing next key means we've reached hard limit, reject
            if limit and Path(self.dname / str(count)).exists():
                return None

            # Soft limit, reject
            soft_count = (self.count + 1) + (HARD_LIMIT - SOFT_LIMIT)
            if soft_count > HARD_LIMIT:
                soft_count = soft_count - HARD_LIMIT

            # Some conditions will ignore this limit
            if limit and Path(self.dname / str(soft_count)).exists():
                return None

            # Commit count
            self.count = count
            return count
        return None


class DirQueue:
    def __init__(self, dname, type=None, lockfn=None):
        self.dname = Path(dname)
        self.type = type
        self.lockfn = lockfn

        if not os.path.exists(self.dname):
            os.mkdir(self.dname)

        if self.type:
            self.lockfile = self.dname / self.lockfn
            self.lock = open(self.lockfile,'w+')

            try:
                fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except IOError as e:
                if e.errno == errno.EAGAIN:
                    print('Process locked. abort')
                    sys.exit(1)

            # Set counter from queue state
            value = self.counter_from_queue(self.type)
            limit = True if self.type == 'writer' else False
            self.counter = Counter(self.dname, value, limit=limit)

    def __del__(self):
        if self.type:
            try:
                fcntl.flock(self.lock, fcntl.LOCK_UN)
                self.lock.close()
                Path(self.lockfile).unlink()
            except:
                pass

    def fname(self, key):
        return self.dname / str(key)

    def serialize(self, data):
        return json.dumps(data)

    def deserialize(self, string):
        return json.loads(string)

    def clear(self):
        for fn in os.listdir(self.dname):
            Path(self.dname / fn).unlink()

    @timeit
    def get(self, key):
        data = None
        with open(self.fname(key)) as f:
            data = self.deserialize(f.read())
        return data

    @timeit
    def done(self, key):
        Path(self.fname(key)).unlink()

    @timeit
    def get_size(self):
        return len(self.get_queue())

    @timeit
    def get_queue(self):
        dirs = os.listdir(self.dname)
        dirs = [f for f in dirs if not f.startswith('.')]
        return sorted(dirs, key=int)

    def counter_from_queue(self, c):
        # reader - return start of sequence
        # writer - return end of sequence
        mod = -1 if c == 'writer' else 0
        items = self.get_queue()
        print(items)
        if len(items) > 1 and int(items[0]) == 1 and int(items[-1]) == HARD_LIMIT:
            for x in range(1,len(items)):
                if int(items[x-1]) != int(items[x]) - 1:
                    value = int(items[x+mod])
                    break
        elif len(items) > 0:
            value = int(items[0+mod])
        else:
            # writer advances counter first
            # reader use counter before advancing
            value = HARD_LIMIT if c == 'writer' else 1
        return value


class Writer(DirQueue):
    def __init__(self, dname):
        super().__init__(dname, type='writer', lockfn='.wlock')

    @timeit
    def put(self, data):
        TS.write_stats(self.dname / ('.' + self.type + '-stats'))

        if 'id' not in data:
            data['id'] = shortuuid.uuid()
        key = self.counter.next()
        if not key:
            TS.set('reject')
            return None

        # A file must always be written, no matter what.
        with open(self.fname(key), 'w') as f:
            out = self.serialize(data)
            f.write(out)
            f.flush()
        return True


class Reader(DirQueue):
    def __init__(self, dname):
        super().__init__(dname, type='reader', lockfn='.rlock')

    def __iter__(self):
        return self

    def next(self, iter_limit=100):
        self.iter_limit = iter_limit
        return self

    def __next__(self):
        TS.write_stats(self.dname / ('.' + self.type + '-stats'))
        if not self.iter_limit:
            raise StopIteration
        c = self.counter.current()
        if not os.path.exists(self.dname / str(c)):
            raise StopIteration
        # Advance count. I'd like advance in done() but then
        # a iter loop without done, will be infinite
        self.counter.next()
        self.iter_limit -= 1
        return c

