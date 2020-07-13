import random
import json
import time
import os
import sys
import shortuuid

from pathlib import Path

import fcntl
import errno

import threading


class TrackStats:
    def __init__(self):
        self.types = ('put','get','list','done','size','reject')
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

    def stats(self):
        out = []
        out.append("stats:")
        for t in TS.types:
            total = getattr(TS, t+'_ttime')
            avg = getattr(TS, t+'_avg') or 0
            cnt = getattr(TS, t+'_cnt')
            ops = 0
            if total > 0:
                ops = cnt / total
            out.append("%-6s cnt:%8d avg:%8.4fs tot:%8.4fs ops:%8d /s" % (
                t, cnt, avg, total, ops
            ))


        return '\n'.join(out)

    def print_stats(self):
        print(self.stats())

TS = TrackStats()


# QueueDir


HARD_LIMIT = 5000
SOFT_LIMIT = 2000


# Thread shared counter
class Counter():
    def __init__(self, dname, value, limit=True):
        self.dname = dname
        self.count = value
        self.limit = limit

        #super().__init__(init=value)
        self._lock = threading.Lock()

    def current(self):
        return self.count

    def next(self):
        limit = self.limit

        with self._lock:
        #if True:
            #self._lock.acquire(blocking=True)
            count = self.count + 1

            # Wrap at hard limit
            if count > HARD_LIMIT:
                count = count - HARD_LIMIT

            # Existing next key means we've reached hard limit, reject
            if limit and os.path.exists(self.dname / str(count)):
                return None

            # Soft limit
            soft_count = (self.count + 1) + (HARD_LIMIT - SOFT_LIMIT)
            if soft_count > HARD_LIMIT:
                soft_count = soft_count - HARD_LIMIT

            # Some conditions will ignore this limit
            if limit and os.path.exists(self.dname / str(soft_count)):
                return None

            # Commit count
            self.count = count
            #self._lock.release()
        #self.increment()
        return count


class QueueDir:
    def __init__(self, dname):
        self.dname = Path(dname)
        if not os.path.exists(self.dname):
            os.mkdir(self.dname)

    def fname(self, key):
        return self.dname / str(key)

    def serialize(self, data):
        return json.dumps(data) + "\n"

    def deserialize(self, string):
        return json.loads(string)

    def clear(self):
        for x in os.listdir(self.dname):
            Path(self.dname / x).unlink()

    def get(self, key):
        begin = time.time()

        data = None
        with open(self.fname(key)) as f:
            data = self.deserialize(f.read())

        elapsed = time.time() - begin
        TS.set('get', elapsed)
        return data

    def done(self, key):
        begin = time.time()
        Path(self.fname(key)).unlink()
        elapsed = time.time() - begin
        TS.set('done', elapsed)

    def get_size(self):
        begin = time.time()
        size = len(self.get_queue())
        elapsed = time.time() - begin
        TS.set('size', elapsed)
        return size

    def get_queue(self):
        begin = time.time()
        dirs = os.listdir(self.dname)
        dirs = [f for f in dirs if not f.startswith('.')]
        dirs = sorted(dirs, key=int)
        elapsed = time.time() - begin
        TS.set('list', elapsed)
        return dirs

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

#WRITE_COUNTER = Counter()

class Writer(QueueDir):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        dname = self.dname
        # Lock reader
        #Path(dname / '.wlock').touch()
        # Determine counter state
        items = self.get_queue()

        #Path(dname / '.wlock').unlink()

        # Initialize counter to current one (LAST)
        value = self.counter_from_queue('writer')
        self.write_counter = Counter(dname, value)
        #self.write_counter = WRITE_COUNTER
        #self.write_counter.dname = dname
        #self.write_counter.value 

        print(items)
        print('set value:',value)
        """
        print('test counter:')
        for i in range(HARD_LIMIT):
            c = self.write_counter.next()
            print(i, 'counter:', c)
        """

    def put(self, data):
        if 'id' not in data:
            data['id'] = shortuuid.uuid()
        key = self.write_counter.next()
        if not key:
            TS.set('reject')
            return None

        begin = time.time()

        # A file must always be written, no matter what.
        with open(self.fname(key), 'w') as f:
            out = self.serialize(data)
            f.write(out)
            f.flush()
        elapsed = time.time() - begin
        TS.set('put', elapsed)
        return True


class Reader(QueueDir):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.lockfile = self.dname / '.rlock'
        self.lock = open(self.lockfile,'w+')
        while True:
            try:
                fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except IOError as e:
                # raise on unrelated IOErrors
                if e.errno != errno.EAGAIN:
                    raise
                else:
                    pid = os.getpid()
                    print('%d locked.. waiting 1 seconds..' % (pid))
                    time.sleep(1)

        # Set counter from queue state
        value = self.counter_from_queue('reader')
        print('read counter value:',value)
        self.read_counter = Counter(self.dname, value, limit=False)

    def __del__(self):
        fcntl.flock(self.lock, fcntl.LOCK_UN)
        self.lock.close()
        Path(self.lockfile).unlink()

    def __iter__(self):
        return self

    def next(self, iter_limit=100):
        self.iter_limit = iter_limit
        return self

    def __next__(self):
        if not self.iter_limit:
            raise StopIteration
        c = self.read_counter.current()
        if not os.path.exists(self.dname / str(c)):
            raise StopIteration
        # Advance count. I'd like advance in done() but then
        # a iter loop without done, will be infinite
        self.read_counter.next()
        self.iter_limit -= 1
        return c






#####################################################3










def writer(num):
    print('writer(%d):' % num)
    q = Writer('queuedir')
    
    print('queue size:',q.get_size())

    for i in range(num):
        side = random.choice(['buy', 'sell'])
        if side == 'buy':
            a_range = (1000,1500)
            p_range = (0.50,12)
        else:
            a_range = (1500,2000)
            p_range = (12,30)
        order = {
            'side': side,
            'account_id': random.randrange(*a_range),
            'price': float(random.uniform(*p_range)),
            'amount': random.uniform(1,10)
        }
        if not q.put(order):
            pass

    print('queue size:',q.get_size())

    TS.print_stats()
    foobar(q)

def consumer(num):
    print()
    print('comsumer(%d):' % num)
    q = Reader('queuedir')


    print('queue size:',q.get_size())
    for key in iter(q.next(num)):
        #print('done',key)
        q.get(key)
        q.done(key)
    print('queue size:',q.get_size())
    TS.print_stats()
    foobar(q)

def foobar(q):
    items = q.get_queue()
    print("queue %5d : %s .. %s" % (len(items), str(items[:3]), str(items[-3:])))
    print()

if __name__ == '__main__':
    begin = time.time()

    opt = sys.argv[-1]

    if opt == 'restart':
        writer(9)
    else:
        #QueueDir('queuedir').clear()

        writer(22)
        consumer(18)
        writer(3)
        consumer(4)

    elapsed = time.time() - begin
    print('done. %.8f secs.' % (elapsed))


