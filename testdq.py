from multiprocessing import Pool, Process, Queue, TimeoutError
import time
import os
import sys
import random

import threading

import dirqueue
from dirqueue import QueueDir, Reader, Writer, TS, foobar

PROCESSES = 4
CYCLES = 2000

BASE_DIR = 'queuedir.out'

if not os.path.exists(BASE_DIR):
    os.mkdir(BASE_DIR)

dirqueue.HARD_LIMIT = 1000
dirqueue.SOFT_LIMIT = 250

def worker(num, q):

    key = num * 1000000
    with open(BASE_DIR + '/worker_' + str(num), 'w') as f:
        while True:
            pid = os.getpid()
            ident = threading.get_ident()
            print('worker %s queue: pid=%d, thread:%s' % (str(num), pid, ident))

            batch = random.randrange(20,50)
            #batch = 10
            for i in range(batch):
                key = key + 1
                #side = random.choice(['buy', 'sell'])
                side = 'buy' if num == 0 else 'sell'
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
                q.put(order)
                time.sleep(random.uniform(0,.01))
                #if num == 1:
                #    time.sleep(.1)

            secs = 1
            print("worker %d sleeping %f seconds.." % (num, secs))
            f.write("puts this second: %d\n" % batch)
            f.write(TS.stats())
            f.flush()
            time.sleep(secs)
            #break


def consumer():
    print('-' * 75)
    pid = os.getpid()
    print('comsume queue: pid=%d' % (pid))

    q = Reader('queuedir')
    BATCH = 1000
    while True:
        print('queue size:',q.get_size())
        for key in iter(q.next(100)):
            q.get(key)
            q.done(key)
        print('queue size:',q.get_size())

        TS.print_stats()
        foobar(q)
        secs = 1
        print("consumer Sleeping %f seconds.." % (secs,))
        time.sleep(secs)



if __name__ == '__main__':
    q = QueueDir('queuedir').clear()

    #proc = Process
    proc = threading.Thread

    # Consumer process
    p = Process(target=consumer)
    p.start()

    wq = Writer('queuedir')

    jobs = []
    for i in range(PROCESSES):
        p = proc(target=worker, args=(i,wq))
        jobs.append(p)
        p.start()



