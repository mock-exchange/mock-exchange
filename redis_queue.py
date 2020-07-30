import shortuuid
import msgpack

class SimpleQueue(object):
    def __init__(self, conn, name):
        self.conn = conn
        self.name = name

    def enqueue(self, method, *args):
        task = [
            str(shortuuid.uuid()),
            method,
            *args
        ]
        msg = msgpack.packb(task)
        self.conn.lpush(self.name, msg)
        return task[0]

    def dequeue(self):
        _, msg = self.conn.brpop(self.name)
        task = msgpack.unpackb(msg)
        return task

    def get_length(self):
        return self.conn.llen(self.name)


class SimpleTask(object):
    def __init__(self, func, *args):
        self.id = str(shortuuid.uuid())
        self.func = func
        self.args = args

    def process_task(self):
        self.func(*self.args)
