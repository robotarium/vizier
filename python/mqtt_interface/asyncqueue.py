import asyncio
import queue
import concurrent.futures as futures
import functools

class AsyncQueue:
    '''
    This is a multi-purpose queue that implements features from queue and asyncio's queues
    '''
    def __init__(self, maxsize=0, executor=futures.ThreadPoolExecutor(max_workers=1)):
        self.queue = queue.Queue(maxsize)
        self.executor = executor

    def put(self, item, block=True, timeout=None):
        self.queue.put(item, block=block, timeout=timeout)

    @asyncio.coroutine
    def async_put(self, loop, item, block=True, timeout=None):
        f = functools.partial(self,put, item, block=block, timeout=timeout)
        return (yield from loop.run_in_executor(self.executor, f))

    def get(self, block=True, timeout=None):
        return self.queue.get(block=block, timeout=timeout)

    @asyncio.coroutine
    def async_get(self, loop, block=True, timeout=None):
        f = functools.partial(self.get, block=block, timeout=timeout)
        return (yield from loop.run_in_executor(self.executor, f))
