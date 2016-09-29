import concurrent.futures
import functools
import queue
import asyncio

def _getOrThrowException(q):

    result = q.get()
    if isinstance(result, Exception):
        raise result
    else:
        return result

class Promise:

    def __init__(self, executor=concurrent.futures.ThreadPoolExecutor(max_workers=1)):
        self.pQueue = queue.Queue(1)
        self.future = executor.submit(functools.partial(_getOrThrowException, self.pQueue))

    def fulfill(self, val):
        self.pQueue.put(val)

    def result(self):
        return self.future.result()


class AsyncPromise:

    def __init__(self, loop, executor=concurrent.futures.ThreadPoolExecutor):
        self.pQueue = queue.Queue(1)
        self.future = loop.run_in_executor(executor, functools.partial(_getOrThrowException, self.pQueue))

    def fulfill(self, val):
        self.pQueue.put(val)

    def result(self):
        return self.future
