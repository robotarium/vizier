import asyncio
import itertools
import functools as ft
import concurrent.futures
#Need to...
#Wait for setup message --> send information to robot --> receieve ack back --> profit

# BUILDING BLOCKS

#TODO: Wrap so that we can call multiple tasks as the first argument!

def wrap(tasks):
    @asyncio.coroutine
    def f(*args, **kwargs):
        wrapped_tasks = [asyncio.async(coroutine(*args, **kwargs)) for coroutine in tasks]
        return (yield from asyncio.gather(*wrapped_tasks))

    return f

def before(this):
    def f(that):
        @asyncio.coroutine
        def coroutine(*args, **kwargs):
            result = (yield from that(*args, **kwargs))
            if result is None:
                return (yield from this())
            else:
                return (yield from this(result))

        return coroutine

    return f

def cleanup_before(this):
    def f(that):
        @asyncio.coroutine
        def coroutine(*args, **kwargs):
            result = (yield from that(*args, **kwargs))
            yield from this()
            return result

        return coroutine

    return f

def pbefore(*this):
    def f(that):
        @asyncio.coroutine
        def coroutine(*args, **kwargs):
            args = yield from that(*args, **kwargs)
            #with concurrent.futures.ThreadPoolExecutor(max_workers=len(this)) as executor:
            if args is None:
                tasks = [asyncio.async(coroutine()) for coroutine in this]
            else:
                tasks = [asyncio.async(coroutine(args)) for coroutine in this]
            #Splat futures and wait for return aggregate
            return (yield from asyncio.gather(*tasks))

        return coroutine

    return f

def compose(a, b):
    def f(*args, **kwargs):
        return a(b(*args, **kwargs))

    return f

def compose_multiple(*fs):
    if(len(fs) == 0):
        return
    return ft.reduce(compose, fs)

def construct(first, *stages, cleanup=[]):

    stages = list(reversed(stages))

    def collapse(x):
        if(type(x) == list):
            return pbefore(*x)
        else:
            return before(x)

    bs = [cleanup_before(c) for c in cleanup] + [collapse(s) for s in stages]

    if(type(first) == list):
        #If first is multiple tasks, just wrap it
        first = wrap(first)

    #TODO: Handle this case more elegantly in the 'compose' function
    if(len(bs) == 0):
        return first
    else:
        return compose_multiple(*bs)(first)
