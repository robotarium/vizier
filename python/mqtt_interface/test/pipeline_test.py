import pipeline
import asyncio

def main():

    def task(str):
        @asyncio.coroutine
        def decorator(loop, *args):
            print("task" + str + "called!")
            end_time = loop.time() + 3
            while True:
                print(str)
                if (loop.time() + 1.0) >= end_time:
                    break
                yield from asyncio.sleep(1)
            return (loop)
        return decorator

    def final_task(str):
        @asyncio.coroutine
        def decorator(loop, *args):
            print("task" + str + "called!")
            end_time = loop.time() + 3
            while True:
                print(str)
                if (loop.time() + 1.0) >= end_time:
                    break
                yield from asyncio.sleep(1)
            return str + "hooray!"
        return decorator

    @asyncio.coroutine
    def task2(loop_vector):
        loop = loop_vector[0]
        print("task" + "seven" + "called!")
        end_time = loop.time() + 3
        while True:
            print("seven")
            if (loop.time() + 1.0) >= end_time:
                break
            yield from asyncio.sleep(1)
        return (loop)

    loop = asyncio.get_event_loop()

    p = pipeline.construct([task("four"), task("five"), task("six")])

    #pipeline = construct_pipeline(task("one"), final_task("final"))

    print(loop.run_until_complete(p(loop)))

    loop.close()

if (__name__ == "__main__"):
    main()
