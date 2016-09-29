import functional_stream.core as f_stream
import time


# head = f_stream.FunctionalStream()
# middle = f_stream.FunctionalStream()
#
# tail = head.map(lambda x: x + 1).map(lambda x: x * 2).map(lambda x: x + 3).chain(lambda x: x / 2)
#
# head.of(1)
# head.of(2)
#
# print(tail.extract())
# print(tail.extract())
#
# head.close()
#
def long_operation(*args, **kwargs):
    time.sleep(5)
    return 5

result = f_stream.lift(long_operation)

# Type: Promise
duplicate1 = result.extend(lambda w: w.extract())
duplicate2 = result.extend(lambda w: w.extract())

duplicate1.fmap(lambda x: x + 1).extract()
duplicate2.fmap(lambda x: x + 2).extract()


#r_ = result.fmap(lambda cv: cv.extract() + 1).fmap(lambda cv: cv.extract()*2)

lambda w:

#print(result.extend(lambda w: w.extract() + 1).extend(lambda w: w.extract()*2).extract())

print(result.fmap(lambda x: x + 1).fmap(lambda x: x * 2).fmap(lambda x: x).fmap(lambda x: x * 3).extract())

# result = beginning.concat(end).map(lambda x: x + 1).map(lambda y: y - 2).extract()
#
# print("The result was: " + repr(result))


# Test extend



# TEST CONCAT
