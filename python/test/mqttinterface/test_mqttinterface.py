from vizier import mqttinterface
import time
import unittest
import concurrent.futures as futures


class TestMQTTInterface(unittest.TestCase):

    def setUp(self):
        self.client_one = mqttinterface.MQTTInterface(port=1883, host='localhost')
        self.client_two = mqttinterface.MQTTInterface(port=1883, host='localhost')

        self.client_one.start()
        self.client_two.start()

    def test_subscribe(self):

        test_message = 'test'
        q = self.client_one.subscribe('test/topic')
        self.client_one.send_message('test/topic', test_message.encode(encoding='UTF-8'))

        message = q.get()
        self.assertEqual(test_message, message.decode(encoding='UTF-8'))

    def test_subscribe_with_callback(self):

        test_value = False

        def test_callback(message):
            nonlocal test_value
            test_value = True

        self.client_one.subscribe_with_callback('test/topic', test_callback)
        self.client_one.send_message('test/topic', 'irrelevant'.encode(encoding='UTF-8'))
        time.sleep(3)
        self.assertEqual(test_value, True)

    def tearDown(self):
        self.client_one.stop()
        self.client_two.stop()


class TestCountDownLatch(unittest.TestCase):

    def setUp(self):
        self.cdl = mqttinterface._CountDownLatch(1)

    def test_wait(self):

        def work():
            time.sleep(1)
            self.cdl.count_down()

        with futures.ThreadPoolExecutor() as e:
            e.submit(work)

            self.cdl.wait()
            self.cdl.wait()
