from mqtt_interface import mqttinterface
import time


client = mqttinterface.MQTTInterface(port=1883, host='localhost')
client_two = mqttinterface.MQTTInterface(port=1883, host='localhost')
client.start()
client_two.start()

global counter
counter = 0

def callback(msg):
    global counter
    counter += 1

q = client_two.subscribe_with_callback('test/topic', callback)

start = time.time()
for _ in range(0, 500):
    client.send_message('test/topic', '1'.encode(encoding='UTF-8'))

elapsed = time.time() - start
print('Took ', elapsed, ' seconds to send ', counter, ' messages ')

client.stop()
client_two.stop()
