from mqtt_interface.mqttinterface import *

topic = 'test/hi'
client = MQTTInterface()
client.start()
_, q = client.subscribe(topic)
client.send_message(topic, 'hi')

print(q.get().payload.decode())

input('Press anything to quit\n')

client.stop()
