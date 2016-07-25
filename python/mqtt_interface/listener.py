import asyncio
import mqttInterface
import protocols.python.setupToServer_pb2 as ssm
import pipeline
import functools
import time
import json
import argparse
import queue

@asyncio.coroutine
def parsePayload(mqtt, message):
    message = json.loads(message.payload.decode(encoding="UTF-8"))
    print("Got MAC Address: " + message['mac'])
    return message['mac']

@asyncio.coroutine
def sendSetupMessage(mqtt, payload, mac):
    #Se
    yield from mqtt.sendMessage("/setup/" + mac, payload)
    return mac

# def publishIdentities(mqtt, experiment_id):
#     mqtt.sendMessage("/experiment/" + experiment_id,

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("-port", type=int, help="MQTT Port", default=8080)
    parser.add_argument("-host", help="MQTT Host IP", default="localhost")
    parser.add_argument("-setup_channel", help="The main setup channel", default="/setup/request")

    args = parser.parse_args()

    # Start the MQTT interface
    try:
        mqtt = mqttInterface.MQTTInterface(port=args.port, host=args.host)
    except ConnectionRefusedError as e:
        print("Couldn't connect to MQTT broker at " + str(args.host) + ":" + str(args.port) + ". Exiting.")
        return -1

    mqtt.start()

    start_time = time.time()

    i = 1

    @asyncio.coroutine
    def subscribeToSetup():
        #Se
        yield from mqtt.subscribe(args.setup_channel)

    @asyncio.coroutine
    def waitForSetupMessage():
        #Se
        return (yield from mqtt.waitForMessage(args.setup_channel))

    mqtt.runPipeline(subscribeToSetup())

    while i < 300:

        try:
            result = mqtt.runPipeline(waitForSetupMessage()).payload
            print(result)
        except queue.Empty as e:
            print("No contact received from robots.  Exiting.")
            mqtt.stop()
            return -1

        i = i + 1

        print(i)


    elapsed_time = i/(time.time() - start_time)

    print(elapsed_time)

    entered = ""
    while(entered != "q"):
        entered = input("Enter 'q' to quit...\n")

    mqtt.stop()

if(__name__ == "__main__"):
    main()
