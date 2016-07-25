import asyncio
import mqttInterface
import pipeline
import functools as ft
import time
import json
import argparse
import queue


def is_link_subset_of(path, link):

    if(len(path) == 0):
        return True

    path_tokens = path.split('/')
    link_tokens = link.split('/')

    for i in range(len(path_tokens)):
        if(link_tokens[i] != path_tokens[i]):
            return False

    return True

def expand_path(path, link):
    if(link[0] == '/'):
        return path + link
    else:
        return link

def generate_links_from_descriptor(descriptor):
    def parse_links(path, link, body):

        link = expand_path(path, link)

        if not is_link_subset_of(path, link):
            print("Cannot have link that is not a subset of the current path")
            print("Link: " + link)
            print("Path: " + path)
            raise ValueError

        if(len(body["links"]) == 0):
            return {link : body["requests"]}

        #Else...

        local_links = {}
        for x in body["links"]:
            local_links.update(parse_links(link, x, body["links"][x]))

            for request in body["links"][x]["requests"]:
                if("response" in request):
                    response_link = expand_path(expand_path(link, x), request["response"]["link"])
                    if not is_link_subset_of(link, response_link):
                        print("Cannot have link that is not a subset of the current path")
                        print("Link: " + response_link)
                        print("Path: " + link)
                        raise ValueError
                    request["response"]["link"] = response_link

        local_links.update({link : body["requests"]})
        return local_links

    return parse_links("vizier", '/' + descriptor["attribute"], descriptor)

#Put this somewhere intelligent
class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WHITE = '\033[97m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

#def handle source nodes

def create_pipeline_print(to_print, end="\n"):
    @asyncio.coroutine
    def f(*arg):
        print(to_print, end = end)
        if(len(arg) != 0):
            return arg[0]

    return f

@asyncio.coroutine
def subscribe_and_return(client, channel):
    yield from client.subscribe(channel)
    return channel

@asyncio.coroutine
def send_and_return(mqtt_client, channel, message):
    yield from mqtt_client.send_message(channel, message)
    return channel


@asyncio.coroutine
def parse_setup_message(message):
    decoded_message = json.loads(message.payload.decode(encoding="UTF-8"))
    return decoded_message

@asyncio.coroutine
def send_ok_message(mqtt_client, message):
    decoded_message = json.loads(message.payload.decode(encoding="UTF-8"))
    response_channel = decoded_message["response"]
    yield from mqtt_client.send_message(response_channel, json.dumps({"status" : "running", "error" : "none"}))
    return decoded_message

@asyncio.coroutine
def wait_and_send(mqtt_client, channel):
    print("waiting for message on : " + channel)
    message = yield from mqtt_client.wait_for_message(channel)
    message = json.loads(message.payload.decode(encoding="UTF-8"))
    response_channel = message["response"]["link"]
    yield from mqtt_client.send_message(response_channel, json.dumps({"status" : "running", "error" : "none"}))
    return message


def ignore_after_args(func, count):
    @ft.wraps(func)
    def newfunc(*args, **kwargs):
        return func(*(args[:count]), **kwargs)
    return newfunc

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("node_descriptor", help=".json file node information")
    parser.add_argument("-port", type=int, help="MQTT Port", default=8080)
    parser.add_argument("-host", help="MQTT Host IP", default="localhost")

    args = parser.parse_args()

    #Ensure that we can open the nodes file
    node_descriptor = None

    try:
        f = open(args.node_descriptor, 'r')
        node_descriptor = json.load(f)
        f.close()
    except Exception as e:
        print(repr(e))
        print("Couldn't open given node file " + args.node_descriptor)
        return -1

    # Ensure that information in our configuration file is accurate

    # I hate using the UDP...let's just ignore that for now...

    setup_information = vizier_node.connect(args.host, args.port, node_descriptor)

    print(setup_information)

    # Attempt to connect to MQTT broker
    try:
        mqtt_client = mqttInterface.MQTTInterface(port=args.port, host=args.host)
    except ConnectionRefusedError as e:
        print("Couldn't connect to MQTT broker at " + str(args.host) + ":" + str(args.port) + ". Exiting.")
        return -1

    # Start the MQTT interface
    mqtt_client.start()

    all_links = generate_links_from_descriptor(node_descriptor)

    # First, we need to grab all the node descriptors

    # DON'T FORGET TO UNSUBSCRIBE FROM CHANNELS
    setup_channel = 'vizier/setup'

    wait_and_sends = []
    subscribes = []
    for requests in all_links.values():
        for request in requests:
            print(request["response"]["link"])
            wait_and_sends.append(ft.partial(wait_and_send, mqtt_client, request["response"]["link"]))
            subscribes.append(ft.partial(mqtt_client.subscribe, request["response"]["link"]))

    pipe = pipeline.construct(subscribes,
                              create_pipeline_print("Subscribing to setup channel: " + repr(all_links) +  " " + bcolors.OKGREEN + "[ ok ]" + bcolors.WHITE),
                              create_pipeline_print("Sending setup message on: " + setup_channel, end = " "),
                              ignore_after_args(ft.partial(mqtt_client.send_message, setup_channel, json.dumps(node_descriptor)), 0),
                              create_pipeline_print(bcolors.OKGREEN + "[ ok ]" + bcolors.WHITE),
                              wait_and_sends,
                              create_pipeline_print(bcolors.OKGREEN + "[ ok ]" + bcolors.WHITE),
                              create_pipeline_print("Sending OK message:", end = " "))

    setup_information = None

    try:
        setup_information = mqtt_client.run_pipeline(pipe())
    except Exception as e:
        print("Couldn't complete vizier handshake for node: " + node_descriptor["attributes"]["attribute"])

    mqtt_client.stop()

    print("SETUP INFO: " + repr(setup_information))

if(__name__ == "__main__"):
    main()
