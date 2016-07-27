import asyncio
import socket
import struct
import json
import concurrent.futures as futures
import threading
import time

#TODO: Put this file somewhere else...

@asyncio.coroutine
def no_op():
    return

class UDPServerProtocol:

    def __init__(self, port, handler, broadcast=True):
        #Handler must be a function that accepts transport data, addr
        self.port = port
        self.handler = handler
        self.broadcast = broadcast
        #self.transport_lock = threading.Lock()
        self.executor = futures.ThreadPoolExecutor(max_workers = 1)
        self.loop = None
        self.transport = None
        self.future = None

    def connection_made(self, transport):
        self.transport = transport

        if(self.broadcast):
            sock = self.transport.get_extra_info('socket')
            group = socket.inet_aton('239.255.255.250')
            mreq = struct.pack('4sL', group, socket.INADDR_ANY)
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)

    def datagram_received(self, data, addr):
        message = data.decode(encoding="UTF-8")
        #print('Received %r from %s' % (message, addr))
        self.handler(self.transport, message, addr)

    def send_message(self, data, addr):
        self.transport.sendto(data, addr)

    def start(self):
        loop = asyncio.new_event_loop()
        self.loop = loop

        def protocolf():
            return self

        print("Starting UDP server")
        # One protocol instance will be created to serve all client requests
        if(self.broadcast):
            listen = loop.create_datagram_endpoint(protocolf, local_addr=('0.0.0.0', self.port), family=socket.AF_INET)
        else:
            listen = loop.create_datagram_endpoint(protocolf, local_addr=('0.0.0.0', self.port), family=socket.AF_INET)

        transport, protocol = loop.run_until_complete(listen)

        self.transport = transport
        print("UDP server started")

        loop.run_forever()

        return True

    def stop(self):
        self.loop.stop()
        self.future.result()
        self.transport.close()
        self.loop.close()
        self.executor.shutdown()

    def async_start(self):
        self.future = self.executor.submit(self.start)

def main():

    def handler(t, d, a):
        print('handled')
        if(d[len(d)-1] == '\x00'):
            d = d[:len(d)-1]

        print(json.loads(d))

    udp_server = UDPServerProtocol(5005, handler)
    udp_server.async_start()

    input('Press enter to quit')

    udp_server.stop()


if __name__ == "__main__":
    main()
