"""
    Client for communication over UDP Multicast
"""
import socket
import uuid
import os
import time
from multicastclient import multicasting
import re
import struct
from queue import Queue, Empty
#from pyloggedthread.loggedthread import LoggedThread as Thread
from threading import Thread
from threading import Lock

if os.name == 'nt':
    IPPROTO_IPV6 = 41  # Workaround for missing constants on Win32 systems.
else:
    # noinspection PyUnresolvedReferences
    from socket import IPPROTO_IPV6

__status__ = 'Development'


class Callback:

    def __init__(self, target):
        self.target = target

    def call(self, client, senderId, signal, mid, message):
        reply = self.target(message)
        if reply:
            client.reply(reply, senderId, signal, mid)


class DetailedCallback:

    def __init__(self, target):
        self.target = target

    def call(self, client, senderId, signal, mid, message):
        reply = self.target(senderId, signal, mid, message)
        if reply:
            client.reply(reply, senderId, signal, mid)


class ThreadedCallback(Callback):

    def __init__(self, target):
        Callback.__init__(self, target)

    def call(self, client, senderId, signal, mid, message):
        Thread(target=self.target, args=message, daemon=True).start()


class ThreadedDetailedCallback(Callback):

    def __init__(self, target):
        Callback.__init__(self, target)

    def call(self, client, senderId, signal, mid, message):
        Thread(target=self.target, args=(client, senderId, signal, mid, message), daemon=True).start()


class Client:
    DefaultTimeout = 30

    def __init__(self, clientId, port, addr):
        self.PORT = port  # 26000
        self.ADDR = addr  # 'ff01::1' #IPV6 Multicast Address
        self.clientId = clientId
        self.closing = False

        addrInfo = socket.getaddrinfo(self.ADDR, None)[0]
        self.socket = socket.socket(addrInfo[0], socket.SOCK_DGRAM)
        self.socket.setsockopt(IPPROTO_IPV6, socket.IPV6_MULTICAST_HOPS, 1)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(('', self.PORT))

        #Join Multicast grp.
        group = socket.inet_pton(addrInfo[0], addrInfo[4][0])
        mreq = group + struct.pack('@I', 0)
        self.socket.setsockopt(IPPROTO_IPV6, socket.IPV6_JOIN_GROUP, mreq)

        self.__registeredBusInterfaces = dict()
        self.subPatterns = dict()
        self.subPatternLock = Lock()
        self.requestQueues = dict()
        self.inbox = []
        if not os.name == 'nt':
            r, w = os.pipe()
            self.sigKill = os.fdopen(w, 'w')
            self.isKilled = os.fdopen(r, 'r')

    def publish(self, message, channel):
        """
        Send a publication on the given channel
        :param message: the message to be published
        :param channel: the channel/topic to public the message on
        :return: the mid of the sent message
        """
        return multicasting.send(message, 'pub/' + channel, self)

    def request(self, receiverId, signal, message, timeout=None, retry=None):
        """
        Send request and block until reply received or timeout has expired
        :param receiverId: the id of a cliemt to receive the request
        :param signal: the interface name of the receiving client
        :param message: the message to the receiving client
        :param timeout: how long to block waiting for reply if None Client.DefaultTimeout is used
        :param retry: true for one retry on timeout or integer for that many retries on timeout
        :return: the frame for the message.
        :raises: Empty: If no reply received before timeout
        """
        timeout = timeout if timeout else Client.DefaultTimeout
        mid = uuid.uuid4().hex
        self.requestQueues[mid] = Queue(1)
        doretry = int(retry) if retry else 0
        try:
            return self.__dorequest('req/' + receiverId + '/' + signal, message, mid, timeout, doretry)
        finally:
            self.requestQueues.pop(mid)

    def __dorequest(self, channel, message, mid, timeout, retry):
        while True:
            multicasting.send(message, channel, self, mid)
            try:
                return self.requestQueues[mid].get(True, timeout)
            except Empty:
                if retry > 0:
                    retry -= 1
                else:
                    break

    def reply(self, reply, senderId, signal, mid):
        multicasting.send(reply, 'rep/' + senderId + '/' + signal, self, mid)

    def registerBusInterface(self, signal, callback):
        self.__registeredBusInterfaces[signal] = callback

    def subscribe(self, pattern, callback):
        try:
            self.subPatternLock.acquire()
            self.subPatterns[pattern] = re.compile(pattern), callback
        finally:
            self.subPatternLock.release()

    def unsubscribe(self, pattern):
        try:
            self.subPatternLock.acquire()
            self.subPatterns.pop(pattern)
        finally:
            self.subPatternLock.release()

    def run(self):
        while not self.closing:
            frame = multicasting.recv(self)
            if not frame:
                continue
            clientId, channel, mid, message = frame.split(',', 3)
            if clientId == self.clientId:
                continue
            if channel.startswith('req/' + self.clientId + '/'):
                signal = channel.split('/', 2)[2]
                if signal in self.__registeredBusInterfaces:
                    self._handleRequest(clientId, signal, mid, message)
            if channel.startswith('pub/'):
                topic = channel.split('/', 1)[1]
                try:
                    self.subPatternLock.acquire()
                    for pattern, callback in self.subPatterns.values():
                        if pattern.match(topic):
                            callback.call(self, clientId, topic, mid, message)
                finally:
                    self.subPatternLock.release()
            if channel.startswith('rep/' + self.clientId + '/'):
                if mid in self.requestQueues:
                    self.requestQueues[mid].put_nowait(frame)

    def _handleRequest(self, senderId, signal, mid, message):
        self.__registeredBusInterfaces[signal].call(self, senderId, signal, mid, message)

    def close(self):
        if self.closing:
            return
        self.closing = True
        if self.socket:
            self.socket.close()
        if not os.name == 'nt':
            if self.sigKill:
                self.sigKill.write("K\r\n")
                self.sigKill.flush()
                self.sigKill.close()
                time.sleep(0.01)
            if self.isKilled:
                self.isKilled.close()


class ThreadedClient(Client):

    def __init__(self, clientId, port=26000, address='ff01::1'):
        Client.__init__(self, clientId, port, address)
        t = Thread(target=self.run, daemon=True)
        t.start()

    def subscribe(self, pattern, callback, threaded=True, detailed=True):
        if isinstance(callback, Callback):
            Client.subscribe(self, pattern, callback)
        else:
            if threaded:
                if detailed:
                    Client.subscribe(self, pattern, ThreadedDetailedCallback(callback))
                else:
                    Client.subscribe(self, pattern, ThreadedCallback(callback))
            else:
                if detailed:
                    Client.subscribe(self, pattern, DetailedCallback(callback))
                else:
                    Client.subscribe(self, pattern, Callback(callback))

    def registerBusInterface(self, signal, callback, threaded=True, detailed=True):
        if isinstance(callback, Callback):
            Client.registerBusInterface(self, signal, callback)
        else:
            if threaded:
                Client.registerBusInterface(self, signal, ThreadedDetailedCallback(callback))
            else:
                if detailed:
                    Client.registerBusInterface(self, signal, DetailedCallback(callback))
                else:
                    Client.registerBusInterface(self, signal, Callback(callback))


