"""
    Client for communication over UDP Multicast
"""
import socket
import uuid
import os
import time
from tiipbusclient import multicasting
import re
import struct
from queue import Queue, Empty
#from pyloggedthread.loggedthread import ErrorLoggedThread as Thread
from threading import Thread
from threading import Lock

if os.name == 'nt':
    IPPROTO_IPV6 = 41  # Workaround for missing constants on Win32 systems.
else:
    # noinspection PyUnresolvedReferences
    from socket import IPPROTO_IPV6

__status__ = 'Development'


class Callback:
    """
        A callback object, for use with the multicast client
    """

    def __init__(self, target):
        """
        Construct a callback object, for use with the multicast client, that uses the target function
        :param target: The function to execute when running the Callback. The function should take a message as argument and return a message as return value
        """
        self.target = target

    def call(self, client, senderId, signal, mid, message):
        """
        The call function that executes the target function.
        :param client: The multicast client that controls this Callback object.
        :param senderId: The id of the multicast client .
        :param signal: The signal of the reply message.
        :param mid: The message id of the request to respond to.
        :param message: The actual message to send as an argument to the target function
        :return: None (but the clients reply function is invoked with the return value of the target function)
        """
        reply = self.target(message)
        if reply:
            client.reply(reply, senderId, signal, mid)


class DetailedCallback(Callback):
    """
        A callback object, for use with the multicast client, that uses longer more detailed callbacks
    """

    def __init__(self, target):
        """
        Construct a callback object, for use with the multicast client, that uses the target function
        :param target: The function to execute when running the Callback. The function should take a client, senderId, signal, mid and message as arguments and return a message as return value
        """
        Callback.__init__(self, target)

    def call(self, client, senderId, signal, mid, message):
        """
        The call function that executes the target function.
        :param client: The multicast client that controls this Callback object.
        :param senderId: The id of the multicast client .
        :param signal: The signal of the reply message.
        :param mid: The message id of the request to respond to.
        :param message: The actual message to send as an argument to the target function
        :return: None (but the clients reply function is invoked with the return value of the target function)
        """
        reply = self.target(senderId, signal, mid, message)
        if reply:
            client.reply(reply, senderId, signal, mid)


class ThreadedCallback(Callback):
    """
        A callback object, for use with the multicast client, that spawns a thread to run the target function
    """

    def __init__(self, target):
        """
        Construct a callback object, for use with the multicast client, that uses the target function
        :param target: The function to execute when running the Callback. The function should take a message as argument and return a message as return value
        """
        Callback.__init__(self, target)

    def call(self, client, senderId, signal, mid, message):
        """
        The call function that executes the target function.
        :param client: The multicast client that controls this Callback object.
        :param senderId: The id of the multicast client .
        :param signal: The signal of the reply message.
        :param mid: The message id of the request to respond to.
        :param message: The actual message to send as an argument to the target function
        :return: None (the reply needs to be handled in the target function)
        """
        Thread(target=self.target, args=message, daemon=True).start()


class ThreadedDetailedCallback(Callback):
    """
        A callback object, for use with the multicast client, that spawns a thread to run the target function and use the longer argument format
    """

    def __init__(self, target):
        """
        Construct a callback object, for use with the multicast client, that uses the target function
        :param target: The function to execute when running the Callback. The function should take a client, senderId, signal, mid and message as arguments and return a message as return value
        """
        Callback.__init__(self, target)

    def call(self, client, senderId, signal, mid, message):
        """
        The call function that executes the target function.
        :param client: The multicast client that controls this Callback object.
        :param senderId: The id of the multicast client .
        :param signal: The signal of the reply message.
        :param mid: The message id of the request to respond to.
        :param message: The actual message to send as an argument to the target function
        :return: None (the reply needs to be handled in the target function)
        """
        Thread(target=self.target, args=(client, senderId, signal, mid, message), daemon=True).start()


class Client:
    """
    A client for sending stringbased messages over UDP multicast in a request, reply, publish, subscribe manner.
    """
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
        self.inboxLock = Lock()
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
        """
        Send a reply to a request
        :param reply: the message to send as a reply
        :param senderId: the id of the client to receive the reply
        :param signal: the signal of the request which is replying
        :param mid: the message id to reply to
        """
        multicasting.send(reply, 'rep/' + senderId + '/' + signal, self, mid)

    def registerBusInterface(self, signal, callback):
        """
        register an API to handle with a callback
        :param signal: the API name to handle
        :param callback: the function to run when the API is called
        """
        self.__registeredBusInterfaces[signal] = callback

    def unregisterBusInterface(self, signal):
        """
        Unregister an API
        :param signal: the API name to remove all callbacks for
        """
        self.__registeredBusInterfaces.pop(signal)

    def subscribe(self, pattern, callback):
        """
        subscribe to messages with the following pattern
        :param pattern: The pattern to match
        :param callback: the Callback object to run when a publication is received
        """
        try:
            self.subPatternLock.acquire()
            self.subPatterns[pattern] = re.compile(pattern), callback
        finally:
            self.subPatternLock.release()

    def unsubscribe(self, pattern):
        """
        remove subscription to pattern
        :param pattern: The pattern to match
        """
        try:
            self.subPatternLock.acquire()
            self.subPatterns.pop(pattern)
        finally:
            self.subPatternLock.release()

    def lock(self):
        self.inboxLock.acquire()

    def unlock(self):
        self.inboxLock.release()


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
        """
        subscribe to messages with the following pattern
        :param pattern: The pattern to match
        :param callback: the Callback object or a function to run when a publication is received
        :param threaded: should the callback be handled in a new thread?  (only if callback is a function)
        :param detailed: should the callback function use the long arg form? (only if callback is a function)
        """
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
        """
        register an API to handle with a callback
        :param signal: the API name to handle
        :param callback: the Callback object or a function to run when a request is received
        :param threaded: should the callback be handled in a new thread?  (only if callback is a function)
        :param detailed: should the callback function use the long arg form? (only if callback is a function)
        """
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


