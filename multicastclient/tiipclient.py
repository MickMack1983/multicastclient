"""
    Client for communication over UDP Multicast with Tiip protocol
"""
from multicastclient.client import Client, Callback
from pytiip.tiip import TIIPMessage
from threading import Thread

__status__ = 'Development'


class TiipCallback(Callback):
    """
        A callback object, for use with the multicast client, that uses TIIP based messages
    """
    def __init__(self, target):
        """
        Construct a callback object, for use with the multicast client, that uses the target function
        :param target: The function to execute when running the Callback. The function should take a TIIPMessage object as arguments and return a TIIPMessage as return value
        """
        Callback.__init__(self, target)

    def call(self, client, senderId, signal, mid, message):
        """
        The call function that executes the target function.
        :param client: The multicast client that controls this Callback object.
        :param senderId: The id of the multicast client .
        :param signal: The signal of the reply message.
        :param mid: The message id of the request to respond to.
        :param message: The actual message to send as an argument to the target function as a string
        :return: None (but the clients reply function is invoked with the return value of the target function)
        """
        req = TIIPMessage(message)
        req.mid = mid
        reply = self.target(req)
        if reply:
            client.reply(str(reply), senderId, signal, mid)


class ThreadedTiipCallback(Callback):
    """
        A callback object, for use with the multicast client, that spawns a thread to run the target function with a TIIPMessage
    """
    def __init__(self, target):
        """
        Construct a callback object, for use with the multicast client, that uses the target function
        :param target: The function to execute when running the Callback. The function should take a TIIPMessage as argument and return a TIIPMessage as return value
        """
        Callback.__init__(self, target)

    def call(self, client, senderId, signal, mid, message):
        """
        The call function that executes the target function.
        :param client: The multicast client that controls this Callback object.
        :param senderId: The id of the multicast client .
        :param signal: The signal of the reply message.
        :param mid: The message id of the request to respond to.
        :param message: a stringified TIIPMessage to send as an argument to the target function
        :return: None (the reply needs to be handled in the target function)
        """
        req = TIIPMessage(message)
        req.mid = mid
        Thread(target=self.target, args=(req,), daemon=True).start()


class TiipClient:
    """
    A TIIPMessage aware multicastclient
    """

    def __init__(self, clientId, port=26000, address='ff01::1'):
        self.c = Client(clientId, port, address)
        t = Thread(target=self.run, daemon=True)
        t.start()

    def publish(self, message):
        message.type = "pub"
        self.c.publish(str(message), message.ch)

    def reply(self, request, reply):
        reply.mid = request.mid
        reply.sig = request.sig
        self.c.reply(str(reply), request.src[-1], reply.sig, reply.mid)

    def unsubscribe(self, pattern):
        self.c.unsubscribe(pattern)

    def lock(self):
        self.c.lock()

    def unlock(self):
        self.c.unlock()

    def run(self):
        self.c.run()

    def close(self):
        self.c.close()

    def request(self, message, timeout=None, retry=None):
        if message.src:
            if not message.src[-1] == self.c.clientId:
                message.src.append(self.c.clientId)
        else:
            message.src = [self.c.clientId]
        responseString = self.c.request("/".join(message.targ), message.sig, str(message), timeout, retry)
        if responseString:
            _, _, _, reply = responseString.split(',', 3)
            return TIIPMessage(reply)

    def subscribe(self, pattern, callback, threaded=True):
        if isinstance(callback, Callback):
            self.c.subscribe(pattern, callback)
        else:
            if threaded:
                self.c.subscribe(pattern, ThreadedTiipCallback(callback))
            else:
                self.c.subscribe(pattern, TiipCallback(callback))

    def registerBusInterface(self, signal, callback, threaded=True):
        if isinstance(callback, Callback):
            self.c.registerBusInterface(signal, callback)
        else:
            if threaded:
                self.c.registerBusInterface(signal, ThreadedTiipCallback(callback))
            else:
                self.c.registerBusInterface(signal, TiipCallback(callback))

    def unregisterBusInterface(self, signal, callback, threaded=True):
        self.c.unregisterBusInterface(signal, TiipCallback(callback))

    def getClosed(self):
        return self.c.closing

    closing = property(getClosed)



