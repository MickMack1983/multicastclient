"""
    Client for communication over UDP Multicast with Tiip protocol
"""
from multicastclient.client import Client, Callback
from pytiip.tiip import TIIPMessage
from threading import Thread

__status__ = 'Development'


class TiipCallback(Callback):
    def __init__(self, target):
        Callback.__init__(self, target)

    def call(self, client, senderId, signal, mid, message):
        req = TIIPMessage(message)
        req.mid = mid
        reply = self.target(req)
        if reply:
            client.reply(str(reply), senderId, signal, mid)


class ThreadedTiipCallback(Callback):

    def __init__(self, target):
        Callback.__init__(self, target)

    def call(self, client, senderId, signal, mid, message):
        req = TIIPMessage(message)
        req.mid = mid
        Thread(target=self.target, args=(req,), daemon=True).start()


class TiipClient:

    def __init__(self, clientId, port=26000, address='ff01::1'):
        self.c = Client(clientId, port, address)
        t = Thread(target=self.run, daemon=True)
        t.start()

    def publish(self, message):
        message.type = "pub"
        self.c.publish(str(message), message.ch)

    def reply(self, reply, senderId):
        self.c.reply(str(reply), senderId, reply.sig, reply.mid)

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

    def getClosed(self):
        return self.c.closing

    closing = property(getClosed)


