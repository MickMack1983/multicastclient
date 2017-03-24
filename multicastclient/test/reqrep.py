import unittest
from threading import Thread, Lock, ThreadError
from multicastclient.client import ThreadedClient
from multicastclient.client import Callback


class ReqRepTestCase(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(ReqRepTestCase, self).__init__(*args, **kwargs)
        self.clientCount = 0

    def __getClient(self, prefix):
        c = ThreadedClient(prefix + str(self.clientCount), 26000, 'ff01::1')
        self.clientCount += 1
        t = Thread(target=c.run)
        t.daemon = True
        t.start()
        return c, t

    def setUp(self):
        self.c, self.t = self.__getClient('client')

    def tearDown(self):
        self.c.close()
        self.c = None
        self.t.join(timeout=10)
        if self.t.is_alive():
            print("failed to terminate c thread")
        self.t = None

    def test_request(self):
        try:
            req , req_t = self.__getClient('request')
            count_lock = Lock()
            calledCount = 0

            def printFrame(self, message):
                nonlocal calledCount
                count_lock.acquire()
                try:
                    calledCount += 1
                finally:
                    count_lock.release()
                self.assertEqual(message, "Hello", "message did not match")
                return "Reply"

            self.c.registerBusInterface("print", Callback(self.printFrame))
            t = Thread(target=self.c.run)
            t.daemon = True
            t.start()


            reply = req.request("client0","print","Hello", 10)
            clientId, channel, mid, message = reply.split(',', 3)

            pub.publish("Hello", "qwerty")
            self.assertEqual(calledCount, 1, 'callback not called the correct amount of times')
        finally:
            if pub:
                pub.close()
                pub = None
                if pub_t:
                    pub_t.join(10)
                    if pub_t.is_alive():
                        print("failed to terminate pub thread")













