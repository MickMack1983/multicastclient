import unittest
from threading import Thread, Lock
from multicastclient.client import ThreadedClient


class PubSubTestCase(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(PubSubTestCase, self).__init__(*args, **kwargs)
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

    def test_subscribe(self):
        pub = None
        pub_t = None
        try:
            pub, pub_t = self.__getClient('pub')
            count_lock = Lock()
            calledCount = 0

            # noinspection PyUnusedLocal
            def subCallback(client, senderId, signal, mid, message):
                nonlocal calledCount
                count_lock.acquire()
                try:
                    calledCount += 1
                finally:
                    count_lock.release()
                self.assertEqual(message, "Hello", "message did not match")

            self.c.subscribe("qwerty", subCallback)
            pub.publish("Hello", "qwerty")
            self.assertEqual(calledCount, 1, 'callback not called the correct amount of times')
        finally:
            if pub:
                pub.close()
                if pub_t:
                    pub_t.join(10)
                    if pub_t.is_alive():
                        print("failed to terminate pub thread")













