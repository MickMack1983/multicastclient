from threading import Thread
from multicastclient.client import Client


class Hepp:

    def __init__(self):
        self.c = Client('c2', 26000, 'ff01::1')
        t = Thread(target=self.c.run)
        t.daemon = True
        t.start()

    def request(self):
        reply = self.c.request('c1', 'print', "This is a message", 10)
        print(reply)


if __name__ == '__main__':
    h = Hepp()
    h.request()
