from threading import Thread
from tiipbusclient.client import Client


class Hepp:

    def __init__(self):
        self.c = Client('c2', 26000, 'ff01::1')
        t = Thread(target=self.c.run)
        t.daemon = True
        t.start()

    def request(self):
        msg = ""
        for i in range(73):
           msg += chr(ord('A') + i % 25)
        print("trying to send message:" + str(msg))
        reply = self.c.request('c1', 'print', msg, 10)

        print(reply)


if __name__ == '__main__':
    h = Hepp()
    h.request()
