from threading import Thread
from multicastclient.client import Client


class Hepp:

    def __init__(self):
        self.c = Client('c3', 26000, 'ff01::1')
        t = Thread(target=self.c.run)
        t.daemon = True
        t.start()

    def publish(self, channel):
        reply = self.c.publish("This is a message" + channel, channel)
        print(reply)

if __name__ == '__main__':
    h = Hepp()
    h.publish("pelle")
    h.publish("nisse")
    h.publish("mojje")
