from threading import Thread
from multicastclient.client import Client, DetailedCallback
import time


class Hepp:
    def __init__(self):
        self.c = Client('c4', 26000, 'ff01::1')
        self.c.subscribe("pelle", DetailedCallback(self.printFrame))
        self.c.subscribe("mo.*", DetailedCallback(self.printFrame))
        self.c.subscribe("moj.*", DetailedCallback(self.printFrame))
        t = Thread(target=self.c.run)
        t.daemon = True
        t.start()

    # noinspection PyUnusedLocal
    @staticmethod
    def printFrame(senderId, topic, mid, message):
        print(topic + "#" + message)

    def unsubscribe(self):
        self.c.unsubscribe("mo.*")

if __name__ == '__main__':
    h = Hepp()
    while True:
        time.sleep(10)
        h.unsubscribe()

