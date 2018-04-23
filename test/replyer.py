from threading import Thread
from multicastclient.client import Client, Callback


class Hepp:

    def __init__(self):
        self.c = Client('c1', 26000, 'ff01::1')
        self.c.registerBusInterface("print", Callback(self.printFrame))
        t = Thread(target=self.c.run)
        t.daemon = True
        t.start()

    @staticmethod
    def printFrame(message):
        print(message)
        return "This was Fun."


if __name__ == '__main__':
    h = Hepp()
    import time
    while True:
        time.sleep(100)

