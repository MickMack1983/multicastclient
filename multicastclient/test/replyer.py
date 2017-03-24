from threading import Thread
from multicastclient.client import Client, Callback
class hepp():
  def __init__(self):
     self.c = Client('c1', 26000, 'ff01::1')
     self.c.registerBusInterface("print", Callback(self.printFrame))
     t = Thread(target=self.c.run)
     t.daemon = True
     t.start()

  def printFrame(self, message):
      print (message)
      return "This was Fun."



h = hepp()
import time
while (True):
    time.sleep(100)

