from threading import Thread
from multicastclient.client import Client, DetailedCallback
class hepp():
  def __init__(self):
     self.c = Client('c4', 26000, 'ff01::1')
     self.c.subscribe("pelle",DetailedCallback(self.printFrame))
     self.c.subscribe("mo.*", DetailedCallback(self.printFrame))
     self.c.subscribe("moj.*", DetailedCallback(self.printFrame))
     t = Thread(target=self.c.run)
     t.daemon = True
     t.start()

  def printFrame(self, senderId, topic, mid, message):
      print (topic + "#" + message)

  def unsubscribe(self):
      self.c.unsubscribe("mo.*")



h = hepp()
import time
while (True):
    time.sleep(10)
    h.unsubscribe()

