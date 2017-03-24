from threading import Thread
from multicastclient.client import Client, Callback
class hepp():
  def __init__(self):
     self.c = Client('c3', 26000, 'ff01::1')
     t = Thread(target=self.c.run)
     t.daemon = True
     t.start()

  def publish(self, channel):
      reply = self.c.publish( "This is a message" + channel, channel)
      print(reply)



h = hepp()
h.publish("pelle")
h.publish("nisse")
h.publish("mojje")
