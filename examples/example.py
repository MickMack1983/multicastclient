from tiipbusclient.tiipclient import TiipClient
from pytiip.tiip import TIIPMessage

class Application:
    def __init__(self, myID):
        self.id = myID
        self.client = TiipClient(myID)
        self.client.registerBusInterface("doStuff", self.doStuff)
        self.client.subscribe("messages", self.handleMessage)
        self.client.publish(TIIPMessage(ch="messages", pl=["I'm Alive"]))

    def doStuff(self, request):
        print (self.id + " was requested: " + str(request))
        self.client.reply(request, TIIPMessage(pl=["Printed your request"]))

    def handleMessage(self, publication):
        print (self.id + " received: " + str(publication))

    def request(self, clientID, api, argDict):
        reply = self.client.request(TIIPMessage(targ=[clientID], sig=api, arg=argDict))
        if reply and reply.ok:
            print("All went well")

if __name__ == '__main__':
    a = Application("First")
    b = Application("Second")
    a.request("Second", "doStuff", {})

