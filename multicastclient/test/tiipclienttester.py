from multicastclient.tiipclient import TiipClient
from pytiip.tiip import TIIPMessage


class A:

    def __init__(self, name):
        self.tc = TiipClient(name)
        self.tc.registerBusInterface("hepp", self.hepp)
        self.tc.subscribe("nisse", self.happ)
        self.tc.subscribe("nisse", self.hipp)
        self.tc.subscribe("mojje", self.happ)

    def hepp(self, tmes):
        print(str(type(tmes)))
        print(str(tmes))
        repl = TIIPMessage(type="rep", ok=True, pl=["reply sent"], sig=tmes.sig, mid=tmes.mid)
        print("message to respond with = " + str(repl))
        self.tc.reply(repl, "/".join(tmes.src))

    def happ(self, tmes):
        print(str(type(tmes)))
        print("happ" + str(tmes))

    def hipp(self, tmes):
        print(str(type(tmes)))
        print("hipp" + str(tmes))

tc = TiipClient("a")
x = A("xxx")
tc.request(TIIPMessage(targ=["xxx"], sig="hepp", pl=["Message is received"]))
tc.publish(TIIPMessage(ch="nisse", pl=["nisse"]))
tc.publish(TIIPMessage(ch="mojje", pl=["asdasd"]))
tc.publish(TIIPMessage(ch="sdfse", pl=["asdasdagfreg"]))
