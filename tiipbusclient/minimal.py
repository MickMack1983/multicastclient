import socket
import struct
import select
import sys

from socket import IPPROTO_IPV6

PORT =  26000
ADDR = 'ff01::1' #IPV6 Multicast Address

addrInfo = socket.getaddrinfo(ADDR, None)[0]
sock = socket.socket(addrInfo[0], socket.SOCK_DGRAM)
sock.setsockopt(IPPROTO_IPV6, socket.IPV6_MULTICAST_HOPS, 1)
sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
sock.bind(('', PORT))

#Join Multicast grp.
group = socket.inet_pton(addrInfo[0], addrInfo[4][0])
mreq = group + struct.pack('@I', 0)
sock.setsockopt(IPPROTO_IPV6, socket.IPV6_JOIN_GROUP, mreq)


message = "A" * int(sys.argv[1])

worked = sock.sendto(bytes(message, 'UTF-8'), (ADDR, PORT))
print(worked)

[rlist, _, _] = select.select([sock], [], [], 2)
if sock in rlist:
    bframe, _ = sock.recvfrom(32768)
    frame = bframe.decode('UTF-8')
    print(frame)

