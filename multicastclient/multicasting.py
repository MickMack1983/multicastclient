import uuid
import time
import select
import os
import logging


class ClientConstants(object):
    MAXSIZE = 16384
    SF_TIMEOUT = 10


def send(msg, channel, client, mid=None):
    mid = mid or uuid.uuid4().hex
    header = bytes(client.clientId + "," + channel + "," + mid + ",", 'UTF-8')
    jsonStringMsg = ""
    if not isinstance(msg, bytes):
        try:
            jsonStringMsg = bytes(msg, 'UTF-8')
        except Exception as e:
            logging.getLogger('multicast-send').error("msg is not a string" + str(type(msg)))
            raise e
    if len(header) > ClientConstants.MAXSIZE:
        raise Exception("Header is longer than msg MAXSIZE. Impossible to send")
    if len(header + jsonStringMsg) > ClientConstants.MAXSIZE:
        maxChunk = ClientConstants.MAXSIZE - len(header)
        for i in range(int(len(jsonStringMsg)/maxChunk)+1):
            client.socket.sendto(header + jsonStringMsg[maxChunk*i:maxChunk*(i+i)], (client.ADDR, client.PORT))
    else:
        client.socket.sendto(header + jsonStringMsg, (client.ADDR, client.PORT))
    time.sleep(0.01)         # Yield time :)
    return mid


def recv(client):  # recv packets
    """
    Receive a full frame of data:
            "<clientId>,<channel>,<mid>,<msg-body>"
    :param client: A multicast client with the following members:
            socket: a readable socket
            isKilled: a file descriptor which is readable only when socket is killed
            inbox: a list to store messages when assembling message parts
    """
    if client.closing:
        return
    if not client.inbox:
        client.inbox = list()
    if len(client.inbox) > 0:
        frame = client.inbox.pop(0)
    else:
        if os.name == 'nt':
            [rlist, _, _] = select.select([client.socket], [], [], ClientConstants.SF_TIMEOUT)
            if client.closing:
                return
        else:
            [rlist, _, _] = select.select([client.socket, client.isKilled], [], [], ClientConstants.SF_TIMEOUT)
        if client.socket in rlist:
            frame, _ = client.socket.recvfrom(ClientConstants.MAXSIZE)
        else:
            return  # Timeout

    if len(frame) == ClientConstants.MAXSIZE:  # Multiframe message
        return __handleMultiframe(client, frame)
    return frame.decode('UTF-8')


def __handleMultiframe(client, frame):
    framesegments = frame.split(',', 3)
    returnval = frame
    timestamp = time.time()
    if len(client.inbox) > 0:
        returnval, done = __multiInInbox(client, framesegments[0:3], returnval)
    else:
        done = False

    while not done and time.time() < (timestamp + ClientConstants.SF_TIMEOUT):
        """
            nextFrame: is either
            * a new meesage,
            * the remaining part(s) of current msg (all parts due to recursion)
            * None Timeout
        """
        nextFrame = recv(client)
        if not nextFrame:
            return  # Timeout
        nextSegments = nextFrame.split(',', 3)
        if framesegments[0:3] == nextSegments[0:3]:  # Compare headers
            returnval += nextSegments[3]  # append nextFrame messgage body
            done = True
        else:
            client.inbox.append(nextFrame)   # Not my message store for later
    if not done:
        return  # Timeout
    else:
        return returnval.decode('UTF-8')


def __multiInInbox(client, header, returnval):
    toberemoved = []
    done = False
    for i in range(len(client.inbox)):
        frame = client.inbox[i]
        framesegments = frame.split(',', 3)
        if header == framesegments[0:3]:
            toberemoved.append(i)
            returnval += framesegments[3]
            if len(frame) < ClientConstants.MAXSIZE:  # Is this the final segment?
                done = True
                break

    toberemoved.reverse()
    for i in toberemoved:  # remove reversed to maintain order
        client.inbox.remove(i)
    return returnval, done
