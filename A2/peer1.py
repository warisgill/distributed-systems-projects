from __future__ import print_function
import Pyro4
import threading
import socket

socket.gethostbyname(socket.gethostname())
HOST_IP = "127.0.0.1"    # Set accordingly (i.e. "192.168.1.99")
HOST_PORT = 9001         # Set accordingly (i.e. 9876)


# print(Pyro4.socketutil.getIpAddress())

'''
we have to tell Pyro what parts of the class should be remotely accessible, and what parts aren’t supposed to
be accessible. @Pyro4.expose decorator is used for this purpose.

@Pyro4.behavior(instance_mode="single") => it is required to properly have a persistent Peer inventory

 '''

@Pyro4.expose
@Pyro4.behavior(instance_mode="single")
class Peer(object):
    def __init__(self):
        # print("> Peer Created.")
        self.timestamp = []
        self.id = 0
    
    def greeting(self):
        return "Welcome"

    def testing(self):
        m = "No blocking"
        # m = input(">Server\n", )
        print(">Server result : +++ ", m)
        return m

    # neighbs will call this method to send me the message
    def message(self,message):
        print(">{0}".format(message))




# ============================== Client Handling ==================
def getNeighboursURI(fname):  
    content = []
    with open(fname) as f:
        content = f.readlines()
    # you may also want to remove whitespace characters like `\n` at the end of each line
    addr_list = [x.strip() for x in content]
    peers_list = [] 
    for addr in addr_list:
        peers_list.append("PYRO:peer@{0}".format(addr))
    return peers_list

def broadCast(m,peers):
    for peer in peers:
        peer.message(HOST_IP + "/{0} says: {1}".format(HOST_PORT,m))

def handleClient(PEER,neighbour_uris):
    FLAG = False
    neig_peers = []    
    while True:
        m = input()
        if FLAG == False:
            FLAG = True
            for uri in neighbour_uris:
                neig_peer = Pyro4.Proxy(uri)
                neig_peers.append(neig_peer)
        
        broadCast(m,neig_peers)
        

def main1(): 
    SERVER_PEER = Peer()
    args_tuple = (SERVER_PEER,getNeighboursURI("ip.txt"))
    
    t = threading.Thread(target=handleClient, args=args_tuple)
    t.start()

    Pyro4.Daemon.serveSimple(
        {
            SERVER_PEER: "peer"
        },
        ns=False, host = HOST_IP, port= HOST_PORT)
    

if __name__ == "__main__": 
    ips=getNeighboursURI(fname="ip.txt")
    print(ips)
    # main1()
