from __future__ import print_function
import sys
import Pyro4
import threading
import socket
import copy
import time
from hashlib import md5
sys.excepthook = Pyro4.util.excepthook

'''
    we have to tell Pyro what parts of the class should be remotely accessible, and what parts aren’t supposed to
    be accessible. @Pyro4.expose decorator is used for this purpose.
 '''

@Pyro4.expose
class Peer(object):
    def __init__(self,IP,PORT):
        self.IP = IP
        self.PORT = PORT
        self.num_bits = 6
        self.ID = self.dhtHash(self.IP + ":" + self.PORT)
        self.FT = [None] * 7 # change it later
        self.NOTES_DICTIONARY = {}
        self.successor_id = -1
        self.successor_peer = None
        self.predecssor_id = -1
        self.predecssor_peer= None
        self.pred_addr = None
        print("> Own ID: {0}".format(self.ID))

    def dhtHash(self,s):
        h = md5(s.encode())  
        key = int(h.hexdigest(),16) % (2**self.num_bits)
        return key

    def connect(self,ip,port):
        uri = "PYRO:peer@"+ip+":"+port
        return Pyro4.Proxy(uri)

    def lookup(self,key):
        # add condition to check the keys values.
        # print("\n(Lookup at node :{0} for key {1}.)".format(self.ID,key))
        temp = "<-N:{0}".format(self.ID)     
        if self.successor_id == -1 and self.predecssor_id == -1: # only 1 node in the system.
            print(">> I am responsible for key {0}".format(key))
            # print("(Lookup ended. 1)\n")
            return (self.IP, self.PORT,temp,self.ID)
        elif key > self.predecssor_id and key  <= self.ID:
            print(">> I am responsible for key {0}".format(key))
            # print("(Lookup ended. 2)\n")
            return (self.IP, self.PORT,temp,self.ID)
        elif key > self.predecssor_id and self.predecssor_id > self.ID:
            print(">> I am responsible for key {0}".format(key))
            # print("(Lookup ended. 3)\n")                           
            return (self.IP, self.PORT,temp,self.ID)
        elif key <= self.ID and self.predecssor_id > self.ID:
            print(">> I am responsible for key {0}".format(key))
            # print("(Lookup ended. 4)\n")
            return (self.IP, self.PORT,temp,self.ID)    
        else:
            # print("(Lookup .... 5)")
            peer = self.findBestFTEntry(key)
            ip,port,path,temp_id  =   peer.lookup(key)
            # print("(Lookup ended. 5)\n")            
            return (ip,port,temp+path,temp_id)
            
    def findBestFTEntry(self,key):
        return self.successor_peer
    
    def getID(self):
        return self.ID

    def getPredID(self):
        return self.predecssor_id
    
    def getPredAddress(self):
        return self.pred_addr

    def setPred(self,id,ip,port):
        self.predecssor_id = id 
        self.predecssor_peer = self.connect(ip,port) #Pyro4.Proxy(uri)
        self.pred_addr = (self.predecssor_id,ip,port)
        print("(Pred Update: Pred ID = {0}, Own ID = {1}, Succ ID: {2})".format(self.predecssor_id, self.ID, self.successor_id))  

    def setSucc(self,id,ip,port):
        self.successor_id = id
        self.successor_peer = self.connect(ip,port)  #Pyro4.Proxy(uri)
        self.FT[0] = (ip,port,id)
        print("(Succ Update: Pred ID = {0}, Own ID = {1}, Succ ID: {2})".format(self.predecssor_id, self.ID, self.successor_id))
    
    def updateFingerTable(self):
        keys = []
        for i in range(0,len(self.FT)):
            temp = (self.ID + 2**i ) % (2**self.num_bits)
            keys.append(temp)
            ip,port,path,temp_id = self.lookup((self.ID + 2**i ) % (2**self.num_bits))
            self.FT[i] = (ip,port,temp_id)
        
        print("Keys : {0}".format(keys))
        print("Finger Table of Node {0}".format(self.ID))
        for i in range(0, len(self.FT)):
            print("{0} | {1}".format(i,self.FT[i][2]))

    def joinSetup(self,ip,port):
        # connecting to the correct successor in chord    
        self.successor_peer = self.connect(ip,port) 
        self.successor_id = self.successor_peer.getID()
        self.FT[0] = (ip,port,self.successor_id)
        if self.successor_peer.getPredID() == -1: # if there is only 1 peer in the chord
            # setting connected node pred and succ
            self.successor_peer.setPred(self.ID,self.IP,self.PORT)
            self.successor_peer.setSucc(self.ID,self.IP,self.PORT)
            # setting own succ and pred
            self.predecssor_id = self.successor_id
            self.predecssor_peer= self.connect(ip,port) 
            self.pred_addr = (self.predecssor_id,ip,port)
            
        else:
            # 1. Get ip and port of succ pred. 
            # 2  Update its pred to yourself. 
            # 3. Set your pred id,ip,port to parent to id, ip, port got in 2nd step. 
            # 4. Ask your pred to update its succ to me
            
            # step 1
            id,ip,port = self.successor_peer.getPredAddress()             
            
            # step 2
            self.successor_peer.setPred(self.ID,self.IP,self.PORT)
            # step 3
            self.predecssor_id = id
            self.predecssor_peer = self.connect(ip,port)
            self.pred_addr = (self.predecssor_id,ip,port)            
            # step 4
            self.predecssor_peer.setSucc(self.ID,self.IP,self.PORT)

        print("(Own ID = {0}, Pred ID = {1}, Succ ID: {2})".format(self.ID,self.predecssor_id,self.successor_id)) 

    
    # ============================== Client Handling ==================
    def handleJoin(self,ip,port):
        # ip = input("Enter friend's IP")
        bootstrap_peer = self.connect(ip,port) #Pyro4.Proxy(uri)
        ip,port,path,temp_id = bootstrap_peer.lookup(self.ID) # (disconnect not handled)
        print(">Lookup Path:  " + path)
        self.joinSetup(ip,port)
        print(">Joining Completed.")

    
    def menu(self,connect_port):
        ip = "localhost"
        if connect_port != "None":
            self.handleJoin(ip,connect_port)
        while True:
            print("\n*************************** Chord Menu  ****************************")
            print("1. Please enter 1 to join the Chord.")
            # print("2. Please enter 2 to leave the Chord.")
            # print("3. Please enter 3 to add a note.")
            # print("4. Please enter 4 to retrieve a note.")
            print("5. Update Finger Table.")
            print("6. Lookup Key.")

            n = input(">")
            
            if n == "1":
                port = input("Enter friends port: ")
                self.handleJoin(ip,port)
            elif n == "5":
                self.updateFingerTable()
            elif n == "6":
                key = input("Enter key: ")
                result =self.lookup(int(key))
                print("Result : {0}".format(result))
            else:
                print(">Error: Please select correct option.")
            print("======================================================================")
        
def main():    
    ip = "localhost"
    if len(sys.argv)==1:
        port =input("Enter PORT:")
    
    port = sys.argv[1]
    # port = int(port)
    PEER = Peer(ip,port)

    temp_port = None
    if len(sys.argv) < 3:
        temp_port = "None"
    else:
        temp_port = sys.argv[2]

    t = threading.Thread(target=PEER.menu, args=(temp_port,))
    t.start()

    Pyro4.Daemon.serveSimple({
            PEER: "peer"
        },
        ns=False,host=PEER.IP,port= int(PEER.PORT))

if __name__ == "__main__": 
    main()

