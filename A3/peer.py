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
    def __init__(self,IP,PORT,daemon,n_bits, intro_ip, intro_port):
        self.IP = IP
        self.PORT = PORT
        self.DAEMON =  daemon
        self.num_bits = n_bits
        self.ID = self.dhtHash(self.IP + ":" + self.PORT)
        self.FT = [None] * self.num_bits # change it later
        self.NOTES_DICTIONARY = {}
        self.successor_id = -1
        self.successor_peer = None
        self.predecssor_id = -1
        self.predecssor_peer= None
        self.pred_addr = None
        self.Timer = None 
        self.interval = 8
        self.lock_FT = threading.Lock()
        self.bug_flag = False
        self.intro_ip = intro_ip
        self.intro_port = intro_port
        # self.timer_flag = False
        print("> Own ID: {0}".format(self.ID))

    def dhtHash(self,s):
        h = md5(s.encode())  
        key = int(h.hexdigest(),16) % (2**self.num_bits)
        return key

    def connect(self,ip,port):
        uri = "PYRO:peer@"+ip+":"+port
        return Pyro4.Proxy(uri)
    
    def getID(self):
        return self.ID

    def getPredID(self):
        return self.predecssor_id
    
    def getPredAddress(self):
        return self.pred_addr

    @Pyro4.oneway
    def setPred(self,id,ip,port):
        if id == self.ID:
            self.successor_id = -1
            self.successor_peer = None
            self.predecssor_id = -1
            self.predecssor_peer = None
            self.Timer.cancel()
            return
        self.predecssor_id = id
        self.predecssor_peer = self.connect(ip,port) #Pyro4.Proxy(uri)
        self.pred_addr = (self.predecssor_id,ip,port)
        print("(Pred Update: Pred ID = {0}, Own ID = {1}, Succ ID: {2})".format(self.predecssor_id, self.ID, self.successor_id))
        
        
    @Pyro4.oneway
    def setSucc(self,id,ip,port):
        
        if id == self.ID:
            self.successor_id = -1
            self.successor_peer = None
            self.predecssor_id = -1
            self.predecssor_peer = None
            self.Timer.cancel()
            return
        
        flag = self.successor_id
        self.successor_id = id
        self.successor_peer = self.connect(ip,port)  #Pyro4.Proxy(uri)
        with self.lock_FT:
            self.FT[0] = (ip,port,id)
        print("(Succ Update: Pred ID = {0}, Own ID = {1}, Succ ID: {2})".format(self.predecssor_id, self.ID, self.successor_id))        
        if flag == -1:
           self.__periodicStabilization()
              
    def updateFingerTable(self):
        keys = []
        flag = False
        with self.lock_FT:
            for i in range(0,len(self.FT)):
                key = (self.ID + 2**i ) % (2**self.num_bits)
                keys.append(key)
                ip,port,path,id = self.lookup(key)
                if self.FT[i] is None or self.FT[i][2] != id:
                    flag = True
                self.FT[i] = (ip,port,id)

        if flag == True:
            print("\n>Finger Table of Node {0}".format(self.ID))
            # print("Keys : {0}".format(keys))
            for i in range(0, len(self.FT)):
                print("         {0} | {1}".format(i,self.FT[i][2]))

        return flag         

    def lookup(self,key):
        def distance(x,y):
            return abs(x-y) % (2**self.num_bits)  
        def findBestFTEntry(key):
            if distance(key, self.ID) <= distance(self.FT[0][2],self.ID):
                # print(">Routed to FT entry successor {0}".format(self.FT[0][2]))
                return self.FT[0]
            for i in range(0,len(self.FT)):
                if distance(key, self.ID) <= distance(self.FT[i + 1][2], self.ID) or distance(key, self.ID) > distance(self.FT[i][2], self.ID):
                    # print(">Routed to FT entry {0}".format(self.FT[i][2]))
                    return self.FT[i]
            # print("> Routed to last FT entry: {0}".format(self.FT[len(self.FT)-1][2]))
            return self.FT[len(self.FT)-1]
        # add condition to check the keys values.
        # print("\n(Lookup at node :{0} for key {1}.)".format(self.ID,key))
        temp = "<-N{0}".format(self.ID)     
        if self.successor_id == -1 and self.predecssor_id == -1: # only 1 node in the system.
            # print("> I am responsible for key {0}".format(key))
            # print("(Lookup ended. 1)\n")
            return (self.IP, self.PORT,temp,self.ID)
        elif key > self.predecssor_id and key  <= self.ID:
            # print("> I am responsible for key {0}".format(key))
            # print("(Lookup ended. 2)\n")
            return (self.IP, self.PORT,temp,self.ID)
        elif key > self.predecssor_id and self.predecssor_id > self.ID:
            # print("> I am responsible for key {0}".format(key))
            # print("(Lookup ended. 3)\n")                           
            return (self.IP, self.PORT,temp,self.ID)
        elif key <= self.ID and self.predecssor_id > self.ID:
            # print("> I am responsible for key {0}".format(key))
            # print("(Lookup ended. 4)\n")
            return (self.IP, self.PORT,temp,self.ID)    
        else:
            # print("(Lookup 5 started.......... )\n")
            ip,port,id = findBestFTEntry(key)
            peer = self.connect(ip,port)
            ip,port,path,temp_id  =   peer.lookup(key)            
            # print("(Lookup ended. 5)\n")
            return (ip,port,temp+path,temp_id)

    def post(self,key,note):
        print("<<*Posting Note=>{0}, with key {1}, posted at node {2}.>>".format(note,key,self.ID))            
        note = note.split(":")
        if key in self.NOTES_DICTIONARY:
            subject,body, key = self.NOTES_DICTIONARY[key]
            self.NOTES_DICTIONARY[key] = (subject, body + "||" + note[1].strip(), key)
        else:
            self.NOTES_DICTIONARY[key] = (note[0],note[1].strip(),key)


    def get(self,key):
        if key in self.NOTES_DICTIONARY:
            return self.NOTES_DICTIONARY[key]
        else: 
            return "NULL"        

    def join(self,node_id):
        notes_list = []
        for key in self.NOTES_DICTIONARY.keys():
            if (key <= node_id) or (key > node_id and key> self.ID):
                notes_list.append(self.NOTES_DICTIONARY[key])
        
        for note in notes_list:
            self.NOTES_DICTIONARY.pop(note[2])
            print(">Key Removed: ", note[2])
        return notes_list

    def leave(self,notes_dict):
        print(">Notes Received from leaving node: {0}".format(len(notes_dict.keys())) )
        for key in notes_dict.keys():
            self.NOTES_DICTIONARY[key] = notes_dict[key]
            print(notes_dict[key])    

    def __periodicStabilization(self):
        self.bug_flag = True
        flag = self.updateFingerTable()
        if flag == True:
            self.timer_flag = False
            print("(Above FT is updated by timer)")
        self.Timer = threading.Timer(self.interval,self.__periodicStabilization)
        self.Timer.start()
    
    def __handleJoin(self,ip,port):
        # ip = input("Enter friend's IP")
        bootstrap_peer = self.connect(ip,port) #Pyro4.Proxy(uri)
        ip,port,path,temp_id = bootstrap_peer.lookup(self.ID) # (disconnect not handled)
        self.__handleJoinSetup(ip,port)
        self.__periodicStabilization()

        if self.successor_id == self.predecssor_id:
            self.successor_peer.updateFingerTable()
        else:
            self.successor_peer.updateFingerTable()
            self.predecssor_peer.updateFingerTable()

        notes  = self.successor_peer.join(self.ID)    
        for note in notes:
            self.NOTES_DICTIONARY[note[2]] = note
        print("Joined Setup: ( Pred ID = {0}, Own ID = {1}, Succ ID: {2})".format(self.predecssor_id,self.ID,self.successor_id))


        print(">Joining Completed.")
    
    def __handleJoinSetup(self,ip,port):
        # connecting to the correct successor in chord    
        self.successor_peer = self.connect(ip,port) 
        self.successor_id = self.successor_peer.getID()
        with self.lock_FT:
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
            """
            1. Get ip and port of succ pred. 2  Update its pred to yourself. 3. Set your pred id,ip,port to parent to id, ip, port got in 2nd step. 4. Ask your pred to update its succ to me
            """
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
            

    def __handleLeave(self):
        if self.successor_id != -1: 
            self.successor_peer.setPred(self.pred_addr[0],self.pred_addr[1],self.pred_addr[2])
            self.predecssor_peer.setSucc(self.FT[0][2],self.FT[0][0],self.FT[0][1])
            self.successor_peer.leave(self.NOTES_DICTIONARY)
            self.predecssor_peer.leave({})
            self.successor_peer.updateFingerTable()
            self.predecssor_peer.updateFingerTable()
            self.Timer.cancel()
        self.DAEMON.shutdown()
        print("== Good Bye ==")
        sys.exit()

    def __handleNoteInput(self):
        sub = input("Enter subject: ")
        body = input("Enter body: ")
        
        sub = sub.strip()
        body = body.strip()
        key = self.dhtHash(sub)
        line = sub+":"+body
        ip,port,path,id = self.lookup(key)
        print(">Lookup:: Key = {0}, Path = {1}".format(key,path))            
        if self.ID == id:
            self.post(key,line)
        else:
            peer = self.connect(ip,port)
            peer.post(key,line)

    
    def __handleReadNotesFromFile(self):
        fname = "notes.txt"
        # fname = input("Enter file name: ")
        lines = []
        with open(fname) as f:
            lines = f.readlines()
        # print(lines)
        print(">Posting notes from the {0}.".format(fname))
        lookup_times = []
        lookup_paths = []
        # lookup_keys  = []
        for line in lines: 
            note = line.split(':')
            # print(subject, self.dhtHash(subject))
            key = self.dhtHash(note[0])
            
            start = time.time()
            ip,port,path,id = self.lookup(key)
            end = time.time()
            t = end - start
            lookup_times.append(round(t,4))
            lookup_paths.append((key,path,len(path.split("<-"))-1))                
            # print(">Lookup:: Key = {0}, Path = {1}".format(key,path)) 
            # print("I am ")           
            # print("Key = {0}, Subject: {1}, Body: {2}".format(key,note[0],note[1]))
            if self.ID == id:
                self.post(key,line)
            else:
                peer = self.connect(ip,port)
                peer.post(key,line)
        
        print("> Lookup Paths: ", lookup_paths)
        print("> Time for {0} lookups.".format(len(lines)),lookup_times)

     
    def __handleRetrieveNote(self):
        sub = input("Enter the subject of Note:")
        key = self.dhtHash(sub.strip()) 
        ip,port,path,id = self.lookup(key)
        print(">Lookup:: Key = {0}, Path = {1}".format(key,path)) 
        note = None
        if id == self.ID:
            note = self.get(key)
        else:
            peer = self.connect(ip,port)
            note = peer.get(key)
        if note == "NULL":        
            print(">Error: No note with this subject exits in the chord.")
        else: 
            print(">Retrieved Note: ", note[1])
    
    def menu(self):

        if self.intro_ip is None:
            print(">Please select the 1 option to join chord")
        else:
            self.__handleJoin(self.intro_ip,self.intro_port)
            print(">Congratulation you have joinded the chord.")    

        while True:
            print("\n   **** Chord Menu  ****")
            print("1. Join the Chord.")
            print("2. Leave the Chord.")
            print("3. Post a note.")
            print("4. Post notes from text file.")
            print("5. Retrieve a note.")
            print("6. _Update Finger Table.")
            print("7. _Lookup Key.")
            print("8. _Print the stored notes.")

            n = input("Select Option:>")
            
            if n == "1":
                ip = input("Enter friend's IP: ")
                port = input("Enter friend's port: ")
                self.__handleJoin(ip,port)
            elif n == "2":
                self.__handleLeave()
            elif n == "3":
                self.__handleNoteInput()    
            elif n == "4":
                self.__handleReadNotesFromFile()
            elif n == "5":
                self.__handleRetrieveNote()
            elif n == "6":
                self.updateFingerTable()
            elif n == "7":
                key = input("Enter key: ")
                result =self.lookup(int(key))
                print(">>Result : {0}".format(result))
            elif n == "8":
                print("\n>>Stored Notes:")
                for key in self.NOTES_DICTIONARY:
                    print(key,self.NOTES_DICTIONARY[key][0],self.NOTES_DICTIONARY[key][1])
            else:
                print(">>Error: Please select correct option.")
            print("           =========== ")

 # roll 1
 #roll 2

def main():
    print("Please give command line argument in below 2 formats.")
    print("1. python3 peer.py number_of_bits own_port introducer_ip introducer_port")    
    print("2. python3 peer.py number_of_bits")
    print("Warning: If you will not follow the above format process will not run")    
    ip = socket.gethostbyname(socket.gethostname())
    num_bits = 128
    port = "4000"
    intro_ip = None
    intro_port = None

    if len(sys.argv) == 5:
        num_bits = int(sys.argv[1])
        port = sys.argv[2]
        intro_ip = sys.argv[3]
        intro_port = sys.argv[4]
    elif len(sys.argv) == 2:
        num_bits = int(sys.argv[1])
    else:
        print("command line args are incorrect.")
        sys.exit() 

    custom_daemon = Pyro4.Daemon(host=ip,port= int(port))
    PEER = Peer(ip,port,custom_daemon,num_bits,intro_ip,intro_port)
    t = threading.Thread(target=PEER.menu)
    t.start()

    Pyro4.Daemon.serveSimple({
            PEER: "peer"
        },
        ns=False,daemon=PEER.DAEMON)

if __name__ == "__main__": 
    main()

