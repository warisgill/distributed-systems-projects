from __future__ import print_function

import socket
import sys
import threading
import time
from hashlib import md5

import Pyro4

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
        self.predecessor_id = -1
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

    def dhtHash(self, name):
        """

        :param name: variable whose hash has to be taken
        :return: hash value of the name using md5
        """
        hash_of_name = md5(name.encode())
        key = int(hash_of_name.hexdigest(),16) % (2**self.num_bits)
        return key

    def connect(self, ip, port):
        """

        :param ip: IP of the peer to be connected to
        :param port: PORT of the peer to be connected to
        :return: object of the peer connected to
        """
        uri = "PYRO:peer@"+ip+":"+port
        return Pyro4.Proxy(uri)
    
    def getID(self):
        """

        :return: ID of caller node
        """
        return self.ID

    def getPredID(self):
        """

        :return: ID of caller's predecessor node
        """
        return self.predecessor_id
    
    def getPredAddress(self):
        """

        :return: IP, Port and ID of caller's predecessor node
        """
        return self.pred_addr

    @Pyro4.oneway
    def setPred(self, id, ip, port):
        """
        This function sets the predecessor of the caller node and connects to it. If there is only one node in the
        system then successor and predecessor of the caller node is set to -1

        :param id: ID of the predecessor node
        :param ip: IP of the predecessor node
        :param port: PORT of the predecessor node
        :return: None
        """
        if id == self.ID:
            self.successor_id = -1
            self.successor_peer = None
            self.predecessor_id = -1
            self.predecssor_peer = None
            self.Timer.cancel()
            return
        self.predecessor_id = id
        self.predecssor_peer = self.connect(ip,port) #Pyro4.Proxy(uri)
        self.pred_addr = (self.predecessor_id,ip,port)
        print("(Pred Update: Pred ID = {0}, Own ID = {1}, Succ ID: {2})".format(self.predecessor_id, self.ID, self.successor_id))

    @Pyro4.oneway
    def setSucc(self, id, ip, port):
        """
        This function sets the successor of the caller node and connects to it. If there is only one node in the
        system then successor and predecessor of the caller node is set to -1. The finger table of the caller node is
        also updated.

        :param id: ID of the successor node
        :param ip: IP of the successor node
        :param port: PORT of the successor node
        :return: None
        """
        if id == self.ID:
            self.successor_id = -1
            self.successor_peer = None
            self.predecessor_id = -1
            self.predecssor_peer = None
            self.Timer.cancel()
            return
        
        flag = self.successor_id
        self.successor_id = id
        self.successor_peer = self.connect(ip,port)
        with self.lock_FT:
            self.FT[0] = (ip, port, id)
        print("(Succ Update: Pred ID = {0}, Own ID = {1}, Succ ID: {2})".format(self.predecessor_id, self.ID, self.successor_id))        
        if flag == -1:
           self.__periodicStabilization()
              
    def updateFingerTable(self):
        """
        This function updates the finger table of the caller node
        :return: flag suggesting that the finger table of the caller node is successfully updated
        """
        keys = []
        flag = False
        with self.lock_FT:
            for i in range(0, len(self.FT)):
                key = (self.ID + 2**i ) % (2**self.num_bits)
                keys.append(key)
                ip, port, path, id = self.lookup(key)
                if self.FT[i] is None or self.FT[i][2] != id:
                    flag = True
                self.FT[i] = (ip, port, id)

        if flag == True:
            print("\n>Finger Table of Node {0}".format(self.ID))
            for i in range(0, len(self.FT)):
                print("         {0} | {1}".format(i, self.FT[i][2]))

        return flag         

    def lookup(self,key):
        """

        :param key: key for which it's responsible node has to be found
        :return: IPm PORT and ID of the node responsible for the key
        """
        def distance(x,y):
            return abs(x-y) % (2**self.num_bits)  
        def findBestFTEntry(key):
            if distance(key, self.ID) <= distance(self.FT[0][2],self.ID):
                return self.FT[0]
            for i in range(0,len(self.FT)):
                if distance(key, self.ID) <= distance(self.FT[i + 1][2], self.ID) or distance(key, self.ID) > distance(self.FT[i][2], self.ID):
                    return self.FT[i]
            return self.FT[len(self.FT)-1]
        temp = "<-N{0}".format(self.ID)     
        if self.successor_id == -1 and self.predecessor_id == -1:
            return (self.IP, self.PORT,temp,self.ID)
        elif key > self.predecessor_id and key  <= self.ID:
            return (self.IP, self.PORT,temp,self.ID)
        elif key > self.predecessor_id and self.predecessor_id > self.ID:
            return (self.IP, self.PORT,temp,self.ID)
        elif key <= self.ID and self.predecessor_id > self.ID:
            return (self.IP, self.PORT,temp,self.ID)    
        else:
            ip,port,id = findBestFTEntry(key)
            peer = self.connect(ip,port)
            ip,port,path,temp_id  =   peer.lookup(key)
            return (ip,port,temp+path,temp_id)

    def post(self, key, note, poster_id):
        """
        This function stores the note in the caller NOTES_DICTIONARY. If it recieves a note with the same subject as it
        already has, then it appends the new body with old body for that key
        :param key: hash value of subject of the note
        :param note: subject:body
        :return: None
        """
        print("<< Poster ID: {0}, Posting Note: {1}, with key {2}, Posted at node {3}.>>".format(poster_id, note,
                                                                                                 key, self.ID))
        note = note.split(":")
        if key in self.NOTES_DICTIONARY:
            subject,body, key = self.NOTES_DICTIONARY[key]
            self.NOTES_DICTIONARY[key] = (subject, note[1].strip() + " " + body, key)
        else:
            self.NOTES_DICTIONARY[key] = (note[0],note[1].strip(),key)

    def get(self, key):
        """
        This function retrieves the body of the note for the given key
        :param key: hash value of subject of the note
        :return: NULL or the body of note related to key of the subject
        """
        if key in self.NOTES_DICTIONARY:
            return self.NOTES_DICTIONARY[key]
        else: 
            return "NULL"        

    def join(self, node_id):
        """

        :param node_id: ID of incoming node
        :return: list of notes that should be shiftedd to the incoming node
        """
        send_notes_list = []
        my_remaining_notes = {}
        for key in self.NOTES_DICTIONARY.keys():
            if key > node_id and key  <= self.ID: # done
                my_remaining_notes[key] =  self.NOTES_DICTIONARY[key]
            elif key > node_id and node_id > self.ID:
                my_remaining_notes[key] =  self.NOTES_DICTIONARY[key]
            elif key <= self.ID and node_id > self.ID:
                my_remaining_notes[key] =  self.NOTES_DICTIONARY[key]   
            else:
                send_notes_list.append(self.NOTES_DICTIONARY[key])

        self.NOTES_DICTIONARY = my_remaining_notes
        for note in send_notes_list:
            print(">Key Removed: ", note[2])
        return send_notes_list

    def leave(self,notes_dict):
        """
        This fucntion recieves dictionary of notes from the leaving node and append to the caller node's NOTES

        :param notes_dict: dictionary of notes recieved from the leaving node
        :return: None
        """
        print(">Notes Received from leaving node: {0}".format(len(notes_dict.keys())) )
        for key in notes_dict.keys():
            self.NOTES_DICTIONARY[key] = notes_dict[key]
            print(notes_dict[key])    

    def __periodicStabilization(self):
        """
        This function updates the finger table of the caller function and starts a Timer for periodiccally updating
        the caller's finger table after fixed intervals
        :return: None
        """
        self.bug_flag = True
        flag = self.updateFingerTable()
        if flag == True:
            self.timer_flag = False
        self.Timer = threading.Timer(self.interval,self.__periodicStabilization)
        self.Timer.start()
    
    def __handleJoin(self, ip, port):
        """
        This function connects the incoming node to the introducer node. Introducter node finds the successor the
         incoming node in the system. Then it tries to updated the finger table of the new node, new node's successor
         and new node's predecessor. Finally it checks if the new node's successor needs to send notes to the new node
        :param ip: IP of the incoming node
        :param port: PORT of the incoming node
        :return: None
        """
        bootstrap_peer = self.connect(ip,port)
        ip,port,path,temp_id = bootstrap_peer.lookup(self.ID)
        self.__handleJoinSetup(ip,port)
        self.__periodicStabilization()

        if self.successor_id == self.predecessor_id:
            self.successor_peer.updateFingerTable()
        else:
            self.successor_peer.updateFingerTable()
            self.predecssor_peer.updateFingerTable()

        notes  = self.successor_peer.join(self.ID)    
        for note in notes:
            self.NOTES_DICTIONARY[note[2]] = note
        print("Joined Setup: ( Pred ID = {0}, Own ID = {1}, Succ ID: {2})".format(self.predecessor_id,self.ID,self.successor_id))
        print(">Joining Completed.")
    
    def __handleJoinSetup(self,ip,port):
        """
        This function connects the incoming node to it's successor and predecessor. It updates the predecessor of
        incoming node's successor and the successor of the incoming node's predecessor
        :param ip: IP of the incoming node
        :param port: PORT of the incoming node
        :return: None
        """
        self.successor_peer = self.connect(ip,port) 
        self.successor_id = self.successor_peer.getID()
        with self.lock_FT:
            self.FT[0] = (ip,port,self.successor_id)
        if self.successor_peer.getPredID() == -1: # if there is only 1 peer in the chord
            self.successor_peer.setPred(self.ID,self.IP,self.PORT)
            self.successor_peer.setSucc(self.ID,self.IP,self.PORT)
            self.predecessor_id = self.successor_id
            self.predecssor_peer= self.connect(ip,port) 
            self.pred_addr = (self.predecessor_id,ip,port)
        else:
            id,ip,port = self.successor_peer.getPredAddress()
            self.successor_peer.setPred(self.ID,self.IP,self.PORT)
            self.predecessor_id = id
            self.predecssor_peer = self.connect(ip,port)
            self.pred_addr = (self.predecessor_id,ip,port)
            self.predecssor_peer.setSucc(self.ID,self.IP,self.PORT)

    def __handleLeave(self):
        """
        This function updates the predecessor and successor finger tables and their successor and predecessor
        respectively. It also moves the notes of the leaving node to it's successor.
        :return: None
        """
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
        """
        This function takes the subject and body of note from user and looks up the key of the subject in the system.
        It informs about the lookup path and it posts the note to the responsible node.
        :return: None
        """
        sub = input("Enter subject: ")
        body = input("Enter body: ")
        
        sub = sub.strip()
        body = body.strip()
        key = self.dhtHash(sub)
        line = sub+":"+body
        ip,port,path,id = self.lookup(key)
        print(">Lookup:: Key = {0}, Path = {1}".format(key,path))            
        if self.ID == id:
            self.post(key, line, self.ID)
        else:
            peer = self.connect(ip,port)
            peer.post(key, line, self.ID)

    def __handleReadNotesFromFile(self):
        """
        Th0is function takes the subject and body of note from the file and looks up the key of the subject in the system.
        It informs about the lookup paths and it posts the notes to the responsible node. Additionally, it informs about
        the time taken for lookup for each note.
        :return: None
        """
        fname = "input.txt"
        lines = []
        with open(fname) as f:
            lines = f.readlines()
        print(">Posting notes from the {0}.".format(fname))
        lookup_times = []
        lookup_paths = []
        for line in lines: 
            note = line.split(':')
            key = self.dhtHash(note[0])
            
            start = time.time()
            ip,port,path,id = self.lookup(key)
            end = time.time()
            t = end - start
            lookup_times.append(round(t,4))
            lookup_paths.append((key,path,len(path.split("<-"))-1))
            if self.ID == id:
                self.post(key,line,self.ID)
            else:
                peer = self.connect(ip,port)
                peer.post(key,line,self.ID)
        
        print("> Lookup Paths: ", lookup_paths)
        print("> Time for {0} lookups.".format(len(lines)),lookup_times)

    def __handleRetrieveNote(self):
        """
        This function gets user input for the subject for which the note is to be retrieved and looks up for that note.
        :return: None
        """
        sub = input("Enter the subject of Note:")
        key = self.dhtHash(sub.strip()) 
        start = time.time()
        ip,port,path,id = self.lookup(key)
        end = time.time()
        t = end - start
        # print("> Lookup Time")
        print(">Lookup:: Key = {0}, Path = {1}, \n Lookup Time (sec) = {2}".format(key,path,t)) 
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
        """
        Display the possible actions of the user to perform on the system.
        """
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
                self.__handleJoin(ip, port)
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


def main():
    """
    This is the beginning of the program, If you are first node then use format 1 to join the system, If you are the
    second or any later node then you can use any format to join the system.
    """
    print("Please give command line argument in below 2 formats.")
    print("1. python3 peer.py number_of_bits")
    print("2. python3 peer.py number_of_bits own_port introducer_ip introducer_port")
    print("Warning: If you will not follow the above format process will not run")    
    ip = socket.gethostbyname(socket.gethostname())
    num_bits = 128
    port = "4444"
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
        ns=False,
        daemon=PEER.DAEMON
    )

if __name__ == "__main__":
    main()

