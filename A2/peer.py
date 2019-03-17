from __future__ import print_function
import sys
import Pyro4
import threading
import socket
import copy
import time
sys.excepthook = Pyro4.util.excepthook


'''
    we have to tell Pyro what parts of the class should be remotely accessible, and what parts aren’t supposed to
    be accessible. @Pyro4.expose decorator is used for this purpose.
 '''


class Peer(object):
    def __init__(self):
        self.fname = "peers.txt"
        self.ID = None
        self.IP = None
        self.PORT = None
        self.n_uris = []
        self.n_peers = []
        self.buffer = []
        self.vector_clock = []
        self.v_lock = threading.Lock()
        self.intialize()

    @Pyro4.expose
    def messagePost(self,message_object):
        """
            This function is accessed remotely by other peers for posting message.
            Upon arrival of the message the recieveing peer compares the Vector clock
            sent with the message with its local vector clock. According to causally ordered multicasting conditions,
            it will either post the message or add it to the buffer.  
            params: 
                message_object: Tuple containing the message, vector clock and ID of the sender peer
        """
        message,vs,ids = message_object
        if self.checkRecv(vs,self.vector_clock,ids):
            with self.v_lock:
                self.vector_clock = vs            
            
            print(">{0},<{1}>,<{2}>".format(message,self.vector_clock,ids))
            self.updateBuffer()
        else:
            self.buffer.append((message,vs,ids))
            print(">Buffer Content After Addition : ", self.buffer)

    def incrementTimeStamp(self):
        """
            Updates the local vector clock by using locks for synchronization
        """
        with self.v_lock:
            self.vector_clock[self.ID] += 1
        print("<Updated local clock {0}>\n".format(self.vector_clock))

    def updateBuffer(self):
        """
            On any event that happens locally on the peer, this function loops over the buffer,
            compares the peer's vector clock with the timestamps of each message in the buffer.
            If any message fulfills causally ordered multicasting conditions, it is taken out of the buffer
            and delievered to the peer.
        """
        if len(self.buffer) == 0:
            return
        temp_buffer = []
        flag = False
        for i in range(0, len(self.buffer)):
            message,vs,ids = self.buffer[i]
            if self.checkRecv(vs,self.vector_clock,ids):
                with self.v_lock:
                    self.vector_clock = vs            
                print("*>{0},<{1}>,<{2}>".format(message,self.vector_clock,ids))
                flag = True
            else:
                temp_buffer.append((message,vs,ids))

        self.buffer = temp_buffer
        if flag == True:
            print(">Buffer Content After Removal : ", self.buffer)


    def checkRecv(self, vs, vr, ids):
        """
            Checks causally ordered multicasting conditions.
            params: 
                vs: vector timestamp of sender with the message
                vr: vector timestamp of the reciever 
                ids: index of the sender peer in the vector clock
            
            returns:
                boolean: if both causally ordered multicasting conditions
                        meet it returns True else False 
        """
        def compare(vs,vr,id):
            for i in range(0,len(vr)):
                if i != id:
                    if vs[i] > vr[i]:
                        return False
            return True  

        if vr[ids]+1 == vs[ids] and compare(vs,vr,ids):
            return True
        else:
            return False

    # ============================== Client Handling ==================
    def intialize(self):
        """
            Reads peers.txt and assigns the local peer its ip and port.
            Also, create URIS for the remote peers for creating connection.
        """  
        content = []
        i = 0
        self.IP = socket.gethostbyname(socket.gethostname())
        with open(self.fname) as f:
            content = f.readlines()
        
        content = [x.strip() for x in content]
        for addr in content:
            self.vector_clock.append(0)
            if self.IP in addr:
                self.ID = i 
                self.PORT = int(addr.split(":")[1])
                continue
            self.n_uris.append("PYRO:peer@"+addr)
            i += 1        

    def multiCast(self, message):
        """
            Multicasts message to remote peers.
            params:
                message: message from the user
            
            Note: We have intentionally added sleep to imitate delays in the messages
            to demonstrate causally ordered multicasting.
        """
        self.incrementTimeStamp() # increment timestap by one before multiCast    
        deep_v_timestamp = copy.deepcopy(self.vector_clock)
        for peer in  self.n_peers:
            m = "{0}/{1} says: {2}".format(self.IP,self.PORT,message)
            peer.messagePost((m,deep_v_timestamp,self.ID))
            deep_v_timestamp = copy.deepcopy(self.vector_clock)
            time.sleep(7*self.ID)
        self.updateBuffer()

    def handleInput(self):
        """
            Input thread will call this function, and this method will take the user 
            input and multicast to its neighbours. 
            params: self,neighbour_uris,h_ip,h_port

        """
        FLAG = False
        print("> Initial Vector Clock :", self.vector_clock)
        while True:
            m = input()
            if FLAG == False:
                FLAG = True
                for uri in self.n_uris:
                    neig_peer = Pyro4.Proxy(uri)
                    self.n_peers.append(neig_peer)
            
            self.multiCast(m)
        

def main():
    """
        This is the entry point of the peer instance. 
        It will intiallize 2 threads 1 for RMI object (main thread) and 1 for user input 
    """
    PEER = Peer()
      
    t = threading.Thread(target=PEER.handleInput, args=())
    t.start()

    Pyro4.Daemon.serveSimple(
        {
            PEER: "peer"
        },
        ns=False,host=PEER.IP,port= PEER.PORT)
    

if __name__ == "__main__": 
    main()
