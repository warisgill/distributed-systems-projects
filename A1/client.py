import sys
import socket
import threading
from random import *


class Client:
    def __init__(self, server_ip, port):
        """
        This is the constructor of the client class. 

        Args:
            server_ip: The IP of the server which you want to connect. 
            port:  Port Number of the server 
        """
        self.server_ip = server_ip
        self.server_port = port
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        self.buffsize = 1024
        self.timestamp = randint(0, 101)
        self.quit = False
        self.username = ""
        self.lock_timestamp = threading.Lock()
        self.lock_quit = threading.Lock()
        self.threads = []

    def connect(self):
        """ 
            This method is used to connect to the server. It also create a new thread for user input
            and handle all events on client side. 
        """
        print("Local Timestamp: ", self.timestamp)
        self.timestamp += 1
        self.client_socket.connect((self.server_ip, self.server_port))

        username = input("Please enter your username: ")
        buffer = "{username},<{timestamp}>".format(
            username=username, timestamp=self.timestamp)

        self.timestamp += 1
        self.client_socket.sendall(bytes(buffer, "utf-8"))

        while True:
            buffer = self.client_socket.recv(self.buffsize)
            if len(buffer) == 0:
                print(">> Handle Server Exit.")
                self.client_socket.close()
                exit()

            buffer = buffer.decode('utf-8')
            #print("\n>>Check Quit", buffer)
            ts = self.getTimeStamp(buffer)
            self.incrementTimeStamp(ts)
            message = buffer.split(",")[0]

            with self.lock_quit:
                if self.quit:
                    break
            if message == "Accepted":
                self.username = username
                t = threading.Thread(target=self.sendMessage, args=())
                t.start()
                self.threads.append(t)
                print("\nAccepted.\n")
            elif message == "Rejected":

                print("Rejected.")
                username = input(
                    "\nYour username has already been taken, enter a new one: ")
                self.incrementTimeStamp(self.timestamp)
                buffer = "{username},<{timestamp}>".format(
                    username=username, timestamp=self.timestamp)
                self.client_socket.sendall(bytes(buffer, "utf-8"))

            elif message == "quit":
                #print("Check: here you go.")
                #self.client_socket.sendall(bytes("11", 'utf-8'))
                print("Exit Done.\n")
                break
            else:
                print("\n{message},<{timestamp}>\n".format(
                    message=message, timestamp=self.timestamp))

        self.client_socket.close()
        print("Exiting receiver thread.")

    def sendMessage(self):
        """
        This message is passed to the thread which is created in the connect() and 
        it handle the user interaction
        """
        while True:
            buffer = input("",)
            check = buffer
            self.incrementTimeStamp(self.timestamp)
            buffer = "{message},<{ts}>".format(
                message=buffer, ts=self.timestamp)
            self.client_socket.sendall(bytes(buffer, 'utf-8'))

            if check == self.username + " quit":
                with self.lock_quit:
                    self.quit = True
                break
        print("Exiting sender thread.")

    def incrementTimeStamp(self, timestamp):
        """
        This method is used to increment the timestamp according to lamport's algorithm.

        Args:    
            timestamp: timestamp to comapare.

        Returns: 
            updated timestamp 
        """
        temp = 0

        with self.lock_timestamp:
            self.timestamp = max(self.timestamp, timestamp) + 1
            temp = self.timestamp

        return temp

    def getTimeStamp(self, message):
        """
        This method will extract the timestamp from message and returns it as integer.
        Args:
            message: Its the message of type string.

        Returns: 
            It will return the timestamp of type int.
        """
        message = message.split('<')[1]
        message = message.split('>')[0]
        return int(message)


if __name__ == "__main__":
    if len(sys.argv) == 1:
        print("Please rerun the program and enter the IP of server.\n")
        exit()

    IP = sys.argv[1]
    PORT = 4444

    client = Client(IP, PORT)
    client.connect()
