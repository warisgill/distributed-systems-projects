import socket
import threading
import time


class Server:
    def __init__(self, ip, port):
        """
        It's the constructor of the server class. 
        Args:    
           ip:  IP of the server
           port: Port of the server at which it will start listening

        """
        self.ip = ip
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind((self.ip, self.port))

        self.timestamp = 0
        self.buffsize = 1024
        self.clients = {}
        self.threads = []
        self.lock_clients = threading.Lock()
        self.lock_timestamp = threading.Lock()

    def listen(self):
        """
            This method will handle incoming connection and will create a new thread for each
            incoming connection.     
        """
        print(">> Local Timestamp: ", self.timestamp)
        try:
            print(">> Server is listening at port 4444.")
            self.socket.listen(4)  # backlog 4 clients at same time
            #self.timestamp += 1
            while True:
                client_socket, client_addr = self.socket.accept()
                self.incrementTimeStamp(self.timestamp)
                # client_socket.timeout(60)
                print(">> Client connected.")
                args_tuple = (client_socket, client_addr)
                t = threading.Thread(
                    target=self.handleNewClient, args=args_tuple)
                t.start()
                self.threads.append(t)
        except KeyboardInterrupt:
            print("Server Closed.")
            self.socket.close()
            exit()

    def handleNewClient(self, client_socket, client_addr):
        """
        This method is passed to the new thread created on each incoming connection and it will
        with all kind of client server interaction.

        Args:    
            client_socket: It's the client socket which is just connected.
            client_addr: It's the tuple which contains client connection info.   

        """
        flag, username = self.registerNewUser(client_socket)
        quit_condition = "{u} quit".format(u=username)

        if flag:
            while True:

                buffer = client_socket.recv(self.buffsize)
                if len(buffer) == 0:
                    print(">> Force Exit.")
                    with self.lock_clients:
                        self.clients.pop(username, None)
                    buffer = "{u} left the messenger room, <{ts}>".format(
                        u=username, ts=self.timestamp)
                    self.broadcast(username, buffer)
                    break

                buffer = buffer.decode('utf-8')
                message = buffer.split(',')[0]
                ts = self.getTimeStamp(buffer)
                self.incrementTimeStamp(ts)

                if message == quit_condition:
                    self.handleQuit(username)
                    break

                buffer = "{u}: {message}, <{timestamp}>".format(
                    u=username, message=message, timestamp=self.timestamp)
                self.broadcast(username, buffer)

    def registerNewUser(self, client_socket):
        """
            This method handles all the interactions of new client registeratins.
        Args:
            client_socket: Client socket who wants to register. 

        Returns:
            returns True after the successful completeion of events.
        """
        buffer = client_socket.recv(self.buffsize)

        if len(buffer) == 0:
            print("Force exit unknown client.")
            client_socket.close()
            return (False, "Error")

        buffer = buffer.decode("utf-8")
        username = buffer.split(',')[0]

        ts = self.getTimeStamp(buffer)
        self.incrementTimeStamp(ts)

        self.incrementTimeStamp(self.timestamp)  # for send
        ts = ', <{timestamp}>.'.format(timestamp=self.timestamp)

        if self.clients.get(username) is None:
            self.clients[username] = client_socket
            client_socket.sendall(bytes("Accepted" + ts, 'utf-8'))
            buffer = "{u} joined the messenger room, <{ts}>.".format(
                u=username, ts=self.timestamp)
            self.broadcast(username, buffer)
        else:
            client_socket.sendall(bytes("Rejected" + ts, 'utf-8'))
            flag, username = self.registerNewUser(client_socket)

        return (True, username)

    def handleQuit(self, username):
        """
            This function handles the client quit event.
        Args:    
            username: The client's username who wants to quit. 

        """
        buffer = "{u} left the messenger room, <{ts}>".format(
            u=username, ts=self.timestamp)
        self.broadcast(username, buffer)
        client_socket = None

        with self.lock_clients:
            client_socket = self.clients[username]
            self.clients.pop(username, None)

        print(">> " + username + " exited")
        client_socket.close()

    def broadcast(self, username, buffer):
        """
            This function broadcast takes the buffer and broadcast to all the connected clients.
        Args:    
            username: This is client username of type string.
            buffer: buffer of type string to transmit. 

        """
        for key in self.clients.keys():
            self.incrementTimeStamp(self.timestamp)
            buffer = buffer.split(",")[0]
            buffer = buffer + ",<{ts}> ".format(ts=self.timestamp)
            self.clients[key].sendall(bytes(buffer, 'utf-8'))

        print(">> Broadcast: ", buffer)

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


if __name__ == "__main__":
    IP = ""  # python differentiate between  local and public
    port = 4444
    server = Server(IP, port)
    server.listen()
    exit()
