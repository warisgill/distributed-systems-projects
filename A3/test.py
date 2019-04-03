import hashlib
import threading
import time
from hashlib import md5

# counter = 0
def dhtHash(s):
    num_bits = 6
    h = md5(s.encode())  
    key = int(h.hexdigest(),16) % (2**num_bits)
    # print(2**num_bits)
    return key


class Test:
    def __init__(self):
        self.counter = 0
        self.timer = None
        # print(self.counter)    
    def hello(self):
        self.counter = self.counter + 1
        print(self.counter)
        self.timer=threading.Timer(1,self.hello)
        
        self.timer.start() 


def main():
    # port = 3999
    # dict = {}
    test = Test()
    test.hello()

    time.sleep(10)
    test.timer.cancel()


    
    
    # timer.start()
    
    # print()
    # for i in range(1,9):
    #     port += 1
    #     s = "localhost:{0}".format(port)
    #     # print(s)        
    #     k = dhtHash(s)
    #     # print(k)
    #     dict[k] = s
    #     print(s,k)
        

    # print(dict)

    # for key in dict.keys().sort():
    #     print() 
        
    




if __name__ == "__main__": 
    main()
