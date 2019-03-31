import hashlib
from hashlib import md5


def dhtHash(s):
    num_bits = 6
    h = md5(s.encode())  
    key = int(h.hexdigest(),16) % (2**num_bits)
    # print(2**num_bits)
    return key




def main():
    port = 3999
    dict = {}
    for i in range(1,9):
        port += 1
        s = "localhost:{0}".format(port)
        # print(s)        
        k = dhtHash(s)
        # print(k)
        dict[k] = s
        print(s,k)
        

    # print(dict)

    # for key in dict.keys().sort():
    #     print() 
        
    




if __name__ == "__main__": 
    main()
