import socket, datetime, time, threading
import sys


HOST = ''   # Symbolic name meaning all available interfaces
PORT = 8888 # Arbitrary non-privileged port
KEEPALIVE_TIME_GAP = 2; #seconds
TIMEOUT = 5


def getExecuteTime():
    now=datetime.datetime.now()
    return time.mktime(now.timetuple())  


class clientThread (threading.Thread):
    def __init__(self, conn, addr):
        threading.Thread.__init__(self)
        self.conn = conn
        self.addr = addr
    def run(self):
        self.conn.send('Welcome to the server. Type something and hit enter\n') #send only takes string
        
        #infinite loop so that function do not terminate and thread do not end.
        while True:
         
            # Receiving from client
            # Listening for Keep Alive Status
            try:
                data = self.conn.recv(1024)
                if data =="keep-alive:indexing":
                    print "got keep-alive:indexing from "+ self.addr[0]+":"+str(self.addr[1])
                elif data =="keep-alive:writing":
                    print "got keep-alive:writing from "+ self.addr[0]+":"+str(self.addr[1])
                elif data == "keep-alive:indexing-done":
                    # update indexer's state to wait_writing on MasterDB 
                    print "Indexing Done by: "+self.addr[0]+":"+str(self.addr[1])
                    break
                elif data == "keep-alive:writing-done":
                    # Remove indexer state on MasterDB for this task 
                    # Remove indexer state on StateDB for this task 
                    print "Writing Done by: "+self.addr[0]+":"+str(self.addr[1])
                    break
                #if not data: 
                #    break
     
                #conn.sendall(reply)
            except socket.timeout:
                    # update workingProcessDB setting state as dead 
                    print "timeout"
                    break
            except socket.error:
                    # update workingProcessDB setting state as dead 
                    print "close: "+self.addr[0]+":"+str(self.addr[1])
                    break;
            #Timeout occurred, do things
        #if data == "done"        
        #came out of loop
        self.conn.close()
 


s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
print 'Socket created'
 
#Bind socket to local host and port
try:
    s.bind((HOST, PORT))
except socket.error as msg:
    print 'Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
    sys.exit()
     
print 'Socket bind complete'
 
#Start listening on socket
s.listen(5)
print 'Socket now listening'

#now keep talking with the client
while 1:
    #wait to accept a connection - blocking call
    conn, addr = s.accept()
    conn.settimeout(TIMEOUT)
    keepAliveThread = clientThread(conn,addr)
    # Start new Threads
    keepAliveThread.start()
    print 'Connected with ' + addr[0] + ':' + str(addr[1])
 
s.close()